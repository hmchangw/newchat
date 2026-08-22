package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/subject"
)

// roomState is client B's per-room view for one iteration.
type roomState struct {
	known     bool
	seen      map[string]bool
	live      bool
	recovered bool
	dropped   int
	dupes     int
	done      chan struct{}
}

// clientB models the desktop client: login-time subscriptions to its own user
// subjects, plus a per-room subscription opened when a channel subscription
// arrives.
type clientB struct {
	nc          *nats.Conn
	sc          scenario
	renderDelay time.Duration

	mu    sync.Mutex
	rooms map[string]*roomState

	loginSubs []*nats.Subscription
	roomSubs  []*nats.Subscription
}

func newClientB(url string, sc scenario, renderDelay time.Duration) (*clientB, error) {
	nc, err := nats.Connect(url, nats.Name("roomrace-client-b"))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", url, err)
	}
	c := &clientB{nc: nc, sc: sc, renderDelay: renderDelay, rooms: map[string]*roomState{}}

	updSub, err := nc.Subscribe(subject.SubscriptionUpdate(accountB), c.onSubscriptionUpdate)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("subscribe subscription.update: %w", err)
	}
	userRoomSub, err := nc.Subscribe(subject.UserRoomEvent(accountB), c.onUserRoomEvent)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("subscribe user room event: %w", err)
	}
	c.loginSubs = []*nats.Subscription{updSub, userRoomSub}
	if err := nc.Flush(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("flush login subscriptions: %w", err)
	}
	// Warm the request inbox before the run: nats.go registers the reply
	// subscription lazily, and on a cluster that interest has to reach the
	// responder's server, which would otherwise time out the first backfill.
	if _, err := nc.Request(subject.MsgHistory(accountB, "warmup", siteID), nil, 2*time.Second); err != nil {
		nc.Close()
		return nil, fmt.Errorf("warm request inbox: %w", err)
	}
	return c, nil
}

func (c *clientB) close() {
	for _, s := range c.roomSubs {
		_ = s.Unsubscribe()
	}
	for _, s := range c.loginSubs {
		_ = s.Unsubscribe()
	}
	_ = c.nc.Drain()
}

func (c *clientB) expect(roomID string) *roomState {
	st := &roomState{seen: map[string]bool{}, done: make(chan struct{})}
	c.mu.Lock()
	c.rooms[roomID] = st
	c.mu.Unlock()
	return st
}

func (c *clientB) state(roomID string) *roomState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rooms[roomID]
}

// onSubscriptionUpdate is the client's "a new chat appeared" handler: render
// the row, then (channels only) open the room subscription.
func (c *clientB) onSubscriptionUpdate(m *nats.Msg) {
	var evt subUpdate
	if err := json.Unmarshal(m.Data, &evt); err != nil {
		return
	}
	st := c.state(evt.RoomID)
	if st == nil {
		return
	}
	defer close(st.done)

	time.Sleep(c.renderDelay) // render the sidebar row / mount the room

	c.mu.Lock()
	st.known = true
	c.mu.Unlock()

	if !c.sc.subscribeOnUpdate || roomKind(evt.Kind) != kindChannel {
		return
	}
	sub, err := c.nc.Subscribe(subject.RoomEvent(evt.RoomID, true), c.onRoomEvent)
	if err != nil {
		return
	}
	c.mu.Lock()
	c.roomSubs = append(c.roomSubs, sub)
	c.mu.Unlock()

	if c.sc.flushAfterSub {
		_ = c.nc.Flush()
	}
	if c.sc.backfillAfterSub {
		c.backfill(evt.RoomID)
	}
}

// backfill is the Zulip-style catch-up: subscribe first, then read the durable
// log, then merge by message id.
func (c *clientB) backfill(roomID string) {
	reply, err := c.nc.Request(subject.MsgHistory(accountB, roomID, siteID), nil, time.Second)
	if err != nil {
		return
	}
	var out struct {
		Messages []string `json:"messages"`
	}
	if err := json.Unmarshal(reply.Data, &out); err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.rooms[roomID]
	if st == nil {
		return
	}
	for _, id := range out.Messages {
		if st.seen[id] {
			continue
		}
		st.seen[id] = true
		st.recovered = true
	}
}

func (c *clientB) onUserRoomEvent(m *nats.Msg) { c.record(m, true) }

func (c *clientB) onRoomEvent(m *nats.Msg) { c.record(m, false) }

// record applies an inbound room event. onUserRoom=true is the per-user subject
// (DM fan-out, and the grace-window copy); false is the per-room subject.
func (c *clientB) record(m *nats.Msg, onUserRoom bool) {
	var evt roomEvent
	if err := json.Unmarshal(m.Data, &evt); err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.rooms[evt.RoomID]
	if st == nil {
		return
	}
	if onUserRoom && !st.known && c.sc.dropsUnknownRoom {
		st.dropped++
		return
	}
	if st.seen[evt.LastMsgID] {
		st.dupes++
		return
	}
	st.seen[evt.LastMsgID] = true
	st.live = true
}

// roomSeq keeps every room id in a process unique, so a late event from an
// earlier iteration can never be mistaken for the current one.
var roomSeq atomic.Uint64

type outcome struct {
	SendMS     int     `json:"sendMs"`
	Iterations int     `json:"iterations"`
	Live       int     `json:"live"`
	Recovered  int     `json:"recovered"`
	Missed     int     `json:"missed"`
	Dropped    int     `json:"droppedUnknownRoom"`
	Duplicates int     `json:"duplicates"`
	MissRate   float64 `json:"missRate"`
}

func runScenario(opt *options, svc *services, sc scenario, sendDelay time.Duration) (outcome, error) {
	c, err := newClientB(opt.subURL, sc, time.Duration(opt.renderMS)*time.Millisecond)
	if err != nil {
		return outcome{}, err
	}
	defer c.close()

	settle := time.Duration(opt.settleMS) * time.Millisecond
	o := outcome{Iterations: opt.iterations}

	for i := range opt.iterations {
		roomID := fmt.Sprintf("rr%08d", roomSeq.Add(1))
		msgID := fmt.Sprintf("msg%017d", i)
		st := c.expect(roomID)

		if err := svc.publishSubscriptionAdded(roomID, sc.kind); err != nil {
			return outcome{}, err
		}
		time.Sleep(sendDelay)

		svc.persist(roomID, msgID)
		if err := svc.publishNewMessage(roomID, msgID, sc); err != nil {
			return outcome{}, err
		}

		select {
		case <-st.done:
		case <-time.After(15 * time.Second):
			return outcome{}, fmt.Errorf("client handler stalled on room %s", roomID)
		}
		time.Sleep(settle)

		c.mu.Lock()
		switch {
		case st.live:
			o.Live++
		case st.recovered:
			o.Recovered++
		default:
			o.Missed++
		}
		o.Dropped += st.dropped
		o.Duplicates += st.dupes
		delete(c.rooms, roomID)
		c.mu.Unlock()
	}
	o.MissRate = float64(o.Missed) / float64(o.Iterations)
	return o, nil
}
