package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// directPool owns one nats.Conn per simulated user plus one subscription per
// user-room pair. Each subscription callback records broadcast-arrival time
// against the shared Collector for latency correlation.
type directPool struct {
	url       string
	credsFile string
	collector *Collector

	mu    sync.Mutex
	users map[string]*directUser
}

type directUser struct {
	id   string
	nc   *nats.Conn
	subs []*nats.Subscription
}

func newDirectPool(natsURL, credsFile string, c *Collector) *directPool {
	return &directPool{
		url: natsURL, credsFile: credsFile, collector: c, users: make(map[string]*directUser),
	}
}

// Add opens a connection for u and subscribes to every room in u.Rooms,
// plus the user-scoped subject for DM broadcasts. Safe to call concurrently
// for different users.
//
// Channel-room broadcasts arrive on subject.RoomEvent(roomID); DM and BotDM
// broadcasts arrive on subject.UserRoomEvent(account) — both are needed for
// realistic IM coverage since daily presets are DM-heavy.
//
// Each room is subscribed on BOTH the global and local lanes so a run stays
// mode-agnostic: under ROOM_SUBJECT_MODE=local, hard-coding the global lane would
// silently receive nothing and report degraded latency instead of failing loudly.
func (p *directPool) Add(u *userState) error {
	nc, err := connectWithCredsHealth(p.url, "loadgen-daily-"+u.ID, p.credsFile, "daily", p.collector.m)
	if err != nil {
		return fmt.Errorf("connect for %s: %w", u.ID, err)
	}
	du := &directUser{id: u.ID, nc: nc}
	for _, roomID := range u.Rooms {
		for _, global := range []bool{true, false} {
			sub, err := nc.Subscribe(subject.RoomEvent(roomID, global), func(m *nats.Msg) {
				p.onBroadcast(m)
			})
			if err != nil {
				_ = nc.Drain()
				return fmt.Errorf("subscribe room %s/%s: %w", u.ID, roomID, err)
			}
			du.subs = append(du.subs, sub)
		}
	}
	// User-scoped subscription for DM broadcasts.
	userSub, err := nc.Subscribe(subject.UserRoomEvent(u.Account), func(m *nats.Msg) {
		p.onBroadcast(m)
	})
	if err != nil {
		_ = nc.Drain()
		return fmt.Errorf("subscribe user %s: %w", u.ID, err)
	}
	du.subs = append(du.subs, userSub)
	// Flush so SUB commands reach the server before Add returns; otherwise
	// a publish immediately after Add can be dropped because the broker
	// hasn't registered interest yet. Same rationale as multiplexPool.Add.
	if err := nc.Flush(); err != nil {
		_ = nc.Drain()
		return fmt.Errorf("flush subs for %s: %w", u.ID, err)
	}
	p.mu.Lock()
	p.users[u.ID] = du
	p.mu.Unlock()
	return nil
}

// Size reports the number of users currently in the pool.
func (p *directPool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.users)
}

func (p *directPool) onBroadcast(m *nats.Msg) {
	var evt model.RoomEvent
	if err := json.Unmarshal(m.Data, &evt); err != nil {
		return // ignore malformed
	}
	if evt.LastMsgID == "" {
		return
	}
	p.collector.RecordBroadcast(evt.LastMsgID, time.Now())
}

// Close drains all connections.
func (p *directPool) Close() {
	p.mu.Lock()
	users := p.users
	p.users = nil
	p.mu.Unlock()
	for _, du := range users {
		_ = du.nc.Drain()
	}
}

// multiplexPool fans M shared NATS connections across N users. Each shared
// connection subscribes (with reference counting) to the union of room
// broadcast subjects for its assigned users. Incoming messages are routed
// to per-user inbox channels via the dispatch map.
type multiplexPool struct {
	url       string
	collector *Collector
	conns     []*nats.Conn

	mu        sync.Mutex
	roomRefs  map[string]int              // roomID -> ref count on the shared conns
	dispatch  map[string][]chan *nats.Msg // roomID -> per-user inboxes
	userInbox map[string]chan *nats.Msg   // userID -> that user's inbox channel
	nextConn  int                         // round-robin assignment
}

func newMultiplexPool(natsURL, credsFile string, c *Collector, size int) (*multiplexPool, error) {
	p := &multiplexPool{
		url: natsURL, collector: c,
		roomRefs:  make(map[string]int),
		dispatch:  make(map[string][]chan *nats.Msg),
		userInbox: make(map[string]chan *nats.Msg),
	}
	for i := 0; i < size; i++ {
		nc, err := connectWithCredsHealth(natsURL, fmt.Sprintf("loadgen-daily-mux-%d", i), credsFile, "daily", c.m)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("multiplex conn %d: %w", i, err)
		}
		p.conns = append(p.conns, nc)
	}
	return p, nil
}

func connectWithCredsHealth(
	url,
	name,
	credsFile,
	pool string,
	metrics *Metrics,
) (*nats.Conn, error) {
	health := newLoadgenNATSHealth(pool, metrics, nil)
	if health == nil {
		return nil, fmt.Errorf("unknown loadgen NATS pool %q", pool)
	}
	opts := []nats.Option{
		nats.Name(name),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { health.disconnected(err) }),
		nats.ReconnectHandler(func(connection *nats.Conn) { health.reconnected(connection.ConnectedUrlRedacted()) }),
		nats.ClosedHandler(func(_ *nats.Conn) { health.closed() }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if errors.Is(err, nats.ErrSlowConsumer) {
				health.bufferFull(err)
				return
			}
			health.asyncError(err)
		}),
	}
	if credsFile != "" {
		opts = append(opts, nats.UserCredentials(credsFile))
	}
	connection, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	health.connected()
	return connection, nil
}

// Add registers a user with the multiplex pool. Subscribes the shared
// connection BEFORE mutating dispatch/refcount maps so a failed subscribe
// leaves the pool consistent (no orphaned inbox in dispatch).
func (p *multiplexPool) Add(u *userState) error {
	inbox := make(chan *nats.Msg, 128)
	p.mu.Lock()
	defer p.mu.Unlock()

	// First pass: subscribe to any new room subjects via round-robin conn.
	// Track which rooms we subscribed *in this Add* so partial failures can
	// be undone. (roomRefs already > 0 means an earlier user already
	// subscribed — no new sub needed.)
	for _, roomID := range u.Rooms {
		if p.roomRefs[roomID] > 0 || len(p.conns) == 0 {
			continue
		}
		nc := p.conns[p.nextConn%len(p.conns)]
		p.nextConn++
		// Subscribe both lanes (see directPool.Add) — mode-agnostic against ROOM_SUBJECT_MODE.
		for _, global := range []bool{true, false} {
			if _, err := nc.Subscribe(subject.RoomEvent(roomID, global), p.route); err != nil {
				return fmt.Errorf("multiplex subscribe %s: %w", roomID, err)
			}
		}
		// Mark provisionally with refcount 0 — the second pass below will
		// increment it. We don't increment here so a subsequent Subscribe
		// failure doesn't leave a dangling subscription.
	}

	// User-scoped subject for DM broadcasts. Subscribed per-user (no
	// refcount needed since UserRoomEvent is scoped to the account).
	if len(p.conns) > 0 {
		nc := p.conns[p.nextConn%len(p.conns)]
		p.nextConn++
		if _, err := nc.Subscribe(subject.UserRoomEvent(u.Account), p.route); err != nil {
			return fmt.Errorf("multiplex subscribe user %s: %w", u.ID, err)
		}
	}

	// Second pass: mutate state only after every Subscribe succeeded.
	p.userInbox[u.ID] = inbox
	for _, roomID := range u.Rooms {
		p.dispatch[roomID] = append(p.dispatch[roomID], inbox)
		p.roomRefs[roomID]++
	}

	// Flush every shared conn so the SUB commands reach the server before
	// Add returns. Without this, a caller (or test) that publishes
	// immediately after Add() may see the broadcast dropped because the
	// server hasn't registered the subscription interest yet. Production
	// emitters tick on a 1s schedule so they don't hit this race, but
	// tests and synchronous callers do. Flush per conn is one round-trip;
	// dominated by the Subscribe overhead already incurred.
	for _, nc := range p.conns {
		if err := nc.Flush(); err != nil {
			return fmt.Errorf("multiplex flush %s: %w", u.ID, err)
		}
	}
	return nil
}

// route is called by every shared conn's subscription callback. It looks up
// the destination inboxes by RoomID and does a non-blocking send.
// All inbox sends happen under p.mu so Close can safely set userInbox=nil
// without racing against an in-flight send-on-closed-channel.
func (p *multiplexPool) route(m *nats.Msg) {
	var evt model.RoomEvent
	if err := json.Unmarshal(m.Data, &evt); err != nil {
		return
	}
	roomID := evt.RoomID
	if roomID == "" {
		roomID = parseRoomFromSubject(m.Subject)
	}
	p.mu.Lock()
	inboxes := p.dispatch[roomID]
	if evt.LastMsgID != "" && p.collector != nil {
		p.collector.RecordBroadcast(evt.LastMsgID, time.Now())
	}
	dropCount := 0
	for _, ch := range inboxes {
		select {
		case ch <- m:
		default:
			dropCount++
		}
	}
	p.mu.Unlock()
	if dropCount > 0 && p.collector != nil {
		for i := 0; i < dropCount; i++ {
			p.collector.RecordMultiplexDrop()
		}
	}
}

// parseRoomFromSubject extracts the room ID from a "chat.room.<id>.event" subject.
func parseRoomFromSubject(subj string) string {
	parts := strings.Split(subj, ".")
	if len(parts) >= 3 && parts[0] == "chat" && parts[1] == "room" {
		return parts[2]
	}
	return ""
}

// Close drains shared conns. Inbox channels are NOT closed — letting GC
// reclaim them avoids a race between Close and an in-flight route() that
// holds a pre-lock-release inbox snapshot (would panic on send-on-closed).
// Once Drain returns, no further callbacks fire, so the channels are no
// longer referenced and become garbage.
func (p *multiplexPool) Close() {
	p.mu.Lock()
	p.userInbox = nil
	p.dispatch = nil
	p.roomRefs = nil
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, nc := range conns {
		_ = nc.Drain()
	}
}
