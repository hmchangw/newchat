package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

const rpcTimeout = 10 * time.Second

type histMsg struct {
	MessageID string `json:"messageId"`
	Msg       string `json:"msg"`
	Type      string `json:"t"`
	CreatedAt string `json:"createdAt"`
}

type historyReply struct {
	Messages []histMsg `json:"messages"`
}

// client is one desktop client: the login-time subscriptions to its own user
// subjects, plus a room subscription opened at the moment the behaviour under
// test says it opens.
type client struct {
	account string
	nc      *nats.Conn
	rec     *recorder

	subscribeOnUpdate bool

	mu           sync.Mutex
	liveMessages []string
	roomSubs     []*nats.Subscription
	roomOpen     map[string]bool
}

func newClient(account string, nc *nats.Conn, rec *recorder) *client {
	return &client{
		account:  account,
		nc:       nc,
		rec:      rec,
		roomOpen: map[string]bool{},
	}
}

// login mirrors what the frontend subscribes to at connect time.
func (c *client) login() error {
	sub, err := c.nc.Subscribe(fmt.Sprintf("chat.user.%s.event.>", c.account), c.onUserEvent)
	if err != nil {
		return fmt.Errorf("subscribe user events for %s: %w", c.account, err)
	}
	c.roomSubs = append(c.roomSubs, sub)
	return c.nc.Flush()
}

func (c *client) onUserEvent(m *nats.Msg) {
	switch {
	case strings.HasSuffix(m.Subject, ".event.subscription.update"):
		var evt model.SubscriptionUpdateEvent
		if err := json.Unmarshal(m.Data, &evt); err != nil {
			return
		}
		c.rec.add(c.account, "user-subject", "subscription.update "+evt.Action, "", evt.Subscription.RoomID)
		if evt.Action != "added" || evt.Subscription.RoomID == "" {
			return
		}
		if c.subscribeOnUpdate {
			c.openRoom(evt.Subscription.RoomID)
		}
	case strings.HasSuffix(m.Subject, ".event.room"):
		var evt model.RoomEvent
		if err := json.Unmarshal(m.Data, &evt); err != nil {
			return
		}
		c.rec.add(c.account, "user-subject", string(evt.Type), evt.LastMsgID, "")
	}
}

// openRoom subscribes to the room's event subject. ROOM_SUBJECT_MODE=dual means
// a same-site room publishes on both lanes, so both are opened and deduped.
func (c *client) openRoom(roomID string) {
	c.mu.Lock()
	if c.roomOpen[roomID] {
		c.mu.Unlock()
		return
	}
	c.roomOpen[roomID] = true
	c.mu.Unlock()

	for _, subj := range []string{subject.RoomEvent(roomID, false), subject.RoomEvent(roomID, true)} {
		sub, err := c.nc.Subscribe(subj, c.onRoomEvent)
		if err != nil {
			c.rec.add(c.account, "room-subject", "SUBSCRIBE FAILED "+subj, "", err.Error())
			continue
		}
		c.mu.Lock()
		c.roomSubs = append(c.roomSubs, sub)
		c.mu.Unlock()
	}
	_ = c.nc.Flush()
	c.rec.add(c.account, "room-subject", "subscribed", "", roomID)
}

func (c *client) waitRoomOpen(roomID string, d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		open := c.roomOpen[roomID]
		c.mu.Unlock()
		if open {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *client) onRoomEvent(m *nats.Msg) {
	var evt model.RoomEvent
	if err := json.Unmarshal(m.Data, &evt); err != nil {
		return
	}
	id := evt.LastMsgID
	body := ""
	if evt.Message != nil {
		id = evt.Message.ID
		body = evt.Message.Content
		if evt.Message.Type != "" {
			body = "[" + evt.Message.Type + "]"
		}
	}
	c.mu.Lock()
	dup := false
	for _, seen := range c.liveMessages {
		if seen == id {
			dup = true
			break
		}
	}
	if !dup && id != "" {
		c.liveMessages = append(c.liveMessages, id)
	}
	c.mu.Unlock()
	if dup {
		return
	}
	c.rec.add(c.account, "room-subject", string(evt.Type), id, body)
}

// createChannel runs the two-phase create: the sync accept, then the async job
// result on chat.user.{account}.response.{requestId}.
func (c *client) createChannel(name, other string) (string, error) {
	requestID := idgen.GenerateRequestID()
	resultSub, err := c.nc.SubscribeSync(subject.UserResponse(c.account, requestID))
	if err != nil {
		return "", fmt.Errorf("subscribe async result: %w", err)
	}
	defer func() { _ = resultSub.Unsubscribe() }()
	if err := c.nc.Flush(); err != nil {
		return "", fmt.Errorf("flush async result sub: %w", err)
	}

	body, err := json.Marshal(model.CreateRoomRequest{Name: name, Users: []string{other}})
	if err != nil {
		return "", fmt.Errorf("marshal create request: %w", err)
	}
	req := nats.NewMsg(subject.RoomCreate(c.account, *siteID))
	req.Data = body
	req.Header.Set("X-Request-ID", requestID)

	reply, err := c.nc.RequestMsg(req, rpcTimeout)
	if err != nil {
		return "", fmt.Errorf("room.create rpc: %w", err)
	}
	var accepted model.CreateRoomReply
	if err := json.Unmarshal(reply.Data, &accepted); err != nil {
		return "", fmt.Errorf("decode create reply %s: %w", reply.Data, err)
	}
	if accepted.RoomID == "" {
		return "", fmt.Errorf("create rejected: %s", reply.Data)
	}
	c.rec.add(c.account, "rpc", "room.create sync reply "+accepted.Status, "", accepted.RoomID)

	msg, err := resultSub.NextMsg(rpcTimeout)
	if err != nil {
		return "", fmt.Errorf("await create async result: %w", err)
	}
	var res model.AsyncJobResult
	if err := json.Unmarshal(msg.Data, &res); err != nil {
		return "", fmt.Errorf("decode async result: %w", err)
	}
	if res.Status != model.AsyncJobStatusOK {
		return "", fmt.Errorf("create job failed: %s", msg.Data)
	}
	return accepted.RoomID, nil
}

// send publishes on the client msg.send subject and waits for the async reply.
func (c *client) send(roomID, msgID, content string) error {
	requestID := idgen.GenerateRequestID()
	replySub, err := c.nc.SubscribeSync(subject.UserResponse(c.account, requestID))
	if err != nil {
		return fmt.Errorf("subscribe send reply: %w", err)
	}
	defer func() { _ = replySub.Unsubscribe() }()
	if err := c.nc.Flush(); err != nil {
		return fmt.Errorf("flush send reply sub: %w", err)
	}

	body, err := json.Marshal(model.SendMessageRequest{ID: msgID, Content: content, RequestID: requestID})
	if err != nil {
		return fmt.Errorf("marshal send request: %w", err)
	}
	if err := c.nc.Publish(subject.MsgSend(c.account, roomID, *siteID), body); err != nil {
		return fmt.Errorf("publish msg.send: %w", err)
	}
	if err := c.nc.Flush(); err != nil {
		return fmt.Errorf("flush msg.send: %w", err)
	}
	reply, err := replySub.NextMsg(rpcTimeout)
	if err != nil {
		return fmt.Errorf("await send reply: %w", err)
	}
	if strings.Contains(string(reply.Data), `"code"`) {
		return fmt.Errorf("send rejected: %s", reply.Data)
	}
	return nil
}

// loadHistory issues the msg.history RPC. When retry is set it keeps asking
// until wantMsgID shows up, which is what a gap-detecting client would do.
func (c *client) loadHistory(roomID string, retry bool, wantMsgID string) ([]histMsg, int) {
	deadline := time.Now().Add(3 * time.Second)
	tries := 0
	var last []histMsg
	for {
		tries++
		req := map[string]any{"limit": 50}
		if *hint {
			// A hint the client already holds: the room is at least as fresh as
			// the event it just received. Without it the scan ceiling is the room
			// doc's lastMsgAt, which the just-sent message has not reached yet.
			req["meta"] = map[string]any{"lastMsgAt": time.Now().UTC().UnixMilli()}
		}
		body, err := json.Marshal(req)
		if err != nil {
			return nil, tries
		}
		reply, err := c.nc.Request(subject.MsgHistory(c.account, roomID, *siteID), body, rpcTimeout)
		if err != nil {
			c.rec.add(c.account, "history", "msg.history FAILED", "", err.Error())
			return last, tries
		}
		var out historyReply
		if err := json.Unmarshal(reply.Data, &out); err != nil {
			c.rec.add(c.account, "history", "msg.history undecodable", "", trimTo(string(reply.Data), 60))
			return last, tries
		}
		last = out.Messages
		if !retry || wantMsgID == "" {
			return last, tries
		}
		for _, m := range last {
			if m.MessageID == wantMsgID {
				return last, tries
			}
		}
		if time.Now().After(deadline) {
			return last, tries
		}
		time.Sleep(50 * time.Millisecond)
	}
}
