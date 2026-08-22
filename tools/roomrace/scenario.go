package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/subject"
)

const (
	accountB = "bob"
	siteID   = "site-local"
)

type roomKind string

const (
	kindDM      roomKind = "dm"
	kindChannel roomKind = "channel"
)

// scenario models one (transport shape, client behaviour) pair.
type scenario struct {
	name string
	kind roomKind
	fix  string

	// dropsUnknownRoom: the client discards a room event whose room is not in
	// local state yet - the behaviour Problem 1 describes.
	dropsUnknownRoom bool
	// subscribeOnUpdate: the client opens chat.room.{id}.event when it handles
	// subscription.update - the behaviour Problem 2 describes.
	subscribeOnUpdate bool
	flushAfterSub     bool
	backfillAfterSub  bool
	// graceWindow: the SERVER additionally publishes the channel room event to
	// chat.user.{account}.event.room for freshly joined members.
	graceWindow bool
}

func scenarios() []scenario {
	return []scenario{
		{name: "dm / client drops unknown room", kind: kindDM, fix: "none (today)", dropsUnknownRoom: true},
		{name: "dm / client buffers unknown room", kind: kindDM, fix: "client tolerates", dropsUnknownRoom: false},

		{name: "channel / subscribe on update", kind: kindChannel, fix: "none (today)", subscribeOnUpdate: true},
		{name: "channel / subscribe + flush", kind: kindChannel, fix: "client flush", subscribeOnUpdate: true, flushAfterSub: true},
		{name: "channel / subscribe + flush + backfill", kind: kindChannel, fix: "client backfill", subscribeOnUpdate: true, flushAfterSub: true, backfillAfterSub: true},
		{name: "channel / server join grace window", kind: kindChannel, fix: "server grace window", subscribeOnUpdate: true, flushAfterSub: true, graceWindow: true},
		{name: "channel / grace window, client still drops", kind: kindChannel, fix: "server only", subscribeOnUpdate: true, flushAfterSub: true, graceWindow: true, dropsUnknownRoom: true},
	}
}

// roomEvent is the shape the harness publishes; only the fields the race turns
// on are modelled (pkg/model.RoomEvent carries far more).
type roomEvent struct {
	Type      string `json:"type"`
	RoomID    string `json:"roomId"`
	LastMsgID string `json:"lastMsgId"`
}

type subUpdate struct {
	Action string `json:"action"`
	RoomID string `json:"roomId"`
	Kind   string `json:"roomType"`
}

// services is the server side: room-worker's subscription.update publish,
// broadcast-worker's fan-out, and a history-service stand-in that serves the
// backfill on the real msg.history subject.
type services struct {
	nc      *nats.Conn
	mu      sync.Mutex
	stored  map[string][]string
	histSub *nats.Subscription
}

func newServices(url string) (*services, error) {
	nc, err := nats.Connect(url, nats.Name("roomrace-services"))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", url, err)
	}
	s := &services{nc: nc, stored: map[string][]string{}}
	sub, err := nc.Subscribe(subject.MsgHistoryWildcard(siteID), func(m *nats.Msg) {
		_, roomID, ok := subject.ParseUserRoomSubject(m.Subject)
		if !ok {
			return
		}
		s.mu.Lock()
		ids := append([]string(nil), s.stored[roomID]...)
		s.mu.Unlock()
		data, err := json.Marshal(map[string]any{"messages": ids})
		if err != nil {
			return
		}
		_ = m.Respond(data)
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("subscribe history stub: %w", err)
	}
	s.histSub = sub
	if err := nc.Flush(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("flush history stub: %w", err)
	}
	return s, nil
}

func (s *services) close() {
	if s.histSub != nil {
		_ = s.histSub.Unsubscribe()
	}
	_ = s.nc.Drain()
}

// persist models message-worker writing to Cassandra: the message is durable
// server-side, which is what makes a backfill able to recover it.
func (s *services) persist(roomID, msgID string) {
	s.mu.Lock()
	s.stored[roomID] = append(s.stored[roomID], msgID)
	s.mu.Unlock()
}

func (s *services) publishSubscriptionAdded(roomID string, kind roomKind) error {
	data, err := json.Marshal(subUpdate{Action: "added", RoomID: roomID, Kind: string(kind)})
	if err != nil {
		return fmt.Errorf("marshal subscription.update: %w", err)
	}
	if err := s.nc.Publish(subject.SubscriptionUpdate(accountB), data); err != nil {
		return fmt.Errorf("publish subscription.update: %w", err)
	}
	return s.nc.Flush()
}

// publishNewMessage mirrors broadcast-worker: DMs fan out per member on the
// user subject, channels go once to the room subject.
func (s *services) publishNewMessage(roomID, msgID string, sc scenario) error {
	data, err := json.Marshal(roomEvent{Type: "new_message", RoomID: roomID, LastMsgID: msgID})
	if err != nil {
		return fmt.Errorf("marshal room event: %w", err)
	}
	if sc.kind == kindDM {
		if err := s.nc.Publish(subject.UserRoomEvent(accountB), data); err != nil {
			return fmt.Errorf("publish dm event: %w", err)
		}
		return s.nc.Flush()
	}
	if err := s.nc.Publish(subject.RoomEvent(roomID, true), data); err != nil {
		return fmt.Errorf("publish room event: %w", err)
	}
	if sc.graceWindow {
		if err := s.nc.Publish(subject.UserRoomEvent(accountB), data); err != nil {
			return fmt.Errorf("publish grace-window copy: %w", err)
		}
	}
	return s.nc.Flush()
}
