//go:build integration

package main

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestDirectPool_ReceivesBroadcast(t *testing.T) {
	url := testutil.NATS(t)
	ncPub, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { ncPub.Close() })

	col := NewCollector(NewMetrics(), "test")
	pool := newDirectPool(url, "" /*no creds: testcontainer NATS allows anonymous*/, col)
	t.Cleanup(pool.Close)

	u := &userState{ID: "u-1", Account: "user-1", Rooms: []string{"room-test"}}
	require.NoError(t, pool.Add(u))

	// Publish a fake broadcast event with LastMsgID set.
	evt := model.RoomEvent{Type: model.RoomEventNewMessage, LastMsgID: "msg-42", RoomID: "room-test"}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	col.RecordPublishBroadcastOnly("msg-42", time.Now())
	require.NoError(t, ncPub.Publish(subject.RoomEvent("room-test", true), data))
	require.NoError(t, ncPub.Flush())

	require.Eventually(t, func() bool {
		return col.E2Count() == 1
	}, 2*time.Second, 20*time.Millisecond)
}

func TestDirectPool_ReceivesLocalBroadcast(t *testing.T) {
	url := testutil.NATS(t)
	ncPub, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { ncPub.Close() })

	col := NewCollector(NewMetrics(), "test")
	pool := newDirectPool(url, "" /*no creds: testcontainer NATS allows anonymous*/, col)
	t.Cleanup(pool.Close)

	u := &userState{ID: "u-1", Account: "user-1", Rooms: []string{"room-test"}}
	require.NoError(t, pool.Add(u))

	// Publish on the LOCAL lane (ROOM_SUBJECT_MODE=local): the pool must still
	// receive it, proving Add subscribes both lanes rather than global only.
	evt := model.RoomEvent{Type: model.RoomEventNewMessage, LastMsgID: "msg-42", RoomID: "room-test"}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	col.RecordPublishBroadcastOnly("msg-42", time.Now())
	require.NoError(t, ncPub.Publish(subject.RoomEvent("room-test", false), data))
	require.NoError(t, ncPub.Flush())

	require.Eventually(t, func() bool {
		return col.E2Count() == 1
	}, 2*time.Second, 20*time.Millisecond)
}

func TestMultiplexPool_RoutesLocalBroadcastToInbox(t *testing.T) {
	url := testutil.NATS(t)
	ncPub, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { ncPub.Close() })

	col := NewCollector(NewMetrics(), "test")
	pool, err := newMultiplexPool(url, "" /*no creds*/, col, 2 /*pool size*/)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	uA := &userState{ID: "u-a", Account: "ua", Rooms: []string{"r-1"}}
	require.NoError(t, pool.Add(uA))

	// Publish on the LOCAL lane — the multiplex pool must route it to the inbox.
	col.RecordPublishBroadcastOnly("msg-1", time.Now())
	data, err := json.Marshal(model.RoomEvent{LastMsgID: "msg-1", RoomID: "r-1"})
	require.NoError(t, err)
	require.NoError(t, ncPub.Publish(subject.RoomEvent("r-1", false), data))
	require.NoError(t, ncPub.Flush())

	require.Eventually(t, func() bool {
		return col.E2Count() >= 1
	}, 2*time.Second, 20*time.Millisecond)
}

func TestMultiplexPool_RoutesBroadcastToInbox(t *testing.T) {
	url := testutil.NATS(t)
	ncPub, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { ncPub.Close() })

	col := NewCollector(NewMetrics(), "test")
	pool, err := newMultiplexPool(url, "" /*no creds*/, col, 2 /*pool size*/)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	uA := &userState{ID: "u-a", Account: "ua", Rooms: []string{"r-1"}}
	uB := &userState{ID: "u-b", Account: "ub", Rooms: []string{"r-1", "r-2"}}
	require.NoError(t, pool.Add(uA))
	require.NoError(t, pool.Add(uB))

	col.RecordPublishBroadcastOnly("msg-1", time.Now())
	data, err := json.Marshal(model.RoomEvent{LastMsgID: "msg-1", RoomID: "r-1"})
	require.NoError(t, err)
	require.NoError(t, ncPub.Publish(subject.RoomEvent("r-1", true), data))
	require.NoError(t, ncPub.Flush())

	require.Eventually(t, func() bool {
		return col.E2Count() >= 1
	}, 2*time.Second, 20*time.Millisecond)
}

func TestMultiplexPool_DropsCountedOnInboxFull(t *testing.T) {
	col := NewCollector(NewMetrics(), "test")
	pool := &multiplexPool{
		collector: col,
		dispatch:  make(map[string][]chan *nats.Msg),
	}
	// Wire one room with one zero-capacity (unbuffered) inbox with no reader.
	full := make(chan *nats.Msg)
	pool.dispatch["r-1"] = []chan *nats.Msg{full}

	pool.route(&nats.Msg{Subject: subject.RoomEvent("r-1", true), Data: []byte(`{"lastMsgId":"x","roomId":"r-1"}`)})

	require.Equal(t, int64(1), col.MultiplexDrops())
}

// countingSink tallies deliveries per message ID. Guarded, because NATS
// subscription callbacks fire from readLoop goroutines.
type countingSink struct {
	mu sync.Mutex
	n  map[string]int
}

func (s *countingSink) RecordDelivery(_, msgID, _ string, _ lane, _ time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == nil {
		s.n = make(map[string]int)
	}
	s.n[msgID]++
}

func (s *countingSink) count(msgID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n[msgID]
}

// TestDirectPool_SubscribeRoom_RepeatedJoinDeliversOnce pins the phantom
// duplicate_delivery fix end-to-end. `verify`'s churn can re-add a floater it
// removed earlier — removeMember deliberately leaves the room subscription in
// place, while applyChange rebuilds its candidate set from the membership
// model, so the same user becomes a join target again and addMember calls
// SubscribeRoom a second time on the identical subject. Two live subscriptions
// deliver every later probe twice on the same lane, which the tracker reports
// as duplicate_delivery against a system that delivered exactly once.
func TestDirectPool_SubscribeRoom_RepeatedJoinDeliversOnce(t *testing.T) {
	url := testutil.NATS(t)
	ncPub, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { ncPub.Close() })

	pool := newDirectPool(url, "" /*no creds: testcontainer NATS allows anonymous*/, NewCollector(NewMetrics(), "test"))
	t.Cleanup(pool.Close)
	sink := &countingSink{}
	pool.attachSink(sink)

	require.NoError(t, pool.Add(&userState{ID: "u-1", Account: "user-1"}))

	// The join, then the remove→re-add cycle's second join on the same room.
	require.NoError(t, pool.SubscribeRoom("u-1", "room-churn"))
	require.NoError(t, pool.SubscribeRoom("u-1", "room-churn"))

	pool.mu.Lock()
	subs := len(pool.users["u-1"].subs)
	pool.mu.Unlock()
	// One user-scoped sub from Add, plus exactly one sub per selected room lane.
	require.Equal(t, 1+len(pool.laneSelection()), subs,
		"a repeated join must yield one subscription set, not two")

	publish := func(msgID string) {
		data, err := json.Marshal(model.RoomEvent{
			Type: model.RoomEventNewMessage, LastMsgID: msgID, RoomID: "room-churn"})
		require.NoError(t, err)
		require.NoError(t, ncPub.Publish(subject.RoomEvent("room-churn", true), data))
	}
	publish("msg-probe")
	// Sentinel: NATS preserves per-subscription order, so once this has been
	// delivered any duplicate of msg-probe would already have landed. That makes
	// the negative assertion below deterministic without a sleep.
	publish("msg-sentinel")
	require.NoError(t, ncPub.Flush())

	require.Eventually(t, func() bool {
		return sink.count("msg-sentinel") == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 1, sink.count("msg-probe"),
		"a re-added member must not receive the probe twice")
}
