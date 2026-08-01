//go:build integration

package main

import (
	"encoding/json"
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
