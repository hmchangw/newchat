package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// recordingSink captures deliveries for assertions without a NATS connection.
type recordingSink struct {
	calls []sinkCall
}

type sinkCall struct {
	userID, msgID, roomID string
	ln                    lane
}

func (s *recordingSink) RecordDelivery(userID, msgID, roomID string, ln lane, _ time.Time) {
	s.calls = append(s.calls, sinkCall{userID, msgID, roomID, ln})
}

func TestLaneForSubject_Global(t *testing.T) {
	assert.Equal(t, laneGlobal, laneForSubject("chat.room.r-1.event"))
}

func TestLaneForSubject_User(t *testing.T) {
	assert.Equal(t, laneUser, laneForSubject("chat.user.alice.room.event"))
}

func TestLaneForSubject_Local(t *testing.T) {
	assert.Equal(t, laneLocal, laneForSubject("chat.local.room.r-1.event"))
}

func TestDirectPool_NilSink_IsNoop(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	// Must not panic when no sink is attached — this is daily's path.
	p.deliver("u-1", []byte(`{"roomId":"r-1","lastMsgId":"m-1"}`), laneGlobal)
}

func TestDirectPool_Deliver_AttributesToUser(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	sink := &recordingSink{}
	p.attachSink(sink)

	p.deliver("u-1", []byte(`{"roomId":"room-small-000001","lastMsgId":"m-1"}`), laneGlobal)

	assert.Equal(t, []sinkCall{
		{userID: "u-1", msgID: "m-1", roomID: "room-small-000001", ln: laneGlobal},
	}, sink.calls)
}

func TestDirectPool_Deliver_SkipsEventsWithoutMessageID(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	sink := &recordingSink{}
	p.attachSink(sink)

	// Membership and rename events carry no lastMsgId.
	p.deliver("u-1", []byte(`{"roomId":"room-small-000001","type":"room_renamed"}`), laneGlobal)

	assert.Empty(t, sink.calls)
}

func TestDirectPool_Deliver_IgnoresMalformedPayload(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	sink := &recordingSink{}
	p.attachSink(sink)

	p.deliver("u-1", []byte(`{not json`), laneGlobal)

	assert.Empty(t, sink.calls)
}

// TestDirectPool_AttachSinkDeliver_ConcurrentAccessIsRaceFree pins the
// locking contract between attachSink (writer) and deliver (reader) of
// p.sink: NATS subscription callbacks call deliver from readLoop goroutines
// concurrently with SubscribeRoom/attachSink mutating pool state. Without
// p.mu guarding both sides, -race reports a data race on p.sink even though
// no test assertion depends on the interleaving — the race detector catches
// the unsynchronized access itself.
func TestDirectPool_AttachSinkDeliver_ConcurrentAccessIsRaceFree(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	sink := &recordingSink{}

	const iterations = 2000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			p.deliver("u-1", []byte(`{"roomId":"r-1","lastMsgId":"m-1"}`), laneGlobal)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			p.attachSink(sink)
		}
	}()
	wg.Wait()
}
