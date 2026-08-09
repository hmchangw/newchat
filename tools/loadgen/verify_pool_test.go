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

func TestDirectPool_DefaultLanes_AreBoth(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	// daily must keep subscribing both lanes so it stays ROOM_SUBJECT_MODE-agnostic.
	assert.Equal(t, []bool{true, false}, p.laneSelection())
}

func TestDirectPool_SetLanes_NarrowsSelection(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	p.setLanes([]bool{false})
	assert.Equal(t, []bool{false}, p.laneSelection())
}

func TestDirectPool_SetLanes_IgnoresEmptySelection(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	// Subscribing to nothing would make every message look lost.
	p.setLanes(nil)
	assert.Equal(t, []bool{true, false}, p.laneSelection())
}

func TestDirectPool_MissingUsers(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	p.users["u-1"] = &directUser{id: "u-1"}
	p.users["u-3"] = &directUser{id: "u-3"}

	// Size() would report 2 for a designated set of 3 backfilled by a
	// background user — membership is the assertion that actually holds.
	assert.Equal(t, []string{"u-2"}, p.MissingUsers([]string{"u-1", "u-2", "u-3"}))
	assert.Empty(t, p.MissingUsers([]string{"u-1", "u-3"}))
}

func TestDirectPool_RecordDisconnect_CountsMidRunLoss(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	p.recordDisconnect()
	p.recordDisconnect()
	assert.Equal(t, int64(2), p.DroppedRecipients())
}

func TestDirectPool_RecordDisconnect_IgnoresOrderlyShutdown(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	// Close drains every recipient conn; counting those would put every run
	// on the INCONCLUSIVE branch.
	p.Close()
	p.recordDisconnect()
	assert.Zero(t, p.DroppedRecipients())
}
