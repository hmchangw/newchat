package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
// synchronisation contract between attachSink (writer) and deliver (reader) of
// p.sink: NATS subscription callbacks call deliver from readLoop goroutines
// concurrently with SubscribeRoom/attachSink mutating pool state. Without the
// atomic pointer on both sides, -race reports a data race on p.sink even though
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

// TestDirectPool_SubscribeRoom_IsIdempotent pins the fix for the phantom
// duplicate_delivery bug. removeMember deliberately leaves the room
// subscription in place, but applyChange rebuilds its candidate set from
// mm.Members(roomID) — so the just-removed floater is a legitimate join target
// again and addMember calls SubscribeRoom a second time on the identical
// subject. Two live subscriptions deliver every later probe twice on the same
// lane, which the tracker reports as duplicate_delivery against a system that
// delivered exactly once.
//
// du.nc is nil here: reaching the NATS call at all returns
// nats.ErrInvalidConnection, so a nil error proves the short-circuit fired.
func TestDirectPool_SubscribeRoom_IsIdempotent(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	du := &directUser{id: "u-1", rooms: map[string]struct{}{"r-1": {}}}
	p.users["u-1"] = du

	require.NoError(t, p.SubscribeRoom("u-1", "r-1"))
	require.NoError(t, p.SubscribeRoom("u-1", "r-1"))

	assert.Empty(t, du.subs, "a room already held must not open a second subscription")
}

// TestDirectPool_SubscribeRoom_NewRoomStillSubscribes is the other half of the
// gate: the short-circuit must be keyed on the room, not swallow every call.
func TestDirectPool_SubscribeRoom_NewRoomStillSubscribes(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	du := &directUser{id: "u-1", rooms: map[string]struct{}{"r-1": {}}}
	p.users["u-1"] = du

	err := p.SubscribeRoom("u-1", "r-2")

	require.Error(t, err, "a room the user does not hold must reach the subscribe path")
	assert.NotContains(t, du.rooms, "r-2", "a failed subscribe must not claim the room")
}

func TestDirectPool_SubscribeRoom_UnknownUserErrors(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)

	assert.Error(t, p.SubscribeRoom("u-missing", "r-1"))
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

// TestDirectPool_RecordDisconnectFor_IgnoresUnregisteredUser pins the fix for
// the phantom-drop bug: Add arms the disconnect handler before it subscribes,
// and every one of its error paths Drains the connection it just opened. Drain
// closes the conn, the handler fires, and an unregistered connection would be
// counted as a lost recipient — feeding VerifyInputs.DroppedRecipients and
// reporting INCONCLUSIVE for a background user's activation hiccup.
func TestDirectPool_RecordDisconnectFor_IgnoresUnregisteredUser(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	du := &directUser{id: "u-1"} // never reached p.users

	p.recordDisconnectFor(du)

	assert.Zero(t, p.DroppedRecipients(),
		"a connection Add abandoned was never a tracked recipient")
}

func TestDirectPool_RecordDisconnectFor_CountsRegisteredUser(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	du := &directUser{id: "u-1"}
	du.registered.Store(true)

	p.recordDisconnectFor(du)

	assert.Equal(t, int64(1), p.DroppedRecipients(),
		"a registered recipient going away mid-run does invalidate delivery claims")
}

func TestDirectPool_RecordDisconnectFor_IgnoresOrderlyShutdown(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	du := &directUser{id: "u-1"}
	du.registered.Store(true)
	p.Close()

	p.recordDisconnectFor(du)

	assert.Zero(t, p.DroppedRecipients())
}

// TestDirectPool_Add_RegistersOnlyOnSuccess pins the other half of the gate:
// Add must not leave a user's registered flag set unless the user actually
// entered the pool. A connect failure (no server at this URL) is the cheapest
// error path to exercise without NATS.
func TestDirectPool_Add_ConnectFailure_RecordsNoDrop(t *testing.T) {
	p := newDirectPool("nats://127.0.0.1:1", "", nil)

	err := p.Add(&userState{ID: "u-1", Account: "a-1", Rooms: []string{"r-1"}})

	require.Error(t, err)
	assert.Zero(t, p.Size())
	assert.Zero(t, p.DroppedRecipients(),
		"a failed activation must not be reported as a dropped recipient")
}

// TestDirectPool_Deliver_DoesNotTakePoolLock pins that the receive hot path is
// lock-free with respect to the pool-global mutex. `daily` defaults to 20000
// direct users with no sink attached, so every broadcast every one of them
// receives runs through deliver; serialising that on one mutex is the ceiling
// Collector shards 64 ways to avoid (see collector.go). The test holds p.mu and
// asserts deliver still completes.
func TestDirectPool_Deliver_DoesNotTakePoolLock(t *testing.T) {
	tests := []struct {
		name   string
		attach bool
	}{
		{name: "nil sink (daily)", attach: false},
		{name: "attached sink (verify)", attach: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newDirectPool("nats://unused", "", nil)
			sink := &recordingSink{}
			if tt.attach {
				p.attachSink(sink)
			}

			p.mu.Lock()
			defer p.mu.Unlock()

			done := make(chan struct{})
			go func() {
				defer close(done)
				p.deliver("u-1", []byte(`{"roomId":"r-1","lastMsgId":"m-1"}`), laneGlobal)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				// The deferred Unlock releases the goroutine after the test.
				t.Fatal("deliver blocked on the pool-global mutex")
			}

			if tt.attach {
				assert.Len(t, sink.calls, 1)
			} else {
				assert.Empty(t, sink.calls)
			}
		})
	}
}
