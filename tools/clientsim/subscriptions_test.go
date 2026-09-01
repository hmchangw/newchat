package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestSimClient_LiveUpdate_AddRemoveFlipAgainstConn(t *testing.T) {
	fc := newFakeConn(subListPage{HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	updSubj := subject.SubscriptionUpdate("user-lc")
	require.Eventually(t, func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return fc.cbSubs[updSubj] != nil
	}, 3*time.Second, 5*time.Millisecond)

	// added -> open on global namespace
	fc.deliverCB(t, updSubj, updJSON("added", "r9", "channel", nil))
	fc.mu.Lock()
	_, hasGlobal := fc.chanSubs[subject.RoomEvent("r9", true)]
	fc.mu.Unlock()
	assert.True(t, hasGlobal, "added channel must open the global-namespace sub")

	// crossSite flip -> close old, open local
	fa := false
	fc.deliverCB(t, updSubj, updJSON("added", "r9", "channel", &fa))
	fc.mu.Lock()
	globalSub := fc.subs[subject.RoomEvent("r9", true)]
	_, hasLocal := fc.chanSubs[subject.RoomEvent("r9", false)]
	fc.mu.Unlock()
	assert.Equal(t, int64(1), globalSub.unsubs.Load(), "flip must close the old namespace")
	assert.True(t, hasLocal, "flip must open the new namespace")

	// removed -> close
	fc.deliverCB(t, updSubj, updJSON("removed", "r9", "channel", nil))
	fc.mu.Lock()
	localSub := fc.subs[subject.RoomEvent("r9", false)]
	fc.mu.Unlock()
	assert.Equal(t, int64(1), localSub.unsubs.Load(), "removed must unsubscribe")
	s.mu.Lock()
	_, still := s.roomSubs["r9"]
	s.mu.Unlock()
	assert.False(t, still)
}

func TestSimClient_SubscribeFailureIsRetriedOnNextAdd(t *testing.T) {
	fc := newFakeConn(subListPage{HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	updSubj := subject.SubscriptionUpdate("user-lc")
	require.Eventually(t, func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return fc.cbSubs[updSubj] != nil
	}, 3*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 3*time.Second, 5*time.Millisecond)

	fc.mu.Lock()
	fc.subChanErr = errors.New("boom")
	fc.mu.Unlock()
	fc.deliverCB(t, updSubj, updJSON("added", "rX", "channel", nil))
	s.mu.Lock()
	_, open := s.roomSubs["rX"]
	s.mu.Unlock()
	require.False(t, open, "failed open must not be recorded")
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"a failed live open must demote readiness")

	// Because roomSubs is the dedupe source of truth, a repeat add retries.
	fc.deliverCB(t, updSubj, updJSON("added", "rX", "channel", nil))
	s.mu.Lock()
	_, open = s.roomSubs["rX"]
	s.mu.Unlock()
	assert.True(t, open, "repeat added must retry after a failed open")
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"repairing the last missing room must restore readiness")
}

func TestSimClient_LiveRepairWaitsForEveryMissingRoom(t *testing.T) {
	fc := newFakeConn(subListPage{HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	updSubj := subject.SubscriptionUpdate("user-lc")
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 3*time.Second, 5*time.Millisecond)

	for _, roomID := range []string{"r1", "r2"} {
		fc.mu.Lock()
		fc.subChanErr = errors.New("boom")
		fc.mu.Unlock()
		fc.deliverCB(t, updSubj, updJSON("added", roomID, "channel", nil))
	}
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001)

	fc.deliverCB(t, updSubj, updJSON("added", "r1", "channel", nil))
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"one repaired room must not hide another missing room")

	fc.deliverCB(t, updSubj, updJSON("added", "r2", "channel", nil))
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"readiness returns only after every missing room is repaired")
}

func TestSimClient_BootstrapWalk_DoesNotRevertMidWalkUpdates(t *testing.T) {
	// The walk's RPC is gated; while it is in flight a live update adds a
	// room the server snapshot does not know about. The walk must not
	// close it (generation skip), and must still open its own rooms.
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "from-walk", RoomType: "channel"}}, HasMore: false})
	fc.reqGate = make(chan struct{})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	updSubj := subject.SubscriptionUpdate("user-lc")
	require.Eventually(t, func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return fc.cbSubs[updSubj] != nil
	}, 3*time.Second, 5*time.Millisecond, "lanes subscribe before the walk completes")

	fc.deliverCB(t, updSubj, updJSON("added", "live-room", "channel", nil))
	close(fc.reqGate) // let the walk's RPC return its stale snapshot

	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, walkOpen := s.roomSubs["from-walk"]
		return walkOpen
	}, 3*time.Second, 5*time.Millisecond)
	s.mu.Lock()
	_, liveOpen := s.roomSubs["live-room"]
	s.mu.Unlock()
	assert.True(t, liveOpen, "walk must not revert a live update that landed during its RPC")
	fc.mu.Lock()
	liveSub := fc.subs[subject.RoomEvent("live-room", true)]
	fc.mu.Unlock()
	assert.Equal(t, int64(0), liveSub.unsubs.Load())
}

func TestSimClient_Resync_CoalescesConcurrentWalks(t *testing.T) {
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r-new", RoomType: "channel"}}, HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.roomSubs) == 1
	}, 3*time.Second, 5*time.Millisecond)

	before := fc.reqCount.Load()
	ctx := context.Background()

	// Pin one walk inside its RPC so it demonstrably holds the resync state.
	// Racing five goroutines and asserting "fewer than five ran" instead
	// would pass only while the scheduler interleaves them — under load they
	// serialize and every one runs.
	fc.reqGate = make(chan struct{})
	fc.reqEntered = make(chan struct{}, 1)

	var first sync.WaitGroup
	first.Add(1)
	go func() { defer first.Done(); s.resync(ctx) }()
	select {
	case <-fc.reqEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("the first resync never reached its walk")
	}

	var rest sync.WaitGroup
	for i := 0; i < 4; i++ {
		rest.Add(1)
		go func() { defer rest.Done(); s.resync(ctx) }()
	}
	rest.Wait()
	assert.Equal(t, before, fc.reqCount.Load(),
		"resyncs arriving while a walk is in flight must collapse, not queue")

	close(fc.reqGate)
	first.Wait()
	assert.Equal(t, before+2, fc.reqCount.Load(),
		"the in-flight walk plus exactly one coalesced follow-up — collapsed, but never dropped")
}

func TestSimClient_Resync_RetriesUntilResponderRecovers(t *testing.T) {
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}}, HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 3*time.Second, 5*time.Millisecond)

	before := fc.reqCount.Load()
	fc.failNextRequests(nats.ErrNoResponders)
	s.markConnDown()
	s.markConnUp()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		s.resync(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return fc.reqCount.Load() >= before+2 &&
			promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 3*time.Second, 10*time.Millisecond,
		"a transient no-responders window must not leave the client permanently not-ready")
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("resync did not finish after the responder recovered")
	}
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.Errors.WithLabelValues("resync")), 0.001)
}

// A room subscription that fails to open leaves the client silently
// missing that room's traffic forever (nothing retries it without a
// reconnect or a live update). It must not count as ready, and it must
// leave error evidence behind.
func TestBootstrapWalk_PartialPlanIsNotReady(t *testing.T) {
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}, {RoomID: "r2", RoomType: "channel"}},
		HasMore:       false,
	})
	fc.subChanErr = assert.AnError // the first room subscribe fails
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)

	startClient(t, s)

	// Wait on the walk's own output. ConnsActive is set in connect(), which
	// runs before subscribeLanes and the walk, so gating on it would assert
	// against a walk that may not have happened yet — green by scheduling
	// luck, and vacuous when green.
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.Errors.WithLabelValues("room_subscribe")) == 1
	}, 2*time.Second, 10*time.Millisecond, "the walk never reported its failed room subscribe")

	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.ConnsActive), 0.001, "still connected")
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"a client missing part of its subscription plan is connected but not ready")
}

// The happy path must actually reach ready, or the gate would fail every run.
func TestBootstrapWalk_FullPlanIsReady(t *testing.T) {
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}, {RoomID: "r2", RoomType: "channel"}},
		HasMore:       false,
	})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)

	startClient(t, s)

	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 2*time.Second, 10*time.Millisecond, "a fully-subscribed client never became ready")
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.Errors.WithLabelValues("room_subscribe")), 0.001)
}
