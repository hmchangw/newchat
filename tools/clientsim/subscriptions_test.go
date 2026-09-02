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

// A live update repairs the rooms the client knows about; it cannot vouch for
// the ones it has not learned of yet. After a reconnect whose walk is still
// failing, the plan is unverified — an empty missing set means "nothing known
// to be broken", not "plan complete", and promoting on it reports a fleet as
// ready during exactly the fault window an operator is reading the gauge in.
func TestSimClient_LiveUpdateDoesNotPromoteAnUnverifiedPlan(t *testing.T) {
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}}, HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 3*time.Second, 5*time.Millisecond)

	// Reconnect. The walk that would re-verify the plan has not run yet —
	// deliberately not started here, so the assertion turns on the invariant
	// rather than on whether a background resync got scheduled in time.
	s.invalidatePlan() // what the disconnect handler does alongside markConnDown
	s.markConnDown()
	s.markConnUp()
	require.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"a reconnected client has not re-verified its plan yet")

	fc.deliverCB(t, subject.SubscriptionUpdate("user-lc"), updJSON("added", "r9", "channel", nil))

	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"one successful live open must not stand in for a completed walk")
}

// A walk that fetched its plan over a connection that has since dropped must
// not report that plan as verified. Its snapshot describes a dead connection,
// so letting it set planVerified hands a later live update the very licence
// planVerified exists to withhold.
func TestSimClient_StaleWalkDoesNotReVerifyAfterDisconnect(t *testing.T) {
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}}, HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 3*time.Second, 5*time.Millisecond)

	// Pin a walk inside its RPC, then drop the connection under it.
	fc.reqGate = make(chan struct{})
	fc.reqEntered = make(chan struct{}, 1)
	walkDone := make(chan struct{})
	go func() { defer close(walkDone); _ = s.bootstrapWalk(context.Background()) }()
	select {
	case <-fc.reqEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("the walk never reached its RPC")
	}
	s.invalidatePlan()
	s.markConnDown()
	close(fc.reqGate) // the stale walk now completes against a dead connection
	<-walkDone

	s.markConnUp() // reconnected; no walk has succeeded since
	fc.deliverCB(t, subject.SubscriptionUpdate("user-lc"), updJSON("added", "r9", "channel", nil))

	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"a walk from the previous connection must not stand in for a post-reconnect one")
}

// --- room subscriptions cover BOTH lanes the real client opens ---

func TestApplyChanges_OpensMessageAndMemberSubjects(t *testing.T) {
	fc := newFakeConn(subListPage{HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.conn = fc

	s.mu.Lock()
	s.applyChangesLocked([]subChange{{Op: subOpen, RoomID: "r1", Global: true}})
	s.mu.Unlock()

	fc.mu.Lock()
	_, hasMsg := fc.chanSubs[subject.RoomEvent("r1", true)]
	_, hasMember := fc.chanSubs[subject.RoomMemberEvent("r1", true)]
	fc.mu.Unlock()
	assert.True(t, hasMsg, "must open the room message subject")
	assert.True(t, hasMember, "must open the room member subject")

	s.mu.Lock()
	_, missing := s.missingRooms["r1"]
	s.mu.Unlock()
	assert.False(t, missing, "both subscribes succeeded, so the room is not missing")
}

func TestApplyChanges_MemberSubscribeFailureRollsBackTheMessageSub(t *testing.T) {
	fc := newFakeConn(subListPage{HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.conn = fc
	// A room is subscribed only when BOTH lanes are open; a half-open room
	// would silently miss every member event while counting as ready.
	fc.failSubscribeChanOn(subject.RoomMemberEvent("r2", true), errors.New("permissions violation"))

	s.mu.Lock()
	s.applyChangesLocked([]subChange{{Op: subOpen, RoomID: "r2", Global: true}})
	_, recorded := s.roomSubs["r2"]
	_, missing := s.missingRooms["r2"]
	s.mu.Unlock()

	assert.False(t, recorded, "a half-open room must not be recorded as subscribed")
	assert.True(t, missing, "a half-open room must be remembered as missing")

	fc.mu.Lock()
	msgSub := fc.subs[subject.RoomEvent("r2", true)]
	fc.mu.Unlock()
	require.NotNil(t, msgSub, "the message lane was opened before the member lane failed")
	assert.Equal(t, int64(1), msgSub.unsubs.Load(), "the opened lane must be rolled back")
}

func TestApplyChanges_CloseUnsubscribesBothLanes(t *testing.T) {
	fc := newFakeConn(subListPage{HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.conn = fc

	s.mu.Lock()
	s.applyChangesLocked([]subChange{{Op: subOpen, RoomID: "r3", Global: true}})
	s.applyChangesLocked([]subChange{{Op: subClose, RoomID: "r3"}})
	s.mu.Unlock()

	fc.mu.Lock()
	msgSub := fc.subs[subject.RoomEvent("r3", true)]
	memberSub := fc.subs[subject.RoomMemberEvent("r3", true)]
	fc.mu.Unlock()
	require.NotNil(t, msgSub)
	require.NotNil(t, memberSub)
	assert.Equal(t, int64(1), msgSub.unsubs.Load())
	assert.Equal(t, int64(1), memberSub.unsubs.Load())
}

func TestRoomLane_SplitsMemberEventsOntoTheirOwnCounter(t *testing.T) {
	assert.Equal(t, "member", roomLane(subject.RoomMemberEvent("r4", true)))
	assert.Equal(t, "member", roomLane(subject.RoomMemberEvent("r4", false)))
	assert.Equal(t, "channel", roomLane(subject.RoomEvent("r4", true)))
	assert.Equal(t, "channel", roomLane(subject.RoomEvent("r4", false)))
}

// --- an async subscription fault is sticky for the life of the connection ---

func TestAsyncFault_SurvivesAnUnrelatedLiveUpdate(t *testing.T) {
	// A SUB permission violation arrives after Subscribe already returned nil,
	// so nothing is recorded in missingRooms. A one-shot markNotReady is then
	// undone by the next live update that produces any change at all, because
	// updateReadyLocked only asks about planVerified and missingRooms.
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}},
		HasMore:       false,
	})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 2*time.Second, 10*time.Millisecond, "client never became ready")

	s.handleAsyncError(nil, errors.New("nats: Permissions Violation for Subscription to \"chat.room.r1.event.member\""))
	require.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"an async fault must demote the client")

	// An unrelated room is added. It says nothing about the denied subscription.
	fc.deliverCB(t, subject.SubscriptionUpdate("user-lc"), updJSON("added", "r2", "channel", nil))

	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"an unrelated update must not vouch for a subscription the server denied")
}

func TestAsyncFault_ClearsWithTheConnectionItBelongedTo(t *testing.T) {
	// The fault is scoped to one connection: its permissions came from that
	// connection's JWT. A reconnect re-subscribes, so a still-denied room
	// raises the fault again within milliseconds — but a transient fault must
	// not poison the client forever.
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}},
		HasMore:       false,
	})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.ConnsReady) == 1
	}, 2*time.Second, 10*time.Millisecond)

	s.handleAsyncError(nil, errors.New("nats: Permissions Violation for Subscription"))
	require.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001)

	s.invalidatePlan() // what the disconnect handler does
	s.mu.Lock()
	faulted := s.asyncFault
	s.mu.Unlock()
	assert.False(t, faulted, "the fault belonged to the connection that went away")

	// A fresh walk on the new connection may promote again.
	require.NoError(t, s.bootstrapWalk(context.Background()))
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"a clean connection with a verified plan is ready again")
}

func TestSimClient_RemovalClearsAFailedRoomAndRestoresReady(t *testing.T) {
	fc := newFakeConn(subListPage{
		Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}},
		HasMore:       false,
	})
	// r1's member lane is denied, so the room lands in missingRooms.
	fc.failSubscribeChanOn(subject.RoomMemberEvent("r1", true), errors.New("permissions violation"))
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, missing := s.missingRooms["r1"]
		return missing
	}, 2*time.Second, 10*time.Millisecond, "the half-open room must be recorded missing")
	require.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsReady), 0.001)

	// The room is removed server-side. Nothing is broken any more.
	fc.deliverCB(t, subject.SubscriptionUpdate("user-lc"), updJSON("removed", "r1", "channel", nil))

	s.mu.Lock()
	_, stillMissing := s.missingRooms["r1"]
	s.mu.Unlock()
	assert.False(t, stillMissing, "a removed room cannot still be missing")
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.ConnsReady), 0.001,
		"with nothing left to repair the client must return to ready")
}

func TestBootstrapWalk_StaleEpochIsNotFatal(t *testing.T) {
	// A disconnect during the initial walk is an expected race: the reconnect
	// handler already scheduled a resync. Tearing the client down instead
	// burns its restart budget for something that would have self-healed.
	fc := newFakeConn(subListPage{HasMore: false})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.conn = fc
	s.invalidatePlan() // the walk below will observe a changed epoch

	err := func() error {
		s.mu.Lock()
		startEpoch := s.planEpoch
		s.mu.Unlock()
		s.planEpoch = startEpoch // no-op, keeps the intent readable
		return nil
	}()
	require.NoError(t, err)

	// Force the race: bump the epoch while the walk's RPC is in flight.
	fc.reqGate = make(chan struct{})
	fc.reqEntered = make(chan struct{}, 1)
	walkErr := make(chan error, 1)
	go func() { walkErr <- s.bootstrapWalk(context.Background()) }()
	<-fc.reqEntered
	s.invalidatePlan()
	close(fc.reqGate)

	got := <-walkErr
	require.Error(t, got, "the walk still fails: its plan came from a dead connection")
	assert.True(t, errors.Is(got, errPlanEpochChanged),
		"the stale-epoch case must be distinguishable so run() does not tear the client down")
}
