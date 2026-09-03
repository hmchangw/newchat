package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestSimClient_RunLifecycle_SubscribesWalksAndDelivers(t *testing.T) {
	fc := newFakeConn(
		subListPage{Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}}, HasMore: true},
		subListPage{Subscriptions: []subRow{{RoomID: "d1", RoomType: "dm"}}, HasMore: false},
	)
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	require.Eventually(t, func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		_, ok := fc.chanSubs[subject.RoomEvent("r1", true)]
		return ok
	}, 3*time.Second, 5*time.Millisecond, "walk must open the channel room sub")
	assert.GreaterOrEqual(t, fc.reqCount.Load(), int64(2), "walk must paginate")
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.ConnsActive), 0.001)

	// User lane delivery via the callback sub.
	evt, err := json.Marshal(deliveredEnvelope{Timestamp: time.Now().Add(-5 * time.Millisecond).UnixMilli()})
	require.NoError(t, err)
	fc.deliverCB(t, subject.UserRoomEvent("user-lc"), evt)
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.Delivered.WithLabelValues("user")), 0.001)

	// Channel delivery via the shared pump channel.
	fc.mu.Lock()
	roomCh := fc.chanSubs[subject.RoomEvent("r1", true)]
	fc.mu.Unlock()
	roomCh <- &nats.Msg{Data: evt}
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.Delivered.WithLabelValues("channel")) >= 1
	}, 3*time.Second, 5*time.Millisecond, "pump must drain the shared room channel")

	s.close()
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsActive), 0.001)
	assert.Equal(t, int64(1), fc.closes.Load())
	s.close() // idempotent
	assert.Equal(t, int64(1), fc.closes.Load())
}

func TestSimClient_RunFailsCleanlyWhenWalkFails(t *testing.T) {
	fc := newFakeConn() // no pages: the walk RPC errors
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.run(ctx)
	require.Error(t, err, "run must fail when the walk cannot decode a page")
	assert.Equal(t, int64(1), fc.closes.Load(), "a failed startup must not leave a zombie conn")
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsActive), 0.001)
}

func TestSimClient_HealthCheckExitsOnAPermanentlyClosedConnection(t *testing.T) {
	// nats.go closes a connection for good after repeated auth failures even
	// with MaxReconnects(-1). Without the health tick run() would sit in hold
	// forever holding a dead socket: never ready, never reported as exited,
	// so the swarm never restarts it and the fleet silently shrinks.
	fc := newFakeConn(emptySubListPage())
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.healthInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.run(ctx) }()

	require.Eventually(t, func() bool { return s.connSnapshot() != nil }, 3*time.Second, 5*time.Millisecond)
	fc.closed.Store(true)

	select {
	case err := <-done:
		require.Error(t, err, "a permanently closed connection must end the client, not park it")
		assert.Contains(t, err.Error(), "closed permanently")
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after the connection was closed permanently")
	}
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.Errors.WithLabelValues("conn_closed")), 0.001)
}

func TestSimClient_HealthCheckLeavesAReconnectingConnectionAlone(t *testing.T) {
	// nats.ws reports isClosed()=false while it is still retrying, and the
	// real client treats that as alive so the health check never shortcuts
	// the backoff curve.
	fc := newFakeConn(emptySubListPage())
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.healthInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.run(ctx) }()

	require.Eventually(t, func() bool { return s.connSnapshot() != nil }, 3*time.Second, 5*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("health check ended a live client: %v", err)
	case <-time.After(100 * time.Millisecond): // many ticks, no exit
	}
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("run did not exit on cancel")
	}
}

func TestSimClient_HealthTicksDoNotPostponeTheJWTRefresh(t *testing.T) {
	// The refresh timer must be armed once and re-armed only after it fires.
	// Rebuilding it on every loop iteration silently defeats the schedule:
	// nextRefreshDelay is 80% of *remaining* life, so each health tick moves
	// the deadline out and the fire time converges on T - healthInterval/4
	// instead of 0.8*T. With production numbers (2h token, 3m tick) that is
	// ~116 minutes rather than ~96 — the refresh lands minutes before expiry
	// instead of comfortably inside the token's life.
	fc := newFakeConn(emptySubListPage())
	s, mint := newLifecycleClient(t, fc, jwtModeProactive)
	s.refreshRand = func() float64 { return 0.5 } // exactly 80%, no jitter
	s.healthInterval = 2 * time.Millisecond
	s.minRefreshDelay = time.Millisecond // the floor is not what this test measures
	s.conn = fc
	// The expiry is set directly rather than through a minted token: JWT
	// claims carry whole seconds, which is far too coarse to pin a deadline.
	// One second of life means the schedule fires at 800ms; postponed, it
	// would not fire until ~999ms.
	s.cache.mu.Lock()
	s.cache.token, s.cache.expiresAt = "primed", time.Now().Add(time.Second)
	s.cache.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.hold(ctx) }()

	require.Eventually(t, func() bool { return mint.calls.Load() >= 1 }, 900*time.Millisecond, 5*time.Millisecond,
		"the proactive refresh must fire on its own schedule, not be pushed out by health ticks")
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("hold did not exit on cancel")
	}
}

// The pump is the only consumer of roomCh, and run cancelled it without
// waiting. Messages already queued at shutdown were discarded uncounted, so
// the run's delivery total was biased low by however deep the queue was —
// silently, in a tool whose entire product is that count.
func TestSimClient_ShutdownDrainsQueuedDeliveries(t *testing.T) {
	fc := newFakeConn(subListPage{Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}}})
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.conn = fc
	s.markConnUp()

	const queued = 32
	for i := 0; i < queued; i++ {
		s.roomCh <- &nats.Msg{Subject: subject.RoomEvent("r1", true), Data: []byte(`{"type":"other"}`)}
	}
	require.Len(t, s.roomCh, queued, "precondition: the pump has not started")

	pumpCtx, stopPump := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.pump(pumpCtx) }()
	s.drainPump(stopPump, done)

	assert.InDelta(t, queued, promtestutil.ToFloat64(s.m.Delivered.WithLabelValues("channel")), 0.001,
		"every queued delivery must be counted before the client goes away")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drainPump returned without joining the pump goroutine")
	}
}

// The drain is bounded: a pump that cannot keep up must not hold the whole
// fleet's shutdown past its budget.
func TestSimClient_ShutdownDrainIsBounded(t *testing.T) {
	fc := newFakeConn(emptySubListPage())
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.conn = fc
	s.pumpDrainTimeout = 20 * time.Millisecond
	s.roomCh <- &nats.Msg{Subject: subject.RoomEvent("r1", true), Data: []byte(`{"type":"other"}`)}

	// No pump is running, so the queue never empties.
	stopPump := func() {}
	done := make(chan struct{})
	close(done)
	start := time.Now()
	s.drainPump(stopPump, done)

	assert.Less(t, time.Since(start), time.Second, "the drain must give up on its own deadline")
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.Errors.WithLabelValues("drain_timeout")), 0.001,
		"a drain that gave up with messages queued is evidence, not silence")
}
