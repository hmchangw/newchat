package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexGate_Ready_EnsuresOnceThenPasses(t *testing.T) {
	var calls atomic.Int32
	g := newIndexGate("t (k)", func(context.Context) error { calls.Add(1); return nil })

	require.NoError(t, g.Ready(context.Background()))
	require.NoError(t, g.Ready(context.Background()))
	assert.EqualValues(t, 1, calls.Load(), "a confirmed index is not re-ensured")
}

func TestIndexGate_Ready_RetriesAfterFailure(t *testing.T) {
	var calls atomic.Int32
	fail := true
	g := newIndexGate("t (k)", func(context.Context) error {
		calls.Add(1)
		if fail {
			return errors.New("server selection timeout")
		}
		return nil
	})

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	g.now = clock.Now

	err := g.Ready(context.Background())
	require.Error(t, err, "an unconfirmed index blocks the caller")
	assert.EqualValues(t, 1, calls.Load())

	fail = false
	clock.Advance(indexRetryMax)
	require.NoError(t, g.Ready(context.Background()), "the next probe after the retry-after passes once MongoDB is back")
	assert.EqualValues(t, 2, calls.Load())
	require.NoError(t, g.Ready(context.Background()))
	assert.EqualValues(t, 2, calls.Load())
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// A persistent failure (duplicate data no index build gets past) must not turn every write and NAK
// redelivery into another collection-wide createIndexes: remembered, re-probed once per doubling interval.
func TestIndexGate_Ready_BacksOffAfterFailure(t *testing.T) {
	var calls atomic.Int32
	g := newIndexGate("t (k)", func(context.Context) error {
		calls.Add(1)
		return errors.New("E11000 duplicate key")
	})
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	g.now = clock.Now

	require.Error(t, g.Ready(context.Background()))
	require.Error(t, g.Ready(context.Background()), "the remembered failure is returned")
	assert.EqualValues(t, 1, calls.Load(), "no second probe inside the retry-after")

	clock.Advance(indexRetryMin)
	require.Error(t, g.Ready(context.Background()))
	assert.EqualValues(t, 2, calls.Load(), "one probe once the interval elapsed")

	clock.Advance(indexRetryMin)
	require.Error(t, g.Ready(context.Background()))
	assert.EqualValues(t, 2, calls.Load(), "the interval doubled; the second probe is not due yet")
	clock.Advance(indexRetryMin)
	require.Error(t, g.Ready(context.Background()))
	assert.EqualValues(t, 3, calls.Load())

	// The cap: after enough failures the interval stops growing.
	for i := 0; i < 10; i++ {
		clock.Advance(indexRetryMax)
		require.Error(t, g.Ready(context.Background()))
	}
	assert.EqualValues(t, 13, calls.Load(), "one probe per capped interval")
}

func TestIndexGate_Ready_ConcurrentCallersEnsureOnce(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	g := newIndexGate("t (k)", func(context.Context) error {
		calls.Add(1)
		<-release
		return nil
	})

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = g.Ready(context.Background())
		}(i)
	}
	close(release)
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, calls.Load(), "waiters serialize behind one ensure instead of racing it")
}

// Waiters share a failed attempt instead of each repeating the round trip before they can NAK.
func TestIndexGate_Ready_WaitersShareOneFailedAttempt(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	g := newIndexGate("t (k)", func(context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return errors.New("server selection timeout")
	})

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	g.now = clock.Now

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(1)
	go func() { defer wg.Done(); errs[0] = g.Ready(context.Background()) }()
	<-started
	for i := 1; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = g.Ready(context.Background()) }(i)
	}
	// Sharing the flight or getting the remembered failure: either way no waiter starts a new probe.
	close(release)
	wg.Wait()

	for i, err := range errs {
		require.Error(t, err, "waiter %d", i)
	}
	assert.EqualValues(t, 1, calls.Load(), "one attempt serves every caller that was waiting on it")

	// A later arrival, once the retry-after has elapsed, starts a fresh attempt.
	clock.Advance(indexRetryMax)
	require.Error(t, g.Ready(context.Background()))
	assert.EqualValues(t, 2, calls.Load())
}

// The ensure must not inherit the caller's open-ended context (a stalled command would hold every
// reply), nor may a leader cancelled mid-attempt abort it: the leader is released, the attempt completes.
func TestIndexGate_Ready_BoundsTheEnsureAndDetachesCancellation(t *testing.T) {
	var sawDeadline bool
	var cancelledWhileRunning bool
	started := make(chan struct{})
	release := make(chan struct{})
	g := newIndexGate("t (k)", func(ctx context.Context) error {
		_, sawDeadline = ctx.Deadline()
		close(started)
		<-release
		cancelledWhileRunning = ctx.Err() != nil
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- g.Ready(ctx) }()
	<-started
	cancel()
	require.ErrorIs(t, <-leaderDone, context.Canceled, "the cancelled leader is released")

	close(release)
	require.NoError(t, g.Ready(context.Background()), "the attempt completes and confirms the index")
	assert.True(t, sawDeadline, "ensure runs under its own deadline")
	assert.False(t, cancelledWhileRunning, "the leader's cancellation did not reach the attempt")
}

// A gate must not replace a sooner caller deadline with its own budget: startup shares one across both.
func TestIndexGate_Ready_HonoursASoonerCallerDeadline(t *testing.T) {
	var seen time.Time
	g := newIndexGate("t (k)", func(ctx context.Context) error {
		seen, _ = ctx.Deadline()
		return nil
	})
	callerDeadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()
	require.NoError(t, g.Ready(ctx))
	assert.False(t, seen.After(callerDeadline), "ensure's deadline %v must not exceed the caller's %v", seen, callerDeadline)
}

// A waiter cancelled mid-wait (consumer draining) is released promptly; the shared attempt runs on.
func TestIndexGate_Ready_ReleasesACancelledWaiterWithoutAbortingTheAttempt(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	g := newIndexGate("t (k)", func(context.Context) error {
		calls.Add(1)
		close(started)
		<-release
		return nil
	})

	leaderDone := make(chan error, 1)
	go func() { leaderDone <- g.Ready(context.Background()) }()
	<-started

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- g.Ready(waiterCtx) }()
	cancelWaiter()

	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled, "a cancelled waiter returns its own ctx error")
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter stayed blocked behind the in-flight attempt")
	}

	close(release)
	require.NoError(t, <-leaderDone, "the attempt completes for the leader")
	require.NoError(t, g.Ready(context.Background()))
	assert.EqualValues(t, 1, calls.Load(), "the cancelled waiter did not abort or repeat the attempt")
}
