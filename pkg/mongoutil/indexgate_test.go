package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestIndexGate_Ready_EnsuresOnceThenPasses(t *testing.T) {
	var calls atomic.Int32
	g := NewIndexGate("t (k)", func(context.Context) error { calls.Add(1); return nil })

	require.NoError(t, g.Ready(context.Background()))
	require.NoError(t, g.Ready(context.Background()))
	assert.EqualValues(t, 1, calls.Load(), "a confirmed index is not re-ensured")
}

func TestIndexGate_Ready_RetriesAfterFailure(t *testing.T) {
	var calls atomic.Int32
	fail := true
	g := NewIndexGate("t (k)", func(context.Context) error {
		calls.Add(1)
		if fail {
			return errors.New("E11000 duplicate key")
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
func TestIndexGate_Ready_BacksOffAfterAServerReportedFailure(t *testing.T) {
	var calls atomic.Int32
	g := NewIndexGate("t (k)", func(context.Context) error {
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

// An outage (network error, server selection timeout) is re-probed every indexRetryMin: a failed
// createIndexes over a dead server is cheap, and a memo that outlives the outage only delays recovery.
func TestIndexGate_Ready_TransientFailureRetriesAtTheFloor(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"network error label", mongo.CommandError{Message: "connection reset", Labels: []string{"NetworkError"}}},
		{"attempt deadline", context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			g := NewIndexGate("t (k)", func(context.Context) error {
				calls.Add(1)
				return fmt.Errorf("create index: %w", tt.err)
			})
			clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
			g.now = clock.Now

			require.Error(t, g.Ready(context.Background()))
			for i := 0; i < 5; i++ {
				clock.Advance(indexRetryMin)
				require.Error(t, g.Ready(context.Background()))
			}
			assert.EqualValues(t, 6, calls.Load(), "one probe per indexRetryMin; the interval never doubles for an outage")
		})
	}
}

// A failure that is the caller's own deadline expiring (startup's shared budget nearly spent) says
// nothing about the index, so it must not close the gate for the callers that follow.
func TestIndexGate_Ready_DoesNotRememberTheCallersExpiredDeadline(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	g := NewIndexGate("t (k)", func(ctx context.Context) error {
		calls.Add(1)
		if fail.Load() {
			<-ctx.Done()
			return fmt.Errorf("create index: %w", ctx.Err())
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, g.Ready(ctx), context.DeadlineExceeded)

	fail.Store(false)
	require.Eventually(t, func() bool { return g.Ready(context.Background()) == nil }, time.Second, time.Millisecond,
		"the next caller must probe again immediately instead of inheriting the expired caller's failure")
	assert.EqualValues(t, 2, calls.Load())
}

func TestIndexGate_Ready_ConcurrentCallersEnsureOnce(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	g := NewIndexGate("t (k)", func(context.Context) error {
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
	g := NewIndexGate("t (k)", func(context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return errors.New("E11000 duplicate key")
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
	g := NewIndexGate("t (k)", func(ctx context.Context) error {
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

// A caller without a deadline (a JetStream handler) gets IndexProbeTimeout, kept well under the 30s
// ACK_WAIT default so a stalled createIndexes cannot hold a delivery past its ack deadline.
func TestIndexGate_Ready_HotPathAttemptIsBoundedBelowAckWait(t *testing.T) {
	var deadline time.Time
	var ok bool
	g := NewIndexGate("t (k)", func(ctx context.Context) error { deadline, ok = ctx.Deadline(); return nil })
	before := time.Now()
	require.NoError(t, g.Ready(context.Background()))
	require.True(t, ok, "the hot-path attempt must carry a deadline")
	assert.LessOrEqual(t, deadline.Sub(before), IndexProbeTimeout+time.Second)
	assert.Less(t, IndexProbeTimeout, 30*time.Second, "must stay under the consumer ACK_WAIT default")
}

// A caller that states a budget (startup's shared ensure context) keeps it, capped at IndexEnsureTimeout.
func TestIndexGate_Ready_CallerBudgetIsHonouredUpToIndexEnsureTimeout(t *testing.T) {
	var deadline time.Time
	var ok bool
	g := NewIndexGate("t (k)", func(ctx context.Context) error { deadline, ok = ctx.Deadline(); return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	before := time.Now()
	require.NoError(t, g.Ready(ctx))
	require.True(t, ok)
	assert.Greater(t, deadline.Sub(before), IndexProbeTimeout, "a stated budget is not cut to the hot-path bound")
	assert.LessOrEqual(t, deadline.Sub(before), IndexEnsureTimeout+time.Second, "but never exceeds the startup budget")
}

// A gate must not replace a sooner caller deadline with its own budget: startup shares one across both.
func TestIndexGate_Ready_HonoursASoonerCallerDeadline(t *testing.T) {
	var seen time.Time
	var ok bool
	g := NewIndexGate("t (k)", func(ctx context.Context) error {
		seen, ok = ctx.Deadline()
		return nil
	})
	callerDeadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()
	require.NoError(t, g.Ready(ctx))
	require.True(t, ok, "the attempt must carry a deadline at all")
	assert.False(t, seen.After(callerDeadline), "ensure's deadline %v must not exceed the caller's %v", seen, callerDeadline)
}

// A waiter cancelled mid-wait (consumer draining) is released promptly; the shared attempt runs on.
func TestIndexGate_Ready_ReleasesACancelledWaiterWithoutAbortingTheAttempt(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	g := NewIndexGate("t (k)", func(context.Context) error {
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

// A failure the server reported (a conflicting index, duplicate data) holds dependent writes until an
// operator or the owner acts, so it is logged at error level once per probe; an outage is not.
func TestIndexGate_Ready_LogsAServerReportedFailureAtErrorLevel(t *testing.T) {
	t.Run("spec conflict is loud", func(t *testing.T) {
		h := captureLogs(t)
		g := NewIndexGate("things (k)", func(context.Context) error {
			return fmt.Errorf("ensure index on things: %w", ErrIndexSpecConflict)
		})
		require.Error(t, g.Ready(context.Background()))
		errs := h.at(slog.LevelError)
		require.Len(t, errs, 1)
		assert.Equal(t, "things (k)", errs[0].index)
	})
	t.Run("an outage is not", func(t *testing.T) {
		h := captureLogs(t)
		g := NewIndexGate("things (k)", func(context.Context) error {
			return fmt.Errorf("create index: %w", mongo.CommandError{Message: "reset", Labels: []string{"NetworkError"}})
		})
		require.Error(t, g.Ready(context.Background()))
		assert.Empty(t, h.at(slog.LevelError))
	})
}

// The unique-index gate names the collection and key fields so a NAK log says which index is unconfirmed.
func TestNewUniqueIndexGate_NamesTheIndexInErrors(t *testing.T) {
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(100 * time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	g := NewUniqueIndexGate(client.Database("db").Collection("things"),
		bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 1}})
	err = g.Ready(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "confirm unique index things (a,b)")
}
