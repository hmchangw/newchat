package flushloop

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// call is what one flush looked like from the inside. Err and Deadline are
// snapshotted at flush time, not read back afterwards: Run cancels each flush's
// context as soon as the flush returns, so a context inspected after the loop
// has finished always reads as cancelled and would make the assertion vacuous.
type call struct {
	err         error
	hasDeadline bool
	ctx         context.Context // values only; they do not change on cancel
}

// recorder records every flush without blocking the loop: a channel deep enough
// to matter would still stall the ticker if the test drained it slowly, and a
// stalled ticker cannot observe cancellation.
type recorder struct {
	mu        sync.Mutex
	calls_    []call
	err       error
	panicOnce bool

	firstOnce sync.Once
	first     chan struct{}
}

func newRecorder() *recorder { return &recorder{first: make(chan struct{})} }

func (r *recorder) flush(ctx context.Context) error {
	_, hasDeadline := ctx.Deadline()
	r.mu.Lock()
	r.calls_ = append(r.calls_, call{err: ctx.Err(), hasDeadline: hasDeadline, ctx: ctx})
	err, shouldPanic := r.err, r.panicOnce
	r.panicOnce = false
	r.mu.Unlock()
	r.firstOnce.Do(func() { close(r.first) })
	if shouldPanic {
		panic("boom")
	}
	return err
}

func (r *recorder) calls() []call {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]call(nil), r.calls_...)
}

func (r *recorder) last() call {
	c := r.calls()
	return c[len(c)-1]
}

// runUntilCancel starts the loop, waits for the first tick, cancels, and waits
// for the loop to return. Synchronised on channels rather than sleeps.
func runUntilCancel(t *testing.T, cfg Config, r *recorder) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, cfg, r.flush)
	}()
	select {
	case <-r.first:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the loop never flushed")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return after cancellation")
	}
}

func testConfig() Config {
	return Config{Name: "test", Interval: time.Millisecond, PerFlush: time.Second, FinalTimeout: time.Second}
}

func TestRun_FlushesOnTickAndOnceMoreAfterCancellation(t *testing.T) {
	r := newRecorder()

	runUntilCancel(t, testConfig(), r)

	assert.GreaterOrEqual(t, len(r.calls()), 2, "at least one tick plus the final flush")
}

// The whole reason this loop exists: a batch buffered when shutdown begins must
// still land. Running the final flush on the cancelled context would abort it.
func TestRun_FinalFlushRunsOnALiveContext(t *testing.T) {
	r := newRecorder()

	runUntilCancel(t, testConfig(), r)

	assert.NoError(t, r.last().err, "the final flush must not inherit the cancellation")
}

// WithoutCancel, not Background: the final flush stays inside the trace and
// keeps the request-scoped values every other flush carried.
func TestRun_FinalFlushKeepsParentValues(t *testing.T) {
	type key struct{}
	r := newRecorder()
	parent := context.WithValue(context.Background(), key{}, "carried")
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, testConfig(), r.flush)
	}()
	<-r.first
	cancel()
	<-done

	assert.Equal(t, "carried", r.last().ctx.Value(key{}), "the final flush must stay traceable")
}

// An unbounded final flush can outlast the process's whole shutdown budget.
func TestRun_FinalFlushIsBounded(t *testing.T) {
	r := newRecorder()

	runUntilCancel(t, testConfig(), r)

	assert.True(t, r.last().hasDeadline, "the final flush must carry a deadline")
}

func TestRun_PeriodicFlushIsBoundedByPerFlush(t *testing.T) {
	r := newRecorder()

	runUntilCancel(t, testConfig(), r)

	assert.True(t, r.calls()[0].hasDeadline, "a periodic flush must carry the per-flush deadline")
}

// A zero PerFlush leaves the periodic flush on the caller's own context, for a
// writer that already bounds itself.
func TestRun_ZeroPerFlushLeavesTheContextUnbounded(t *testing.T) {
	cfg := testConfig()
	cfg.PerFlush = 0
	r := newRecorder()

	runUntilCancel(t, cfg, r)

	assert.False(t, r.calls()[0].hasDeadline, "a zero PerFlush must not impose a deadline")
}

// The loop drives user-derived data through a bulk write; a panic there must not
// take the process down, and the loop must keep ticking afterwards.
func TestRun_PanicDoesNotStopTheLoop(t *testing.T) {
	r := newRecorder()
	r.panicOnce = true

	assert.NotPanics(t, func() { runUntilCancel(t, testConfig(), r) })

	assert.GreaterOrEqual(t, len(r.calls()), 2, "the loop must keep flushing after a panic")
}

func TestRun_ErrorDoesNotStopTheLoop(t *testing.T) {
	r := newRecorder()
	r.err = errors.New("write failed")

	runUntilCancel(t, testConfig(), r)

	assert.GreaterOrEqual(t, len(r.calls()), 2)
}

// A caller with its own scoped logger must not have its failures land on the
// default one, where they lose the collection/site fields it carries.
func TestRun_UsesTheConfiguredLogger(t *testing.T) {
	var buf bytes.Buffer
	cfg := testConfig()
	cfg.Logger = slog.New(slog.NewJSONHandler(&buf, nil))
	r := newRecorder()
	r.err = errors.New("write failed")

	runUntilCancel(t, cfg, r)

	assert.Contains(t, buf.String(), "write failed", "the failure must reach the supplied logger")
}

// A zero FinalTimeout must take the package default, not build an
// already-expired context for the one drain this package exists to land.
func TestRun_ZeroFinalTimeoutTakesTheDefault(t *testing.T) {
	cfg := testConfig()
	cfg.FinalTimeout = 0
	r := newRecorder()

	runUntilCancel(t, cfg, r)

	assert.NoError(t, r.last().err, "the final flush must not start already expired")
	assert.True(t, r.last().hasDeadline, "it must still be bounded")
}

// A non-positive interval would panic time.NewTicker and kill the process from
// inside a goroutine. Callers validate at startup; this is the backstop.
func TestRun_NonPositiveIntervalReturnsWithoutFlushing(t *testing.T) {
	cfg := testConfig()
	cfg.Interval = 0
	r := newRecorder()
	done := make(chan struct{})

	go func() {
		defer close(done)
		assert.NotPanics(t, func() { Run(context.Background(), cfg, r.flush) })
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return on a non-positive interval")
	}
	assert.Empty(t, r.calls())
}
