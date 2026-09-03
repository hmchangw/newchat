package loopguard

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errLoopDied = errors.New("iterator closed unexpectedly")

// A guard whose loop has not exited must report ready, or every pod would
// start its life failing readiness.
func TestGuard_ReadyBeforeStop(t *testing.T) {
	g := New("consume-loop", nil)
	assert.NoError(t, g.Check().Probe(context.Background()))
}

// Once the loop has exited the pod cannot do its job, so readiness must fail
// and carry the reason, whatever that reason is.
func TestGuard_StoppedFailsReadinessWithCause(t *testing.T) {
	g := New("consume-loop", nil)
	g.Stopped(errLoopDied)

	err := g.Check().Probe(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, errLoopDied)
}

// The readiness row is named by the caller so a service with several lanes
// (outbox-worker's per-peer consumers) can tell which one died.
func TestGuard_CheckIsNamedByCaller(t *testing.T) {
	g := New("outbox-ordered-siteB", nil)
	assert.Equal(t, "outbox-ordered-siteB", g.Check().Name)
}

// Failing readiness is not recovery for a queue worker — nothing routes traffic
// to it and liveness always answers 200 — so an unexpected exit must ask the
// process to terminate and let the supervisor rebuild the iterator.
func TestGuard_UnexpectedStopFiresHook(t *testing.T) {
	fired := make(chan struct{}, 1)
	g := New("consume-loop", func() { fired <- struct{}{} })

	g.Stopped(errLoopDied)

	select {
	case <-fired:
	default:
		t.Fatal("an unexpected stop must fire the restart hook")
	}
}

// The deliberate iter.Stop() during shutdown must not be mistaken for a
// failure — the process is already going down, and signalling it again would
// be noise. Readiness still fails so the pod drains from the load balancer.
func TestGuard_StopAfterBeginShutdownDoesNotFireHook(t *testing.T) {
	fired := make(chan struct{}, 1)
	g := New("consume-loop", func() { fired <- struct{}{} })
	g.BeginShutdown()

	g.Stopped(errLoopDied)

	select {
	case <-fired:
		t.Fatal("a shutdown-initiated stop must not fire the restart hook")
	default:
	}
	assert.Error(t, g.Check().Probe(context.Background()),
		"readiness must still fail while shutting down, so the pod drains")
	assert.True(t, g.Stopping())
}

// A loop with two observers (the loop's own exit and a Closed() watcher) may
// report the same death twice; the process must be signalled once.
func TestGuard_HookFiresAtMostOnce(t *testing.T) {
	var mu sync.Mutex
	fired := 0
	g := New("consume-loop", func() { mu.Lock(); fired++; mu.Unlock() })

	g.Stopped(errLoopDied)
	g.Stopped(errors.New("closed watcher"))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, fired)
}

// A nil hook is the "readiness only" mode used by tests; it must not panic.
func TestGuard_NilHookIsSafe(t *testing.T) {
	g := New("consume-loop", nil)
	require.NotPanics(t, func() { g.Stopped(errLoopDied) })
}

// A graceful stop is the normal end of every pod's life, and reporting it at
// ERROR trips error-rate alerting on every deploy.
func TestGuard_GracefulStopIsLoggedAtInfo(t *testing.T) {
	logs := captureLogs(t)
	g := New("consume-loop", func() {})
	g.BeginShutdown()

	g.Stopped(errLoopDied)

	lvl, found := logs.levelFor("consume loop stopped")
	require.True(t, found, "the stop must still be reported")
	assert.Equal(t, slog.LevelInfo, lvl)
}

// The unexpected stop keeps ERROR: nothing else observes the loop dying, and
// this is the line that says the worker has silently stopped doing its job.
func TestGuard_UnexpectedStopIsLoggedAtError(t *testing.T) {
	logs := captureLogs(t)
	g := New("consume-loop", func() {})

	g.Stopped(errLoopDied)

	lvl, found := logs.levelFor("consume loop stopped")
	require.True(t, found)
	assert.Equal(t, slog.LevelError, lvl)
}

// A callback-style consumer (jetstream.Consume) exposes its death only as a
// closed channel; the guard must turn that into the same Stopped path.
func TestGuard_WatchClosedReportsWhenChannelCloses(t *testing.T) {
	fired := make(chan struct{}, 1)
	g := New("consume-loop", func() { fired <- struct{}{} })
	closed := make(chan struct{})

	g.WatchClosed(closed)
	assert.NoError(t, g.Check().Probe(context.Background()), "an open channel is a live loop")

	close(closed)
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("closing the watched channel must report the loop stopped")
	}
	assert.Error(t, g.Check().Probe(context.Background()))
}

// SelfShutdown must raise SIGTERM on this very process so shutdown.WaitOn runs
// the ordinary graceful teardown. The test arms a handler first, exactly as a
// worker must, so the signal is observed rather than fatal.
func TestSelfShutdown_RaisesSIGTERMOnOwnProcess(t *testing.T) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	defer signal.Stop(sig)

	SelfShutdown()

	select {
	case got := <-sig:
		assert.Equal(t, syscall.SIGTERM, got)
	case <-time.After(2 * time.Second):
		t.Fatal("SelfShutdown must deliver SIGTERM to the current process")
	}
}

// capturingHandler records the level and message of everything logged, so a
// test can assert HOW a stop was reported rather than merely that it was.
type capturingHandler struct {
	mu    sync.Mutex
	lines []loggedLine
}

type loggedLine struct {
	level slog.Level
	msg   string
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

// The by-value slog.Record is fixed by the slog.Handler interface.
//
//nolint:gocritic // hugeParam: signature is dictated by slog.Handler
func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, loggedLine{level: r.Level, msg: r.Message})
	return nil
}
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) levelFor(substr string) (slog.Level, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, l := range h.lines {
		if strings.Contains(l.msg, substr) {
			return l.level, true
		}
	}
	return 0, false
}

// captureLogs installs a recording default logger for one test and restores the
// previous one afterwards, so the level assertion cannot leak between tests.
func captureLogs(t *testing.T) *capturingHandler {
	t.Helper()
	h := &capturingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}
