package mongoutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// captureSlog swaps the default logger for one writing JSON into a buffer and
// restores it on cleanup, so tests can assert on level and attributes.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func decodeLogLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for dec.More() {
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		out = append(out, m)
	}
	return out
}

// warnLine returns the last WARN line captured in buf, or nil if there is none.
func warnLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var warn map[string]any
	for _, line := range decodeLogLines(t, buf) {
		if line["level"] == "WARN" {
			warn = line
		}
	}
	return warn
}

func TestEnsureIndexes_SuccessIsQuiet(t *testing.T) {
	buf := captureSlog(t)
	called := false

	err := EnsureIndexes(context.Background(), Step("test-store", func(context.Context) error {
		called = true
		return nil
	}))

	require.NoError(t, err)
	assert.True(t, called, "the index step must actually run")
	assert.Nil(t, warnLine(t, buf), "success must not warn")
}

// TestEnsureIndexes_TransientFailureWarnsAndContinues is the case the helper
// exists for: MongoDB unreachable must not stop a service booting.
func TestEnsureIndexes_TransientFailureWarnsAndContinues(t *testing.T) {
	buf := captureSlog(t)

	err := EnsureIndexes(context.Background(), Step("user-service subscriptions", func(context.Context) error {
		return errors.New("connection refused")
	}))

	require.NoError(t, err, "a transient failure must not be reported to the caller")
	warn := warnLine(t, buf)
	require.NotNil(t, warn, "failure must log at WARN, got %v", decodeLogLines(t, buf))
	assert.Equal(t, "user-service subscriptions", warn["indexes"],
		"the failing step must be identifiable for alerting")
	assert.Contains(t, warn["error"], "connection refused")
}

// TestEnsureIndexes_PermanentFailureIsReturned pins the fail-fast half: no
// restart clears an index conflict or pre-existing duplicates, and the stores
// raising them carry operator remediation text worth surfacing.
func TestEnsureIndexes_PermanentFailureIsReturned(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"duplicate key blocks a unique index", mongo.WriteException{
			WriteErrors: []mongo.WriteError{{Code: 11000, Message: "E11000 duplicate key error"}},
		}},
		{"index options conflict", mongo.CommandError{Code: 85, Name: "IndexOptionsConflict"}},
		{"index key specs conflict", mongo.CommandError{Code: 86, Name: "IndexKeySpecsConflict"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureSlog(t)

			err := EnsureIndexes(context.Background(), Step("user-service users", func(context.Context) error {
				return tc.err
			}))

			require.Error(t, err, "a permanent failure must reach the caller so it can exit")
			assert.Contains(t, err.Error(), "user-service users", "the failing step must be identifiable")
			assert.Nil(t, warnLine(t, buf), "a permanent failure is returned, not downgraded to a warning")
		})
	}
}

// TestEnsureIndexes_RunsStepsConcurrently guards the property that keeps boot
// time flat: a service with several stores must not pay the timeout once per
// store while MongoDB is unreachable.
func TestEnsureIndexes_RunsStepsConcurrently(t *testing.T) {
	captureSlog(t)
	const steps = 4

	// A step reports itself in flight, then parks until every sibling has done
	// the same. Parking selects on the context too, so a serial implementation
	// unblocks on the shared budget instead of wedging the test.
	started := make(chan struct{}, steps)
	release := make(chan struct{})
	all := make([]IndexStep, 0, steps)
	for range steps {
		all = append(all, Step("store", func(c context.Context) error {
			started <- struct{}{}
			select {
			case <-release:
			case <-c.Done():
				return c.Err()
			}
			return nil
		}))
	}

	done := make(chan error, 1)
	go func() { done <- EnsureIndexes(context.Background(), all...) }()

	// Serial steps only ever report one in flight, so this waits out its bound
	// and fails rather than blocking until the test binary's own timeout.
	inFlight := 0
	deadline := time.After(5 * time.Second)
	for inFlight < steps {
		select {
		case <-started:
			inFlight++
		case <-deadline:
			close(release)
			<-done
			t.Fatalf("only %d of %d steps ran concurrently", inFlight, steps)
		}
	}

	close(release)
	require.NoError(t, <-done)
}

func TestEnsureIndexes_NilStepIsSkipped(t *testing.T) {
	buf := captureSlog(t)
	assert.NotPanics(t, func() {
		require.NoError(t, EnsureIndexes(context.Background(), Step("test-store", nil)))
	})
	assert.Nil(t, warnLine(t, buf), "a nil step is not a failure")
}

func TestEnsureIndexes_NoStepsIsNoOp(t *testing.T) {
	captureSlog(t)
	require.NoError(t, EnsureIndexes(context.Background()))
}

// TestEnsureIndexes_BoundsTheSteps pins the timeout the helper owns: during an
// outage it must give up on its own, since most callers pass a startup context
// with no deadline.
func TestEnsureIndexes_BoundsTheSteps(t *testing.T) {
	captureSlog(t)

	var deadline time.Time
	var ok bool
	require.NoError(t, EnsureIndexes(context.Background(), Step("test-store", func(c context.Context) error {
		deadline, ok = c.Deadline()
		return nil
	})))

	require.True(t, ok, "the steps must run under a deadline even when the caller sets none")
	assert.LessOrEqual(t, time.Until(deadline), EnsureIndexesTimeout)
}

// TestEnsureIndexes_KeepsShorterCallerDeadline checks the helper bounds without
// loosening: a caller that wants a tighter budget keeps it.
func TestEnsureIndexes_KeepsShorterCallerDeadline(t *testing.T) {
	captureSlog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var remaining time.Duration
	require.NoError(t, EnsureIndexes(ctx, Step("test-store", func(c context.Context) error {
		d, _ := c.Deadline()
		remaining = time.Until(d)
		return nil
	})))

	assert.LessOrEqual(t, remaining, 50*time.Millisecond, "the helper must not extend a tighter caller deadline")
}

func TestEnsureIndexes_PassesContextThrough(t *testing.T) {
	captureSlog(t)
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")

	var got any
	require.NoError(t, EnsureIndexes(ctx, Step("test-store", func(c context.Context) error {
		got = c.Value(ctxKey{})
		return nil
	})))

	assert.Equal(t, "v", got, "the caller's context must reach the index step")
}

// TestEnsureIndexes_CancelledContextIsTransient covers the outage shape where
// the caller's startup context is already done: still a warning, not a
// fail-fast, since a restart can clear it.
func TestEnsureIndexes_CancelledContextIsTransient(t *testing.T) {
	buf := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := EnsureIndexes(ctx, Step("test-store", func(c context.Context) error { return c.Err() }))

	require.NoError(t, err)
	assert.NotNil(t, warnLine(t, buf), "a cancelled-context failure must still warn")
}
