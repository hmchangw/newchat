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

func TestEnsureIndexesBestEffort_SuccessIsQuiet(t *testing.T) {
	buf := captureSlog(t)
	called := false

	EnsureIndexesBestEffort(context.Background(), "test-store", func(context.Context) error {
		called = true
		return nil
	})

	assert.True(t, called, "the index step must actually run")
	assert.Nil(t, warnLine(t, buf), "success must not warn")
}

func TestEnsureIndexesBestEffort_FailureWarnsAndContinues(t *testing.T) {
	buf := captureSlog(t)

	// Must not panic and must not exit — returning normally is the contract.
	EnsureIndexesBestEffort(context.Background(), "user-service subscriptions", func(context.Context) error {
		return errors.New("connection refused")
	})

	warn := warnLine(t, buf)
	require.NotNil(t, warn, "failure must log at WARN, got %v", decodeLogLines(t, buf))
	assert.Equal(t, "user-service subscriptions", warn["indexes"],
		"the failing step must be identifiable for alerting")
	assert.Contains(t, warn["error"], "connection refused")
}

func TestEnsureIndexesBestEffort_PassesContextThrough(t *testing.T) {
	captureSlog(t)
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")

	var got any
	EnsureIndexesBestEffort(ctx, "test-store", func(c context.Context) error {
		got = c.Value(ctxKey{})
		return nil
	})

	assert.Equal(t, "v", got, "the caller's context must reach the index step")
}

func TestEnsureIndexesBestEffort_NilStepIsNoOp(t *testing.T) {
	buf := captureSlog(t)
	assert.NotPanics(t, func() {
		EnsureIndexesBestEffort(context.Background(), "test-store", nil)
	})
	assert.Nil(t, warnLine(t, buf), "a nil step is not a failure")
}

// TestEnsureIndexesBestEffort_BoundsTheStep pins the timeout the helper owns:
// during an outage each step must give up well before the driver's 30s
// server-selection timeout, or a service with several stores spends minutes
// booting.
func TestEnsureIndexesBestEffort_BoundsTheStep(t *testing.T) {
	captureSlog(t)

	var deadline time.Time
	var ok bool
	EnsureIndexesBestEffort(context.Background(), "test-store", func(c context.Context) error {
		deadline, ok = c.Deadline()
		return nil
	})

	require.True(t, ok, "the step must run under a deadline even when the caller sets none")
	assert.LessOrEqual(t, time.Until(deadline), EnsureIndexesTimeout)
}

// TestEnsureIndexesBestEffort_KeepsShorterCallerDeadline checks the helper
// bounds without loosening: a caller that wants a tighter budget keeps it.
func TestEnsureIndexesBestEffort_KeepsShorterCallerDeadline(t *testing.T) {
	captureSlog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var remaining time.Duration
	EnsureIndexesBestEffort(ctx, "test-store", func(c context.Context) error {
		d, _ := c.Deadline()
		remaining = time.Until(d)
		return nil
	})

	assert.LessOrEqual(t, remaining, 50*time.Millisecond, "the helper must not extend a tighter caller deadline")
}

// TestEnsureIndexesBestEffort_CancelledContextStillWarns covers the outage
// shape where the caller's startup context is already done.
func TestEnsureIndexesBestEffort_CancelledContextStillWarns(t *testing.T) {
	buf := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	EnsureIndexesBestEffort(ctx, "test-store", func(c context.Context) error {
		return c.Err()
	})

	assert.NotNil(t, warnLine(t, buf), "a cancelled-context failure must still warn")
}
