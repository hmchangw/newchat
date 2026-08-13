package mongoutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

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

func TestEnsureIndexesBestEffort_SuccessIsQuiet(t *testing.T) {
	buf := captureSlog(t)
	called := false

	EnsureIndexesBestEffort(context.Background(), "test-store", func(context.Context) error {
		called = true
		return nil
	})

	assert.True(t, called, "the index step must actually run")
	for _, line := range decodeLogLines(t, buf) {
		assert.NotEqual(t, "WARN", line["level"], "success must not warn: %v", line)
	}
}

func TestEnsureIndexesBestEffort_FailureWarnsAndContinues(t *testing.T) {
	buf := captureSlog(t)

	// Must not panic and must not exit — returning normally is the contract.
	EnsureIndexesBestEffort(context.Background(), "user-service subscriptions", func(context.Context) error {
		return errors.New("connection refused")
	})

	lines := decodeLogLines(t, buf)
	require.NotEmpty(t, lines, "failure must be logged")

	var warn map[string]any
	for _, line := range lines {
		if line["level"] == "WARN" {
			warn = line
		}
	}
	require.NotNil(t, warn, "failure must log at WARN, got %v", lines)
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
	for _, line := range decodeLogLines(t, buf) {
		assert.NotEqual(t, "WARN", line["level"], "a nil step is not a failure: %v", line)
	}
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

	var sawWarn bool
	for _, line := range decodeLogLines(t, buf) {
		if line["level"] == "WARN" {
			sawWarn = true
		}
	}
	assert.True(t, sawWarn, "a cancelled-context failure must still warn")
}
