package valkeyutil

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

// captureDegraded points slog at a buffer and pins the clock, returning the
// buffer and a knob to advance time. Each test gets a fresh throttle so the
// tests stay independent of one another's windows.
func captureDegraded(t *testing.T) (*bytes.Buffer, func(time.Duration)) {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	now := time.Unix(0, 0)
	oldNow, oldSeen := degradedNow, degradedSeen
	degradedNow = func() time.Time { return now }
	degradedSeen = map[string]time.Time{}

	t.Cleanup(func() {
		slog.SetDefault(old)
		degradedNow, degradedSeen = oldNow, oldSeen
	})
	return &buf, func(d time.Duration) { now = now.Add(d) }
}

// levels returns the level of each JSON record written so far.
func levels(t *testing.T, buf *bytes.Buffer) []string {
	t.Helper()
	var out []string
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal(line, &rec))
		out = append(out, rec["level"].(string))
	}
	return out
}

// One Warn says Valkey is degraded; the thousandth says nothing new. Tightening
// the read budget to 150ms means a failure now costs milliseconds rather than
// the old multi-second stall, so nothing throttles the log rate any more — an
// outage emits one line per failed call for its whole duration.
func TestLogDegraded_WarnsOnceThenDropsToDebug(t *testing.T) {
	buf, _ := captureDegraded(t)
	ctx := context.Background()
	boom := errors.New("i/o timeout")

	for range 5 {
		LogDegraded(ctx, "room meta L2 read failed", boom, "room_id", "r1")
	}

	assert.Equal(t, []string{"WARN", "DEBUG", "DEBUG", "DEBUG", "DEBUG"}, levels(t, buf))
}

// The throttle must not silence a still-degraded system forever: the window
// reopens so a long outage keeps a periodic heartbeat at Warn.
func TestLogDegraded_WarnsAgainAfterTheWindow(t *testing.T) {
	buf, advance := captureDegraded(t)
	ctx := context.Background()
	boom := errors.New("i/o timeout")

	LogDegraded(ctx, "room meta L2 read failed", boom)
	LogDegraded(ctx, "room meta L2 read failed", boom)
	advance(degradedWindow + time.Second)
	LogDegraded(ctx, "room meta L2 read failed", boom)

	assert.Equal(t, []string{"WARN", "DEBUG", "WARN"}, levels(t, buf))
}

// Throttling is per message, so one noisy path cannot mask the first report of
// a different failure.
func TestLogDegraded_ThrottlesPerMessage(t *testing.T) {
	buf, _ := captureDegraded(t)
	ctx := context.Background()
	boom := errors.New("i/o timeout")

	LogDegraded(ctx, "room meta L2 read failed", boom)
	LogDegraded(ctx, "room meta L2 read failed", boom)
	LogDegraded(ctx, "roomsubcache set failed", boom)

	assert.Equal(t, []string{"WARN", "DEBUG", "WARN"}, levels(t, buf))
}

// Only the level moves. The cause and the caller's own fields are what make
// these logs worth having, so they must survive the demotion.
func TestLogDegraded_KeepsContextAtEveryLevel(t *testing.T) {
	buf, _ := captureDegraded(t)
	ctx := context.Background()
	boom := errors.New("i/o timeout")

	LogDegraded(ctx, "room meta L2 read failed", boom, "room_id", "r1")
	LogDegraded(ctx, "room meta L2 read failed", boom, "room_id", "r2")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.Len(t, lines, 2)
	for i, want := range []string{"r1", "r2"} {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(lines[i], &rec))
		assert.Equal(t, "room meta L2 read failed", rec["msg"])
		assert.Equal(t, want, rec["room_id"], "caller context must survive the level change")
		assert.Equal(t, boom.Error(), rec["error"], "the cause must always be attached")
	}
}
