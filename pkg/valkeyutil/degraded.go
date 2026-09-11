package valkeyutil

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// degradedWindow is how long one message stays demoted after its first Warn.
// Long enough that a sustained outage is a periodic heartbeat rather than a
// stream, short enough that the heartbeat still shows the outage is ongoing.
const degradedWindow = 30 * time.Second

// Swappable for tests: the throttle is package state, so a test that could not
// pin the clock or reset the map would depend on its neighbours' timing.
var (
	degradedNow  = time.Now
	degradedMu   sync.Mutex
	degradedSeen = map[string]time.Time{}
)

// LogDegraded reports a Valkey call that fell back to its backing store,
// attaching err and the caller's own context args.
//
// It is the single place that decides how loud to be while Valkey is down. The
// first report of a message is news and logs at Warn; repeats inside
// degradedWindow drop to Debug, still available on demand without drowning the
// log. This matters more since the timeout profiles landed: a failure used to
// cost seconds, which throttled the log rate by itself, and now costs
// milliseconds — so an outage would otherwise emit one line per failed call,
// per room, for its whole duration.
//
// Callers keep their own message and fields, so the diagnostic context that
// makes these logs worth having survives; only the level moves. That is why
// this is a log helper rather than logging inside the client, which would
// flatten every distinct message into one generic line.
//
// Throttle keys are the caller's message strings — a small fixed set of
// compile-time constants and tier labels — so the map does not grow with
// traffic.
func LogDegraded(ctx context.Context, msg string, err error, args ...any) {
	level := slog.LevelDebug
	if firstInDegradedWindow(msg) {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, msg, append(args, "error", err)...)
}

// firstInDegradedWindow reports whether msg has gone unreported long enough to
// deserve a Warn, and records this report when it has.
func firstInDegradedWindow(msg string) bool {
	now := degradedNow()

	degradedMu.Lock()
	defer degradedMu.Unlock()

	if last, seen := degradedSeen[msg]; seen && now.Sub(last) < degradedWindow {
		return false
	}
	degradedSeen[msg] = now
	return true
}
