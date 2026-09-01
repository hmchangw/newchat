package main

import (
	"sync"
	"time"
)

// dropLimiter bounds how many messages this pod may destroy per window. It is the
// brake for the classifier's one adjacent risk: `Invalid` (0x2200) is request class
// because it is deterministic per message, but Cassandra also returns it for
// `unconfigured table`, `Undefined column name` and a failed re-prepare after a
// column is dropped or retyped — faults that hit EVERY write, exactly like the
// ConfigError the classifier refuses to drop on. Without a rate cap, such a wave
// destroys the whole feed once the retry window elapses, and the only mitigation is a
// human noticing the class-labelled metric and flipping the kill switch inside the
// hour. At 02:00 on a weekend nobody does.
//
// A rate cap is preferred over a "what fraction of failures are request class?" test
// because it needs no threshold tuning and it bounds loss deterministically regardless
// of WHY the wave is happening: a genuine burst of hundreds of distinct bad rows is
// bounded too, where a ratio test would wave it through.
//
// Refusing a drop is not refusing it forever: the caller NAKs, the message returns on
// the backoff schedule, and it may drop in a later window. The cap spreads destruction
// out and gives an operator time to act; it does not create an unbounded retry loop.
//
// Deliberately per-pod and in-process: no shared state, no round trip on the failure
// path. N pods therefore allow N × the cap in aggregate, which is the accepted
// trade-off for a brake that cannot itself fail.
//
// Fixed window, not a token bucket: an interval straddling a window boundary can pass
// up to 2×max−1 drops (max at the tail of one window, max at the head of the next),
// which a token bucket of equal rate would not do. The aggregate bound is unaffected —
// windows are disjoint and independently capped, so total drops over a duration D stay
// ≤ max × ceil(D/window) per pod — and the burst is bounded and small at these
// settings, so the simpler counter wins.
type dropLimiter struct {
	max    uint64
	window time.Duration
	now    func() time.Time

	mu sync.Mutex
	// windowStart is the start of the current counting window; its zero value makes
	// the first Allow open a fresh window.
	windowStart time.Time
	count       uint64
}

// newDropLimiter builds a fixed-window counter allowing max drops per window.
// clock may be nil (real UTC time); tests inject one so a window roll needs no sleep.
func newDropLimiter(maxPerWindow uint64, window time.Duration, clock func() time.Time) *dropLimiter {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &dropLimiter{max: maxPerWindow, window: window, now: clock}
}

// Allow reports whether a drop may proceed, consuming a slot when it may.
//
// A nil limiter answers false. That is the fail-safe direction: a policy assembled
// without a brake must cost retries, never messages.
//
// The slot is consumed before the Ack, so a drop whose Ack then fails has spent
// budget on a message that is still alive. That errs toward fewer drops, which is the
// side to err on.
func (l *dropLimiter) Allow() bool {
	if l == nil {
		return false
	}
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.windowStart) >= l.window {
		l.windowStart = now
		l.count = 0
	}
	if l.count >= l.max {
		return false
	}
	l.count++
	return true
}
