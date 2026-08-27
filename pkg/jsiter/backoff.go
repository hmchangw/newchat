package jsiter

import (
	"context"
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used
	"time"
)

// RebuildBackoff spaces rebuild attempts. The last entry repeats forever, so a
// peer site that stays down is retried every 30s rather than given up on.
var RebuildBackoff = []time.Duration{
	100 * time.Millisecond,
	1 * time.Second,
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
}

// TransientEscalation caps consecutive recoverable errors before the consumer
// is rebuilt anyway: a stall that never delivers looks just like heartbeat
// misses, and retrying forever against it is the silent stall in a new shape.
const TransientEscalation = 3

// EscalationWindow bounds a run of failures. Failures inside it are one flap
// and escalate together; one arriving after it is unrelated and starts the
// schedule over.
const EscalationWindow = 2 * time.Minute

// SeedAttempt returns the RebuildBackoff index a recovery run starting at now
// should use, given the running attempt count and when the last run started.
//
// The run is bounded by time, not by progress, because no consumer offers a
// trustworthy "healthy again" signal: a replacement can deliver one queued
// message, or answer one empty poll, and die immediately after. Treating either
// as proof pins every later rebuild at the first step, which is the
// control-plane storm this backoff exists to avoid.
func SeedAttempt(attempt int, last, now time.Time) int {
	if !last.IsZero() && now.Sub(last) > EscalationWindow {
		return 0
	}
	return attempt
}

// BackoffStep returns the RebuildBackoff entry for attempt, repeating the last
// one once attempts run past the schedule.
func BackoffStep(attempt int) time.Duration {
	if attempt >= len(RebuildBackoff) {
		attempt = len(RebuildBackoff) - 1
	}
	return RebuildBackoff[attempt]
}

// SleepUntil parks for a jittered d, returning false when done closes or ctx is
// cancelled. It is the one backoff wait every recovering consumer should use.
func SleepUntil(ctx context.Context, done <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(Jitter(d))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-done:
		return false
	case <-ctx.Done():
		return false
	}
}

// Jitter applies AWS "equal jitter": half the step plus a random amount up to
// the other half, so a fleet that lost one gateway does not retry in lockstep.
func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	// #nosec G404 -- rebuild jitter, not security-sensitive
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
