// Package jsretry centralizes the JetStream per-message ack/retry decision used
// by the worker services: Ack on success, Ack-drop permanent (poison) failures
// so JetStream stops redelivering, and NakWithDelay transient failures on a
// caller-supplied backoff schedule.
//
// Spacing the retries matters: a plain Nak() triggers *instant* redelivery, so
// without a delay a brief Cassandra/Mongo/NATS blip burns through the consumer's
// MaxDeliver in milliseconds and the message is dropped. Centralizing the logic
// keeps every worker consistent and avoids re-deriving the (overflow-safe)
// backoff math at each call site.
//
// Two entry points mirror the errnats.Reply / ReplyQuiet split: use Settle when
// the settle point owns error logging, and SettleQuiet when the error was
// already logged upstream (e.g. by errcode.Classify) and re-logging would
// double-log.
package jsretry

import (
	"context"
	"log/slog"
	"math"
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// minNakDelay floors every nak delay. A delay of 0 serializes as a bare -NAK,
// which redelivers instantly and ignores the consumer's BackOff — the one thing
// this package exists to prevent — so a degenerate schedule still yields a
// positive delay.
const minNakDelay = time.Millisecond

// Msg is the subset of the JetStream message API the settle path needs. Both
// jetstream.Msg and oteljetstream.Msg (which embeds it) satisfy it.
type Msg interface {
	Metadata() (*jetstream.MsgMetadata, error)
	Ack() error
	NakWithDelay(time.Duration) error
}

// Backoff schedules below are passed to Settle/SettleQuiet. The last entry is
// reused once attempts exceed the schedule. Treat them as read-only.
//
// DefaultBackoff suits workers whose retries can be spaced generously because
// the work is not latency-sensitive — enough to ride out a brief Cassandra or
// Mongo outage without exhausting the consumer's MaxDeliver. The first four
// entries are Synadia's published reference schedule; the 10m tail extends the
// budget to 12.6 minutes across MaxDeliver=6.
var DefaultBackoff = []time.Duration{
	1 * time.Second,
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}

// LowLatencyBackoff suits fan-out / delivery workers where the first retry must
// be near-immediate so a sub-second hiccup isn't user-visible, while a genuine
// outage is still spaced out.
var LowLatencyBackoff = []time.Duration{
	200 * time.Millisecond,
	1 * time.Second,
	5 * time.Second,
	30 * time.Second,
}

// Settle resolves a processed message and logs the business error once:
//   - err == nil            → Ack
//   - permanent (errcode)   → Ack (drop poison; it can never succeed) + WARN
//   - otherwise (transient) → NakWithDelay(backoff for this attempt) + ERROR
//
// backoff must be non-empty; the delay is selected by the message's
// NumDelivered and the last entry is reused once attempts exceed the schedule.
func Settle(ctx context.Context, msg Msg, backoff []time.Duration, err error) {
	settle(ctx, msg, backoff, err, true)
}

// SettleQuiet behaves like Settle but does NOT log the business error — use it
// when the caller already logged the failure (e.g. errcode.Classify did). It
// still logs the rare case where the Ack/Nak network call itself fails.
func SettleQuiet(ctx context.Context, msg Msg, backoff []time.Duration, err error) {
	settle(ctx, msg, backoff, err, false)
}

// Nak schedules msg for redelivery after this attempt's jittered backoff delay.
// Use it where a handler settles its own ack/nak instead of returning an error to
// Settle; the caller keeps the business-error log, reason labels the redelivery.
func Nak(ctx context.Context, msg Msg, backoff []time.Duration, reason string) {
	nak(ctx, msg, backoffFor(msg, backoff), reason)
}

// nak is the single NakWithDelay path; a failed nak network call is logged even
// when the caller owns the business-error log.
func nak(ctx context.Context, msg Msg, delay time.Duration, reason string) {
	if err := msg.NakWithDelay(delay); err != nil {
		slog.ErrorContext(ctx, "failed to nak message", "reason", reason,
			"delay", delay.String(), "error", err, "request_id", natsutil.RequestIDFromContext(ctx))
	}
}

func settle(ctx context.Context, msg Msg, backoff []time.Duration, err error, logBusiness bool) {
	if err == nil {
		if ackErr := msg.Ack(); ackErr != nil {
			slog.ErrorContext(ctx, "failed to ack message", "error", ackErr, "request_id", natsutil.RequestIDFromContext(ctx))
		}
		return
	}
	if _, isPermanent := errcode.IsPermanent(err); isPermanent {
		if logBusiness {
			slog.WarnContext(ctx, "permanent message failure — dropping (Ack)", "error", err, "request_id", natsutil.RequestIDFromContext(ctx))
		}
		if ackErr := msg.Ack(); ackErr != nil {
			slog.ErrorContext(ctx, "failed to ack permanent message", "error", ackErr, "request_id", natsutil.RequestIDFromContext(ctx))
		}
		return
	}
	delay := backoffFor(msg, backoff)
	if logBusiness {
		slog.ErrorContext(ctx, "message failed — retrying", "error", err, "delay", delay.String(), "request_id", natsutil.RequestIDFromContext(ctx))
	}
	nak(ctx, msg, delay, "transient failure")
}

// delayAt returns the schedule entry that governs the i'th (0-based) delay a
// message waits out, reusing the last entry once i runs past the schedule.
// backoffFor and ElapsedFor both index the schedule through here, so the delay
// actually handed to NakWithDelay and the accumulated wait a give-up deadline is
// measured against cannot drift apart. backoff must be non-empty.
func delayAt(backoff []time.Duration, i uint64) time.Duration {
	if i >= uint64(len(backoff)) {
		return backoff[len(backoff)-1]
	}
	return backoff[i]
}

// maxElapsed is the saturation ceiling for ElapsedFor. Returning it beats
// wrapping: a negative duration would read as "barely retried" to a caller
// deciding whether a message has run out of retry budget.
const maxElapsed = time.Duration(math.MaxInt64)

// ElapsedFor reports how long a message has already spent waiting on this
// backoff schedule, given its NumDelivered. It is the schedule's own arithmetic,
// exposed so a caller can express a give-up deadline in time units instead of a
// delivery count that silently depends on the schedule's shape.
//
// A message on its numDelivered'th attempt has waited out the delays served
// after deliveries 1..numDelivered-1 — the same delays backoffFor hands to
// NakWithDelay, so the two never drift. Only time actually spent retrying is
// counted; the message's age on the stream is deliberately not part of it,
// because a replayed backlog is already hours old before its first retry.
func ElapsedFor(backoff []time.Duration, numDelivered uint64) time.Duration {
	if len(backoff) == 0 || numDelivered <= 1 {
		return 0
	}
	served := numDelivered - 1
	scheduled := served
	if scheduled > uint64(len(backoff)) {
		scheduled = uint64(len(backoff))
	}
	var total time.Duration
	for i := uint64(0); i < scheduled; i++ {
		total += delayAt(backoff, i)
	}
	repeats := served - scheduled
	if repeats == 0 {
		return total
	}
	// Reached only once the schedule is exhausted: repeats > 0 requires
	// served > len(backoff), so every remaining wait is the last entry.
	tail := backoff[len(backoff)-1]
	if tail <= 0 {
		return total
	}
	headroom := (maxElapsed - total) / tail
	// #nosec G115 -- headroom is a non-negative int64 (total <= maxElapsed, tail > 0), so the conversion is exact
	if repeats > uint64(headroom) {
		return maxElapsed
	}
	// #nosec G115 -- repeats <= headroom, so the product stays inside a time.Duration
	return total + time.Duration(repeats)*tail
}

// backoffFor selects the delay for the next redelivery, indexed by how many
// times the message has already been delivered; the last entry is reused once
// attempts exceed the schedule (see delayAt), and the result is jittered. Falls
// back to the first entry when metadata is unavailable or carries no delivery
// count.
func backoffFor(msg Msg, backoff []time.Duration) time.Duration {
	if len(backoff) == 0 {
		return minNakDelay
	}
	meta, err := msg.Metadata()
	if err != nil || meta == nil || meta.NumDelivered == 0 {
		return jitter(backoff[0])
	}
	// NumDelivered is 1 on the first delivery, so the delay served after it is
	// the schedule's first entry.
	return jitter(delayAt(backoff, meta.NumDelivered-1))
}

// jitter applies AWS "equal jitter": half the base delay plus a random amount up
// to the other half. Decorrelates a fleet that all parked during one outage —
// the server-side BackOff cannot do this, it is a literal duration list.
//
// ElapsedFor deliberately sums un-jittered schedule entries: it is the budget a
// give-up deadline is measured against, and it must be reproducible rather than
// depend on the draws this message happened to get. Real elapsed time therefore
// runs ~0.75x of what ElapsedFor reports, so a duration-expressed deadline fires
// somewhat sooner in wall-clock terms than its nominal value.
func jitter(d time.Duration) time.Duration {
	if d <= minNakDelay {
		return minNakDelay
	}
	half := d / 2
	// #nosec G404 -- retry jitter, not security-sensitive
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
}
