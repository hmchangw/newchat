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
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
)

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
// budget to ~13 minutes across MaxDeliver=6.
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

// backoffFor selects the delay for the next redelivery, indexed by how many
// times the message has already been delivered; the last entry is reused once
// attempts exceed the schedule, and the result is jittered. Falls back to the
// first entry when metadata is unavailable. The uint64 counter is walked rather
// than converted to int, avoiding any narrowing-overflow concern.
func backoffFor(msg Msg, backoff []time.Duration) time.Duration {
	base := backoff[0]
	// NumDelivered is 1 on the first delivery, so the i'th redelivery uses
	// backoff[i]; once attempts exceed the schedule the last entry is reused.
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		for i := uint64(1); i < meta.NumDelivered && i < uint64(len(backoff)); i++ {
			base = backoff[i]
		}
	}
	return jitter(base)
}

// jitter applies AWS "equal jitter": half the base delay plus a random amount up
// to the other half. Decorrelates a fleet that all parked during one outage —
// the server-side BackOff cannot do this, it is a literal duration list.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	half := d / 2
	// #nosec G404 -- retry jitter, not security-sensitive
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
}
