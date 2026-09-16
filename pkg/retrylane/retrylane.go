package retrylane

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// PublishFunc publishes an escalated message. Injected rather than holding a
// JetStream handle, mirroring pkg/outbox.Publish, so tests capture escalations
// without a real NATS connection. It carries headers, unlike outbox's form —
// the escalation's metadata is entirely in headers.
type PublishFunc func(ctx context.Context, subj string, data []byte, hdr nats.Header, msgID string) error

// Msg widens jsretry.Msg with the accessors escalation needs. jetstream.Msg and
// oteljetstream.Msg both satisfy it.
type Msg interface {
	jsretry.Msg
	Data() []byte
	Subject() string
	Headers() nats.Header
}

// Lane settles messages for one consumer, escalating to RETRY-{SiteID} when the
// in-place fast-rung budget is spent. The zero value is disabled.
type Lane struct {
	// Consumer names this service's durable; it becomes the subject token that
	// routes an escalation back to exactly this consumer.
	Consumer string
	SiteID   string
	Publish  PublishFunc

	// Enabled is the rollback switch. When false, Settle is exactly jsretry.Settle.
	Enabled bool

	// FastSteps is how many deliveries stay in place before escalating; it
	// indexes into the backoff schedule rather than redefining it, so the total
	// retry budget is unchanged and only the ack-pending occupancy moves.
	FastSteps int

	// OnEscalate runs after a successful republish and before the Ack, so the
	// delivery is labelled `escalated` rather than `ack` — an escalation is not
	// a completion, the work moved. Optional; set it per message via
	// WithEscalationHook, never on a shared Lane.
	OnEscalate func()
}

// WithEscalationHook returns a shallow copy of l whose OnEscalate is fn. Call it
// per message: the hook closes over one delivery's metrics recorder, so
// mutating a shared Lane instead would be a data race across the worker's
// message goroutines.
func (l *Lane) WithEscalationHook(fn func()) *Lane {
	copied := *l
	copied.OnEscalate = fn
	return &copied
}

// Settle resolves a processed message:
//   - err == nil            → Ack
//   - permanent (errcode)   → Ack-drop (never escalates: it can never succeed)
//   - within FastSteps      → NakWithDelay, in place
//   - budget spent          → republish to RETRY, then Ack
//
// A failed republish falls back to a Nak: acking a message that was never
// handed on would lose it silently.
func (l *Lane) Settle(ctx context.Context, msg Msg, backoff []time.Duration, err error) {
	if !l.shouldEscalate(msg, err) {
		jsretry.Settle(ctx, msg, backoff, err)
		return
	}
	if escErr := l.escalate(ctx, msg, err); escErr != nil {
		slog.ErrorContext(ctx, "retry-lane escalation failed — falling back to in-place redelivery",
			"consumer", l.Consumer, "error", escErr,
			"request_id", natsutil.RequestIDFromContext(ctx))
		jsretry.Nak(ctx, msg, backoff, "escalation publish failed")
		return
	}
	if l.OnEscalate != nil {
		l.OnEscalate()
	}
	if ackErr := msg.Ack(); ackErr != nil {
		slog.ErrorContext(ctx, "failed to ack escalated message", "error", ackErr,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}

// shouldEscalate reports whether this delivery has spent its in-place budget.
// Every negative answer routes to jsretry, which keeps the existing semantics
// as the single fallback path.
func (l *Lane) shouldEscalate(msg Msg, err error) bool {
	if err == nil || !l.Enabled || l.Publish == nil || l.FastSteps <= 0 {
		return false
	}
	if _, isPermanent := errcode.IsPermanent(err); isPermanent {
		return false
	}
	meta, metaErr := msg.Metadata()
	if metaErr != nil || meta == nil {
		return false
	}
	return meta.NumDelivered > uint64(l.FastSteps)
}

// escalate republishes the message onto the RETRY stream. The body is passed
// through untouched — re-marshalling could change bytes that dedup keys and
// wire-compat tests pin.
func (l *Lane) escalate(ctx context.Context, msg Msg, err error) error {
	meta, metaErr := msg.Metadata()
	if metaErr != nil {
		return fmt.Errorf("read message metadata: %w", metaErr)
	}
	hdr := BuildHeaders(msg.Headers(), meta, msg.Subject(), l.Consumer, ReasonFor(err), time.Now())
	subj := subject.Retry(l.SiteID, l.Consumer, subject.RetryTierSlow)
	msgID := DedupID(meta.Stream, meta.Sequence.Stream, l.Consumer)

	if pubErr := l.Publish(ctx, subj, msg.Data(), hdr, msgID); pubErr != nil {
		return fmt.Errorf("publish to retry lane %s: %w", subj, pubErr)
	}
	return nil
}
