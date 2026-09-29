package retrylane

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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

// MsgPublisher is the one JetStream method escalation needs. Both
// jetstream.JetStream and o11y's wrapper satisfy it, so the constructor below
// takes either without this package importing the o11y nats wrapper.
type MsgPublisher interface {
	PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// JetStreamPublish is the PublishFunc every adopter wants: a plain JetStream
// publish carrying the escalation's headers, with the deterministic msgID as
// Nats-Msg-Id so a crash between publish and Ack re-escalates as a no-op inside
// the stream's Duplicates window.
func JetStreamPublish(js MsgPublisher) PublishFunc {
	return func(ctx context.Context, subj string, data []byte, hdr nats.Header, msgID string) error {
		_, err := js.PublishMsg(ctx, &nats.Msg{Subject: subj, Data: data, Header: hdr},
			jetstream.WithMsgID(msgID))
		return err
	}
}

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
// backoff is the FULL schedule, never a pre-truncated prefix. Callers must not
// slice it to FastSteps: jsretry.backoffFor walks only as far as this delivery,
// so an in-place nak inside the fast budget already draws from the fast rungs
// alone — truncating buys nothing there, and it silently shortens the one path
// that does read past them. A failed republish falls back to a Nak (acking a
// message that was never handed on would lose it silently) and must ride the
// schedule the message would have had without the lane; on a prefix,
// backoffFor would instead reuse the last fast rung for every later delivery
// and burn the consumer's MaxDeliver long before its outage budget intends.
func (l *Lane) Settle(ctx context.Context, msg Msg, backoff []time.Duration, err error) {
	l.settle(ctx, msg, backoff, err, jsretry.Settle)
}

// SettleQuiet is Settle without jsretry's own failure log, for a caller that has
// already logged the failure itself. message-worker's drop suppression is the
// one such caller: it logs a cql_code-labelled warning and deliberately keeps the
// raw CQL error out of the log, because an "Invalid" message echoes the offending
// value — untrusted message content. Letting jsretry log it would put that text
// back, at the volume a schema-drift wave re-evaluates at.
//
// The escalation log is NOT suppressed: it is a state change on-call acts on,
// built from bounded fields, not a restatement of the error.
func (l *Lane) SettleQuiet(ctx context.Context, msg Msg, backoff []time.Duration, err error) {
	l.settle(ctx, msg, backoff, err, jsretry.SettleQuiet)
}

// settle holds the body both variants share; inPlace is the jsretry entry point
// used when this delivery is not escalated, so the two cannot drift on anything
// but the logging they were chosen for.
func (l *Lane) settle(ctx context.Context, msg Msg, backoff []time.Duration, err error,
	inPlace func(context.Context, jsretry.Msg, []time.Duration, error),
) {
	meta, ok := l.shouldEscalate(msg, err)
	if !ok {
		inPlace(ctx, msg, backoff, err)
		return
	}
	hdr, escErr := l.escalate(ctx, msg, meta, err)
	if escErr != nil {
		slog.ErrorContext(ctx, "retry-lane escalation failed — falling back to in-place redelivery",
			"consumer", l.Consumer, "error", escErr,
			"request_id", natsutil.RequestIDFromContext(ctx))
		jsretry.Nak(ctx, msg, backoff, "escalation publish failed")
		return
	}
	// On-call reaches this line from the escalated consumer outcome, so it has to
	// carry enough to act on and nothing more: bounded fields only, never the
	// message body and never the error text (ReasonFor is a category label).
	attempt, _ := strconv.ParseUint(hdr.Get(HeaderAttempt), 10, 64)
	slog.WarnContext(ctx, "escalated to the retry lane — fast rungs spent",
		"consumer", l.Consumer,
		"origin_subject", OriginSubject(msg.Headers(), msg.Subject()),
		"attempt", attempt,
		"reason", hdr.Get(HeaderReason),
		"request_id", natsutil.RequestIDFromContext(ctx))
	if l.OnEscalate != nil {
		l.OnEscalate()
	}
	if ackErr := msg.Ack(); ackErr != nil {
		slog.ErrorContext(ctx, "failed to ack escalated message", "error", ackErr,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}

// shouldEscalate reports whether this delivery has spent its in-place budget,
// returning the metadata it had to read to decide so escalate does not parse it
// a second time — jetstream.Msg.Metadata re-tokenizes the ack subject on every
// call. Every negative answer routes to jsretry, which keeps the existing
// semantics as the single fallback path.
func (l *Lane) shouldEscalate(msg Msg, err error) (*jetstream.MsgMetadata, bool) {
	if err == nil || !l.Enabled || l.Publish == nil || l.FastSteps <= 0 {
		return nil, false
	}
	if _, isPermanent := errcode.IsPermanent(err); isPermanent {
		return nil, false
	}
	meta, metaErr := msg.Metadata()
	if metaErr != nil || meta == nil {
		return nil, false
	}
	return meta, meta.NumDelivered > uint64(l.FastSteps)
}

// escalate republishes the message onto the RETRY stream and returns the headers
// it published with, so the caller can log the escalation from the same bounded
// values that went on the wire. The body is passed through untouched —
// re-marshalling could change bytes that dedup keys and wire-compat tests pin.
func (l *Lane) escalate(ctx context.Context, msg Msg, meta *jetstream.MsgMetadata, err error) (nats.Header, error) {
	hdr := BuildHeaders(msg.Headers(), meta, msg.Subject(), l.Consumer, ReasonFor(err), time.Now())
	subj := subject.Retry(l.SiteID, l.Consumer, subject.RetryTierSlow)
	msgID := DedupID(meta.Stream, meta.Sequence.Stream, l.Consumer)

	if pubErr := l.Publish(ctx, subj, msg.Data(), hdr, msgID); pubErr != nil {
		return nil, fmt.Errorf("publish to retry lane %s: %w", subj, pubErr)
	}
	return hdr, nil
}
