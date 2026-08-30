package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/flushloop"
	"github.com/hmchangw/chat/pkg/jsretry"
)

// flusher owns the pending batch and drains it to MongoDB on a ticker. Held
// messages are settled only after their batch lands, so JetStream — not an
// in-memory buffer — is what survives a MongoDB outage.
type flusher struct {
	store Store

	// mentionBudget drains the batch early once it holds this many DISTINCT
	// mention intents. Zero leaves the ticker as the only trigger.
	mentionBudget int
	// flushTimeout bounds one early drain, mirroring the periodic one.
	flushTimeout time.Duration

	mu      sync.Mutex
	pending *batch
}

// newFlusher builds the flusher. mentionBudget and flushTimeout configure the
// early drain, and zero for either leaves the ticker as the only trigger.
//
// The early drain bounds the one write map MaxAckPending does not. held, rooms
// and lastSeen are all bounded by the un-acked message count. mentions is not:
// it grows with mentioned accounts per message, and mention.Parse caps neither
// the token count nor its input beyond the 20KB content limit. One
// maximum-size message yields thousands of accounts, so a window in which
// flushes are slow can hold MaxAckPending times that — enough that
// BulkSetMentions cannot complete inside flushTimeout, and under MaxDeliver=-1
// that is a Nak-rebuild-Nak livelock that never exits, with a readiness probe
// still reporting green.
//
// A budget rather than a per-message cap: capping mentions at derive time would
// silently drop real badges, and a lost badge is invisible to everyone,
// including whoever is meant to notice. Draining early instead keeps every
// badge and turns the growth into back-pressure — the consume loop pays for the
// write inline, which is precisely the desired effect.
func newFlusher(store Store, mentionBudget int, flushTimeout time.Duration) *flusher {
	return &flusher{
		store:         store,
		mentionBudget: mentionBudget,
		flushTimeout:  flushTimeout,
		pending:       newBatch(nil),
	}
}

// add merges one message's intents and reports whether the pending batch has
// reached its mention budget, i.e. whether the caller should drain now rather
// than wait for the ticker. Always false when no budget is configured.
//
//nolint:gocritic // hugeParam: matches batch.add's writeIntents-by-value signature
func (f *flusher) add(in writeIntents, msg heldMsg) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending.add(in, msg)
	return f.mentionBudget > 0 && len(f.pending.mentions) >= f.mentionBudget
}

// flushNow drains outside the ticker. The caller's context supplies the trace
// and request id but not its cancellation: it belongs to one message, and the
// batch being drained holds many others whose writes must not die with it.
func (f *flusher) flushNow(ctx context.Context) {
	timeout := f.flushTimeout
	if timeout <= 0 {
		timeout = flushloop.DefaultFinalTimeout
	}
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	f.Flush(drainCtx)
}

// Flush swaps the pending batch out and writes it, then settles every message
// the batch was holding. The lock is held only for the swap, so the writes do
// not block new intents arriving from the consume loop.
func (f *flusher) Flush(ctx context.Context) {
	f.mu.Lock()
	if f.pending.empty() {
		f.mu.Unlock()
		return
	}
	b := f.pending
	f.pending = newBatch(b)
	f.mu.Unlock()

	f.settle(ctx, b, f.write(ctx, b))
}

// flushOutcome is the result of applying one batch's writes: err is nil on
// full success, the raw (wrapped) error on a transient failure, or an
// errors.Join of one-or-more permanent errors when every remaining failure in
// the batch was a document-level rejection. stageCodes carries one entry per
// permanent stage as "<stage name>=<mongo WriteError codes>" — both
// lastSeenAt and mentions write to the same subscriptions collection, so the
// codes alone can't say which stage was poison; pairing each code list with
// its stage name is what makes the poison-batch log line diagnosable. It is
// only ever non-empty alongside a permanent err.
type flushOutcome struct {
	err        error
	stageCodes []string
	// mongoErrs carries the driver's own text per failing stage. The permanent
	// err wraps it via errcode.WithCause, but that cause is unexported and only
	// errcode.Classify reads it — and Classify logs under "request failed",
	// which would cost the greppable poison-vs-retry distinction settle keeps.
	// So the detail rides here instead, for the one branch that loses data.
	mongoErrs []string
}

// write applies the batch in a fixed order: rooms, then lastSeenAt, then
// mentions. lastSeenAt precedes mentions so the mention filter observes the
// sender's own advance, preserving broadcast-worker's sequential semantics —
// a message that @-mentions its own sender does not badge the sender.
//
// Each stage's error is classified as it happens:
//   - transient  → stop immediately and return it. Mongo is likely
//     unreachable, so attempting later stages only burns AckWait on more
//     timeouts; the whole batch is Nak'd and retried, which is safe because
//     every write is replay-safe.
//   - permanent  → record it and CONTINUE to the remaining stages. A
//     poison document in one stage must not discard the lastSeenAt advances
//     or mention badges a later stage would otherwise have written — those
//     writes can only ever be lost, never regained on redelivery, once this
//     batch is Ack-dropped.
//
// If every failure across the batch was permanent, the joined permanent
// errors are returned so the whole batch Acks (drops); if any stage was
// transient, that stage's error is returned instead and the batch Naks.
func (f *flusher) write(ctx context.Context, b *batch) flushOutcome {
	var permanentErrs []error
	var stageCodes []string
	var mongoErrs []string

	// stage classifies one stage's raw store error. A non-nil return is a
	// transient failure and means the batch must stop right here; nil means
	// proceed to the next stage (success, or a permanent failure recorded for
	// later).
	stage := func(name string, err error) error {
		if err == nil {
			return nil
		}
		classified := classifyFlushErr(fmt.Errorf("flush %s: %w", name, err))
		if _, ok := errcode.IsPermanent(classified); !ok {
			return classified
		}
		permanentErrs = append(permanentErrs, classified)
		var bwe mongo.BulkWriteException
		if errors.As(err, &bwe) {
			// Pair the stage name with its codes right here — codes alone
			// can't tell lastSeenAt and mentions apart, since both stages
			// write the subscriptions collection.
			stageCodes = append(stageCodes, fmt.Sprintf("%s=%v", name, bwe.ErrorCodes()))
			mongoErrs = append(mongoErrs, fmt.Sprintf("%s=%s", name, bwe.Error()))
		}
		return nil
	}

	if err := stage("room last message", f.store.BulkUpdateRoomLastMessage(ctx, b.rooms)); err != nil {
		return flushOutcome{err: err}
	}
	if err := stage("subscription last seen", f.store.BulkAdvanceLastSeen(ctx, b.lastSeen)); err != nil {
		return flushOutcome{err: err}
	}
	if err := stage("subscription mentions", f.store.BulkSetMentions(ctx, b.mentions)); err != nil {
		return flushOutcome{err: err}
	}

	if len(permanentErrs) == 0 {
		return flushOutcome{}
	}
	return flushOutcome{err: errors.Join(permanentErrs...), stageCodes: stageCodes, mongoErrs: mongoErrs}
}

// settle resolves every held message against the flush outcome. SettleQuiet is
// used because the batch-level failure is logged once here — per-message
// logging would emit one identical line per held message. The permanent and
// transient branches log distinct, greppable messages: an operator must be
// able to tell "this batch was silently dropped, data is gone" apart from
// "this batch will retry" from the log line alone.
func (f *flusher) settle(ctx context.Context, b *batch, out flushOutcome) {
	if out.err != nil {
		msg := "room-list state flush failed, retrying"
		attrs := []any{
			"error", out.err,
			"rooms", len(b.rooms),
			"last_seen", len(b.lastSeen),
			"mentions", len(b.mentions),
			"held", len(b.held),
		}
		if _, ok := errcode.IsPermanent(out.err); ok {
			msg = "room-list state flush dropped poison batch"
			attrs = append(attrs, "mongo_stage_codes", out.stageCodes, "mongo_errors", out.mongoErrs)
		}
		slog.ErrorContext(ctx, msg, attrs...)
	}
	// out.err is either a single transient error or an errors.Join of ONLY
	// permanent ones — never a mixture. That matters here and is not local:
	// SettleQuiet asks errcode.IsPermanent, which is an errors.As any-match over
	// the join, so one permanent error in a mixed join would Ack-drop the
	// transient failures riding with it. What keeps a mixed join unreachable is
	// write's early return on the first transient stage, three frames away.
	// A stage that RECORDS a transient error instead of returning it would break
	// this silently — an Ack is indistinguishable from success on the wire.
	// TestFlusher_PermanentThenTransientNaksAndSkipsThirdStage is the guard.
	for _, h := range b.held {
		jsretry.SettleQuiet(h.ctx, h.msg, jsretry.DefaultBackoff, out.err)
	}
}

// permanentMongoCodes is the allow-list of MongoDB server error codes that
// mean "this exact document is rejected identically on every retry" — i.e.
// genuine document-level poison, not a transient replica-set/timeout/shutdown
// condition. Deny-by-default: any code NOT in this set stays transient, so a
// batch that mixes one poison document with an in-flight failover (whose
// per-write codes carry no error label — labels are a top-level property of
// the response, not of an individual WriteError) still retries instead of
// silently discarding the transient writes riding alongside it.
var permanentMongoCodes = map[int]struct{}{
	2:     {}, // BadValue
	9:     {}, // FailedToParse
	14:    {}, // TypeMismatch
	52:    {}, // DollarPrefixedFieldName
	66:    {}, // ImmutableField
	121:   {}, // DocumentValidationFailure
	11000: {}, // DuplicateKey
	10334: {}, // BSONObjectTooLarge
	16837: {}, // Location16837: invalid modifier specified
}

// classifyFlushErr marks a bulk write exception permanent ONLY IF every
// WriteError code it carries is in permanentMongoCodes — if even one code is
// transient or unrecognised, the whole exception stays transient and retries.
// This converges correctly: transient errors eventually succeed on
// redelivery, and once only the genuinely poison document remains, the batch
// classifies permanent and is dropped. A WriteConcernError or a
// RetryableWriteError label always forces transient regardless of the
// WriteError codes present, since both describe a durability/consensus
// failure rather than the document's content.
//
// With MaxDeliver=-1 a rejected document would otherwise redeliver forever
// and wedge the consumer — the exact stall this service exists to avoid.
func classifyFlushErr(err error) error {
	if err == nil {
		return nil
	}
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) || len(bwe.WriteErrors) == 0 {
		return err
	}
	if bwe.WriteConcernError != nil || bwe.HasErrorLabel("RetryableWriteError") {
		return err
	}
	for _, code := range bwe.ErrorCodes() {
		if _, ok := permanentMongoCodes[code]; !ok {
			return err
		}
	}
	return errcode.Permanent(errcode.Internal("mongo rejected room-list state bulk write", errcode.WithCause(err)))
}

// Run drives the flush ticker until ctx is cancelled, then performs one final
// flush so a buffered batch still lands — and its messages still settle — even
// though the supplied ctx is already done. That final flush takes
// flushloop.DefaultFinalTimeout.
//
// perFlushTimeout bounds each periodic flush. Flush is driven synchronously, so
// an unbounded write does not merely delay its own batch — it stops every later
// flush, the pending batch keeps growing, and the held messages behind it pass
// AckWait and redeliver into a MongoDB that is evidently already unwell.
// Bounding it turns that into an ordinary transient failure: the batch is
// Nak'd, the loop keeps ticking, and back-pressure engages instead of a silent
// stall. Must stay comfortably below CONSUMER_ACK_WAIT for the same reason
// MONGO_SERVER_SELECTION_TIMEOUT stays below it.
//
// Flush reports its own failures — it has the batch sizes and the poison-vs-
// retry distinction the loop cannot see — so nothing is returned to flushloop
// to log a second time.
func (f *flusher) Run(ctx context.Context, interval, perFlushTimeout time.Duration) {
	flushloop.Run(ctx, flushloop.Config{
		Name:     "room-list state flush",
		Interval: interval,
		PerFlush: perFlushTimeout,
	}, func(ctx context.Context) error {
		f.Flush(ctx)
		return nil
	})
}
