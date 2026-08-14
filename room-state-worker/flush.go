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
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
)

// flusher owns the pending batch and drains it to MongoDB on a ticker. Held
// messages are settled only after their batch lands, so JetStream — not an
// in-memory buffer — is what survives a MongoDB outage.
type flusher struct {
	store   Store
	backoff []time.Duration

	mu      sync.Mutex
	pending *batch
}

func newFlusher(store Store) *flusher {
	return &flusher{
		store:   store,
		backoff: jsretry.DefaultBackoff,
		pending: newBatch(),
	}
}

//nolint:gocritic // hugeParam: matches batch.add's writeIntents-by-value signature
func (f *flusher) add(in writeIntents, msg heldMsg) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending.add(in, msg)
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
	f.pending = newBatch()
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

	// stage classifies one stage's raw store error. It returns (outcome, true)
	// when the batch must stop right here (transient failure), or (_, false)
	// when the caller should proceed to the next stage (success, or a
	// permanent failure recorded for later).
	stage := func(name string, err error) (flushOutcome, bool) {
		if err == nil {
			return flushOutcome{}, false
		}
		classified := classifyFlushErr(fmt.Errorf("flush %s: %w", name, err))
		if _, ok := errcode.IsPermanent(classified); !ok {
			return flushOutcome{err: classified}, true
		}
		permanentErrs = append(permanentErrs, classified)
		var bwe mongo.BulkWriteException
		if errors.As(err, &bwe) {
			// Pair the stage name with its codes right here — codes alone
			// can't tell lastSeenAt and mentions apart, since both stages
			// write the subscriptions collection.
			stageCodes = append(stageCodes, fmt.Sprintf("%s=%v", name, bwe.ErrorCodes()))
		}
		return flushOutcome{}, false
	}

	if out, stop := stage("room last message", f.store.BulkUpdateRoomLastMessage(ctx, b.rooms)); stop {
		return out
	}
	if out, stop := stage("subscription last seen", f.store.BulkAdvanceLastSeen(ctx, b.lastSeen)); stop {
		return out
	}
	if out, stop := stage("subscription mentions", f.store.BulkSetMentions(ctx, b.mentions)); stop {
		return out
	}

	if len(permanentErrs) == 0 {
		return flushOutcome{}
	}
	return flushOutcome{err: errors.Join(permanentErrs...), stageCodes: stageCodes}
}

// settle resolves every held message against the flush outcome. SettleQuiet is
// used because the batch-level failure is logged once here — per-message
// logging would emit one identical line per held message. The permanent and
// transient branches log distinct, greppable messages: an operator must be
// able to tell "this batch was silently dropped, data is gone" apart from
// "this batch will retry" from the log line alone.
func (f *flusher) settle(ctx context.Context, b *batch, out flushOutcome) {
	if out.err != nil {
		if _, ok := errcode.IsPermanent(out.err); ok {
			slog.ErrorContext(ctx, "room-state flush dropped poison batch",
				"error", out.err,
				"mongo_stage_codes", out.stageCodes,
				"rooms", len(b.rooms),
				"last_seen", len(b.lastSeen),
				"mentions", len(b.mentions),
				"held", len(b.held))
		} else {
			slog.ErrorContext(ctx, "room-state flush failed, retrying",
				"error", out.err,
				"rooms", len(b.rooms),
				"last_seen", len(b.lastSeen),
				"mentions", len(b.mentions),
				"held", len(b.held))
		}
	}
	for _, h := range b.held {
		jsretry.SettleQuiet(h.ctx, h.msg, f.backoff, out.err)
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
	return errcode.Permanent(errcode.Internal("mongo rejected room-state bulk write", errcode.WithCause(err)))
}

// Run drives the flush ticker until ctx is cancelled, then performs one final
// flush on a fresh context so a buffered batch still lands — and its messages
// still settle — even though the supplied ctx is already done.
//
// Every call into Flush is jobguard-recovered: Flush runs user-derived data
// (a batch built from event content) through BulkWrite, and a panic there
// must not kill this goroutine — an unrecovered panic here crashes the whole
// process, and with MaxDeliver=-1 the held-but-un-acked batch redelivers
// forever after every restart, turning a deterministic panic into a crash
// loop with no way for the message to fall out of the stream.
func (f *flusher) Run(ctx context.Context, interval, finalTimeout time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.Background(), finalTimeout)
			jobguard.Guard("room-state flush", func() { f.Flush(finalCtx) })
			cancel()
			return
		case <-t.C:
			jobguard.Guard("room-state flush", func() { f.Flush(ctx) })
		}
	}
}
