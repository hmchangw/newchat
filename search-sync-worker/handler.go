package main

import (
	"context"
	"log/slog"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// pendingMsg tracks a JetStream message and the range of bulk actions it
// produced. A single JetStream message may fan out into zero, one, or multiple
// actions. The message is acked once ALL of its actions succeed; if any action
// fails the whole message is nakked for redelivery.
type pendingMsg struct {
	ctx         context.Context
	jsMsg       jetstream.Msg
	actionStart int // starting index into Handler.actions
	actionCount int // number of actions contributed by this message
}

// Handler buffers JetStream messages and the ES bulk actions they produce,
// then flushes the actions as a single ES bulk request.
//
// Two counts are tracked separately because they can diverge for fan-out
// collections (one JetStream message producing N ES actions):
//
//   - MessageCount() reports buffered source messages. Used for per-source
//     ack/nak accounting at flush time.
//   - ActionCount() reports buffered ES bulk actions. This is what bounds
//     the size of the next ES bulk request and should drive the flush
//     decision in the consumer loop.
//
// For 1:1 collections (messages, and the single-subscription path of
// spotlight/user-room) MessageCount() == ActionCount(). For fan-out
// collections (bulk-invite spotlight/user-room) ActionCount() >=
// MessageCount().
type Handler struct {
	store      Store
	collection Collection
	bulkSize   int // soft cap on buffered actions; callers drive flush via ActionCount()
	tracer     trace.Tracer
	mu         sync.Mutex
	pending    []pendingMsg
	actions    []searchengine.BulkAction
}

// NewHandler creates a Handler with the given store, collection, and bulk
// batch size. `bulkSize` is the soft cap on buffered actions before a flush
// is triggered — the consumer loop compares it against `ActionCount()` to
// decide when to call `Flush`.
func NewHandler(store Store, collection Collection, bulkSize int, tracers ...trace.Tracer) *Handler {
	tracer := otel.Tracer("search-sync-worker")
	if len(tracers) > 0 && tracers[0] != nil {
		tracer = tracers[0]
	}
	return &Handler{
		store:      store,
		collection: collection,
		bulkSize:   bulkSize,
		tracer:     tracer,
		pending:    make([]pendingMsg, 0, bulkSize),
		actions:    make([]searchengine.BulkAction, 0, bulkSize),
	}
}

// Add parses a JetStream message via the collection and adds its actions
// to the buffer. If the collection produces zero actions, the message is
// acked without touching the buffer.
func (h *Handler) Add(msg jetstream.Msg) {
	h.AddWithContext(context.Background(), msg)
}

// AddWithContext is Add plus the consumer span context carried by the o11y
// JetStream facade. Flush uses it as a span link source because one ES bulk
// request can contain actions from multiple source messages.
func (h *Handler) AddWithContext(ctx context.Context, msg jetstream.Msg) {
	data, err := decodePayload(msg)
	if err != nil {
		slog.Error("decode payload", "error", err)
		natsutil.Ack(msg, "decode payload failed")
		return
	}
	actions, err := h.collection.BuildAction(data)
	if err != nil {
		slog.Error("build action", "error", err)
		natsutil.Ack(msg, "build action failed")
		return
	}

	if len(actions) == 0 {
		natsutil.Ack(msg, "filtered, no actions")
		return
	}

	h.mu.Lock()
	h.pending = append(h.pending, pendingMsg{
		ctx:         ctx,
		jsMsg:       msg,
		actionStart: len(h.actions),
		actionCount: len(actions),
	})
	h.actions = append(h.actions, actions...)
	h.mu.Unlock()
}

// Flush sends all buffered actions to ES and acks/naks per source message.
func (h *Handler) Flush(ctx context.Context) {
	h.mu.Lock()
	if len(h.pending) == 0 {
		h.mu.Unlock()
		return
	}
	pending := h.pending
	actions := h.actions
	h.pending = make([]pendingMsg, 0, h.bulkSize)
	h.actions = make([]searchengine.BulkAction, 0, h.bulkSize)
	h.mu.Unlock()

	bulkCtx, span := h.startFlushSpan(ctx, pending, len(actions))
	defer span.End()

	results, err := h.store.Bulk(bulkCtx, actions)
	if err != nil {
		slog.Error("bulk request failed", "error", err, "actions", len(actions))
		nakAll(pending, "bulk request failed")
		return
	}

	if len(results) != len(actions) {
		// Defensive guard for a protocol-level anomaly: ES bulk API normally
		// returns one result per input action in input order. Nak-all is safe
		// because every action type we emit is idempotent on redelivery:
		//   - ActionIndex / ActionDelete: external versioning makes a stale
		//     redelivery return 409 (handled as ack below); a successful
		//     redelivery is identical to the original write.
		//   - ActionUpdate: the painless scripts in user_room.go check a
		//     per-room timestamp guard (params.ts > stored) and short-circuit
		//     via ctx.op = 'none' on a redelivery, so a redelivered update
		//     is at worst a no-op.
		// No duplicate processing, no lost events.
		slog.Error("bulk result count mismatch", "expected", len(actions), "actual", len(results))
		nakAll(pending, "bulk result count mismatch")
		return
	}

	for _, p := range pending {
		allOK := true
		for i := p.actionStart; i < p.actionStart+p.actionCount; i++ {
			if searchengine.IsBulkItemSuccess(actions[i].Action, results[i]) {
				continue
			}
			allOK = false
			slog.Error("bulk item failed",
				"status", results[i].Status,
				"error", results[i].Error,
				"docID", actions[i].DocID,
				"index", actions[i].Index,
			)
			break
		}
		if allOK {
			natsutil.Ack(p.jsMsg, "bulk actions succeeded")
		} else {
			natsutil.Nak(p.jsMsg, "bulk action failed")
		}
	}
}

func (h *Handler) startFlushSpan(ctx context.Context, pending []pendingMsg, actionCount int) (context.Context, trace.Span) {
	links := make([]trace.Link, 0, len(pending))
	seen := make(map[string]struct{}, len(pending))
	for _, p := range pending {
		sc := trace.SpanContextFromContext(p.ctx)
		if !sc.IsValid() {
			continue
		}
		key := sc.TraceID().String() + "/" + sc.SpanID().String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		links = append(links, trace.Link{SpanContext: sc})
	}

	return h.tracer.Start(ctx, "search-sync bulk flush",
		trace.WithAttributes(
			attribute.String("chat.search.collection", h.collection.ConsumerName()),
			attribute.Int("chat.search.bulk.messages", len(pending)),
			attribute.Int("chat.search.bulk.actions", actionCount),
		),
		trace.WithLinks(links...),
	)
}

// nakAll naks every buffered source message for redelivery. Used on the
// two defensive paths in Flush where the whole batch can't be processed
// (bulk request failed, or the ES response item count didn't match the
// request). The shared `reason` is logged against every message so an
// operator grepping by cause sees all of them together.
func nakAll(pending []pendingMsg, reason string) {
	for _, p := range pending {
		natsutil.Nak(p.jsMsg, reason)
	}
}

// MessageCount returns the number of buffered source JetStream messages.
// This is used for diagnostics and for the per-source ack/nak accounting at
// flush time; it is NOT the quantity that should drive the flush decision
// for fan-out collections — use ActionCount() for that.
func (h *Handler) MessageCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.pending)
}

// ActionCount returns the number of buffered ES bulk actions. For 1:1
// collections this equals MessageCount(); for fan-out collections (bulk
// invites producing N actions per event) it is ≥ MessageCount(). The
// consumer loop compares this against the configured bulk batch size to
// decide when to flush so ES bulk requests stay bounded regardless of
// fan-out.
func (h *Handler) ActionCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.actions)
}
