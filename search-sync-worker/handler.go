package main

import (
	"context"
	"log/slog"
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// pendingMsg tracks a JetStream message and the range of bulk actions it
// produced. A single JetStream message may fan out into zero, one, or multiple
// actions. The message is acked once ALL of its actions succeed; if any action
// fails the whole message is nakked for redelivery.
type pendingMsg struct {
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
	mu         sync.Mutex
	pending    []pendingMsg
	actions    []searchengine.BulkAction
}

// NewHandler creates a Handler with the given store, collection, and bulk
// batch size. `bulkSize` is the soft cap on buffered actions before a flush
// is triggered — the consumer loop compares it against `ActionCount()` to
// decide when to call `Flush`.
func NewHandler(store Store, collection Collection, bulkSize int) *Handler {
	return &Handler{
		store:      store,
		collection: collection,
		bulkSize:   bulkSize,
		pending:    make([]pendingMsg, 0, bulkSize),
		actions:    make([]searchengine.BulkAction, 0, bulkSize),
	}
}

// Add parses a JetStream message via the collection and adds its actions to
// the buffer. If the collection produces zero actions (e.g., a filtered
// event), the message is immediately acked without touching the buffer.
func (h *Handler) Add(msg jetstream.Msg) {
	actions, err := h.collection.BuildAction(msg.Data())
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

	results, err := h.store.Bulk(ctx, actions)
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
			if isBulkItemSuccess(actions[i].Action, results[i]) {
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

// esErrDocumentMissing is the Elasticsearch `_bulk` response `error.type`
// for an update against a missing document — a benign idempotent outcome
// we ack rather than retry. All other 404 error types (including
// `index_not_found_exception`) are treated as real failures.
const esErrDocumentMissing = "document_missing_exception"

// isBulkItemSuccess maps an ES bulk item result to a logical success/failure
// per action type.
//
//   - 2xx is always success.
//   - 409 is success ONLY for externally-versioned writes (ActionIndex,
//     ActionDelete): it means external versioning rejected a stale write and
//     the desired state is already reached or newer. ActionUpdate does NOT
//     use external versioning (the adapter omits version/version_type on
//     _update); idempotency there comes from the painless LWW guard via
//     `params.ts > stored`. A 409 on an update means an internal
//     version_conflict_engine_exception from concurrent writers — the script
//     did NOT execute, so the update was dropped. We NAK so JetStream
//     redelivers and retries.
//   - 404 is success ONLY for specific idempotent outcomes:
//   - ActionDelete with no error type set: delete of a missing doc
//     sets `result:"not_found"` and no error block — desired state
//     already reached.
//   - ActionUpdate with ErrorType == "document_missing_exception": the
//     user-room remove path emits a scriptless update which 404s if the
//     user doc doesn't exist yet — desired state already reached.
//     404 with any other error type (notably `index_not_found_exception`,
//     which means the target ES index is missing / misconfigured) is
//     treated as a real failure so we don't silently drop messages when the
//     backing index/template is wrong.
//   - ActionIndex 404 is always a failure because indexing is supposed to
//     create the doc; a 404 there only happens when the index itself is
//     missing.
func isBulkItemSuccess(action searchengine.ActionType, result searchengine.BulkResult) bool {
	if result.Status >= 200 && result.Status < 300 {
		return true
	}
	if result.Status == 409 {
		switch action {
		case searchengine.ActionIndex, searchengine.ActionDelete:
			return true
		case searchengine.ActionUpdate:
			return false
		}
	}
	if result.Status == 404 {
		switch action {
		case searchengine.ActionDelete:
			// Delete on a missing doc returns status=404 with result=not_found
			// and NO error block (ErrorType is empty). Any other error type
			// at 404 (e.g., index_not_found_exception) is a real failure.
			return result.ErrorType == ""
		case searchengine.ActionUpdate:
			// Update on a missing doc reports `document_missing_exception`.
			// Update on a missing INDEX reports `index_not_found_exception`
			// — we want that to fail loudly, not get silently acked.
			return result.ErrorType == esErrDocumentMissing
		case searchengine.ActionIndex:
			// Index is supposed to CREATE the doc; a 404 here only happens
			// when the index itself is missing (config error). Always fail.
			return false
		}
	}
	return false
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
