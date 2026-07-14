package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// sourceLookup fetches the current full message doc from the source by _id.
type sourceLookup interface {
	FindByID(ctx context.Context, id string) ([]byte, error)
}

// oplogEvent mirrors model.OplogEvent's wire shape (decoded from the consumed message).
type oplogEvent struct {
	EventID           string          `json:"eventId"`
	Op                string          `json:"op"`
	Collection        string          `json:"coll"`
	DocumentKey       json.RawMessage `json:"documentKey"`
	ClusterTime       int64           `json:"clusterTime"` // source op time, unix ms — the delete/soft-delete timestamp.
	FullDocument      json.RawMessage `json:"fullDocument"`
	UpdateDescription json.RawMessage `json:"updateDescription"`
	// Degraded is true when the connector couldn't encode an opaque field (left nil) but still
	// published the event. The transformer recovers the missing field via a source lookup, not poison.
	Degraded       bool   `json:"degraded"`
	DegradedReason string `json:"degradedReason"`
}

// inserter is the canonicalPublisher surface the handler needs (lets tests fake it).
type inserter interface {
	publishInsert(ctx context.Context, msg model.Message) error
}

type handler struct {
	collection     string // the watched message collection name (cfg.SourceMessageCollection)
	softDeleteType string // source marker for a removed message (cfg.SoftDeleteType, e.g. "rm")
	publisher      inserter
	history        historyClient
	lookup         sourceLookup
	metrics        *metrics // nil-safe; set in main, nil in unit tests
}

// skipSystem skips a system/event message (not user content, deferred from migration), records a
// skip metric, and returns migration.ErrSkipped so the caller Acks without counting it as processed.
func (h *handler) skipSystem(ctx context.Context, t, eventID string) error {
	slog.Info("skipping system message", "t", t, "eventId", eventID, "request_id", natsutil.RequestIDFromContext(ctx))
	h.metrics.onSkipped(ctx, "system_message")
	return migration.ErrSkipped
}

// skipForeign skips a message authored at a remote site (federation.origin set) — foreign copies
// arrive via the new app's own federation. origin is a site id, safe to log. Returns migration.ErrSkipped.
func (h *handler) skipForeign(ctx context.Context, origin, eventID string) error {
	slog.Info("skipping foreign-origin message", "origin", origin, "eventId", eventID, "request_id", natsutil.RequestIDFromContext(ctx))
	h.metrics.onSkipped(ctx, "foreign_origin")
	return migration.ErrSkipped
}

// handle processes one decoded oplog event. nil = ack+count; migration.ErrSkipped = ack-without-counting
// (deliberate drop, already metered); migration.ErrPoison => Term; any other error => Nak (transient).
//
//nolint:gocritic // ev passed by value: it's the decoded event the consume loop hands off, one per message off the hot path.
func (h *handler) handle(ctx context.Context, ev oplogEvent) error {
	if ev.Collection != h.collection {
		slog.Debug("skip non-message collection", "collection", ev.Collection, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "other_collection")
		return migration.ErrSkipped
	}
	switch ev.Op {
	case "insert":
		return h.handleInsert(ctx, ev)
	case "update":
		return h.handleUpdate(ctx, ev)
	case "replace":
		return h.handleReplace(ctx, ev)
	case "delete":
		return h.handleDelete(ctx, ev)
	default:
		slog.Warn("unknown op skipped", "op", ev.Op, "eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "unknown_op")
		return migration.ErrSkipped
	}
}

//nolint:gocritic // ev passed by value to mirror handle's signature; one per insert event, off the hot path.
func (h *handler) handleInsert(ctx context.Context, ev oplogEvent) error {
	doc := ev.FullDocument
	if len(doc) == 0 {
		if !ev.Degraded {
			// A non-degraded insert with no fullDocument is a contract violation — the connector
			// always carries the doc for inserts unless it degraded it. Poison.
			return fmt.Errorf("%w: insert without fullDocument", migration.ErrPoison)
		}
		recovered, err := h.recoverDegradedDoc(ctx, ev)
		if err != nil {
			return err
		}
		doc = recovered
	}
	rc, err := decodeRocketchatMessage(doc)
	if err != nil {
		// Single %w keeps migration.ErrPoison matchable; the decode error is folded in with %v (nothing checks
		// errors.Is on it, and one sentinel per chain satisfies the semgrep multi-wrap guard).
		return fmt.Errorf("%w: %v", migration.ErrPoison, err) //nolint:errorlint // intentional single-%w sentinel wrap; decode err is informational only
	}
	// Foreign-origin messages are migrated by their home site, not here — the connector's $match
	// drops them at the source; this is defense-in-depth in case one slips through.
	if isForeignOrigin(rc) {
		return h.skipForeign(ctx, rc.Federation.Origin, ev.EventID)
	}
	// Only t==null docs are real user messages; any typed doc (system event, or a stray rm) is
	// not user content and is skipped rather than ingested as a message.
	if rc.T != "" {
		return h.skipSystem(ctx, rc.T, ev.EventID)
	}
	msg := mapToMessage(rc)
	if rc.TMID != "" {
		// A thread reply: message-worker needs the parent's createdAt to stamp thread_room_id on the
		// parent (else the thread is unreachable). Best-effort — a missing/corrupt parent publishes without the link; only a transient lookup error retries.
		parentCreatedAt, err := h.resolveParentCreatedAt(ctx, rc.TMID, ev.EventID)
		if err != nil {
			return err
		}
		msg.ThreadParentMessageCreatedAt = parentCreatedAt
	}
	return h.publisher.publishInsert(ctx, msg)
}

// resolveParentCreatedAt returns the thread parent's createdAt so a migrated reply links to it.
// A transient lookup error Naks; a missing/undecodable parent returns (nil, nil) — published best-effort, unlinked.
func (h *handler) resolveParentCreatedAt(ctx context.Context, parentID, eventID string) (*time.Time, error) {
	doc, err := h.lookup.FindByID(ctx, parentID)
	if err != nil {
		return nil, fmt.Errorf("lookup thread parent %q: %w", parentID, err)
	}
	if doc == nil {
		// Thread-parent miss => publish best-effort (nil,nil): the parent is a different, possibly-deleted
		// doc; the reply is preserved, only its link dropped. Unlike the recover/update misses, which Nak/ack-skip.
		slog.Warn("thread parent not found in source — reply won't link to its thread", "parentId", parentID, "eventId", eventID, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onThreadLinkDropped(ctx, "missing")
		return nil, nil
	}
	parent, err := decodeRocketchatMessage(doc)
	if err != nil {
		slog.Warn("thread parent decode failed — reply won't link to its thread", "parentId", parentID, "eventId", eventID, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onThreadLinkDropped(ctx, "corrupt")
		//nolint:nilerr // best-effort: a corrupt parent doesn't fail the reply — publish it without the thread link rather than Nak forever. The raw decode err is intentionally not logged (may echo doc bytes).
		return nil, nil
	}
	ts := parent.TS
	return &ts, nil
}

// documentKeyID decodes documentKey → _id. Returns migration.ErrPoison when missing/malformed.
func documentKeyID(documentKey json.RawMessage) (string, error) {
	var key struct {
		ID string `json:"_id"`
	}
	if err := json.Unmarshal(documentKey, &key); err != nil || key.ID == "" {
		return "", fmt.Errorf("%w: bad documentKey", migration.ErrPoison)
	}
	return key.ID, nil
}

// resolveDocumentKeyID resolves the event's _id. A degraded event's documentKey may itself be nil,
// so a failure is transient (plain error → Nak); a non-degraded malformed documentKey is poison.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func resolveDocumentKeyID(ev oplogEvent) (string, error) {
	id, err := documentKeyID(ev.DocumentKey)
	if err != nil && ev.Degraded {
		return "", fmt.Errorf("degraded event missing documentKey (reason %q): cannot resolve _id", ev.DegradedReason)
	}
	return id, err
}

// recoverDegradedDoc re-reads the live source doc for a degraded event whose fullDocument was dropped.
// Every failure is transient (plain error → Nak); a degraded event is never Term-ed (lossless contract).
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) recoverDegradedDoc(ctx context.Context, ev oplogEvent) ([]byte, error) {
	id, err := resolveDocumentKeyID(ev)
	if err != nil {
		// documentKey is degraded/nil — cannot resolve _id, so Nak (plain error), never Term.
		return nil, err
	}
	doc, err := h.lookup.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("recover degraded %q: %w", id, err)
	}
	if doc == nil {
		// The source is live; the doc may not have converged yet. Nak/retry, bounded by MaxDeliver.
		return nil, fmt.Errorf("recover degraded %q: source lookup miss", id)
	}
	slog.Warn("recovered degraded event", "eventId", ev.EventID, "reason", ev.DegradedReason, "request_id", natsutil.RequestIDFromContext(ctx))
	h.metrics.onRecovered(ctx, ev.Collection)
	return doc, nil
}

//nolint:gocritic // ev passed by value to mirror handle's signature; one per update event, off the hot path.
func (h *handler) handleUpdate(ctx context.Context, ev oplogEvent) error {
	id, err := resolveDocumentKeyID(ev)
	if err != nil {
		return err
	}
	doc, err := h.lookup.FindByID(ctx, id)
	if err != nil {
		return fmt.Errorf("lookup %q: %w", id, err)
	}
	if doc == nil {
		// Doc gone from source — nothing to apply, so ack-skip (terminal for this update). Distinct
		// from the recover Nak (own live doc must exist) and thread-parent miss (best-effort publish).
		slog.Warn("update lookup miss — skipping", "id", id, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "update_lookup_miss")
		return migration.ErrSkipped
	}
	return h.applyUpdate(ctx, ev, id, doc)
}

//nolint:gocritic // ev passed by value to mirror handle's signature; one per replace event, off the hot path.
func (h *handler) handleReplace(ctx context.Context, ev oplogEvent) error {
	id, err := resolveDocumentKeyID(ev)
	if err != nil {
		return err
	}
	doc := ev.FullDocument
	if len(doc) == 0 {
		if !ev.Degraded {
			return fmt.Errorf("%w: replace without fullDocument", migration.ErrPoison)
		}
		recovered, rerr := h.recoverDegradedDoc(ctx, ev)
		if rerr != nil {
			return rerr
		}
		doc = recovered
	}
	return h.applyUpdate(ctx, ev, id, doc)
}

// handleDelete routes a hard delete through the same id-only delete-by-id path as a soft-delete:
// the source doc is gone, so no lookup — the target resolves room/createdAt from the id. deletedAt = clusterTime.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; one per delete event, off the hot path.
func (h *handler) handleDelete(ctx context.Context, ev oplogEvent) error {
	id, err := resolveDocumentKeyID(ev)
	if err != nil {
		return err
	}
	return h.history.Delete(ctx, model.MigrationDeleteRequest{
		MessageID: id,
		DeletedAt: deleteTime(ev.ClusterTime),
	})
}

// deleteTime derives the delete timestamp from the source clusterTime, falling back to publish-time
// when clusterTime is missing/non-positive — an anomaly that shouldn't drop a valid delete, only
// avoid persisting a 1970 epoch timestamp.
func deleteTime(clusterTime int64) time.Time {
	if clusterTime <= 0 {
		return time.Now().UTC()
	}
	return time.UnixMilli(clusterTime).UTC()
}

// applyUpdate classifies the resolved doc and routes to the right history handler. The message id is
// the event's documentKey._id (never the looked-up doc's _id, so a dropped _id can't Nak-loop); roomId/createdAt come from the doc.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; one per update/replace event, off the hot path.
func (h *handler) applyUpdate(ctx context.Context, ev oplogEvent, id string, doc []byte) error {
	rc, err := decodeRocketchatMessage(doc)
	if err != nil {
		return fmt.Errorf("%w: %v", migration.ErrPoison, err) //nolint:errorlint // intentional single-%w sentinel wrap; decode err is informational only
	}
	// Foreign-origin filter for update/replace: the connector's $match can't drop these (no
	// fullDocument on update events), so the resolved doc is where we catch them, before classifying.
	if isForeignOrigin(rc) {
		return h.skipForeign(ctx, rc.Federation.Origin, ev.EventID)
	}
	if isSoftDeleted(rc, h.softDeleteType) {
		return h.history.Delete(ctx, model.MigrationDeleteRequest{
			MessageID: id,
			DeletedAt: deleteTime(ev.ClusterTime),
		})
	}
	if isSystemMessage(rc, h.softDeleteType) {
		return h.skipSystem(ctx, rc.T, ev.EventID)
	}
	edited := rc.TS
	if rc.EditedAt != nil {
		edited = *rc.EditedAt
	}
	return h.history.Edit(ctx, model.MigrationEditRequest{
		MessageID: id, RoomID: rc.RID, CreatedAt: rc.TS, Content: rc.Msg, EditedAt: edited,
	})
}
