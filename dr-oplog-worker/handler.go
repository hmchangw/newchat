package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/hmchangw/chat/pkg/migration"
)

// handler applies one decoded DR oplog event to the backup Mongo. It is self-contained: every
// insert/replace/update carries the full post-image (the connector uses updateLookup), so there
// is no source-Mongo re-read and no cross-gateway dependency on the origin site.
type handler struct {
	collections map[string]struct{} // watched DR collections (defense-in-depth vs the subject filter)
	target      targetStore
	metrics     *metrics // nil-safe
}

// handle maps one event to a verbatim target write. nil = ack+count; ErrSkipped =
// ack-without-count; ErrPoison => Term; any other error => Nak (transient).
//
//nolint:gocritic // ev passed by value: one per message off the consume loop, off the hot path.
func (h *handler) handle(ctx context.Context, ev oplogEvent) error {
	if _, ok := h.collections[ev.Collection]; !ok {
		// The consumer's subject filter should prevent this; skip defensively without a write.
		h.metrics.onSkipped(ctx, "other_collection")
		return migration.ErrSkipped
	}

	id, err := documentID(ev.DocumentKey)
	if err != nil {
		return err // poison
	}

	switch ev.Op {
	case "delete":
		if derr := h.target.DeleteByID(ctx, ev.Collection, id); derr != nil {
			return fmt.Errorf("delete %s: %w", ev.Collection, derr)
		}
		h.metrics.onWrite(ctx, ev.Collection, "delete")
		return nil
	case "insert", "replace", "update":
		return h.upsert(ctx, ev, id)
	default:
		// Collection-level or unrecognized op (drop/rename/invalidate) — nothing to write.
		h.metrics.onSkipped(ctx, "unknown_op")
		return migration.ErrSkipped
	}
}

// upsert decodes the inline full document and writes it verbatim by _id. A missing fullDocument on
// a non-delete op can never succeed here (no source to recover from), so it poisons.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) upsert(ctx context.Context, ev oplogEvent, id any) error {
	// Absent (omitted) or an explicit JSON null both mean "no post-image" — either can never
	// succeed on a self-contained applier, so poison rather than upsert an empty/garbage doc.
	if len(ev.FullDocument) == 0 || bytes.Equal(bytes.TrimSpace(ev.FullDocument), []byte("null")) {
		return fmt.Errorf("%w: %s without fullDocument (DR feed requires updateLookup)", migration.ErrPoison, ev.Op)
	}
	doc, err := decodeExtJSONDoc(ev.FullDocument)
	if err != nil {
		return err // poison
	}
	if derr := h.target.UpsertByID(ctx, ev.Collection, id, doc); derr != nil {
		return fmt.Errorf("upsert %s: %w", ev.Collection, derr)
	}
	h.metrics.onWrite(ctx, ev.Collection, "upsert")
	return nil
}
