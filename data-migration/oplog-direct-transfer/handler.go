package main

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// targetStore is the verbatim per-collection write surface, keyed by native-typed _id.
type targetStore interface {
	UpsertByID(ctx context.Context, collection string, id any, doc bson.D) error
	DeleteByID(ctx context.Context, collection string, id any) error
}

type handler struct {
	collections map[string]struct{}               // watched direct-transfer collections (defense-in-depth vs the subject filter)
	lookups     map[string]migration.SourceLookup // one per collection, for the update re-read
	target      targetStore
	metrics     *metrics // nil-safe
}

// handle maps one decoded event to a verbatim target write. nil = ack+count; ErrSkipped =
// ack-without-count (already metered); ErrPoison => Term; any other error => Nak (transient).
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

// upsert resolves the verbatim doc for an insert/replace/update and writes it by _id.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) upsert(ctx context.Context, ev oplogEvent, id any) error {
	doc, err := h.resolveDoc(ctx, ev, id)
	if err != nil {
		return err // includes ErrSkipped when the source doc vanished (already metered)
	}
	if derr := h.target.UpsertByID(ctx, ev.Collection, id, doc); derr != nil {
		return fmt.Errorf("upsert %s: %w", ev.Collection, derr)
	}
	h.metrics.onWrite(ctx, ev.Collection, "upsert")
	return nil
}

// resolveDoc returns the verbatim doc to upsert. insert/replace carry it inline; update — and a
// degraded insert/replace the connector couldn't encode — re-read source by _id (ErrSkipped if gone).
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) resolveDoc(ctx context.Context, ev oplogEvent, id any) (bson.D, error) {
	switch ev.Op {
	case "insert", "replace":
		if len(ev.FullDocument) == 0 {
			if !ev.Degraded {
				// The connector always carries the doc for insert/replace — a non-degraded empty one
				// is a contract violation that can never succeed on redelivery. Poison.
				return nil, fmt.Errorf("%w: %s without fullDocument", migration.ErrPoison, ev.Op)
			}
			// Degraded: the connector couldn't encode fullDocument (left nil) but still published.
			// Recover the live source doc by _id rather than drop it — mirrors oplog-collections-transformer.
			slog.Warn("recovering degraded insert/replace via source lookup",
				"collection", ev.Collection, "op", ev.Op, "eventId", ev.EventID,
				"reason", ev.DegradedReason, "request_id", natsutil.RequestIDFromContext(ctx))
			return h.resolveBySourceLookup(ctx, ev, id)
		}
		return decodeExtJSONDoc(ev.FullDocument)
	case "update":
		return h.resolveBySourceLookup(ctx, ev, id)
	default:
		// Unreachable — handle() dispatches only insert/replace/update here. Fail closed, not open.
		return nil, fmt.Errorf("%w: unexpected op %q in resolveDoc", migration.ErrPoison, ev.Op)
	}
}

// resolveBySourceLookup re-reads the full current source doc by _id (used by update and by a
// degraded insert/replace). Returns ErrSkipped when the doc vanished between the event and re-read.
//
//nolint:gocritic // ev passed by value to mirror resolveDoc's signature; off the hot path.
func (h *handler) resolveBySourceLookup(ctx context.Context, ev oplogEvent, id any) (bson.D, error) {
	strID, ok := id.(string)
	if !ok {
		// SourceLookup keys by string _id; a non-string _id (ObjectID/int) would silently mis-skip the
		// write (data loss), so poison loudly instead (design §12). Follow-up: widen SourceLookup to any.
		return nil, fmt.Errorf("%w: %s with non-string _id (%T) in %q not supported by source lookup",
			migration.ErrPoison, ev.Op, id, ev.Collection)
	}
	lk := h.lookups[ev.Collection]
	if lk == nil {
		return nil, fmt.Errorf("%w: no source lookup for collection %q", migration.ErrPoison, ev.Collection)
	}
	got, err := lk.FindByID(ctx, strID)
	if err != nil {
		return nil, fmt.Errorf("lookup %q: %w", strID, err)
	}
	if got == nil {
		h.metrics.onSkipped(ctx, ev.Op+"_gone")
		slog.Debug("skip — source doc vanished", "collection", ev.Collection, "op", ev.Op,
			"eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
		return nil, migration.ErrSkipped
	}
	return decodeExtJSONDoc(got)
}
