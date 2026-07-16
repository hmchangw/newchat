package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// oplogEvent mirrors model.OplogEvent's wire shape (decoded from the consumed message),
// matching the message transformer's struct so both decode the connector output identically.
type oplogEvent struct {
	EventID           string          `json:"eventId"`
	Op                string          `json:"op"`
	Collection        string          `json:"coll"`
	DocumentKey       json.RawMessage `json:"documentKey"`
	ClusterTime       int64           `json:"clusterTime"` // source op time, unix ms.
	FullDocument      json.RawMessage `json:"fullDocument"`
	UpdateDescription json.RawMessage `json:"updateDescription"`
	// Degraded is true when the connector couldn't encode an opaque field (left nil) but still
	// published the event. The transformer recovers the missing field via a source lookup, not poison.
	Degraded       bool   `json:"degraded"`
	DegradedReason string `json:"degradedReason"`
}

// inboxPublisher publishes a model.InboxEvent into the local INBOX stream.
type inboxPublisher interface {
	Publish(ctx context.Context, evt model.InboxEvent) error
}

// targetStore is the new-stack per-site Mongo access the transformer needs.
type targetStore interface {
	// FindThreadRoom resolves the thread room for a parent message id, returning roomID,
	// threadRoomID, and the thread room's home siteID (thread-subs inherit the room's site, §6).
	FindThreadRoom(ctx context.Context, parentMessageID string) (roomID, threadRoomID, siteID string, found bool, err error)
	FindUserID(ctx context.Context, account string) (userID string, found bool, err error)
	// UpsertRoomMember replaces-or-inserts a migrated room-member doc keyed by its (source-adopted)
	// _id — idempotent under redelivery. DeleteRoomMember removes by _id; deleted is false when
	// the row was already absent (a no-op, not an error).
	UpsertRoomMember(ctx context.Context, rm model.RoomMember) error
	DeleteRoomMember(ctx context.Context, id string) (deleted bool, err error)
}

type handler struct {
	siteID          string
	roomsColl       string
	subsColl        string
	threadSubsColl  string
	roomMembersColl string
	pub             inboxPublisher
	target          targetStore
	// lookups re-read the current source doc on update events (the connector forwards only the
	// delta), keyed by source collection name — one SourceLookup per watched collection.
	lookups map[string]migration.SourceLookup
	metrics *metrics     // nil-safe
	now     func() int64 // injectable clock, defaults to time.Now().UTC().UnixMilli
}

// nowMillis returns the handler's current time in unix ms, defaulting to wall-clock when unset.
func (h *handler) nowMillis() int64 {
	if h.now != nil {
		return h.now()
	}
	return time.Now().UTC().UnixMilli()
}

// handle dispatches one decoded oplog event by collection. nil = ack+count; ErrSkipped =
// ack-without-count (already metered); ErrPoison => Term; any other error => Nak (transient).
//
//nolint:gocritic // ev passed by value: it's the decoded event the consume loop hands off, one per message off the hot path.
func (h *handler) handle(ctx context.Context, ev oplogEvent) error {
	switch ev.Collection {
	case h.roomsColl:
		return h.handleRoom(ctx, ev)
	case h.subsColl:
		return h.handleSubscription(ctx, ev)
	case h.threadSubsColl:
		return h.handleThreadSub(ctx, ev)
	case h.roomMembersColl:
		return h.handleRoomMember(ctx, ev)
	default:
		slog.Debug("skip non-migrated collection",
			"collection", ev.Collection, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "other_collection")
		return migration.ErrSkipped
	}
}

// resolveDoc returns the full current source doc for the event, or (nil, true, nil) to skip.
// insert/replace carry the doc inline; update re-reads by documentKey._id; delete is always skip
// (room-members intercepts deletes in handleRoomMember before calling here).
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) resolveDoc(ctx context.Context, ev oplogEvent) (doc []byte, skip bool, err error) {
	switch ev.Op {
	case "insert", "replace":
		if len(ev.FullDocument) == 0 {
			if !ev.Degraded {
				// The connector always carries the doc for insert/replace — a non-degraded missing
				// one is a contract violation that can never succeed on redelivery. Poison.
				return nil, false, fmt.Errorf("%w: %s without fullDocument", migration.ErrPoison, ev.Op)
			}
			// Degraded: the connector couldn't encode fullDocument (left nil) but still published.
			// Recover the live source doc by _id rather than drop it — mirrors oplog-transformer.
			slog.Warn("recovering degraded insert/replace via source lookup",
				"eventId", ev.EventID, "reason", ev.DegradedReason, "request_id", natsutil.RequestIDFromContext(ctx))
			return h.resolveBySourceLookup(ctx, ev)
		}
		return ev.FullDocument, false, nil
	case "update":
		return h.resolveBySourceLookup(ctx, ev)
	case "delete":
		// Un-actionable: only documentKey._id, and the destination doesn't key by source _id.
		return nil, true, nil
	default:
		// Unknown op — caller meters "unknown_op".
		return nil, true, nil
	}
}

// resolveBySourceLookup re-reads the full current source doc by documentKey._id — used for updates
// (the connector forwards only the delta) and degraded insert/replace (fullDocument couldn't encode).
// skip=true when the doc vanished from source between the event and our re-read.
//
//nolint:gocritic // ev passed by value to mirror resolveDoc's signature; off the hot path.
func (h *handler) resolveBySourceLookup(ctx context.Context, ev oplogEvent) (doc []byte, skip bool, err error) {
	id, idErr := documentKeyID(ev.DocumentKey)
	if idErr != nil {
		return nil, false, idErr
	}
	lk := h.lookups[ev.Collection]
	if lk == nil {
		// No source lookup for this collection is a misconfiguration (filter subjects and the
		// lookups map disagree) — it can never succeed. Poison.
		return nil, false, fmt.Errorf("%w: no source lookup for collection %q", migration.ErrPoison, ev.Collection)
	}
	got, lookupErr := lk.FindByID(ctx, id)
	if lookupErr != nil {
		return nil, false, fmt.Errorf("lookup %q: %w", id, lookupErr)
	}
	if got == nil {
		// Doc vanished from source between the change event and our re-read — nothing to apply.
		return nil, true, nil
	}
	return got, false, nil
}

// documentKeyID decodes documentKey → _id (the common string case). Returns migration.ErrPoison
// when missing/malformed — mirrors the message transformer's documentKeyID.
func documentKeyID(documentKey json.RawMessage) (string, error) {
	var key struct {
		ID string `json:"_id"`
	}
	if err := json.Unmarshal(documentKey, &key); err != nil || key.ID == "" {
		return "", fmt.Errorf("%w: bad documentKey", migration.ErrPoison)
	}
	return key.ID, nil
}

// siteIDFromOrigin returns the record's home siteId: the deployment's siteID when origin is
// absent or "local", else the first dotted label of the origin domain ("0030204.tchat..." → "0030204").
func siteIDFromOrigin(origin, deploymentSiteID string) string {
	if origin == "" || origin == "local" {
		return deploymentSiteID
	}
	if i := strings.IndexByte(origin, '.'); i >= 0 {
		return origin[:i]
	}
	return origin
}
