package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// EnsureIndexes creates indexes that back the store's read paths.
// Must be called once at startup; index creation is idempotent when the key
// spec matches.
func (m *mongoStore) EnsureIndexes(ctx context.Context) error {
	if _, err := m.threadRoomCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "parentMessageId", Value: 1}, {Key: "siteId", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure thread_rooms (parentMessageId, siteId) index: %w", err)
	}
	return nil
}

type mongoStore struct {
	roomCol       *mongo.Collection
	subCol        *mongo.Collection
	threadRoomCol *mongo.Collection
	valkey        valkeyutil.Client // nil disables the L2 tier (pure Mongo)
	metaTTL       time.Duration
	metaRec       roommetacache.Recorder
}

func NewMongoStore(roomCol, subCol, threadRoomCol *mongo.Collection, valkey valkeyutil.Client, metaTTL time.Duration) *mongoStore {
	return &mongoStore{
		roomCol:       roomCol,
		subCol:        subCol,
		threadRoomCol: threadRoomCol,
		valkey:        valkey,
		metaTTL:       metaTTL,
		metaRec:       cachemetrics.For("roommeta", "l2"),
	}
}

func (m *mongoStore) GetRoom(ctx context.Context, roomID string) (*model.Room, error) {
	filter := bson.M{"_id": roomID}
	// Project only the fields consumers read: the delete/edit/pin/react fan-out (id, type,
	// siteId, accounts) and the coalescer overlay + rewind (the lastMsg pointer/preview).
	// _id is returned by default. Extend this set when a new reader needs another field —
	// an unprojected field would silently decode to its zero value.
	opts := options.FindOne().SetProjection(bson.M{
		"type": 1, "siteId": 1, "accounts": 1,
		"lastMsgAt": 1, "lastMsgId": 1, "lastMsg": 1,
	})
	var room model.Room
	if err := m.roomCol.FindOne(ctx, filter, opts).Decode(&room); err != nil {
		return nil, fmt.Errorf("find room %s: %w", roomID, err)
	}
	return &room, nil
}

func (m *mongoStore) ListSubscriptions(ctx context.Context, roomID string) ([]model.Subscription, error) {
	filter := bson.M{"roomId": roomID}
	// The only consumer (publishDMEvents) reads just the member account, so project u.account
	// and drop _id. Every other Subscription field decodes to its zero value by design — extend
	// the projection if a future caller needs more.
	opts := options.Find().SetProjection(bson.M{"u.account": 1, "_id": 0})
	cursor, err := m.subCol.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query subscriptions for room %s: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	var subs []model.Subscription
	if err := cursor.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("decode subscriptions: %w", err)
	}
	return subs, nil
}

func (m *mongoStore) GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error) {
	return roommetacache.ReadThrough(ctx, m.valkey, m.roomCol, roomID, m.metaTTL, m.metaRec)
}

// UpdateRoomLastMessage advances one room's lastMsg* pointer/preview via the same monotonic
// aggregation pipeline as BulkUpdateRoomLastMessage, so an out-of-order message can't regress
// the pointer/preview and updatedAt/lastMentionAllAt keep the max. In production the coalescer
// shadows this method and flushes via BulkUpdateRoomLastMessage; this direct path is kept
// guard-consistent for any non-coalesced caller.
func (m *mongoStore) UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool, preview *model.LastMessagePreview) error {
	// ptrAdvance: stored pointer time missing or strictly older than this update.
	ptrAdvance := bson.M{"$lt": bson.A{bson.M{"$ifNull": bson.A{"$lastMsgAt", nil}}, msgAt}}
	set := bson.M{
		"lastMsgId": bson.M{"$cond": bson.A{ptrAdvance, msgID, "$lastMsgId"}},
		"lastMsgAt": bson.M{"$cond": bson.A{ptrAdvance, msgAt, "$lastMsgAt"}},
		"updatedAt": bson.M{"$max": bson.A{"$updatedAt", msgAt}},
	}
	if preview != nil {
		// prevAdvance is independent of the pointer: a user preview can advance while a newer
		// system message owns the pointer (drift). $literal so a body starting with '$' isn't
		// read as a field path.
		prevAdvance := bson.M{"$lt": bson.A{bson.M{"$ifNull": bson.A{"$lastMsg.createdAt", nil}}, preview.CreatedAt}}
		set["lastMsg"] = bson.M{"$cond": bson.A{prevAdvance, bson.M{"$literal": preview}, "$lastMsg"}}
	}
	if mentionAll {
		set["lastMentionAllAt"] = bson.M{"$max": bson.A{"$lastMentionAllAt", msgAt}}
	}
	filter := bson.M{"_id": roomID}
	res, err := m.roomCol.UpdateOne(ctx, filter, mongo.Pipeline{bson.D{{Key: "$set", Value: set}}})
	if err != nil {
		return fmt.Errorf("update room last message %s: %w", roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update room last message %s: %w", roomID, mongo.ErrNoDocuments)
	}
	return nil
}

// BulkUpdateRoomLastMessage applies buffered room.lastMsg* updates in one
// unordered BulkWrite. Missing rooms aren't surfaced — lastMsgAt is decorative.
//
// Each write is an aggregation pipeline so the pointer (lastMsgId/lastMsgAt) and
// the preview (lastMsg) advance MONOTONICALLY: an older create landing after a
// newer batch already flushed (the pending map is swapped empty each flush, so it
// can't see in-flight state) cannot regress them. updatedAt/lastMentionAllAt keep
// the max. Equal timestamps keep the stored value — the in-memory coalescer already
// tie-broke by message_id before the flush, and equal ts across a flush is negligible.
func (m *mongoStore) BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(updates))
	for roomID, u := range updates {
		// updatedAt = newest mutation, not pointer time (a rewind moves the pointer back).
		updatedAt := u.touchedAt
		if updatedAt.IsZero() {
			updatedAt = u.at
		}
		// ptrAdvance: stored pointer time is missing or strictly older than this update.
		ptrAdvance := bson.M{"$lt": bson.A{bson.M{"$ifNull": bson.A{"$lastMsgAt", nil}}, u.at}}
		set := bson.M{
			"lastMsgId": bson.M{"$cond": bson.A{ptrAdvance, u.msgID, "$lastMsgId"}},
			"lastMsgAt": bson.M{"$cond": bson.A{ptrAdvance, u.at, "$lastMsgAt"}},
			"updatedAt": bson.M{"$max": bson.A{"$updatedAt", updatedAt}},
		}
		if u.preview != nil {
			// prevAdvance is independent of the pointer: a user preview can advance while a
			// newer system message owns the pointer (drift). $literal so message bodies that
			// start with '$' aren't read as field paths.
			prevAdvance := bson.M{"$lt": bson.A{bson.M{"$ifNull": bson.A{"$lastMsg.createdAt", nil}}, u.preview.CreatedAt}}
			set["lastMsg"] = bson.M{"$cond": bson.A{prevAdvance, bson.M{"$literal": u.preview}, "$lastMsg"}}
		}
		if !u.lastMentionAllAt.IsZero() {
			set["lastMentionAllAt"] = bson.M{"$max": bson.A{"$lastMentionAllAt", u.lastMentionAllAt}}
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": roomID}).
			SetUpdate(mongo.Pipeline{bson.D{{Key: "$set", Value: set}}}))
	}
	if _, err := m.roomCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk update room last message (%d rooms): %w", len(updates), err)
	}
	return nil
}

// RewindRoomLastMessage rewinds last-message state after a delete (guarded no-op
// if a newer message won): phase 1 repoints the trio on lastMsgId match; phase 2 patches only lastMsg in the drift case.
func (m *mongoStore) RewindRoomLastMessage(ctx context.Context, roomID, deletedMsgID string, pointer *model.LastMessagePointer, survivor *model.LastMessagePreview, updatedAt time.Time) error {
	// pointer set ⊇ preview set: a non-nil survivor implies a pointer — derive it
	// when a pre-pointer peer (rolling deploy) omitted it.
	if pointer == nil && survivor != nil {
		pointer = &model.LastMessagePointer{MessageID: survivor.MessageID, CreatedAt: survivor.CreatedAt}
	}
	filter := bson.M{"_id": roomID, "lastMsgId": deletedMsgID}
	var update bson.M
	if pointer != nil {
		set := bson.M{
			"lastMsgAt": pointer.CreatedAt,
			"lastMsgId": pointer.MessageID,
			"updatedAt": updatedAt,
		}
		if survivor != nil {
			set["lastMsg"] = survivor
			update = bson.M{"$set": set}
		} else {
			// Only system messages survive: pointer moves to the notice, preview clears.
			update = bson.M{"$set": set, "$unset": bson.M{"lastMsg": ""}}
		}
	} else {
		// Nothing survives: mirror a fresh room (lastMentionAllAt untouched).
		update = bson.M{
			"$set":   bson.M{"lastMsgId": "", "updatedAt": updatedAt},
			"$unset": bson.M{"lastMsgAt": "", "lastMsg": ""},
		}
	}
	res, err := m.roomCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("rewind room last message %s: %w", roomID, err)
	}
	if res.MatchedCount > 0 {
		return nil
	}

	// Phase 2: preview-only rewind for the drift state.
	previewFilter := bson.M{"_id": roomID, "lastMsg.messageId": deletedMsgID}
	var previewUpdate bson.M
	if survivor != nil {
		previewUpdate = bson.M{"$set": bson.M{"lastMsg": survivor, "updatedAt": updatedAt}}
	} else {
		previewUpdate = bson.M{
			"$set":   bson.M{"updatedAt": updatedAt},
			"$unset": bson.M{"lastMsg": ""},
		}
	}
	if _, err := m.roomCol.UpdateOne(ctx, previewFilter, previewUpdate); err != nil {
		return fmt.Errorf("rewind room last message preview %s: %w", roomID, err)
	}
	return nil
}

// SetRoomLastMessageEdited patches lastMsg after an edit, guarded on lastMsg.messageId
// (not lastMsgId) so legacy rooms get no partial subdoc. encMsg!=nil sets the ciphertext, nil $unsets it.
func (m *mongoStore) SetRoomLastMessageEdited(ctx context.Context, roomID, editedMsgID, newMsg string, encMsg json.RawMessage, editedAt time.Time) error {
	filter := bson.M{"_id": roomID, "lastMsg.messageId": editedMsgID}
	set := bson.M{"lastMsg.msg": newMsg, "lastMsg.editedAt": editedAt}
	// $max (not $set) advances rooms.updatedAt without regressing a concurrently-newer value.
	update := bson.M{"$set": set, "$max": bson.M{"updatedAt": editedAt}}
	if encMsg != nil {
		set["lastMsg.encMsg"] = encMsg
	} else {
		update["$unset"] = bson.M{"lastMsg.encMsg": ""}
	}
	if _, err := m.roomCol.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("set room last message edited %s: %w", roomID, err)
	}
	return nil
}

// subscriptionMentionsFilter matches subs that have NOT already read past
// msgCreatedAt. $not/$gte (not $lt) so it still matches a missing/null
// lastSeenAt — plain $lt skips missing fields, wrongly excluding never-read subs (#467).
func subscriptionMentionsFilter(roomID string, accounts []string, msgCreatedAt time.Time) bson.M {
	return bson.M{
		"roomId":     roomID,
		"u.account":  bson.M{"$in": accounts},
		"lastSeenAt": bson.M{"$not": bson.M{"$gte": msgCreatedAt}},
	}
}

func (m *mongoStore) SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string, msgCreatedAt time.Time) error {
	filter := subscriptionMentionsFilter(roomID, accounts, msgCreatedAt)
	update := bson.M{"$set": bson.M{"hasMention": true}}
	_, err := m.subCol.UpdateMany(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("set subscription mentions for room %s: %w", roomID, err)
	}
	return nil
}

// AdvanceSubscriptionLastSeen advances the sender's lastSeenAt via $max so it
// never regresses a sender who already read later. A missing subscription is a
// best-effort no-op (MatchedCount unchecked).
func (m *mongoStore) AdvanceSubscriptionLastSeen(ctx context.Context, roomID, account string, at time.Time) error {
	if _, err := m.subCol.UpdateOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$max": bson.M{"lastSeenAt": at}},
	); err != nil {
		return fmt.Errorf("advance lastSeenAt for %q in room %q: %w", account, roomID, err)
	}
	return nil
}

func (m *mongoStore) GetThreadFollowers(ctx context.Context, parentMessageID string) (map[string]struct{}, error) {
	var doc struct {
		ReplyAccounts []string `bson:"replyAccounts"`
	}
	opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "_id": 0})
	err := m.threadRoomCol.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return map[string]struct{}{}, nil
		}
		return nil, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	out := make(map[string]struct{}, len(doc.ReplyAccounts))
	for _, a := range doc.ReplyAccounts {
		if a != "" {
			out[a] = struct{}{}
		}
	}
	return out, nil
}

func (m *mongoStore) GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error) {
	out := make(map[string]*time.Time, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	filter := bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}}
	opts := options.Find().SetProjection(bson.M{"u.account": 1, "historySharedSince": 1, "_id": 0})
	cursor, err := m.subCol.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query history windows for room %s: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	// Minimal decode shape: the projection returns only u.account + historySharedSince,
	// so decode just those rather than the full model.SubscriptionUser (whose other
	// fields would silently be zero-valued).
	var rows []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
		HistorySharedSince *time.Time `bson:"historySharedSince"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode history windows: %w", err)
	}
	for i := range rows {
		out[rows[i].User.Account] = rows[i].HistorySharedSince
	}
	return out, nil
}
