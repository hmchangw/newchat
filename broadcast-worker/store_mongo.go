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
	var room model.Room
	if err := m.roomCol.FindOne(ctx, filter).Decode(&room); err != nil {
		return nil, fmt.Errorf("find room %s: %w", roomID, err)
	}
	return &room, nil
}

func (m *mongoStore) ListSubscriptions(ctx context.Context, roomID string) ([]model.Subscription, error) {
	filter := bson.M{"roomId": roomID}
	cursor, err := m.subCol.Find(ctx, filter)
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

func (m *mongoStore) UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool, preview *model.LastMessagePreview) error {
	fields := bson.M{
		"lastMsgAt": msgAt,
		"lastMsgId": msgID,
		"updatedAt": msgAt,
	}
	if preview != nil {
		fields["lastMsg"] = preview
	}
	if mentionAll {
		fields["lastMentionAllAt"] = msgAt
	}
	filter := bson.M{"_id": roomID}
	update := bson.M{"$set": fields}

	res, err := m.roomCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update room last message %s: %w", roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update room last message %s: %w", roomID, mongo.ErrNoDocuments)
	}
	return nil
}

// BulkUpdateRoomLastMessage applies a batch of room.lastMsgAt/lastMsgId
// updates in a single unordered BulkWrite. Missing rooms (MatchedCount==0
// per model) are not surfaced — lastMsgAt is decorative and the source-of-
// truth message has already been persisted to Cassandra by message-worker.
func (m *mongoStore) BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(updates))
	for roomID, u := range updates {
		// updatedAt reflects the newest MUTATION, not the pointer time — a
		// rewind can move the pointer backwards without regressing updatedAt.
		updatedAt := u.touchedAt
		if updatedAt.IsZero() {
			updatedAt = u.at
		}
		fields := bson.M{
			"lastMsgAt": u.at,
			"lastMsgId": u.msgID,
			"updatedAt": updatedAt,
		}
		if u.preview != nil {
			fields["lastMsg"] = u.preview
		}
		if !u.lastMentionAllAt.IsZero() {
			fields["lastMentionAllAt"] = u.lastMentionAllAt
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": roomID}).
			SetUpdate(bson.M{"$set": fields}))
	}
	if _, err := m.roomCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk update room last message (%d rooms): %w", len(updates), err)
	}
	return nil
}

// RewindRoomLastMessage rewinds the room's last-message state after a delete,
// in two guarded phases (MatchedCount==0 on both is a benign no-op — a
// concurrent newer message won):
//
//  1. The deleted message was the room's newest overall (lastMsgId matches):
//     rewind lastMsgId/lastMsgAt to pointer (the newest surviving message of
//     ANY type, system notices included — room sorting must not skip past a
//     system message) and lastMsg to survivor (the newest surviving
//     non-system message). pointer == nil means nothing survives: mirror a
//     fresh room.
//  2. Drift state — a newer system message owns lastMsgId but lastMsg still
//     previews the deleted message (system messages advance the pointer
//     without replacing the preview): replace ONLY lastMsg; the pointer pair
//     keeps tracking the newest message including system notices.
//
// Known benign race: the create-path lastMsg write is coalesced (~250ms
// flush), so a delete arriving before the create flush of the same message
// can leave the old pointer in place; the coalescer purges its buffered entry
// (see coalescingStore.RewindRoomLastMessage) and the next event self-heals
// the pointer. A created-event redelivered AFTER its delete can still
// re-buffer the dead message — unchanged from the pre-preview behavior of
// lastMsgId/lastMsgAt.
func (m *mongoStore) RewindRoomLastMessage(ctx context.Context, roomID, deletedMsgID string, pointer *model.LastMessagePointer, survivor *model.LastMessagePreview, updatedAt time.Time) error {
	// Defensive: the pointer set is a superset of the preview set, so a
	// non-nil survivor implies a pointer exists — derive it when a caller
	// (e.g. a pre-pointer peer during a rolling deploy) omitted it.
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
			// Only system messages survive: the pointer moves to the system
			// notice, the preview clears.
			update = bson.M{"$set": set, "$unset": bson.M{"lastMsg": ""}}
		}
	} else {
		// No surviving message: mirror a fresh room (lastMsgId empty string,
		// lastMsgAt/lastMsg absent). lastMentionAllAt is deliberately untouched.
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

// SetRoomLastMessageEdited patches the denormalized lastMsg preview after an
// edit, guarded on lastMsg.messageId — the identity of the PREVIEW, not
// lastMsgId — which covers three cases in one clause: (a) the edited message
// is no longer previewed (benign no-op), (b) legacy rooms with lastMsgId but
// no lastMsg subdoc (no partial preview may be fabricated), (c) drift rooms
// where a newer system message owns lastMsgId but the preview still shows the
// edited user message (must be patched).
// encMsg != nil (encrypted room) $sets lastMsg.encMsg with newMsg==""; encMsg
// == nil (plaintext room / content-less edit) $unsets lastMsg.encMsg so a
// stale ciphertext never survives a plaintext patch.
func (m *mongoStore) SetRoomLastMessageEdited(ctx context.Context, roomID, editedMsgID, newMsg string, encMsg json.RawMessage, editedAt time.Time) error {
	filter := bson.M{"_id": roomID, "lastMsg.messageId": editedMsgID}
	set := bson.M{"lastMsg.msg": newMsg, "lastMsg.editedAt": editedAt}
	update := bson.M{"$set": set}
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
