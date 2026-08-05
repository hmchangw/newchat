package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/preview"
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
	appCol        *mongo.Collection
	valkey        valkeyutil.Client // nil disables the L2 tier (pure Mongo)
	metaTTL       time.Duration
	metaRec       roommetacache.Recorder
}

func NewMongoStore(roomCol, subCol, threadRoomCol, appCol *mongo.Collection, valkey valkeyutil.Client, metaTTL time.Duration) *mongoStore {
	return &mongoStore{
		roomCol: roomCol, subCol: subCol, threadRoomCol: threadRoomCol, appCol: appCol,
		valkey: valkey, metaTTL: metaTTL, metaRec: cachemetrics.For("roommeta", "l2"),
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

// roomLastMsgUpdateModel builds the per-room update. With a preview it becomes
// an aggregation-pipeline update so the watermark guard can compare against the
// stored previewAsOf; without one it stays a plain $set (system messages and
// lastMsgAt-only flushes must never touch the stored preview).
//
//nolint:gocritic // hugeParam: roomLastMsgUpdate is the map value type shared with the coalescer buffer; callers already hold it by value, so a pointer here would just relocate the copy.
func roomLastMsgUpdateModel(u roomLastMsgUpdate) any {
	fields := bson.M{
		"lastMsgAt": u.at,
		"lastMsgId": u.msgID,
		"updatedAt": u.at,
	}
	if !u.lastMentionAllAt.IsZero() {
		fields["lastMentionAllAt"] = u.lastMentionAllAt
	}
	if u.preview == nil {
		return bson.M{"$set": fields}
	}
	for k, v := range preview.GuardedSetFields(u.preview, u.previewAsOf) {
		fields[k] = v
	}
	// Pipeline form: plain values (time.Time, base62 ids) marshal as literals;
	// only the guarded preview fields are aggregation expressions.
	return mongo.Pipeline{{{Key: "$set", Value: fields}}}
}

func (m *mongoStore) UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool, pvw *model.PreviewMessage, previewAsOf int64) error {
	u := roomLastMsgUpdate{msgID: msgID, at: msgAt, preview: pvw, previewAsOf: previewAsOf}
	if mentionAll {
		u.lastMentionAllAt = msgAt
	}
	res, err := m.roomCol.UpdateOne(ctx, bson.M{"_id": roomID}, roomLastMsgUpdateModel(u))
	if err != nil {
		return fmt.Errorf("update room last message %s: %w", roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update room last message %s: %w", roomID, mongo.ErrNoDocuments)
	}
	return nil
}

// BulkUpdateRoomLastMessage applies a batch of room.lastMsgAt/lastMsgId
// (and, when present, watermark-guarded preview) updates in a single
// unordered BulkWrite. Missing rooms (MatchedCount==0 per model) are not
// surfaced — lastMsgAt is decorative and the source-of-truth message has
// already been persisted to Cassandra by message-worker.
func (m *mongoStore) BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(updates))
	for roomID, u := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": roomID}).
			SetUpdate(roomLastMsgUpdateModel(u)))
	}
	if _, err := m.roomCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk update room last message (%d rooms): %w", len(updates), err)
	}
	return nil
}

// SetRoomPreviewMessage persists a post-mutation (edit/delete) preview via a
// watermark-guarded aggregation-pipeline update so redeliveries/races cannot
// regress a newer stored preview.
func (m *mongoStore) SetRoomPreviewMessage(ctx context.Context, roomID string, pvw *model.PreviewMessage, asOf int64) error {
	if pvw == nil {
		return nil
	}
	pipeline := mongo.Pipeline{{{Key: "$set", Value: preview.GuardedSetFields(pvw, asOf)}}}
	if _, err := m.roomCol.UpdateOne(ctx, bson.M{"_id": roomID}, pipeline); err != nil {
		return fmt.Errorf("set room preview %s: %w", roomID, err)
	}
	return nil
}

// ClearRoomPreviewMessage removes the stored preview once a mutation left the room
// with no eligible survivor, via the same watermark-guarded pipeline update so a
// redelivered older write cannot resurrect it.
func (m *mongoStore) ClearRoomPreviewMessage(ctx context.Context, roomID string, asOf int64) error {
	pipeline := mongo.Pipeline{{{Key: "$set", Value: preview.GuardedClearFields(asOf)}}}
	if _, err := m.roomCol.UpdateOne(ctx, bson.M{"_id": roomID}, pipeline); err != nil {
		return fmt.Errorf("clear room preview %s: %w", roomID, err)
	}
	return nil
}

// AppNameByAccount returns the app display name for a bot account
// (assistant.name), or ("", nil) when no app matches.
func (m *mongoStore) AppNameByAccount(ctx context.Context, botAccount string) (string, error) {
	var doc struct {
		Name string `bson:"name"`
	}
	opts := options.FindOne().SetProjection(bson.M{"name": 1, "_id": 0})
	err := m.appCol.FindOne(ctx, bson.M{"assistant.name": botAccount}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}
		return "", fmt.Errorf("find app by assistant name: %w", err)
	}
	return doc.Name, nil
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
