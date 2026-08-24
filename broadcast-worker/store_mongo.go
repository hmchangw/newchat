package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

// EnsureIndexes creates the store's read-path indexes; idempotent, call once at startup.
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
	// Switches the room-doc update from a plain $set to the guarded pipeline.
	previews bool
}

func NewMongoStore(roomCol, subCol, threadRoomCol *mongo.Collection, valkey valkeyutil.Client, metaTTL time.Duration, previews bool) *mongoStore {
	return &mongoStore{
		roomCol:       roomCol,
		subCol:        subCol,
		threadRoomCol: threadRoomCol,
		valkey:        valkey,
		metaTTL:       metaTTL,
		metaRec:       cachemetrics.For("roommeta", "l2"),
		previews:      previews,
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

//nolint:gocritic // hugeParam: roomLastMessage is the Store.UpdateRoomLastMessage contract shared with the coalescer and the mock.
func (m *mongoStore) UpdateRoomLastMessage(ctx context.Context, upd roomLastMessage) error {
	u := asBuffered(upd)
	res, err := m.roomCol.UpdateOne(ctx, bson.M{"_id": upd.RoomID}, m.lastMessageUpdate(&u))
	if err != nil {
		return fmt.Errorf("update room last message %s: %w", upd.RoomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update room last message %s: %w", upd.RoomID, mongo.ErrNoDocuments)
	}
	return nil
}

// asBuffered renders a direct update in the buffered form so both paths share one
// builder. Every field must be carried: one dropped silently changes the write.
//
//nolint:gocritic // hugeParam: matches UpdateRoomLastMessage's by-value contract.
func asBuffered(upd roomLastMessage) roomLastMsgUpdate {
	return roomLastMsgUpdate{
		msgID:            upd.MsgID,
		at:               upd.At,
		lastMentionAllAt: mentionAllAt(upd),
		pvw:              upd.Preview,
		pvwFailed:        upd.PreviewFailed,
		pvwAt:            upd.At,
	}
}

// mentionAllAt renders the flag as the timestamp the buffered form carries.
//
//nolint:gocritic // hugeParam: matches UpdateRoomLastMessage's by-value contract.
func mentionAllAt(upd roomLastMessage) time.Time {
	if !upd.MentionAll {
		return time.Time{}
	}
	return upd.At
}

// BulkUpdateRoomLastMessage applies a batch of room updates in one unordered BulkWrite.
// Missing rooms are not surfaced — the message is already persisted to Cassandra.
func (m *mongoStore) BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(updates))
	for roomID, u := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": roomID}).
			SetUpdate(m.lastMessageUpdate(&u)))
	}
	if _, err := m.roomCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk update room last message (%d rooms): %w", len(updates), err)
	}
	return nil
}

// lastMessageUpdate renders one room's update: $set with previews off, a pipeline with
// them on. lastMsgAt/lastMsgId stay unguarded — a bad future one would freeze ordering.
func (m *mongoStore) lastMessageUpdate(u *roomLastMsgUpdate) any {
	fields := bson.M{
		"lastMsgAt": u.at,
		"lastMsgId": u.msgID,
		"updatedAt": u.at,
	}
	if !u.lastMentionAllAt.IsZero() {
		fields["lastMentionAllAt"] = u.lastMentionAllAt
	}
	if !m.previews {
		return bson.M{"$set": fields}
	}

	// A bare "$"-prefixed string reads as a field path in a pipeline stage.
	for k, v := range fields {
		if s, ok := v.(string); ok {
			fields[k] = bson.M{"$literal": s}
		}
	}
	asOf := u.at.UnixMilli()
	// The preview rides its own clock, and the flush must not collapse the two. u.at names
	// the room's NEWEST message, which may be a later ineligible one; the body was
	// established at pvwAt. Ordering the body by u.at would claim it is as-of a moment it
	// knows nothing about, and would outrank a mutation that landed in between carrying the
	// corrected body — restoring stale content under a key that then equals lastMsgId, so
	// the reader serves it as current. Losing to that mutation instead only costs a walk.
	pvwAsOf := u.pvwAt.UnixMilli()
	var pvwFields bson.M
	switch {
	case u.pvwFailed:
		// The stored body is the PREVIOUS message's and opens under any key later pointing at
		// it, so withholding is not enough — the next ineligible message would revalidate it.
		pvwFields = preview.GuardedClearFields(pvwAsOf)
	case u.pvw != nil:
		// The KEY takes the newest message's identity, the WATERMARK the preview's own clock:
		// "what is this body paired with" and "when was it established" are different
		// questions, and a later ineligible message answers only the first.
		sealed := *u.pvw
		sealed.ForMsgID = u.msgID
		pvwFields = preview.GuardedSetFields(sealed, pvwAsOf)
	default:
		// The key advance IS the newest message's event, so it keeps that clock.
		pvwFields = preview.GuardedAdvanceKeyFields(u.msgID, asOf)
	}
	maps.Copy(fields, pvwFields)
	return mongo.Pipeline{{{Key: "$set", Value: fields}}}
}

// subscriptionMentionsFilter matches subs that have NOT already read past msgCreatedAt.
// $not/$gte, not $lt: plain $lt skips missing fields, excluding never-read subs (#467).
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

// AdvanceSubscriptionLastSeen advances lastSeenAt via $max so it never regresses a
// sender who already read later. A missing subscription is a best-effort no-op.
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
	// Decode only the projected fields — model.SubscriptionUser's rest would be zero.
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
