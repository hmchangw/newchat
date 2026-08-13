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
