package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

type threadStoreMongo struct {
	threadRooms         *mongo.Collection
	threadSubscriptions *mongo.Collection
	subscriptions       *mongo.Collection
}

// Compile-time assertion that *threadStoreMongo satisfies ThreadStore.
var _ ThreadStore = (*threadStoreMongo)(nil)

func newThreadStoreMongo(db *mongo.Database) *threadStoreMongo {
	return &threadStoreMongo{
		threadRooms:         db.Collection("thread_rooms"),
		threadSubscriptions: db.Collection("thread_subscriptions"),
		subscriptions:       db.Collection("subscriptions"),
	}
}

// EnsureIndexes creates the unique indexes required by the thread store.
func (s *threadStoreMongo) EnsureIndexes(ctx context.Context) error {
	if _, err := s.threadRooms.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "parentMessageId", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure thread_rooms parentMessageId index: %w", err)
	}

	// Best-effort: drop the legacy (threadRoomId, userId) unique index so the new
	// (threadRoomId, userAccount) index can be created without a key conflict.
	// The collection or index may not exist (fresh deploy / test container) — ignore all errors.
	_ = s.threadSubscriptions.Indexes().DropOne(ctx, "threadRoomId_1_userId_1") //nolint:errcheck

	if _, err := s.threadSubscriptions.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "threadRoomId", Value: 1}, {Key: "userAccount", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure thread_subscriptions (threadRoomId,userAccount) index: %w", err)
	}

	return nil
}

// EnsureThreadRoom resolves the thread room for room.ParentMessageID in a single round
// trip: an upserting FindOneAndUpdate whose $setOnInsert seeds the new room only when
// absent, returning the post-image either way. The common subsequent-reply path matches
// the existing room (no insert, no dup-key); the first reply inserts and returns it. created
// is reported by comparing the returned _id to the candidate's — they match only on insert.
// A rare concurrent first-reply can still surface a duplicate-key error (two upserts racing
// the unique parentMessageId index); we resolve it by reading the now-existing room.
func (s *threadStoreMongo) EnsureThreadRoom(ctx context.Context, room *model.ThreadRoom) (*model.ThreadRoom, bool, error) {
	candidate := *room
	if candidate.ReplyAccounts == nil {
		candidate.ReplyAccounts = []string{}
	}
	filter := bson.M{"parentMessageId": candidate.ParentMessageID}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	var stored model.ThreadRoom
	err := s.threadRooms.FindOneAndUpdate(ctx, filter, bson.M{"$setOnInsert": candidate}, opts).Decode(&stored)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Lost the insert race to a concurrent first reply: the room now exists, read it.
			if ferr := s.threadRooms.FindOne(ctx, filter).Decode(&stored); ferr != nil {
				return nil, false, fmt.Errorf("read thread room after upsert race for parent %s: %w", candidate.ParentMessageID, ferr)
			}
			return &stored, false, nil
		}
		return nil, false, fmt.Errorf("ensure thread room for parent %s: %w", candidate.ParentMessageID, err)
	}
	return &stored, stored.ID == candidate.ID, nil
}

func (s *threadStoreMongo) InsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error {
	if _, err := s.threadSubscriptions.InsertOne(ctx, sub); err != nil {
		return fmt.Errorf("insert thread subscription: %w", err)
	}
	return nil
}

// UpsertThreadSubscription inserts sub if no document exists for (threadRoomId, userAccount);
// otherwise it is a no-op. $setOnInsert ensures existing subscriptions are never overwritten.
func (s *threadStoreMongo) UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error {
	filter := bson.M{"threadRoomId": sub.ThreadRoomID, "userAccount": sub.UserAccount}
	update := bson.M{"$setOnInsert": sub}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert thread subscription: %w", err)
	}
	return nil
}

// MarkThreadSubscriptionMention sets hasMention=true, skipping subs that already
// read past sub.CreatedAt (else this async write clobbers a read-clear, #467).
// New subs go via $setOnInsert on the upsert; existing ones get a separate
// guarded, non-upsert update, so an already-read sub can't be upserted twice.
func (s *threadStoreMongo) MarkThreadSubscriptionMention(ctx context.Context, sub *model.ThreadSubscription) error {
	filter := bson.M{"threadRoomId": sub.ThreadRoomID, "userAccount": sub.UserAccount}
	upsert := bson.M{
		"$setOnInsert": bson.M{
			"_id":             sub.ID,
			"parentMessageId": sub.ParentMessageID,
			"roomId":          sub.RoomID,
			"threadRoomId":    sub.ThreadRoomID,
			"userId":          sub.UserID,
			"userAccount":     sub.UserAccount,
			"siteId":          sub.SiteID,
			"lastSeenAt":      sub.LastSeenAt,
			"hasMention":      true,
			"createdAt":       sub.CreatedAt,
			"updatedAt":       sub.UpdatedAt,
		},
	}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, filter, upsert, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("mark thread subscription mention: %w", err)
	}

	guardedFilter := bson.M{
		"threadRoomId": sub.ThreadRoomID,
		"userAccount":  sub.UserAccount,
		// $not/$gte (not $lt): $lt is type-bracketed and won't match a null
		// lastSeenAt (never-read sub), which must still be flagged.
		"lastSeenAt": bson.M{"$not": bson.M{"$gte": sub.CreatedAt}},
	}
	guardedSet := bson.M{"$set": bson.M{"hasMention": true, "updatedAt": sub.UpdatedAt}}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, guardedFilter, guardedSet); err != nil {
		return fmt.Errorf("mark thread subscription mention (existing): %w", err)
	}
	return nil
}

func (s *threadStoreMongo) UpdateThreadRoomLastMessage(ctx context.Context, threadRoomID, lastMsgID string, replyAccounts []string, lastMsgAt time.Time) error {
	update := bson.M{
		"$set": bson.M{
			"lastMsgAt": lastMsgAt,
			"lastMsgId": lastMsgID,
			"updatedAt": lastMsgAt,
		},
	}
	if len(replyAccounts) > 0 {
		update["$addToSet"] = bson.M{"replyAccounts": bson.M{"$each": replyAccounts}}
	}
	if _, err := s.threadRooms.UpdateOne(ctx, bson.M{"_id": threadRoomID}, update); err != nil {
		return fmt.Errorf("update thread room last message: %w", err)
	}
	return nil
}

// AdvanceThreadSubscriptionLastSeen advances the replier's lastSeenAt via $max so it
// never regresses a replier who already read later; missing sub is a best-effort no-op.
func (s *threadStoreMongo) AdvanceThreadSubscriptionLastSeen(ctx context.Context, threadRoomID, account string, at time.Time) error {
	if _, err := s.threadSubscriptions.UpdateOne(ctx,
		bson.M{"threadRoomId": threadRoomID, "userAccount": account},
		bson.M{"$max": bson.M{"lastSeenAt": at}},
	); err != nil {
		return fmt.Errorf("advance thread lastSeenAt for %q in thread room %q: %w", account, threadRoomID, err)
	}
	return nil
}

// UpsertThreadSubscriptionAdvancingLastSeen creates the (threadRoomId, userAccount)
// subscription via $setOnInsert when missing and advances its lastSeenAt to at via $max,
// in one write. It folds UpsertThreadSubscription + AdvanceThreadSubscriptionLastSeen for
// the replier on the hot path. lastSeenAt is owned exclusively by $max (never $setOnInsert)
// so the two operators don't conflict: a new sub is seeded with lastSeenAt=at, an existing
// one is moved forward only (never backward).
func (s *threadStoreMongo) UpsertThreadSubscriptionAdvancingLastSeen(ctx context.Context, sub *model.ThreadSubscription, at time.Time) error {
	filter := bson.M{"threadRoomId": sub.ThreadRoomID, "userAccount": sub.UserAccount}
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":             sub.ID,
			"parentMessageId": sub.ParentMessageID,
			"roomId":          sub.RoomID,
			"threadRoomId":    sub.ThreadRoomID,
			"userId":          sub.UserID,
			"userAccount":     sub.UserAccount,
			"siteId":          sub.SiteID,
			"hasMention":      sub.HasMention,
			"createdAt":       sub.CreatedAt,
			"updatedAt":       sub.UpdatedAt,
		},
		"$max": bson.M{"lastSeenAt": at},
	}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert thread subscription advancing lastSeen: %w", err)
	}
	return nil
}

func (s *threadStoreMongo) AddReplyAccounts(ctx context.Context, threadRoomID string, accounts []string) error {
	if len(accounts) == 0 {
		return nil
	}
	_, err := s.threadRooms.UpdateOne(ctx, bson.M{"_id": threadRoomID}, bson.M{
		"$addToSet": bson.M{"replyAccounts": bson.M{"$each": accounts}},
	})
	if err != nil {
		return fmt.Errorf("add reply accounts to thread room %s: %w", threadRoomID, err)
	}
	return nil
}

func (s *threadStoreMongo) GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error) {
	out := make(map[string]*time.Time, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	filter := bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}}
	opts := options.Find().SetProjection(bson.M{"u.account": 1, "historySharedSince": 1, "_id": 0})
	cursor, err := s.subscriptions.Find(ctx, filter, opts)
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
