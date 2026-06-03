package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

var (
	errThreadRoomExists   = errors.New("thread room already exists")
	errThreadRoomNotFound = errors.New("thread room not found")
)

type threadStoreMongo struct {
	threadRooms         *mongo.Collection
	threadSubscriptions *mongo.Collection
}

// Compile-time assertion that *threadStoreMongo satisfies ThreadStore.
var _ ThreadStore = (*threadStoreMongo)(nil)

func newThreadStoreMongo(db *mongo.Database) *threadStoreMongo {
	return &threadStoreMongo{
		threadRooms:         db.Collection("thread_rooms"),
		threadSubscriptions: db.Collection("thread_subscriptions"),
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

func (s *threadStoreMongo) CreateThreadRoom(ctx context.Context, room *model.ThreadRoom) error {
	toInsert := *room
	if toInsert.ReplyAccounts == nil {
		toInsert.ReplyAccounts = []string{}
	}
	_, err := s.threadRooms.InsertOne(ctx, &toInsert)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("insert thread room: %w", errThreadRoomExists)
		}
		return fmt.Errorf("insert thread room: %w", err)
	}
	return nil
}

func (s *threadStoreMongo) GetThreadRoomByParentMessageID(ctx context.Context, parentMessageID string) (*model.ThreadRoom, error) {
	var room model.ThreadRoom
	if err := s.threadRooms.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}).Decode(&room); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, errThreadRoomNotFound)
		}
		return nil, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	return &room, nil
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

// MarkThreadSubscriptionMention sets hasMention=true on the (threadRoomId, userAccount)
// subscription. $setOnInsert / $set split: on insert all fields are written; on update
// only hasMention and updatedAt change so existing subscription state is preserved.
func (s *threadStoreMongo) MarkThreadSubscriptionMention(ctx context.Context, sub *model.ThreadSubscription) error {
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
			"lastSeenAt":      sub.LastSeenAt,
			"createdAt":       sub.CreatedAt,
		},
		"$set": bson.M{
			"hasMention": true,
			"updatedAt":  sub.UpdatedAt,
		},
	}
	if _, err := s.threadSubscriptions.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("mark thread subscription mention: %w", err)
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
