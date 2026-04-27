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

type mongoStore struct {
	roomCol *mongo.Collection
	subCol  *mongo.Collection
}

func NewMongoStore(roomCol, subCol *mongo.Collection) *mongoStore {
	return &mongoStore{roomCol: roomCol, subCol: subCol}
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

func (m *mongoStore) FetchAndUpdateRoom(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool) (*model.Room, error) {
	fields := bson.M{
		"lastMsgAt": msgAt,
		"lastMsgId": msgID,
		"updatedAt": msgAt,
	}
	if mentionAll {
		fields["lastMentionAllAt"] = msgAt
	}
	filter := bson.M{"_id": roomID}
	update := bson.M{"$set": fields}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var room model.Room
	if err := m.roomCol.FindOneAndUpdate(ctx, filter, update, opts).Decode(&room); err != nil {
		return nil, fmt.Errorf("fetch and update room %s: %w", roomID, err)
	}
	return &room, nil
}

func (m *mongoStore) SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string) error {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": bson.M{"$in": accounts},
	}
	update := bson.M{"$set": bson.M{"hasMention": true}}
	_, err := m.subCol.UpdateMany(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("set subscription mentions for room %s: %w", roomID, err)
	}
	return nil
}
