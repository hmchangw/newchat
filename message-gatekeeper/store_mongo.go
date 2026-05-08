package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

type MongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
	}
}

func (s *MongoStore) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	var sub model.Subscription
	filter := bson.M{"u.account": account, "roomId": roomID}
	if err := s.subscriptions.FindOne(ctx, filter).Decode(&sub); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("user %s not subscribed to room %s: %w", account, roomID, errNotSubscribed)
		}
		return nil, fmt.Errorf("find subscription for user %s in room %s: %w", account, roomID, err)
	}
	return &sub, nil
}

// GetRoomUserCount returns the room's userCount via a projected findOne. The
// admission rule is the only consumer and only needs the count, so we avoid
// pulling the rest of the Room document over the wire. Any error (including
// mongo.ErrNoDocuments) is wrapped and returned — the handler treats every
// failure here as an infrastructure error, since reaching this call already
// implies a subscription for the room exists.
func (s *MongoStore) GetRoomUserCount(ctx context.Context, roomID string) (int, error) {
	var doc struct {
		UserCount int `bson:"userCount"`
	}
	opts := options.FindOne().SetProjection(bson.M{"userCount": 1})
	if err := s.rooms.FindOne(ctx, bson.M{"_id": roomID}, opts).Decode(&doc); err != nil {
		return 0, fmt.Errorf("find user count for room %q: %w", roomID, err)
	}
	return doc.UserCount, nil
}
