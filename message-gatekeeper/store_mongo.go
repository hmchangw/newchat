package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type MongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
	valkey        valkeyutil.Client // nil disables the L2 tier (pure Mongo)
	metaTTL       time.Duration
	metaRec       roommetacache.Recorder
}

func NewMongoStore(db *mongo.Database, valkey valkeyutil.Client, metaTTL time.Duration) *MongoStore {
	return &MongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
		valkey:        valkey,
		metaTTL:       metaTTL,
		metaRec:       cachemetrics.For("roommeta", "l2"),
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

func (s *MongoStore) GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error) {
	return roommetacache.ReadThrough(ctx, s.valkey, s.rooms, roomID, s.metaTTL, s.metaRec)
}
