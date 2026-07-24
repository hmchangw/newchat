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

type storeMongo struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
	users         *mongo.Collection
}

func newStoreMongo(db *mongo.Database) *storeMongo {
	return &storeMongo{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
		users:         db.Collection("users"),
	}
}

func (s *storeMongo) FindSubscription(ctx context.Context, roomID, userID string) (*Subscription, error) {
	var doc struct {
		RoomID string `bson:"roomId"`
		SiteID string `bson:"siteId"`
		User   struct {
			ID string `bson:"_id"`
		} `bson:"u"`
	}
	err := s.subscriptions.FindOne(ctx,
		bson.M{"roomId": roomID, "u._id": userID},
		options.FindOne().SetProjection(bson.M{"roomId": 1, "u._id": 1, "siteId": 1}),
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find subscription: %w", err)
	}
	return &Subscription{RoomID: doc.RoomID, UserID: doc.User.ID, SiteID: doc.SiteID}, nil
}

func (s *storeMongo) FindRoom(ctx context.Context, roomID string) (*Room, error) {
	var doc struct {
		ID     string `bson:"_id"`
		Type   string `bson:"t"`
		Name   string `bson:"name"`
		SiteID string `bson:"siteId"`
	}
	err := s.rooms.FindOne(ctx,
		bson.M{"_id": roomID},
		options.FindOne().SetProjection(bson.M{"_id": 1, "t": 1, "name": 1, "siteId": 1}),
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find room: %w", err)
	}
	return &Room{ID: doc.ID, Type: doc.Type, Name: doc.Name, SiteID: doc.SiteID}, nil
}

func (s *storeMongo) ListMemberIDs(ctx context.Context, roomID string) ([]string, error) {
	cur, err := s.subscriptions.Find(ctx,
		bson.M{"roomId": roomID},
		options.Find().SetProjection(bson.M{"u._id": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("list room members: %w", err)
	}
	var docs []struct {
		User struct {
			ID string `bson:"_id"`
		} `bson:"u"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode room members: %w", err)
	}
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = d.User.ID
	}
	return out, nil
}

func (s *storeMongo) FindUser(ctx context.Context, userID string) (*model.User, error) {
	var u model.User
	err := s.users.FindOne(ctx,
		bson.M{"_id": userID},
		options.FindOne().SetProjection(bson.M{
			"_id": 1, "account": 1, "siteId": 1, "engName": 1, "chineseName": 1, "roles": 1,
		}),
	).Decode(&u)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find user: %w", err)
	}
	return &u, nil
}
