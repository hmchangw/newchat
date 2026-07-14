package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
	uploads       *mongo.Collection
}

// NewMongoStore returns a Store backed by the subscriptions, rooms, and uploads collections.
func NewMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
		uploads:       db.Collection("uploads"),
	}
}

func (s *mongoStore) IsMember(ctx context.Context, roomID, account string) (bool, error) {
	// Existence check — a projected FindOne is lighter than CountDocuments
	// (which runs an aggregation) and stops at the first index match.
	err := s.subscriptions.FindOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		options.FindOne().SetProjection(bson.M{"_id": 1}),
	).Err()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, fmt.Errorf("find subscription for room %s: %w", roomID, err)
	}
	return true, nil
}

func (s *mongoStore) GetRoomSiteID(ctx context.Context, roomID string) (string, error) {
	var room struct {
		SiteID string `bson:"siteId"`
	}
	err := s.rooms.FindOne(ctx,
		bson.M{"_id": roomID},
		options.FindOne().SetProjection(bson.M{"siteId": 1}),
	).Decode(&room)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", fmt.Errorf("get room %s: %w", roomID, ErrRoomNotFound)
		}
		return "", fmt.Errorf("get room %s: %w", roomID, err)
	}
	return room.SiteID, nil
}

func (s *mongoStore) GetUpload(ctx context.Context, fileID string) (*upload, error) {
	var up upload
	err := s.uploads.FindOne(ctx,
		bson.M{"_id": fileID},
		options.FindOne().SetProjection(bson.M{
			"userId": 1, "rid": 1, "name": 1, "type": 1, "size": 1, "AmazonS3.path": 1,
		}),
	).Decode(&up)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("get upload %s: %w", fileID, ErrUploadNotFound)
		}
		return nil, fmt.Errorf("get upload %s: %w", fileID, err)
	}
	return &up, nil
}
