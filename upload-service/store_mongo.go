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
	uploads       *mongo.Collection
}

// NewMongoStore returns a Store backed by the subscriptions and uploads collections.
func NewMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		subscriptions: db.Collection("subscriptions"),
		uploads:       db.Collection("uploads"),
	}
}

func (s *mongoStore) MemberSiteID(ctx context.Context, roomID, account string) (string, bool, error) {
	var sub struct {
		SiteID string `bson:"siteId"`
	}
	err := s.subscriptions.FindOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		options.FindOne().SetProjection(bson.M{"siteId": 1, "_id": 0}),
	).Decode(&sub)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("find subscription for room %s: %w", roomID, err)
	}
	return sub.SiteID, true, nil
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
