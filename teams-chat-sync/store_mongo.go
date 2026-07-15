package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// mongoStore implements TeamsUserStore and TeamsChatStore over the teams_user
// and teams_chat collections.
type mongoStore struct {
	users *mongoutil.Collection[model.TeamsUser]
	chats *mongoutil.Collection[model.TeamsChat]
}

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		users: mongoutil.NewCollection[model.TeamsUser](db.Collection("teams_user")),
		chats: mongoutil.NewCollection[model.TeamsChat](db.Collection("teams_chat")),
	}
}

// ListUsers returns every teams_user projected to the sync fields
// (_id, siteId, account, from).
func (s *mongoStore) ListUsers(ctx context.Context) ([]model.TeamsUser, error) {
	users, err := s.users.FindMany(ctx, bson.M{}, mongoutil.WithProjection(bson.M{
		"_id": 1, "siteId": 1, "account": 1, "from": 1,
	}))
	if err != nil {
		return nil, fmt.Errorf("list teams users: %w", err)
	}
	return users, nil
}

// SetFrom advances one user's sync watermark.
func (s *mongoStore) SetFrom(ctx context.Context, userID string, from time.Time) error {
	if _, err := s.users.Raw().UpdateByID(ctx, userID, bson.M{"$set": bson.M{"from": from}}); err != nil {
		return fmt.Errorf("set teams user watermark: %w", err)
	}
	return nil
}

// UpsertChats bulk-upserts chats keyed on _id. oneOnOne chats are insert-only
// (all fields $setOnInsert); other chat types keep createdDateTime and siteID
// $setOnInsert-only while the mutable fields refresh.
func (s *mongoStore) UpsertChats(ctx context.Context, chats []model.TeamsChat) error {
	models := make([]mongo.WriteModel, 0, len(chats))
	//nolint:gocritic // rangeValCopy: c is heavy but using index-range would be less idiomatic
	for _, c := range chats {
		models = append(models, chatUpsertModel(c))
	}
	if _, err := s.chats.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("upsert teams chats: %w", err)
	}
	return nil
}

// chatUpsertModel builds the upsert for one chat. createdDateTime and siteID
// are $setOnInsert-only — once a chat has a siteID it never changes. oneOnOne
// chats put every field under $setOnInsert: they never change after creation,
// so an existing document is never modified (the "ignore oneOnOne update"
// rule enforced atomically, without a read).
//
//nolint:gocritic // hugeParam: c is heavy but unavoidable in this builder pattern
func chatUpsertModel(c model.TeamsChat) mongo.WriteModel {
	filter := bson.M{"_id": c.ID}
	if c.ChatType == model.TeamsChatTypeOneOnOne {
		return mongoutil.UpsertModel(filter, bson.M{"$setOnInsert": bson.M{
			"name":                c.Name,
			"chatType":            c.ChatType,
			"createdDateTime":     c.CreatedDateTime,
			"lastUpdatedDateTime": c.LastUpdatedDateTime,
			"members":             c.Members,
			"siteId":              c.SiteID,
			"updatedAt":           c.UpdatedAt,
			"needMemberSync":      c.NeedMemberSync,
		}})
	}
	return mongoutil.UpsertModel(filter, bson.M{
		"$setOnInsert": bson.M{
			"createdDateTime": c.CreatedDateTime,
			"siteId":          c.SiteID,
		},
		"$set": bson.M{
			"name":                c.Name,
			"chatType":            c.ChatType,
			"lastUpdatedDateTime": c.LastUpdatedDateTime,
			"members":             c.Members,
			"updatedAt":           c.UpdatedAt,
			"needMemberSync":      c.NeedMemberSync,
		},
	})
}
