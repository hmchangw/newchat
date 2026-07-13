package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const (
	teamsUserCollection = "teams_user"
	hrCollection        = "hr"
)

// mongoStore implements Store over two databases: readDB (teams_user diff +
// hr lookup, typically a read-preference client) and writeDB (teams_user
// upserts).
type mongoStore struct {
	readTeamsUsers  *mongo.Collection
	readHR          *mongo.Collection
	writeTeamsUsers *mongoutil.Collection[model.TeamsUser]
}

func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore {
	return &mongoStore{
		readTeamsUsers:  readDB.Collection(teamsUserCollection),
		readHR:          readDB.Collection(hrCollection),
		writeTeamsUsers: mongoutil.NewCollection[model.TeamsUser](writeDB.Collection(teamsUserCollection)),
	}
}

func (s *mongoStore) ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	cur, err := s.readTeamsUsers.Find(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("find existing teams users: %w", err)
	}
	var rows []struct {
		ID string `bson:"_id"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode existing teams user ids: %w", err)
	}
	for _, r := range rows {
		out[r.ID] = struct{}{}
	}
	return out, nil
}

func (s *mongoStore) HRSiteIDs(ctx context.Context, accounts []string) (map[string]string, error) {
	out := make(map[string]string, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	cur, err := s.readHR.Find(ctx,
		bson.M{"accountName": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"accountName": 1, "siteID": 1}))
	if err != nil {
		return nil, fmt.Errorf("find hr accounts: %w", err)
	}
	var rows []struct {
		AccountName string `bson:"accountName"`
		SiteID      string `bson:"siteID"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode hr accounts: %w", err)
	}
	for _, r := range rows {
		out[r.AccountName] = r.SiteID
	}
	return out, nil
}

func (s *mongoStore) UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error {
	if _, err := s.writeTeamsUsers.BulkUpsert(ctx, users, func(u model.TeamsUser) any {
		return bson.M{"_id": u.ID}
	}); err != nil {
		return fmt.Errorf("bulk upsert teams users: %w", err)
	}
	return nil
}
