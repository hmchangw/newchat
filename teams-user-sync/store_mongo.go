package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const (
	teamsUserCollection = "teams_user"
	hrCollection        = "hr_employee"
)

// teamsUserID is the projection decoded by the teams_user diff (only _id).
type teamsUserID struct {
	ID string `bson:"_id"`
}

// hrRow is the projection decoded from hr_employee: the account, its site
// assignment, English name, and mail.
type hrRow struct {
	Account string `bson:"account"`
	SiteID  string `bson:"siteId"`
	EngName string `bson:"engName"`
	Mail    string `bson:"mail"`
}

// mongoStore implements Store over two databases: readDB (teams_user diff +
// hr lookup, typically a read-preference client) and writeDB (teams_user
// upserts). Every collection goes through mongoutil.Collection so projection
// and cursor handling stay in the shared helper.
type mongoStore struct {
	readTeamsUsers  *mongoutil.Collection[teamsUserID]
	readHR          *mongoutil.Collection[hrRow]
	writeTeamsUsers *mongoutil.Collection[model.TeamsUser]
}

func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore {
	return &mongoStore{
		readTeamsUsers:  mongoutil.NewCollection[teamsUserID](readDB.Collection(teamsUserCollection)),
		readHR:          mongoutil.NewCollection[hrRow](readDB.Collection(hrCollection)),
		writeTeamsUsers: mongoutil.NewCollection[model.TeamsUser](writeDB.Collection(teamsUserCollection)),
	}
}

func (s *mongoStore) ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.readTeamsUsers.FindMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		mongoutil.WithProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("find existing teams users: %w", err)
	}
	for _, r := range rows {
		out[r.ID] = struct{}{}
	}
	return out, nil
}

func (s *mongoStore) HRUsers(ctx context.Context, accounts []string) (map[string]hrUser, error) {
	out := make(map[string]hrUser, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	rows, err := s.readHR.FindMany(ctx,
		bson.M{"account": bson.M{"$in": accounts}},
		mongoutil.WithProjection(bson.M{"account": 1, "siteId": 1, "engName": 1, "mail": 1}))
	if err != nil {
		return nil, fmt.Errorf("find hr accounts: %w", err)
	}
	for _, r := range rows {
		out[r.Account] = hrUser{SiteID: r.SiteID, EngName: r.EngName, Mail: r.Mail}
	}
	return out, nil
}

func (s *mongoStore) UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error {
	if _, err := s.writeTeamsUsers.BulkUpsertByID(ctx, users, func(u model.TeamsUser) string {
		return u.ID
	}); err != nil {
		return fmt.Errorf("bulk upsert teams users: %w", err)
	}
	return nil
}
