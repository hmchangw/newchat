package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// mongoHRIdentityStore is this service's own identity store for the Teams migration
// sender resolver, over two collections: teams_user (AAD id → account) and users
// (account → nextgen identity). Per-service store — there is no shared store package.
// The consumer-owned interface it satisfies lives in teamssender.go.
type mongoHRIdentityStore struct {
	users      *mongoutil.Collection[model.User]
	teamsUsers *mongoutil.Collection[model.TeamsUser]
}

var _ HRIdentityStore = (*mongoHRIdentityStore)(nil)

func newMongoHRIdentityStore(db *mongo.Database) *mongoHRIdentityStore {
	return &mongoHRIdentityStore{
		users:      mongoutil.NewCollection[model.User](db.Collection("users")),
		teamsUsers: mongoutil.NewCollection[model.TeamsUser](db.Collection("teams_user")),
	}
}

// AccountByTeamsID resolves an AAD user object id to its account via teams_user (the
// same _id→account mapping teams-chat-member-sync reads); "" when no record exists.
func (s *mongoHRIdentityStore) AccountByTeamsID(ctx context.Context, teamsUserID string) (string, error) {
	u, err := s.teamsUsers.FindByID(ctx, teamsUserID, mongoutil.WithProjection(bson.M{"_id": 1, "account": 1}))
	if err != nil {
		return "", fmt.Errorf("find teams_user by id: %w", err)
	}
	if u == nil {
		return "", nil
	}
	return u.Account, nil
}

// FindUserByAccount returns the single user with account (globally unique), or (nil,nil).
func (s *mongoHRIdentityStore) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	u, err := s.users.FindOne(ctx, bson.M{"account": account})
	if err != nil {
		return nil, fmt.Errorf("find user by account: %w", err)
	}
	return u, nil // FindOne yields (nil,nil) on no match
}
