package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// mongoTeamsUserResolver maps AAD user ids → nextgen (account, user _id) for indexing
// migrated Teams history: teams_user gives id→account, users gives account→_id.
type mongoTeamsUserResolver struct {
	teamsUsers *mongoutil.Collection[model.TeamsUser]
	users      *mongoutil.Collection[model.User]
}

var _ teamsUserResolver = (*mongoTeamsUserResolver)(nil)

func newMongoTeamsUserResolver(db *mongo.Database) *mongoTeamsUserResolver {
	return &mongoTeamsUserResolver{
		teamsUsers: mongoutil.NewCollection[model.TeamsUser](db.Collection("teams_user")),
		users:      mongoutil.NewCollection[model.User](db.Collection("users")),
	}
}

// ResolveIdentities returns account for every teams_user hit and fills UserID from the
// account's users row when it exists (empty until message-worker has created it).
func (r *mongoTeamsUserResolver) ResolveIdentities(ctx context.Context, teamsUserIDs []string) (map[string]teamsIdentity, error) {
	out := make(map[string]teamsIdentity, len(teamsUserIDs))
	if len(teamsUserIDs) == 0 {
		return out, nil
	}
	tus, err := r.teamsUsers.FindMany(ctx, bson.M{"_id": bson.M{"$in": teamsUserIDs}},
		mongoutil.WithProjection(bson.M{"_id": 1, "account": 1}))
	if err != nil {
		return nil, fmt.Errorf("find teams_users by id: %w", err)
	}
	accountToID := make(map[string]string, len(tus)) // account → teamsUserID
	accounts := make([]string, 0, len(tus))
	for i := range tus {
		if tus[i].Account == "" {
			continue
		}
		out[tus[i].ID] = teamsIdentity{Account: tus[i].Account} // UserID filled below
		accountToID[tus[i].Account] = tus[i].ID
		accounts = append(accounts, tus[i].Account)
	}
	if len(accounts) == 0 {
		return out, nil
	}
	users, err := r.users.FindMany(ctx, bson.M{"account": bson.M{"$in": accounts}},
		mongoutil.WithProjection(bson.M{"_id": 1, "account": 1}))
	if err != nil {
		return nil, fmt.Errorf("find users by account: %w", err)
	}
	for i := range users {
		if teamsID, ok := accountToID[users[i].Account]; ok {
			out[teamsID] = teamsIdentity{Account: users[i].Account, UserID: users[i].ID}
		}
	}
	return out, nil
}
