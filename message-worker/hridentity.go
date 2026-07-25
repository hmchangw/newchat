package main

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
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

// UpsertUserIdentities builds the $set doc by hand (never a full-doc replace) so a
// migrated identity write can't wipe roles/password/services on the live auth store.
func (s *mongoHRIdentityStore) UpsertUserIdentities(ctx context.Context, users []model.IUserWithChange) error {
	models := make([]mongo.WriteModel, 0, len(users))
	for i := range users {
		u := &users[i].User
		if u.Account == "" {
			// account is the identity key; an empty one would clobber every keyless row.
			slog.WarnContext(ctx, "skip user identity upsert: empty account")
			continue
		}
		models = append(models, mongo.NewUpdateOneModel().
			// account is globally unique (users has a unique index on it), so it alone keys
			// the identity. Migrated Teams users carry no employeeId — it is left unset.
			SetFilter(bson.M{"account": u.Account}).
			SetUpdate(bson.M{
				"$set": bson.M{
					"siteId": u.SiteID, "engName": u.EngName, "chineseName": u.ChineseName,
				},
				// Fresh UUIDv7 _id on insert — the persisted UserID search-sync reads back by account.
				"$setOnInsert": bson.M{"_id": idgen.GenerateUUIDv7()},
			}).
			SetUpsert(true))
	}
	if len(models) == 0 {
		return nil // BulkWrite errors on an empty slice; no-op cleanly.
	}
	if _, err := s.users.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("bulk upsert user identities: %w", err)
	}
	return nil
}
