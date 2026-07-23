package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// mongoHRIdentityStore is this service's own identity read/write store for the Teams
// migration sender resolver. Per-service store — there is no shared store package.
// The consumer-owned interface it satisfies lives in teamssender.go.
type mongoHRIdentityStore struct {
	users *mongoutil.Collection[model.User]
}

var _ HRIdentityStore = (*mongoHRIdentityStore)(nil)

func newMongoHRIdentityStore(db *mongo.Database) *mongoHRIdentityStore {
	return &mongoHRIdentityStore{users: mongoutil.NewCollection[model.User](db.Collection("users"))}
}

func (s *mongoHRIdentityStore) FindUserByEmployeeId(ctx context.Context, employeeId string) (*model.User, error) {
	u, err := s.users.FindOne(ctx, bson.M{"employeeId": employeeId})
	if err != nil {
		return nil, fmt.Errorf("find user by employeeId: %w", err)
	}
	return u, nil // FindOne yields (nil,nil) on no match
}

// FindUserByDisplayName matches on chineseName — the field the HR sync writes the
// Graph displayName into. Reads 2 rows so a non-unique name is ambiguous (nil,nil).
func (s *mongoHRIdentityStore) FindUserByDisplayName(ctx context.Context, name string) (*model.User, error) {
	users, err := s.users.FindMany(ctx,
		bson.M{"chineseName": name},
		mongoutil.WithLimit(2))
	if err != nil {
		return nil, fmt.Errorf("find user by display name: %w", err)
	}
	if len(users) != 1 {
		return nil, nil
	}
	return &users[0], nil
}

// UpsertUserIdentities builds the $set doc by hand (never a full-doc replace) so a
// migrated identity write can't wipe roles/password/services on the live auth store.
func (s *mongoHRIdentityStore) UpsertUserIdentities(ctx context.Context, users []model.IUserWithChange) error {
	models := make([]mongo.WriteModel, 0, len(users))
	for i := range users {
		u := &users[i].User
		if u.EmployeeID == "" {
			// employeeId is the identity key; an empty one would clobber every keyless row.
			slog.WarnContext(ctx, "skip user identity upsert: empty employeeId", "account", u.Account)
			continue
		}
		models = append(models, mongo.NewUpdateOneModel().
			// employeeId is globally unique, so it alone keys the identity — a person
			// resolves to one row regardless of site.
			SetFilter(bson.M{"employeeId": u.EmployeeID}).
			SetUpdate(bson.M{
				"$set": bson.M{
					"account": u.Account, "siteId": u.SiteID,
					"engName": u.EngName, "chineseName": u.ChineseName,
					"employeeId": u.EmployeeID,
				},
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

// employeeIDFromGraphID derives a deterministic 24-hex bson.ObjectID from the Graph
// object id — the same hash the HR sync uses, so a person resolved by either path is
// one identity. Per-service copy (no shared pkg since the store split).
func employeeIDFromGraphID(graphID string) string {
	sum := sha256.Sum256([]byte(graphID))
	var oid bson.ObjectID
	copy(oid[:], sum[:12])
	return oid.Hex()
}
