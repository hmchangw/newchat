package mongorepo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const permissionGrantsCollection = "permission_grants"

// PermissionRepo is the Mongo read model for service.PermissionRepository.
// permission_grants is an append-only ledger written only by admin-service.
//
// No WithReadPreference option: stays on the primary so a grant/revoke is
// visible immediately (spec §3.7). No EnsureIndexes: admin-service alone
// creates the indexes, to avoid a two-service IndexKeySpecsConflict (spec §3.6).
type PermissionRepo struct {
	grants *mongoutil.Collection[model.PermissionGrant]
}

// NewPermissionRepo builds a PermissionRepo over db.
func NewPermissionRepo(db *mongo.Database) *PermissionRepo {
	return &PermissionRepo{
		grants: mongoutil.NewCollection[model.PermissionGrant](db.Collection(permissionGrantsCollection)),
	}
}

// GetLatestGrant returns the newest row for (siteId, permission,
// subjectAccount) — recordedAt desc, _id desc tie-break — or (nil, nil).
// Uses the raw-driver escape hatch: mongoutil's typed FindOne has no sort
// (WithSort only wires into FindMany — pkg/mongoutil/options.go).
func (r *PermissionRepo) GetLatestGrant(ctx context.Context, siteID string, permission model.PermissionKey, subjectAccount string) (*model.PermissionGrant, error) {
	res := r.grants.Raw().FindOne(ctx,
		bson.M{"siteId": siteID, "permission": permission, "subjectAccount": subjectAccount},
		options.FindOne().
			SetSort(bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}}).
			SetProjection(bson.M{"granted": 1, "effectiveFrom": 1, "expiresAt": 1}),
	)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("find latest permission grant: %w", err)
	}
	var g model.PermissionGrant
	if err := res.Decode(&g); err != nil {
		return nil, fmt.Errorf("find latest permission grant: %w", err)
	}
	return &g, nil
}
