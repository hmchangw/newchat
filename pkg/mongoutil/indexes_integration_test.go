//go:build integration

package mongoutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/testutil"
)

func repairTestColl(t *testing.T, dbName string) *mongo.Collection {
	t.Helper()
	return testutil.MongoDB(t, dbName).Collection("users")
}

// EnsureIndexWithRepair upgrades a pre-existing non-unique index to the unique
// spec it should have — the #159 conflict self-heals instead of crashlooping.
func TestEnsureIndexWithRepair_UpgradesNonUniqueToUnique(t *testing.T) {
	ctx := context.Background()
	coll := repairTestColl(t, "mongoutil_index_repair_test")
	unique := mongo.IndexModel{Keys: bson.D{{Key: "account", Value: 1}}, Options: options.Index().SetUnique(true)}

	// Seed the wrong index: a NON-unique account_1.
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "account", Value: 1}}})
	require.NoError(t, err)

	require.NoError(t, EnsureIndexWithRepair(ctx, coll, unique), "a non-unique account_1 must be repaired to unique")

	// Now unique: a duplicate account insert must fail.
	_, err = coll.InsertOne(ctx, bson.M{"_id": "a", "account": "dup"})
	require.NoError(t, err)
	_, err = coll.InsertOne(ctx, bson.M{"_id": "b", "account": "dup"})
	assert.True(t, mongo.IsDuplicateKeyError(err), "account_1 must be unique after repair")

	// Idempotent: a second call with the correct spec is a no-op.
	require.NoError(t, EnsureIndexWithRepair(ctx, coll, unique))
}

// E11000 (duplicate DATA) is not repairable by dropping an index — the error
// propagates so the caller can surface the dedupe preflight.
func TestEnsureIndexWithRepair_DuplicateDataReturnsError(t *testing.T) {
	ctx := context.Background()
	coll := repairTestColl(t, "mongoutil_index_repair_dup_test")

	_, err := coll.InsertOne(ctx, bson.M{"_id": "a", "account": "dup"})
	require.NoError(t, err)
	_, err = coll.InsertOne(ctx, bson.M{"_id": "b", "account": "dup"})
	require.NoError(t, err)

	err = EnsureIndexWithRepair(ctx, coll, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	require.Error(t, err)
	assert.True(t, mongo.IsDuplicateKeyError(err), "duplicate data must surface E11000, not be silently repaired")
}

// A DropOne on a not-yet-created collection returns NamespaceNotFound (26), which
// IsIndexNotFound must treat as "nothing to drop" — else a fresh-deploy
// EnsureIndexes records a spurious error (regression: #334).
func TestIsIndexNotFound_FreshCollection(t *testing.T) {
	ctx := context.Background()
	coll := repairTestColl(t, "mongoutil_index_notfound_test")

	err := coll.Indexes().DropOne(ctx, "threadRoomId_1_userId_1")
	require.Error(t, err)
	assert.True(t, IsIndexNotFound(err), "fresh-collection DropOne (NamespaceNotFound) must count as not-found")
}

// The #159 dirty env — a non-unique index AND duplicate data — must not leave the
// collection index-less: the unique recreate fails E11000, but the old index is
// restored, and the error still surfaces so the caller shows dedupe guidance.
func TestEnsureIndexWithRepair_DirtyDataRestoresOldIndex(t *testing.T) {
	ctx := context.Background()
	coll := repairTestColl(t, "mongoutil_index_repair_dirty_test")

	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "account", Value: 1}}})
	require.NoError(t, err)
	_, err = coll.InsertMany(ctx, []any{
		bson.M{"_id": "a", "account": "dup"}, bson.M{"_id": "b", "account": "dup"},
	})
	require.NoError(t, err)

	err = EnsureIndexWithRepair(ctx, coll, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	require.Error(t, err)
	assert.True(t, mongo.IsDuplicateKeyError(err), "duplicate data must surface E11000")
	restored := existingIndexByKeys(ctx, coll, bson.D{{Key: "account", Value: 1}})
	require.Equal(t, "account_1", restored.name, "the old account index must be restored, not left dropped")
	u, _ := restored.spec["unique"].(bool)
	assert.False(t, u, "the restored index must faithfully preserve the old (non-unique) spec")
}
