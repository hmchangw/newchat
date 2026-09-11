//go:build integration

package mongoutil

import (
	"context"
	"strings"
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

func TestListIndexUniqueness_ReportsUniqueFlagPerIndex(t *testing.T) {
	ctx := context.Background()
	coll := testutil.MongoDB(t, "mongoutil_idx_uniq").Collection("docs")

	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "plain", Value: 1}},
	})
	require.NoError(t, err)
	_, err = coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "strict", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	have, err := listIndexUniqueness(ctx, coll)
	require.NoError(t, err)

	// An index that lost its constraint must read as present-but-not-unique: the reason this exists.
	assert.False(t, have["plain_1"], "plain_1 must be listed as non-unique")
	assert.True(t, have["strict_1"], "strict_1 must be listed as unique")
	assert.Contains(t, have, "_id_")
}

// indexesWarned returns, per warning message fragment, the index names it named.
func (h *recordHandler) indexesWarned() map[string][]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string][]string{}
	for _, e := range h.entries {
		switch {
		case strings.Contains(e.msg, "missing"):
			out["missing"] = append(out["missing"], e.index)
		case strings.Contains(e.msg, "not unique"):
			out["not unique"] = append(out["not unique"], e.index)
		}
	}
	return out
}

// WarnMissingUniqueIndexes must tell an absent index from one that lost its unique option (which the
// name-only WarnMissingIndexes passes silently) and stay quiet for a unique one.
func TestWarnMissingUniqueIndexes_DistinguishesAbsentFromNonUnique(t *testing.T) {
	ctx := context.Background()
	coll := testutil.MongoDB(t, "mongoutil_warn_unique_test").Collection("things")
	_, err := coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "ok", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "loose", Value: 1}}},
	})
	require.NoError(t, err)

	h := captureLogs(t)

	WarnMissingUniqueIndexes(ctx, coll, "ok_1", "loose_1", "absent_1")

	got := h.indexesWarned()
	assert.Equal(t, []string{"absent_1"}, got["missing"])
	assert.Equal(t, []string{"loose_1"}, got["not unique"])
}

// WarnMissingIndexes shares the listing: an unlistable collection yields one "cannot list", not "missing" per name.
func TestWarnMissingIndexes_ReportsOnlyAbsentNames(t *testing.T) {
	ctx := context.Background()
	coll := testutil.MongoDB(t, "mongoutil_warn_missing_test").Collection("things")
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "loose", Value: 1}}})
	require.NoError(t, err)

	h := captureLogs(t)

	WarnMissingIndexes(ctx, coll, "loose_1", "absent_1")

	got := h.indexesWarned()
	assert.Equal(t, []string{"absent_1"}, got["missing"])
	assert.Empty(t, got["not unique"], "the name-only variant never judges uniqueness")
}

// EnsureIndex creates an absent index, no-ops on an identical one, and never repairs a conflicting one.
func TestEnsureIndex_CreatesAbsentAndRefusesToRepair(t *testing.T) {
	ctx := context.Background()
	unique := mongo.IndexModel{Keys: bson.D{{Key: "account", Value: 1}}, Options: options.Index().SetUnique(true)}

	fresh := repairTestColl(t, "mongoutil_ensure_index_fresh_test")
	require.NoError(t, EnsureIndex(ctx, fresh, unique))
	assert.True(t, testutil.IndexSpecs(t, fresh)["account:1"], "created unique on an index-less collection")
	require.NoError(t, EnsureIndex(ctx, fresh, unique), "identical spec is a no-op")

	dirty := repairTestColl(t, "mongoutil_ensure_index_dirty_test")
	_, err := dirty.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "account", Value: 1}}})
	require.NoError(t, err)
	err = EnsureIndex(ctx, dirty, unique)
	require.ErrorIs(t, err, ErrIndexSpecConflict)
	specs := testutil.IndexSpecs(t, dirty)
	require.Contains(t, specs, "account:1", "the conflicting index must still exist for the owner to repair")
	assert.False(t, specs["account:1"], "and it must still be non-unique: nothing was repaired")
}
