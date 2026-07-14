//go:build integration

package main

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

// findIndex returns the listIndexes spec for name, or false if absent. Key is
// decoded as bson.D so compound-key order can be asserted.
func findIndex(ctx context.Context, t *testing.T, coll *mongo.Collection, name string) (struct {
	Name   string `bson:"name"`
	Key    bson.D `bson:"key"`
	Unique bool   `bson:"unique"`
}, bool) {
	t.Helper()
	type idxSpec struct {
		Name   string `bson:"name"`
		Key    bson.D `bson:"key"`
		Unique bool   `bson:"unique"`
	}
	cur, err := coll.Indexes().List(ctx)
	require.NoError(t, err)
	var specs []idxSpec
	require.NoError(t, cur.All(ctx, &specs))
	for _, s := range specs {
		if s.Name == name {
			return s, true
		}
	}
	return idxSpec{}, false
}

// TestSeed_PreservesServiceIndexes asserts the seeder no longer owns indexes:
// an index created by the services (here a unique compound index mirroring
// room-service's subscriptions key) survives both Seed and Teardown with its
// keys, order, and unique flag intact, while document data is replaced.
func TestSeed_PreservesServiceIndexes(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_seedidx")
	subs := db.Collection("subscriptions")

	// Simulate room-service EnsureIndexes having run before the seed.
	const idxName = "roomId_1_u.account_1"
	_, err := subs.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "roomId", Value: 1}, {Key: "u.account", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	// A stale doc that a reset must clear.
	_, err = subs.InsertOne(ctx, bson.M{"_id": "stale", "roomId": "old"})
	require.NoError(t, err)

	siteID := "site-seedidx"
	preset, _ := BuiltinPreset("small")
	fixtures := BuildFixtures(&preset, 42, siteID)

	requireUnique := func(stage string) {
		spec, ok := findIndex(ctx, t, subs, idxName)
		require.Truef(t, ok, "%s: unique index %q missing", stage, idxName)
		assert.Truef(t, spec.Unique, "%s: index lost its unique flag", stage)
		require.Lenf(t, spec.Key, 2, "%s: index key shape changed", stage)
		assert.Equal(t, "roomId", spec.Key[0].Key, "%s: key order changed", stage)
		assert.Equal(t, "u.account", spec.Key[1].Key, "%s: key order changed", stage)
	}

	require.NoError(t, Seed(ctx, db, &fixtures))
	requireUnique("after Seed")

	n, err := subs.CountDocuments(ctx, bson.M{"_id": "stale"})
	require.NoError(t, err)
	assert.Zero(t, n, "Seed should clear stale documents")

	require.NoError(t, Teardown(ctx, db))
	requireUnique("after Teardown")
}
