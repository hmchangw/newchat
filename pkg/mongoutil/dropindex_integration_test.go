//go:build integration

package mongoutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/testutil"
)

// TestDropIndexIfExists_RetiresAnExistingIndex covers the first boot after a
// legacy index is replaced: the drop happens and is reported as success.
func TestDropIndexIfExists_RetiresAnExistingIndex(t *testing.T) {
	ctx := context.Background()
	col := testutil.MongoDB(t, "mongoutil_dropindex_test").Collection("docs")

	name, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "legacy", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	require.NoError(t, DropIndexIfExists(ctx, col, name))

	specs, err := col.Indexes().ListSpecifications(ctx)
	require.NoError(t, err)
	for _, spec := range specs {
		assert.NotEqual(t, name, spec.Name, "the legacy index must be gone")
	}
}

// TestDropIndexIfExists_AbsentIsSuccess covers every boot after the first: the
// index and even the collection are gone, which is the expected steady state.
func TestDropIndexIfExists_AbsentIsSuccess(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "mongoutil_dropindex_test")

	assert.NoError(t, DropIndexIfExists(ctx, db.Collection("never_created"), "legacy_1"),
		"an absent collection means there is nothing to retire")

	require.NoError(t, db.CreateCollection(ctx, "empty"))
	assert.NoError(t, DropIndexIfExists(ctx, db.Collection("empty"), "legacy_1"),
		"an absent index means there is nothing to retire")
}

// TestDropIndexIfExists_UnreachableMongoIsReported is the case the old
// discard-everything call sites hid: the retirement did not happen, so the
// caller must hear about it rather than record startup as clean.
func TestDropIndexIfExists_UnreachableMongoIsReported(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := Connect(ctx, testutil.UnreachableMongoURI(t), "", "", WithLazyConnect())
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	err = DropIndexIfExists(ctx, client.Database("mongoutil_dropindex_down").Collection("docs"), "legacy_1")
	require.Error(t, err, "a drop that never reached MongoDB must not report success")
	assert.Contains(t, err.Error(), "legacy_1", "the failing index must be identifiable")
}
