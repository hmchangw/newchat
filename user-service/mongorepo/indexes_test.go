//go:build integration

package mongorepo

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/testutil"
)

// indexKeySpecs returns index key specs rendered as "field:dir,..." strings (preserves bson.D order)
// so tests assert the exact compound index, not just a count.
func indexKeySpecs(t *testing.T, coll *mongo.Collection) map[string]struct{} {
	t.Helper()
	cur, err := coll.Indexes().List(context.Background())
	require.NoError(t, err)
	var idx []struct {
		Key bson.D `bson:"key"`
	}
	require.NoError(t, cur.All(context.Background(), &idx))
	out := make(map[string]struct{}, len(idx))
	for _, ix := range idx {
		parts := make([]string, 0, len(ix.Key))
		for _, e := range ix.Key {
			parts = append(parts, fmt.Sprintf("%s:%v", e.Key, e.Value))
		}
		out[strings.Join(parts, ",")] = struct{}{}
	}
	return out
}

// indexNames returns the set of index names on coll.
func indexNames(t *testing.T, coll *mongo.Collection) map[string]struct{} {
	t.Helper()
	cur, err := coll.Indexes().List(context.Background())
	require.NoError(t, err)
	var idx []struct {
		Name string `bson:"name"`
	}
	require.NoError(t, cur.All(context.Background(), &idx))
	out := make(map[string]struct{}, len(idx))
	for _, ix := range idx {
		out[ix.Name] = struct{}{}
	}
	return out
}

func TestEnsureIndexes_Integration(t *testing.T) {
	subRepo, _ := newTestSubscriptionRepo(t)
	userRepo, _ := newTestUserRepo(t)

	subKeys := indexKeySpecs(t, subRepo.subscriptions.Raw())
	// {u.account, roomType} serves the account+roomType match on every list/count
	// path. (The retention window keys on room.lastMsgAt, not a subscription field.)
	require.Contains(t, subKeys, "u.account:1,roomType:1")
	require.Contains(t, subKeys, "roomId:1,u.account:1")
	require.Contains(t, subKeys, "name:1,roomType:1")

	userKeys := indexKeySpecs(t, userRepo.users.Raw())
	require.Contains(t, userKeys, "account:1")
}

// A user holds at most one subscription per room — (roomId, u.account) is the unique key shared with room-service.
func TestSubscriptionUniqueIndex_Integration(t *testing.T) {
	subRepo, _ := newTestSubscriptionRepo(t)
	ctx := context.Background()
	col := subRepo.subscriptions.Raw()
	doc := func(id string) bson.M {
		return bson.M{"_id": id, "roomId": "r1", "u": bson.M{"account": "alice"}}
	}
	_, err := col.InsertOne(ctx, doc("sub-1"))
	require.NoError(t, err)
	_, err = col.InsertOne(ctx, doc("sub-2"))
	require.True(t, mongo.IsDuplicateKeyError(err), "expected duplicate-key error, got %v", err)
}

// Duplicate accounts cause E11000 on unique-index creation; error must point the operator at the dedupe preflight.
func TestUserEnsureIndexes_DuplicateAccounts_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "user-service")
	ctx := context.Background()
	seed(t, db, usersCollection,
		bson.M{"_id": "u1", "account": "dup"},
		bson.M{"_id": "u2", "account": "dup"},
	)
	err := NewUserRepo(db).EnsureIndexes(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dedupe preflight")
}

// A pre-existing non-unique account_1 index conflicts (code 85); error must tell the operator to drop it.
func TestUserEnsureIndexes_NonUniqueAccountConflict_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "user-service")
	ctx := context.Background()
	_, err := db.Collection(usersCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}},
	})
	require.NoError(t, err)
	err = NewUserRepo(db).EnsureIndexes(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drop the old non-unique account_1 index")
}

// The apps index backs assistant.name lookups (GetAppsByAssistants / the bot-DM
// $lookup), removing the COLLSCAN. It must carry the SAME name as room-service's
// so the two services' CreateOne calls agree instead of colliding with
// IndexOptionsConflict.
func TestAppEnsureIndexes_Integration(t *testing.T) {
	appRepo, _ := newTestAppRepo(t)
	ctx := context.Background()

	require.NoError(t, appRepo.EnsureIndexes(ctx))
	// Idempotent: a second call (e.g. restart, or after room-service created it)
	// must not error.
	require.NoError(t, appRepo.EnsureIndexes(ctx))

	keys := indexKeySpecs(t, appRepo.apps.Raw())
	require.Contains(t, keys, "assistant.name:1")

	names := indexNames(t, appRepo.apps.Raw())
	require.Contains(t, names, "assistant_name_idx")

	// The compound {name,_id} index backs the apps.categories {name:1,_id:1}
	// sort — the sort keys must be a full index prefix for Mongo to use it.
	catKeys := indexKeySpecs(t, appRepo.categories.Raw())
	require.Contains(t, catKeys, "name:1,_id:1")
	require.Contains(t, indexNames(t, appRepo.categories.Raw()), "fab_domain_name_id_idx")
}
