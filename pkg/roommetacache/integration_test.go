//go:build integration

package roommetacache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func setupValkey(t *testing.T) valkeyutil.Client {
	t.Helper()
	t.Cleanup(func() { testutil.FlushValkey(t) })
	return valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
}

func TestReadThrough_MissPopulatesThenServesFromL2(t *testing.T) {
	ctx := context.Background()
	client := setupValkey(t)
	db := testutil.MongoDB(t, "roommetacache")
	rooms := db.Collection("rooms")

	_, err := rooms.InsertOne(ctx, bson.M{
		"_id": "r1", "name": "general", "type": model.RoomTypeChannel,
		"siteId": "site-a", "userCount": 3,
	})
	require.NoError(t, err)

	// First read: L2 miss → Mongo → populate L2.
	got, err := readThrough(ctx, client, rooms, "r1", time.Minute, &fakeRecorder{})
	require.NoError(t, err)
	assert.Equal(t, "general", got.Name)
	assert.Equal(t, 3, got.UserCount)

	// Prove L2 now serves it: delete the Mongo doc, second read still hits.
	_, err = rooms.DeleteOne(ctx, bson.M{"_id": "r1"})
	require.NoError(t, err)

	again, err := readThrough(ctx, client, rooms, "r1", time.Minute, &fakeRecorder{})
	require.NoError(t, err)
	assert.Equal(t, "general", again.Name)
	assert.Equal(t, 3, again.UserCount)
}

func TestReadThrough_NilClientReadsMongo(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roommetacache")
	rooms := db.Collection("rooms")
	_, err := rooms.InsertOne(ctx, bson.M{
		"_id": "r2", "name": "ops", "type": model.RoomTypeChannel,
		"siteId": "site-a", "userCount": 1,
	})
	require.NoError(t, err)

	got, err := readThrough(ctx, nil, rooms, "r2", time.Minute, &fakeRecorder{})
	require.NoError(t, err)
	assert.Equal(t, "ops", got.Name)
}

func TestFetchFromMongo_CrossSite(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roommetacache")
	rooms := db.Collection("rooms")

	_, err := rooms.InsertOne(ctx, bson.M{"_id": "rG", "siteId": "site-a", "crossSite": true})
	require.NoError(t, err)
	meta, err := FetchFromMongo(ctx, rooms, "rG")
	require.NoError(t, err)
	require.NotNil(t, meta.CrossSite)
	assert.True(t, *meta.CrossSite)

	_, err = rooms.InsertOne(ctx, bson.M{"_id": "rI", "siteId": "site-a", "crossSite": false})
	require.NoError(t, err)
	meta, err = FetchFromMongo(ctx, rooms, "rI")
	require.NoError(t, err)
	require.NotNil(t, meta.CrossSite)
	assert.False(t, *meta.CrossSite)

	// A room with no crossSite field at all (unclassified/unbackfilled) must
	// decode as nil, never false — the fail-safe treats nil as global, so
	// coercing it to false here would silently misroute a genuinely cross-site
	// room that just hasn't been backfilled yet.
	_, err = rooms.InsertOne(ctx, bson.M{"_id": "rH", "siteId": "site-a"})
	require.NoError(t, err)
	meta, err = FetchFromMongo(ctx, rooms, "rH")
	require.NoError(t, err)
	assert.Nil(t, meta.CrossSite)
}

func TestBustMeta_RemovesL2Entry(t *testing.T) {
	ctx := context.Background()
	client := setupValkey(t)
	db := testutil.MongoDB(t, "roommetacache")
	rooms := db.Collection("rooms")

	_, err := rooms.InsertOne(ctx, bson.M{
		"_id": "r3", "name": "first", "type": model.RoomTypeChannel,
		"siteId": "site-a", "userCount": 2,
	})
	require.NoError(t, err)

	// Populate L2.
	_, err = readThrough(ctx, client, rooms, "r3", time.Minute, &fakeRecorder{})
	require.NoError(t, err)

	// Authoritative write + bust.
	_, err = rooms.UpdateOne(ctx, bson.M{"_id": "r3"}, bson.M{"$set": bson.M{"name": "second"}})
	require.NoError(t, err)
	BustMeta(ctx, client, "r3")

	// Next read repopulates from Mongo with the fresh value.
	got, err := readThrough(ctx, client, rooms, "r3", time.Minute, &fakeRecorder{})
	require.NoError(t, err)
	assert.Equal(t, "second", got.Name)
}
