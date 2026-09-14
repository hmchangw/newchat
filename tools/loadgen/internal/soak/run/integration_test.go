//go:build integration

package run

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestSoakRunMongo_SeedReloadHeartbeatAndTeardown(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_run_package")
	store := NewMongo(db)
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })

	users := makeUsers(10, "site-a")
	documents := make([]any, len(users))
	for i := range users {
		documents[i] = users[i]
	}
	_, err := db.Collection("users").InsertMany(ctx, documents)
	require.NoError(t, err)

	input := validSeedInput()
	seeded, err := Seed(ctx, store, keyStore, &input, sequenceIDs())
	require.NoError(t, err)
	require.NotEmpty(t, seeded.Rooms)

	found, err := store.FindManifest(ctx, input.RunID)
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, StateSeeded, found.State)
	require.ErrorIs(t, store.TouchHeartbeat(ctx, input.RunID, time.Now().UTC()), ErrRunNotActive)
	_, err = store.GetManifest(ctx, "missing")
	require.ErrorIs(t, err, ErrManifestNotFound)
	missing, err := store.FindManifest(ctx, "missing")
	require.NoError(t, err)
	assert.Nil(t, missing)

	loaded, err := store.LoadTopology(ctx, input.RunID, input.SiteID)
	require.NoError(t, err)
	assert.Equal(t, userIDs(seeded.ActiveUsers), userIDs(loaded.ActiveUsers))
	assert.Len(t, loaded.Rooms, len(seeded.Rooms))

	manifest, err := store.GetManifest(ctx, input.RunID)
	require.NoError(t, err)
	manifest.State = StateRunning
	require.NoError(t, store.PutManifest(ctx, manifest))
	heartbeat := time.Now().UTC()
	require.NoError(t, store.TouchHeartbeat(ctx, input.RunID, heartbeat))
	manifest, err = store.GetManifest(ctx, input.RunID)
	require.NoError(t, err)
	require.NotNil(t, manifest.LastHeartbeatAt)
	assert.WithinDuration(t, heartbeat, *manifest.LastHeartbeatAt, time.Millisecond)

	room := loaded.Rooms[0]
	name, exists, err := store.RoomName(ctx, room.ID)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, room.Name, name)

	var subscription model.Subscription
	for i := range loaded.Subscriptions {
		if loaded.Subscriptions[i].RoomID == room.ID {
			subscription = loaded.Subscriptions[i]
			break
		}
	}
	require.NotEmpty(t, subscription.ID)
	muted, exists, err := store.SubscriptionMuted(ctx, room.ID, subscription.User.Account)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.False(t, muted)
	_, exists, err = store.SubscriptionLastSeen(ctx, room.ID, subscription.User.Account)
	require.NoError(t, err)
	assert.True(t, exists)
	_, exists, err = store.RoomName(ctx, "missing")
	require.NoError(t, err)
	assert.False(t, exists)
	_, exists, err = store.SubscriptionMuted(ctx, "missing", "nobody")
	require.NoError(t, err)
	assert.False(t, exists)
	_, exists, err = store.SubscriptionLastSeen(ctx, "missing", "nobody")
	require.NoError(t, err)
	assert.False(t, exists)
	isMember, err := store.IsRoomMember(ctx, "missing", "nobody")
	require.NoError(t, err)
	assert.False(t, isMember)

	_, err = db.Collection("room_members").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "member-1"}, {Key: "rid", Value: room.ID},
		{Key: "member", Value: bson.D{{Key: "account", Value: subscription.User.Account}}},
	})
	require.NoError(t, err)
	isMember, err = store.IsRoomMember(ctx, room.ID, subscription.User.Account)
	require.NoError(t, err)
	assert.True(t, isMember)

	hasDEK, err := store.HasWrappedDEK(ctx, room.ID)
	require.NoError(t, err)
	assert.False(t, hasDEK)
	_, err = db.Collection(atrest.CollectionName).InsertOne(ctx, bson.D{
		{Key: "_id", Value: room.ID}, {Key: "wrappedDEK", Value: []byte("wrapped")},
	})
	require.NoError(t, err)
	hasDEK, err = store.HasWrappedDEK(ctx, room.ID)
	require.NoError(t, err)
	assert.True(t, hasDEK)

	createdRoomID := "created-room"
	_, err = db.Collection("rooms").InsertOne(ctx, bson.D{
		{Key: "_id", Value: createdRoomID},
		{Key: "name", Value: CreatedRoomPrefix(input.RunID) + "001"},
		{Key: "siteId", Value: input.SiteID},
	})
	require.NoError(t, err)
	require.NoError(t, store.AppendOwnedRooms(ctx, input.RunID, []string{createdRoomID}))
	resolved, exists, err := store.RoomIDByName(
		ctx, input.SiteID, CreatedRoomPrefix(input.RunID)+"001",
	)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, createdRoomID, resolved)
	_, exists, err = store.RoomIDByName(ctx, input.SiteID, "missing")
	require.NoError(t, err)
	assert.False(t, exists)
	_, err = store.NextOwnershipPage(ctx, input.RunID, "", 0)
	require.Error(t, err)
	created, err := store.CountCreatedRooms(ctx, input.RunID)
	require.NoError(t, err)
	assert.Equal(t, 1, created)
	require.NoError(t, store.DeleteOwnedRoomBatch(ctx, input.RunID, []string{"missing"}))

	_, err = db.Collection("thread_rooms").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "thread-1"}, {Key: "roomId", Value: room.ID},
	})
	require.NoError(t, err)
	_, err = db.Collection("thread_subscriptions").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "thread-sub-1"}, {Key: "threadRoomId", Value: "thread-1"},
	})
	require.NoError(t, err)

	manifest.State = StateSeeded
	require.NoError(t, store.PutManifest(ctx, manifest))
	cleaned, err := Teardown(ctx, store, nil, &TeardownConfig{
		RunID: input.RunID, CassandraCleanup: "none", HeartbeatStaleAfter: time.Minute,
		BatchRooms: 2, BatchTimeout: time.Second,
	}, "chat")
	require.NoError(t, err)
	assert.True(t, cleaned)
	remaining, err := db.Collection("rooms").CountDocuments(
		ctx, bson.D{{Key: "soakRunId", Value: input.RunID}},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(0), remaining)
	remaining, err = db.Collection("thread_rooms").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), remaining)
	remaining, err = db.Collection("thread_subscriptions").CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), remaining)
	require.Error(t, store.MarkCleaned(ctx, "missing"))
}
