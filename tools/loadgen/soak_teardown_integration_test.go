//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestTeardownSoak_DeletesOnlySelectedMongoRun(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_teardown")
	store := &mongoSoakStore{db: db}
	now := time.Now().UTC()

	require.NoError(t, store.PutManifest(ctx, &soakManifest{
		ID: "run-a", State: soakManifestSeeded, StartedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, store.PutManifest(ctx, &soakManifest{
		ID: "run-b", State: soakManifestSeeded, StartedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, store.ReplaceOwnershipChunks(ctx, "run-a", [][]string{{"room-a"}}))
	require.NoError(t, store.ReplaceOwnershipChunks(ctx, "run-b", [][]string{{"room-b"}}))

	_, err := db.Collection("users").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "borrowed-user"},
		{Key: "account", Value: "borrowed"},
	})
	require.NoError(t, err)
	for collection, documents := range map[string][]any{
		"rooms": {
			bson.D{{Key: "_id", Value: "room-a"}, {Key: "soakRunId", Value: "run-a"}},
			bson.D{{Key: "_id", Value: "room-b"}, {Key: "soakRunId", Value: "run-b"}},
		},
		"subscriptions": {
			bson.D{{Key: "_id", Value: "sub-a"}, {Key: "roomId", Value: "room-a"}, {Key: "soakRunId", Value: "run-a"}},
			bson.D{{Key: "_id", Value: "sub-b"}, {Key: "roomId", Value: "room-b"}, {Key: "soakRunId", Value: "run-b"}},
		},
		"thread_rooms": {
			bson.D{{Key: "_id", Value: "thread-a"}, {Key: "roomId", Value: "room-a"}},
			bson.D{{Key: "_id", Value: "thread-b"}, {Key: "roomId", Value: "room-b"}},
		},
		"thread_subscriptions": {
			bson.D{
				{Key: "_id", Value: "thread-sub-a"},
				{Key: "roomId", Value: "stale-room-id"},
				{Key: "threadRoomId", Value: "thread-a"},
			},
			bson.D{
				{Key: "_id", Value: "thread-sub-b"},
				{Key: "roomId", Value: "room-b"},
				{Key: "threadRoomId", Value: "thread-b"},
			},
		},
		atrest.CollectionName: {
			bson.D{{Key: "_id", Value: "room-a"}, {Key: "wrappedDEK", Value: []byte("a")}},
			bson.D{{Key: "_id", Value: "room-b"}, {Key: "wrappedDEK", Value: []byte("b")}},
		},
	} {
		_, err := db.Collection(collection).InsertMany(ctx, documents)
		require.NoError(t, err, collection)
	}

	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"
	cfg.CassandraCleanup = "none"
	found, err := teardownSoak(ctx, store, nil, &cfg, "chat")
	require.NoError(t, err)
	assert.True(t, found)

	assert.Equal(t, int64(1), countDocuments(t, db.Collection("users"), bson.D{}))
	for collection, id := range map[string]string{
		"rooms":                "room-b",
		"subscriptions":        "sub-b",
		"thread_rooms":         "thread-b",
		"thread_subscriptions": "thread-sub-b",
		atrest.CollectionName:  "room-b",
	} {
		assert.Equal(t, int64(1), countDocuments(t, db.Collection(collection), bson.D{}), collection)
		assert.Equal(t, int64(1), countDocuments(
			t,
			db.Collection(collection),
			bson.D{{Key: "_id", Value: id}},
		), collection)
	}
	assert.Equal(t, int64(0), countDocuments(
		t,
		db.Collection(soakOwnershipCollection),
		bson.D{{Key: "soakRunId", Value: "run-a"}},
	))
	assert.Equal(t, int64(1), countDocuments(
		t,
		db.Collection(soakOwnershipCollection),
		bson.D{{Key: "soakRunId", Value: "run-b"}},
	))

	manifest, err := store.FindManifest(ctx, "run-a")
	require.NoError(t, err)
	require.NotNil(t, manifest)
	assert.Equal(t, soakManifestCleaned, manifest.State)
	require.NotNil(t, manifest.CleanedAt)

	found, err = teardownSoak(ctx, store, nil, &cfg, "chat")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(1), countDocuments(t, db.Collection("rooms"), bson.D{}))
}

func TestTeardownSoak_PreservesArtifactsForUnownedRoomInOwnershipChunk(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_teardown_unowned")
	store := &mongoSoakStore{db: db}
	now := time.Now().UTC()

	require.NoError(t, store.PutManifest(ctx, &soakManifest{
		ID: "run-a", State: soakManifestSeeded, StartedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, store.ReplaceOwnershipChunks(
		ctx,
		"run-a",
		[][]string{{"shared-room"}},
	))
	for collection, document := range map[string]any{
		"rooms": bson.D{
			{Key: "_id", Value: "shared-room"},
			{Key: "soakRunId", Value: "another-run"},
		},
		"thread_rooms": bson.D{
			{Key: "_id", Value: "shared-thread"},
			{Key: "roomId", Value: "shared-room"},
		},
		"thread_subscriptions": bson.D{
			{Key: "_id", Value: "shared-thread-sub"},
			{Key: "roomId", Value: "shared-room"},
		},
		atrest.CollectionName: bson.D{
			{Key: "_id", Value: "shared-room"},
			{Key: "wrappedDEK", Value: []byte("keep-me")},
		},
	} {
		_, err := db.Collection(collection).InsertOne(ctx, document)
		require.NoError(t, err, collection)
	}

	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"
	cfg.TeardownBatchDelay = 0
	found, err := teardownSoak(ctx, store, nil, &cfg, "chat")

	require.NoError(t, err)
	assert.True(t, found)
	for _, collection := range []string{
		"rooms",
		"thread_rooms",
		"thread_subscriptions",
		atrest.CollectionName,
	} {
		assert.Equal(t, int64(1), countDocuments(
			t,
			db.Collection(collection),
			bson.D{},
		), collection)
	}
}
