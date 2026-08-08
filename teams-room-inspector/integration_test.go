//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoStore_RoomStates(t *testing.T) {
	db := testutil.MongoDB(t, "teamsinspector")
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertMany(ctx, []any{
		bson.M{"_id": "roomA", "name": "A", "userCount": 3},
		bson.M{"_id": "roomB", "name": "B", "userCount": 0},
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "s1", "roomId": "roomA"},
		bson.M{"_id": "s2", "roomId": "roomA"},
		bson.M{"_id": "s3", "roomId": "roomC"}, // subscriptions without a room doc
	})
	require.NoError(t, err)

	store := newMongoStore(db)
	got, err := store.RoomStates(ctx, []string{"roomA", "roomB", "roomMissing", "roomC"})
	require.NoError(t, err)

	assert.Equal(t, RoomState{Exists: true, UserCount: 3, SubscriptionCount: 2}, got["roomA"])
	assert.Equal(t, RoomState{Exists: true, UserCount: 0, SubscriptionCount: 0}, got["roomB"])
	assert.NotContains(t, got, "roomMissing", "a room with neither doc nor subs has no entry")
	assert.Equal(t, RoomState{Exists: false, SubscriptionCount: 1}, got["roomC"],
		"orphan subscriptions are reported with Exists=false")
}

func TestMongoStore_RoomStates_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teamsinspector")
	store := newMongoStore(db)
	got, err := store.RoomStates(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
