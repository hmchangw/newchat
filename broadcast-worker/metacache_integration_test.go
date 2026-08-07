//go:build integration

package main

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

func TestMongoStore_GetRoomMeta_ReadsThroughL2(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { testutil.FlushValkey(t) })
	client := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	db := testutil.MongoDB(t, "bw-meta")
	rooms := db.Collection("rooms")

	_, err := rooms.InsertOne(ctx, bson.M{
		"_id": "r1", "name": "general", "type": model.RoomTypeChannel,
		"siteId": "site-a", "userCount": 4,
	})
	require.NoError(t, err)

	store := NewMongoStore(rooms, db.Collection("subscriptions"), db.Collection("thread_rooms"), client, time.Minute)

	got, err := store.GetRoomMeta(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, "general", got.Name)
	assert.Equal(t, "site-a", got.SiteID)
	assert.Equal(t, 4, got.UserCount)
	assert.Equal(t, model.RoomTypeChannel, got.Type)

	// Served from L2 after the Mongo doc is gone. Assert the full Meta to guard
	// the L2 JSON round-trip (all tagged fields), not just Name.
	_, err = rooms.DeleteOne(ctx, bson.M{"_id": "r1"})
	require.NoError(t, err)
	again, err := store.GetRoomMeta(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, "general", again.Name)
	assert.Equal(t, "site-a", again.SiteID)
	assert.Equal(t, 4, again.UserCount)
	assert.Equal(t, model.RoomTypeChannel, again.Type)
}
