//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoStore_EnrichLookups(t *testing.T) {
	db := testutil.MongoDB(t, "search_service_test") // per-test isolated DB
	ctx := context.Background()
	store := newMongoStore(db)

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "s1", "u": bson.M{"account": "alice"}, "roomId": "rDM", "roomType": "dm", "name": "bob"},
		bson.M{"_id": "s2", "u": bson.M{"account": "alice"}, "roomId": "rBot", "roomType": "botDM", "name": "helper.bot", "isSubscribed": true},
		bson.M{"_id": "s3", "u": bson.M{"account": "alice"}, "roomId": "rCh", "roomType": "channel", "name": "General"},
		bson.M{"_id": "s4", "u": bson.M{"account": "carol"}, "roomId": "rDM", "roomType": "dm", "name": "alice"},
	})
	require.NoError(t, err)
	_, err = db.Collection("users").InsertMany(ctx, []any{
		bson.M{"_id": "u-bob", "account": "bob", "engName": "Bob Chan", "chineseName": "陳大文"},
	})
	require.NoError(t, err)
	_, err = db.Collection("apps").InsertMany(ctx, []any{
		bson.M{"_id": "app-1", "name": "Helper", "description": "a bot", "version": "1.0", "assistant": bson.M{"name": "helper.bot", "enabled": true}},
	})
	require.NoError(t, err)

	subs, err := store.SubscriptionsByRoomIDs(ctx, "alice", []string{"rDM", "rBot", "rCh", "rMissing"})
	require.NoError(t, err)
	assert.Equal(t, model.RoomTypeDM, subs["rDM"].RoomType)
	assert.Equal(t, "bob", subs["rDM"].Name)
	assert.Equal(t, model.RoomTypeBotDM, subs["rBot"].RoomType)
	assert.Equal(t, "helper.bot", subs["rBot"].Name)
	assert.True(t, subs["rBot"].IsSubscribed)
	assert.False(t, subs["rDM"].IsSubscribed) // absent in fixture → zero value
	assert.Equal(t, model.RoomTypeChannel, subs["rCh"].RoomType)
	_, missing := subs["rMissing"]
	assert.False(t, missing)
	// carol's row for rDM must NOT leak into alice's result
	assert.Equal(t, "bob", subs["rDM"].Name)

	users, err := store.UsersByAccounts(ctx, []string{"bob", "nobody"})
	require.NoError(t, err)
	assert.Equal(t, "Bob Chan", users["bob"].EngName)
	assert.Equal(t, "陳大文", users["bob"].ChineseName)
	_, ok := users["nobody"]
	assert.False(t, ok)

	apps, err := store.AppsByAssistantNames(ctx, []string{"helper.bot", "ghost.bot"})
	require.NoError(t, err)
	assert.Equal(t, "Helper", apps["helper.bot"].Name)
	assert.Equal(t, "app-1", apps["helper.bot"].ID)
	assert.Equal(t, "helper.bot", apps["helper.bot"].Assistant.Name)
	// projection: only _id, name and assistant.name come back
	assert.Empty(t, apps["helper.bot"].Description)
	assert.Empty(t, apps["helper.bot"].Version)
	_, ok = apps["ghost.bot"]
	assert.False(t, ok)

	// empty input → empty map, no query error
	empty, err := store.SubscriptionsByRoomIDs(ctx, "alice", nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
