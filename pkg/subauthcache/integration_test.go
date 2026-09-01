//go:build integration

package subauthcache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestFetchFromMongo_Subscribed(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "subauthcache")
	subs := db.Collection("subscriptions")

	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := subs.InsertOne(ctx, bson.M{
		"_id":                "sub1",
		"u":                  bson.M{"_id": "u1", "account": "alice"},
		"roomId":             "room1",
		"roomType":           model.RoomTypeChannel,
		"roles":              []model.Role{model.RoleOwner},
		"historySharedSince": since,
	})
	require.NoError(t, err)

	got, subscribed, err := FetchFromMongo(ctx, subs, "room1", "alice")
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, "alice", got.Account)
	assert.Equal(t, []model.Role{model.RoleOwner}, got.Roles)
	assert.Equal(t, model.RoomTypeChannel, got.RoomType)
	require.NotNil(t, got.HistorySharedSince)
	assert.Equal(t, since.UnixMilli(), *got.HistorySharedSince)
}

func TestFetchFromMongo_NotSubscribed(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "subauthcache")
	subs := db.Collection("subscriptions")

	got, subscribed, err := FetchFromMongo(ctx, subs, "room1", "nobody")
	require.NoError(t, err)
	assert.False(t, subscribed)
	assert.Equal(t, SubAuth{}, got)
}
