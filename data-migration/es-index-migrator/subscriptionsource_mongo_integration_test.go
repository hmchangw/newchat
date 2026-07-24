//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoSubscriptionSource_RoomIDs_ScopedBySite(t *testing.T) {
	db := testutil.MongoDB(t, "esmig")
	source := newMongoSubscriptionSource(db)
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		model.Subscription{ID: "s1", SiteID: "site-a", RoomID: "room1", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: time.Now()},
		model.Subscription{ID: "s2", SiteID: "site-a", RoomID: "room2", User: model.SubscriptionUser{Account: "bob"}, JoinedAt: time.Now()},
		model.Subscription{ID: "s3", SiteID: "site-b", RoomID: "room3", User: model.SubscriptionUser{Account: "carol"}, JoinedAt: time.Now()},
	})
	require.NoError(t, err)

	got, err := source.RoomIDs(ctx, "site-a")

	require.NoError(t, err)
	require.ElementsMatch(t, []string{"room1", "room2"}, got)
}

func TestMongoSubscriptionSource_Subscriptions_ReturnsFullDocsForSite(t *testing.T) {
	db := testutil.MongoDB(t, "esmig")
	source := newMongoSubscriptionSource(db)
	ctx := context.Background()
	joined := time.Now().Truncate(time.Millisecond)

	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s1", SiteID: "site-a", RoomID: "room1", RoomType: model.RoomTypeChannel,
		Name: "general", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: joined,
	})
	require.NoError(t, err)

	got, err := source.Subscriptions(ctx, "site-a")

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "alice", got[0].User.Account)
	require.Equal(t, "general", got[0].Name)
	require.Equal(t, model.RoomTypeChannel, got[0].RoomType)
	require.True(t, got[0].JoinedAt.Equal(joined))
}

func TestMongoSubscriptionSource_Subscriptions_EmptyForUnknownSite(t *testing.T) {
	db := testutil.MongoDB(t, "esmig")
	source := newMongoSubscriptionSource(db)

	got, err := source.Subscriptions(context.Background(), "no-such-site")

	require.NoError(t, err)
	require.Empty(t, got)
}
