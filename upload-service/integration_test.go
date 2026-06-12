//go:build integration

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoStore_IsMemberAndGetRoomSiteID(t *testing.T) {
	db := testutil.MongoDB(t, "uploadsvc")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "sub1", "roomId": "r1", "u": bson.M{"_id": "u1", "account": "alice"},
	})
	require.NoError(t, err)
	_, err = db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r1", "name": "Room 1", "siteId": "site-x"})
	require.NoError(t, err)

	s := NewMongoStore(db)

	member, err := s.IsMember(ctx, "r1", "alice")
	require.NoError(t, err)
	require.True(t, member)

	member, err = s.IsMember(ctx, "r1", "bob")
	require.NoError(t, err)
	require.False(t, member)

	siteID, err := s.GetRoomSiteID(ctx, "r1")
	require.NoError(t, err)
	require.Equal(t, "site-x", siteID)

	_, err = s.GetRoomSiteID(ctx, "missing")
	require.True(t, errors.Is(err, ErrRoomNotFound))
}
