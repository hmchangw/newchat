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

func TestMongoStore_ListAndMark(t *testing.T) {
	db := testutil.MongoDB(t, "teamsroom")
	col := db.Collection("teams_chat")
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []any{
		bson.M{"_id": "c1", "name": "A", "siteId": "site-a", "needCreateRoom": true,
			"members": []bson.M{{"account": "alice", "visibleHistoryStartDateTime": nil}}},
		bson.M{"_id": "c2", "name": "B", "siteId": "site-b", "needCreateRoom": true, "members": []bson.M{}},
		bson.M{"_id": "c3", "name": "C", "siteId": "site-a", "needCreateRoom": false, "members": []bson.M{}},
	})
	require.NoError(t, err)

	store := newMongoStore(db, db)

	got, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, c := range got {
		ids[c.ID] = true
	}
	assert.Len(t, got, 2)
	assert.True(t, ids["c1"] && ids["c2"])
	assert.False(t, ids["c3"], "needCreateRoom=false must be excluded")

	require.NoError(t, store.MarkRoomsCreated(ctx, []string{"c1"}))
	after, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	assert.Len(t, after, 1)
	assert.Equal(t, "c2", after[0].ID)
}

func TestMongoStore_MarkRoomsCreated_EmptyIsNoop(t *testing.T) {
	db := testutil.MongoDB(t, "teamsroom")
	store := newMongoStore(db, db)
	ctx := context.Background()
	require.NoError(t, store.MarkRoomsCreated(ctx, nil))
	require.NoError(t, store.MarkRoomsCreated(ctx, []string{}))
}
