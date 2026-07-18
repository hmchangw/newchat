//go:build integration

package main

import (
	"context"
	"testing"
	"time"

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
	ua := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)

	_, err := col.InsertMany(ctx, []any{
		bson.M{"_id": "c1", "name": "A", "siteId": "site-a", "needCreateRoom": true, "updatedAt": ua,
			"members": []bson.M{{"account": "alice", "visibleHistoryStartDateTime": nil}}},
		bson.M{"_id": "c2", "name": "B", "siteId": "site-b", "needCreateRoom": true, "updatedAt": ua, "members": []bson.M{}},
		bson.M{"_id": "c3", "name": "C", "siteId": "site-a", "needCreateRoom": false, "updatedAt": ua, "members": []bson.M{}},
	})
	require.NoError(t, err)

	store := newMongoStore(db, db)

	got, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	byID := map[string]RoomCreatedRef{}
	for _, c := range got {
		byID[c.ID] = RoomCreatedRef{ID: c.ID, UpdatedAt: c.UpdatedAt}
	}
	assert.Len(t, got, 2)
	assert.Contains(t, byID, "c1")
	assert.Contains(t, byID, "c2")
	assert.NotContains(t, byID, "c3", "needCreateRoom=false must be excluded")

	// Clear c1 with the updatedAt we read (compare-and-set matches).
	require.NoError(t, store.MarkRoomsCreated(ctx, []RoomCreatedRef{byID["c1"]}))
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
	require.NoError(t, store.MarkRoomsCreated(ctx, []RoomCreatedRef{}))
}

// TestMongoStore_MarkRoomsCreated_StaleRefIsNoop is the compare-and-set guard:
// a ref whose updatedAt no longer matches (member-sync re-wrote the chat and
// re-flagged needCreateRoom after it was listed) must NOT clear the flag, so the
// re-flagged chat is re-published next run instead of being silently dropped.
func TestMongoStore_MarkRoomsCreated_StaleRefIsNoop(t *testing.T) {
	db := testutil.MongoDB(t, "teamsroom")
	col := db.Collection("teams_chat")
	ctx := context.Background()
	current := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	stale := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)

	_, err := col.InsertOne(ctx, bson.M{"_id": "c1", "name": "A", "siteId": "site-a",
		"needCreateRoom": true, "updatedAt": current, "members": []bson.M{}})
	require.NoError(t, err)

	store := newMongoStore(db, db)

	// Stale updatedAt: CAS misses, flag stays set.
	require.NoError(t, store.MarkRoomsCreated(ctx, []RoomCreatedRef{{ID: "c1", UpdatedAt: stale}}))
	got, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	assert.Len(t, got, 1, "stale ref must not clear the flag")

	// Current updatedAt: CAS matches, flag clears.
	require.NoError(t, store.MarkRoomsCreated(ctx, []RoomCreatedRef{{ID: "c1", UpdatedAt: current}}))
	after, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	assert.Empty(t, after, "matching ref clears the flag")
}
