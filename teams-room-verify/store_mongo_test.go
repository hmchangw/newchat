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

func TestMongoStore_ListChatsNeedingVerify(t *testing.T) {
	db := testutil.MongoDB(t, "teamsverify")
	col := db.Collection("teams_chat")
	ctx := context.Background()
	ua := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)

	_, err := col.InsertMany(ctx, []any{
		bson.M{"_id": "c1", "siteId": "site-a", "needVerify": true, "updatedAt": ua,
			"members": []bson.M{{"account": "alice"}, {"account": "bob"}}},
		bson.M{"_id": "c2", "siteId": "site-b", "needVerify": true, "updatedAt": ua, "members": []bson.M{}},
		bson.M{"_id": "c3", "siteId": "site-a", "needVerify": false, "updatedAt": ua, "members": []bson.M{}},
	})
	require.NoError(t, err)

	store := newMongoStore(db, db)

	got, err := store.ListChatsNeedingVerify(ctx)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "c1", got[0].ID, "results are sorted by _id")
	assert.Equal(t, "site-a", got[0].SiteID)
	assert.Len(t, got[0].Members, 2, "members are projected — the expected count comes from them")
	assert.Equal(t, ua, got[0].UpdatedAt.UTC())
	assert.Equal(t, "c2", got[1].ID)
}

func TestMongoStore_MarkVerified_ClearsOnlyMatchingRefs(t *testing.T) {
	db := testutil.MongoDB(t, "teamsverify")
	col := db.Collection("teams_chat")
	ctx := context.Background()
	current := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	stale := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)

	_, err := col.InsertOne(ctx, bson.M{"_id": "c1", "siteId": "site-a",
		"needVerify": true, "updatedAt": current, "members": []bson.M{}})
	require.NoError(t, err)

	store := newMongoStore(db, db)

	// Stale updatedAt: the chat was re-written since it was listed, so the CAS
	// misses and the flag stays for the next run.
	require.NoError(t, store.MarkVerified(ctx, []VerifiedRef{{ID: "c1", UpdatedAt: stale}}))
	got, err := store.ListChatsNeedingVerify(ctx)
	require.NoError(t, err)
	assert.Len(t, got, 1, "stale ref must not clear the flag")

	require.NoError(t, store.MarkVerified(ctx, []VerifiedRef{{ID: "c1", UpdatedAt: current}}))
	after, err := store.ListChatsNeedingVerify(ctx)
	require.NoError(t, err)
	assert.Empty(t, after, "matching ref clears the flag")
}

func TestMongoStore_MarkVerified_EmptyIsNoop(t *testing.T) {
	db := testutil.MongoDB(t, "teamsverify")
	store := newMongoStore(db, db)
	ctx := context.Background()
	require.NoError(t, store.MarkVerified(ctx, nil))
	require.NoError(t, store.MarkVerified(ctx, []VerifiedRef{}))
}
