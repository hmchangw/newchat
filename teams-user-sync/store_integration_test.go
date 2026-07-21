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

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoStore_ExistingIDs(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	ctx := context.Background()
	store := newMongoStore(db, db)

	_, err := db.Collection("teams_user").InsertMany(ctx, []any{
		bson.M{"_id": "u1", "upn": "a@x", "account": "a", "siteId": "site-a"},
		bson.M{"_id": "u2", "upn": "b@x", "account": "b", "siteId": "site-a"},
	})
	require.NoError(t, err)

	got, err := store.ExistingIDs(ctx, []string{"u1", "u2", "u3"})
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{"u1": {}, "u2": {}}, got)
}

func TestMongoStore_ExistingIDs_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	store := newMongoStore(db, db)

	got, err := store.ExistingIDs(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMongoStore_HRSiteIDs(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	ctx := context.Background()
	store := newMongoStore(db, db)

	_, err := db.Collection("hr_employee").InsertMany(ctx, []any{
		bson.M{"account": "alice", "siteId": "site-a", "unrelated": "x"},
		bson.M{"account": "bob", "siteId": "site-b"},
	})
	require.NoError(t, err)

	got, err := store.HRSiteIDs(ctx, []string{"alice", "bob", "carol"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"alice": "site-a", "bob": "site-b"}, got)
}

func TestMongoStore_HRSiteIDs_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	store := newMongoStore(db, db)

	got, err := store.HRSiteIDs(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMongoStore_UpsertTeamsUsers_InsertAndIdempotentRerun(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	ctx := context.Background()
	store := newMongoStore(db, db)

	users := []model.TeamsUser{
		{ID: "u1", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a"},
		{ID: "u2", UPN: "bob@corp.example", Account: "bob", SiteID: "site-b"},
	}
	require.NoError(t, store.UpsertTeamsUsers(ctx, users))
	// rerun with identical payload must not duplicate or error
	require.NoError(t, store.UpsertTeamsUsers(ctx, users))

	n, err := db.Collection("teams_user").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)

	var got model.TeamsUser
	require.NoError(t, db.Collection("teams_user").FindOne(ctx, bson.M{"_id": "u1"}).Decode(&got))
	assert.Equal(t, users[0], got)
}

func TestMongoStore_UpsertTeamsUsers_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	store := newMongoStore(db, db)
	require.NoError(t, store.UpsertTeamsUsers(context.Background(), nil))
}
