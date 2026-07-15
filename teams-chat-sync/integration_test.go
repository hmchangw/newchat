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
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func seedUsers(t *testing.T, store *mongoStore, users ...model.TeamsUser) {
	t.Helper()
	docs := make([]any, 0, len(users))
	for _, u := range users {
		docs = append(docs, u)
	}
	_, err := store.users.Raw().InsertMany(context.Background(), docs)
	require.NoError(t, err)
}

func TestMongoStore_ListUsers(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db)
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	seedUsers(t, store,
		model.TeamsUser{ID: "u1", SiteID: "site-a", Account: "alice", From: &from},
		model.TeamsUser{ID: "u2", SiteID: "site-b", Account: "bob"},
	)

	users, err := store.ListUsers(context.Background())
	require.NoError(t, err)
	require.Len(t, users, 2)
	byID := map[string]model.TeamsUser{users[0].ID: users[0], users[1].ID: users[1]}
	require.NotNil(t, byID["u1"].From)
	assert.True(t, byID["u1"].From.Equal(from))
	assert.Equal(t, "site-a", byID["u1"].SiteID)
	assert.Equal(t, "alice", byID["u1"].Account)
	assert.Nil(t, byID["u2"].From, "user without watermark loads with nil From")
}

func TestMongoStore_SetFrom(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db)
	seedUsers(t, store, model.TeamsUser{ID: "u1", SiteID: "site-a", Account: "alice"})

	to := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	require.NoError(t, store.SetFrom(context.Background(), "u1", to))

	got, err := store.users.FindByID(context.Background(), "u1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.From)
	assert.True(t, got.From.Equal(to))
}

func groupChat(id, name, siteID string, updatedAt time.Time) model.TeamsChat {
	return model.TeamsChat{
		ID: id, Name: name, ChatType: "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}},
		SiteID:              siteID,
		UpdatedAt:           updatedAt,
		NeedMemberSync:      true,
	}
}

func TestMongoStore_UpsertChats_SiteIDImmutable(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db)
	ctx := context.Background()
	now1 := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)
	now2 := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g1", "First", "site-a", now1)}))
	// Second sync computes a different majority and a new name.
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g1", "Renamed", "site-b", now2)}))

	got, err := store.chats.FindByID(ctx, "19:g1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "site-a", got.SiteID, "siteID must never change after insert")
	assert.Equal(t, "Renamed", got.Name, "mutable fields must refresh")
	assert.True(t, got.UpdatedAt.Equal(now2), "updatedAt refreshes to the second write's stamp")
	assert.True(t, got.NeedMemberSync)
}

func TestMongoStore_UpsertChats_OneOnOneInsertOnly(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db)
	ctx := context.Background()
	now1 := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)
	now2 := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

	one := model.TeamsChat{
		ID: "19:one1", ChatType: model.TeamsChatTypeOneOnOne,
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}, {ID: "u2", Account: "bob"}},
		SiteID:              "site-a",
		UpdatedAt:           now1,
	}
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{one}))

	changed := one
	changed.SiteID = "site-b"
	changed.LastUpdatedDateTime = time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	changed.UpdatedAt = now2
	changed.NeedMemberSync = true // must NOT stick: oneOnOne re-upsert never modifies the doc
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{changed}))

	got, err := store.chats.FindByID(ctx, "19:one1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "site-a", got.SiteID)
	assert.True(t, got.LastUpdatedDateTime.Equal(one.LastUpdatedDateTime), "oneOnOne doc must be untouched by re-upsert")
	assert.True(t, got.UpdatedAt.Equal(now1))
	assert.False(t, got.NeedMemberSync)
}

func TestMongoStore_UpsertChats_MixedBatchAndEmpty(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db)
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)

	require.NoError(t, store.UpsertChats(ctx, nil), "empty batch is a no-op")

	one := model.TeamsChat{ID: "19:one2", ChatType: model.TeamsChatTypeOneOnOne, SiteID: "site-a",
		CreatedDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: now}
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g2", "G", "site-a", now), one}))

	n, err := store.chats.Raw().CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
}
