//go:build integration

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func seedChats(t *testing.T, store *mongoStore, chats ...model.TeamsChat) {
	t.Helper()
	docs := make([]any, 0, len(chats))
	for _, c := range chats {
		docs = append(docs, c)
	}
	_, err := store.writeChats.Raw().InsertMany(context.Background(), docs)
	require.NoError(t, err)
}

func TestMongoStore_ListChatsToSync(t *testing.T) {
	db := testutil.MongoDB(t, "teamsmembersync")
	store := newMongoStore(db, db)
	updatedAt1 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	updatedAt2 := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	seedChats(t, store,
		model.TeamsChat{ID: "19:need1", ChatType: "group", NeedMemberSync: true, UpdatedAt: updatedAt1},
		model.TeamsChat{ID: "19:done1", ChatType: "group", NeedMemberSync: false},
		model.TeamsChat{ID: "19:need2", ChatType: "meeting", NeedMemberSync: true, UpdatedAt: updatedAt2},
	)

	chats, err := store.ListChatsToSync(context.Background())
	require.NoError(t, err)
	ids := make([]string, 0, len(chats))
	byID := make(map[string]ChatToSync, len(chats))
	for _, c := range chats {
		ids = append(ids, c.ID)
		byID[c.ID] = c
	}
	assert.ElementsMatch(t, []string{"19:need1", "19:need2"}, ids)
	require.Contains(t, byID, "19:need1")
	require.Contains(t, byID, "19:need2")
	assert.True(t, byID["19:need1"].UpdatedAt.Equal(updatedAt1))
	assert.True(t, byID["19:need2"].UpdatedAt.Equal(updatedAt2))
}

func TestMongoStore_SetMembersSynced(t *testing.T) {
	db := testutil.MongoDB(t, "teamsmembersync")
	store := newMongoStore(db, db)
	ctx := context.Background()
	seededUpdatedAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	seedChats(t, store, model.TeamsChat{
		ID: "19:g1", ChatType: "group", NeedMemberSync: true, NeedCreateRoom: false,
		Members:   []model.TeamsChatMember{{ID: "old", Account: "old"}},
		UpdatedAt: seededUpdatedAt,
	})

	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	members := []model.TeamsChatMember{
		{ID: "u1", Account: "alice", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "u2", Account: "bob"},
	}
	require.NoError(t, store.SetMembersSynced(ctx, "19:g1", seededUpdatedAt, members, now))

	got, err := store.writeChats.FindByID(ctx, "19:g1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.NeedMemberSync, "needMemberSync cleared")
	assert.True(t, got.NeedCreateRoom, "needCreateRoom set")
	assert.True(t, got.UpdatedAt.Equal(now))
	require.Len(t, got.Members, 2, "members fully replaced")
	assert.Equal(t, "u1", got.Members[0].ID)
	assert.Equal(t, "alice", got.Members[0].Account)
	assert.True(t, got.Members[0].VisibleHistoryStartDateTime.Equal(members[0].VisibleHistoryStartDateTime))
}

func TestMongoStore_SetMembersSynced_Superseded(t *testing.T) {
	db := testutil.MongoDB(t, "teamsmembersync")
	store := newMongoStore(db, db)
	ctx := context.Background()
	seededUpdatedAt := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	seededMembers := []model.TeamsChatMember{{ID: "old", Account: "old"}}
	seedChats(t, store, model.TeamsChat{
		ID: "19:g1", ChatType: "group", NeedMemberSync: true, NeedCreateRoom: false,
		Members:   seededMembers,
		UpdatedAt: seededUpdatedAt,
	})

	staleSeenAt := seededUpdatedAt.Add(-time.Hour)
	now := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	members := []model.TeamsChatMember{{ID: "u1", Account: "alice"}}

	err := store.SetMembersSynced(ctx, "19:g1", staleSeenAt, members, now)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errSuperseded))

	got, err := store.writeChats.FindByID(ctx, "19:g1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.NeedMemberSync, "needMemberSync left true for retry")
	assert.True(t, got.UpdatedAt.Equal(seededUpdatedAt), "updatedAt unchanged")
	assert.Equal(t, seededMembers, got.Members, "members unchanged")
}

func TestMongoStore_AccountsByIDs(t *testing.T) {
	db := testutil.MongoDB(t, "teamsmembersync")
	store := newMongoStore(db, db)
	ctx := context.Background()
	_, err := store.readUsers.Raw().InsertMany(ctx, []any{
		model.TeamsUser{ID: "u1", UPN: "alice@corp.example", Account: "alice", SiteID: "site-a"},
		model.TeamsUser{ID: "u2", UPN: "bob@corp.example", Account: "bob", SiteID: "site-b"},
	})
	require.NoError(t, err)

	got, err := store.AccountsByIDs(ctx, []string{"u1", "u2", "ghost"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u1": "alice", "u2": "bob"}, got, "unknown id absent from map")

	empty, err := store.AccountsByIDs(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
