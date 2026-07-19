//go:build integration

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

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
	_, err := store.writeUsers.Raw().InsertMany(context.Background(), docs)
	require.NoError(t, err)
}

func TestMongoStore_ListUsers(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db, db)
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

// TestMongoStore_ListUsers_OrdersByWatermarkNullFirst pins the least-first
// dispatch order: a user that has never synced (no `from`) is returned before
// any user with a watermark, and the rest come oldest-watermark first.
func TestMongoStore_ListUsers_OrdersByWatermarkNullFirst(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db, db)
	ctx := context.Background()

	old := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	// Inserted deliberately out of watermark order to prove the query sorts.
	seedUsers(t, store,
		model.TeamsUser{ID: "u-new", SiteID: "site-a", Account: "newbie", From: &newer},
		model.TeamsUser{ID: "u-null", SiteID: "site-a", Account: "never"}, // no From (never synced)
		model.TeamsUser{ID: "u-old", SiteID: "site-a", Account: "oldie", From: &old},
	)

	users, err := store.ListUsers(ctx)
	require.NoError(t, err)
	require.Len(t, users, 3)
	assert.Equal(t, []string{"u-null", "u-old", "u-new"},
		[]string{users[0].ID, users[1].ID, users[2].ID},
		"never-synced (null from) first, then oldest watermark to newest")
}

// teamsIndex is the subset of an index spec the EnsureIndexes test asserts on.
type teamsIndex struct {
	Name    string `bson:"name"`
	Key     bson.D `bson:"key"`
	Partial bson.M `bson:"partialFilterExpression"`
}

// listIndexes returns col's indexes keyed by name.
func listIndexes(t *testing.T, col *mongo.Collection) map[string]teamsIndex {
	t.Helper()
	cur, err := col.Indexes().List(context.Background())
	require.NoError(t, err)
	var raw []teamsIndex
	require.NoError(t, cur.All(context.Background(), &raw))
	out := make(map[string]teamsIndex, len(raw))
	for _, ix := range raw {
		out[ix.Name] = ix
	}
	return out
}

// keySpec renders an index key as "field:dir,..." (order-preserving), so a
// compound index is asserted exactly and dir type (int32/int) doesn't matter.
func keySpec(k bson.D) string {
	parts := make([]string, 0, len(k))
	for _, e := range k {
		parts = append(parts, fmt.Sprintf("%s:%v", e.Key, e.Value))
	}
	return strings.Join(parts, ",")
}

func TestMongoStore_EnsureIndexes(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db, db)
	ctx := context.Background()

	require.NoError(t, store.EnsureIndexes(ctx))
	require.NoError(t, store.EnsureIndexes(ctx), "EnsureIndexes must be idempotent")

	userIdx := listIndexes(t, store.writeUsers.Raw())
	fromIdx, ok := userIdx["from_1"]
	require.True(t, ok, "teams_user must have the from index")
	assert.Equal(t, "from:1", keySpec(fromIdx.Key))
	assert.Nil(t, fromIdx.Partial, "from index is full (non-partial) so null/never-synced users are ordered too")

	chatIdx := listIndexes(t, store.writeChats.Raw())
	memberSync, ok := chatIdx["needMemberSync_pending"]
	require.True(t, ok, "teams_chat must have the needMemberSync pending index")
	assert.Equal(t, "needMemberSync:1", keySpec(memberSync.Key))
	assert.Equal(t, true, memberSync.Partial["needMemberSync"], "indexes only needMemberSync=true docs")

	createRoom, ok := chatIdx["needCreateRoom_pending"]
	require.True(t, ok, "teams_chat must have the needCreateRoom pending index")
	assert.Equal(t, "needCreateRoom:1,_id:1", keySpec(createRoom.Key),
		"compound with _id serves find(needCreateRoom:true).sort(_id) without an in-memory sort")
	assert.Equal(t, true, createRoom.Partial["needCreateRoom"], "indexes only needCreateRoom=true docs")
}

func TestMongoStore_SetFrom(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db, db)
	seedUsers(t, store, model.TeamsUser{ID: "u1", SiteID: "site-a", Account: "alice"})

	to := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	require.NoError(t, store.SetFrom(context.Background(), "u1", to))

	got, err := store.writeUsers.FindByID(context.Background(), "u1")
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
	store := newMongoStore(db, db)
	ctx := context.Background()
	now1 := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)
	now2 := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g1", "First", "site-a", now1)}))
	// Second sync computes a different majority and a new name.
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g1", "Renamed", "site-b", now2)}))

	got, err := store.writeChats.FindByID(ctx, "19:g1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "site-a", got.SiteID, "siteID must never change after insert")
	assert.Equal(t, "Renamed", got.Name, "mutable fields must refresh")
	assert.True(t, got.UpdatedAt.Equal(now2), "updatedAt refreshes to the second write's stamp")
	assert.True(t, got.NeedMemberSync)
	assert.False(t, got.NeedCreateRoom, "group defers room creation to member-sync")
	assert.Empty(t, got.Members, "group members are owned by member-sync, not this sync")
}

// TestMongoStore_UpsertChats_GroupReSyncRetriggersButNeverClobbers guards the
// membership-sync invariant on a group re-sync (a chat is re-listed whenever its
// lastUpdatedDateTime moves): needMemberSync is re-set true to re-trigger member
// sync, while needCreateRoom is $setOnInsert so a re-sync can never overwrite the
// true that member-sync sets — which is what would lose a membership-change event.
func TestMongoStore_UpsertChats_GroupReSyncRetriggersButNeverClobbers(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db, db)
	ctx := context.Background()
	now1 := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)
	now2 := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

	// Case A — re-sync must NOT clobber needCreateRoom that member-sync just set.
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:gA", "First", "site-a", now1)}))
	// member-sync resolved the roster and advanced it (room not yet created).
	_, err := store.writeChats.Raw().UpdateByID(ctx, "19:gA",
		bson.M{"$set": bson.M{"needMemberSync": false, "needCreateRoom": true}})
	require.NoError(t, err)
	// chat-sync re-syncs before room-create ran.
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:gA", "Renamed", "site-a", now2)}))
	gotA, err := store.writeChats.FindByID(ctx, "19:gA")
	require.NoError(t, err)
	require.NotNil(t, gotA)
	assert.Equal(t, "Renamed", gotA.Name, "metadata refreshes on re-sync")
	assert.True(t, gotA.NeedMemberSync, "re-sync re-triggers member sync")
	assert.True(t, gotA.NeedCreateRoom, "re-sync must NOT clobber member-sync's needCreateRoom")

	// Case B — after the room was created (needCreateRoom cleared), a re-sync
	// re-triggers member sync but does not itself re-set needCreateRoom;
	// member-sync will flip it true again once it re-resolves the roster.
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:gB", "First", "site-a", now1)}))
	_, err = store.writeChats.Raw().UpdateByID(ctx, "19:gB",
		bson.M{"$set": bson.M{"needMemberSync": false, "needCreateRoom": false}})
	require.NoError(t, err)
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:gB", "Renamed", "site-a", now2)}))
	gotB, err := store.writeChats.FindByID(ctx, "19:gB")
	require.NoError(t, err)
	require.NotNil(t, gotB)
	assert.True(t, gotB.NeedMemberSync, "re-sync re-triggers member sync")
	assert.False(t, gotB.NeedCreateRoom, "chat-sync re-sync does not set needCreateRoom; member-sync owns that")
}

// TestMongoStore_UpsertChats_SmallGroupFinalizesInline covers the inline-finalize
// path: a non-oneOnOne chat with needMemberSync=false (a complete inline roster)
// is finalized by this sync itself — members are written and needCreateRoom is
// set true — and a re-sync refreshes the roster and re-flags needCreateRoom
// (mirroring what member-sync would write for a larger chat), while siteID stays
// immutable.
func TestMongoStore_UpsertChats_SmallGroupFinalizesInline(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db, db)
	ctx := context.Background()
	now1 := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)
	now2 := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

	small := func(name, siteID string, members []model.TeamsChatMember, updatedAt time.Time) model.TeamsChat {
		return model.TeamsChat{
			ID: "19:small", Name: name, ChatType: "group",
			CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			Members:             members,
			SiteID:              siteID,
			UpdatedAt:           updatedAt,
			NeedMemberSync:      false, // inline roster complete -> finalized here
		}
	}

	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{
		small("First", "site-a", []model.TeamsChatMember{{ID: "u1", Account: "alice"}}, now1),
	}))
	got, err := store.writeChats.FindByID(ctx, "19:small")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.NeedMemberSync, "small chat skips member sync")
	assert.True(t, got.NeedCreateRoom, "small chat is finalized room-ready by this sync")
	require.Len(t, got.Members, 1)
	assert.Equal(t, "alice", got.Members[0].Account, "the inline roster is written directly")

	// Room-creation clears the flag; then a re-sync with a changed roster must
	// refresh the members and re-flag needCreateRoom, without changing siteID.
	_, err = store.writeChats.Raw().UpdateByID(ctx, "19:small", bson.M{"$set": bson.M{"needCreateRoom": false}})
	require.NoError(t, err)
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{
		small("Renamed", "site-b", []model.TeamsChatMember{{ID: "u1", Account: "alice"}, {ID: "u2", Account: "bob"}}, now2),
	}))
	got2, err := store.writeChats.FindByID(ctx, "19:small")
	require.NoError(t, err)
	require.NotNil(t, got2)
	assert.Equal(t, "site-a", got2.SiteID, "siteID must never change after insert")
	assert.Equal(t, "Renamed", got2.Name, "metadata refreshes on re-sync")
	assert.True(t, got2.UpdatedAt.Equal(now2), "updatedAt refreshes to the second write's stamp")
	assert.False(t, got2.NeedMemberSync)
	assert.True(t, got2.NeedCreateRoom, "re-sync re-flags needCreateRoom so room-creation re-syncs the roster")
	require.Len(t, got2.Members, 2, "the roster refreshes to the new inline member list")
}

func TestMongoStore_UpsertChats_OneOnOneInsertOnly(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db, db)
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

	got, err := store.writeChats.FindByID(ctx, "19:one1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "site-a", got.SiteID)
	assert.True(t, got.LastUpdatedDateTime.Equal(one.LastUpdatedDateTime), "oneOnOne doc must be untouched by re-upsert")
	assert.True(t, got.UpdatedAt.Equal(now1))
	assert.False(t, got.NeedMemberSync)
	assert.True(t, got.NeedCreateRoom, "oneOnOne is ready for room creation on insert")
}

func TestMongoStore_UpsertChats_MixedBatchAndEmpty(t *testing.T) {
	db := testutil.MongoDB(t, "teamsstore")
	store := newMongoStore(db, db)
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)

	require.NoError(t, store.UpsertChats(ctx, nil), "empty batch is a no-op")

	one := model.TeamsChat{ID: "19:one2", ChatType: model.TeamsChatTypeOneOnOne, SiteID: "site-a",
		CreatedDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: now}
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g2", "G", "site-a", now), one}))

	n, err := store.writeChats.Raw().CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
}
