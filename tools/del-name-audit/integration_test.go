//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func newTestStore(t *testing.T) (*mongoStore, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "del-name-audit")
	return newMongoStore(db), db
}

func seed(t *testing.T, db *mongo.Database, collection string, docs ...any) {
	t.Helper()
	if len(docs) == 0 {
		return
	}
	_, err := db.Collection(collection).InsertMany(context.Background(), docs)
	require.NoError(t, err)
}

func seedFixture(t *testing.T, db *mongo.Database) {
	t.Helper()
	seed(t, db, roomsCollection,
		bson.M{"_id": "r-del", "name": "Del-general", "type": "channel"},
		bson.M{"_id": "r-del-empty", "name": "Del-orphan", "type": "channel"},
		bson.M{"_id": "r-live", "name": "quarterly", "type": "channel"},
	)
	seed(t, db, subscriptionsCollection,
		// Del- room, both subs mirrored — the agreeing case.
		bson.M{"_id": "s1", "name": "Del-general", "roomId": "r-del", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "s2", "name": "Del-general", "roomId": "r-del", "roomType": "channel", "siteId": "site-a"},
		// Live room, live name — must not appear in either direction.
		bson.M{"_id": "s3", "name": "quarterly", "roomId": "r-live", "roomType": "channel", "siteId": "site-a"},
		// Del--named sub whose room has no local doc — cross-site, unverifiable.
		bson.M{"_id": "s4", "name": "Del-remote", "roomId": "r-absent", "roomType": "channel", "siteId": "site-b"},
	)
}

func TestMongoStore_DelRooms_Integration(t *testing.T) {
	s, db := newTestStore(t)
	seedFixture(t, db)
	ctx := context.Background()

	rooms, err := s.delRooms(ctx, 0)
	require.NoError(t, err)
	names := map[string]string{}
	for _, room := range rooms {
		names[room.ID] = room.Name
	}
	assert.Len(t, rooms, 2, "only ^Del- rooms are scanned")
	assert.Equal(t, "Del-general", names["r-del"])
	assert.Contains(t, names, "r-del-empty")
	assert.NotContains(t, names, "r-live")
}

func TestMongoStore_DelRooms_LimitCaps_Integration(t *testing.T) {
	s, db := newTestStore(t)
	seedFixture(t, db)

	rooms, err := s.delRooms(context.Background(), 1)
	require.NoError(t, err)
	assert.Len(t, rooms, 1)
}

func TestMongoStore_SubsForRooms_Integration(t *testing.T) {
	s, db := newTestStore(t)
	seedFixture(t, db)
	ctx := context.Background()

	subs, err := s.subsForRooms(ctx, []string{"r-del", "r-del-empty"})
	require.NoError(t, err)
	require.Len(t, subs, 2)
	for _, sub := range subs {
		assert.Equal(t, "r-del", sub.RoomID)
		assert.Equal(t, "channel", sub.RoomType)
		assert.Equal(t, "site-a", sub.SiteID)
		assert.NotEmpty(t, sub.Name, "the projection must carry name")
	}

	t.Run("empty input issues no query", func(t *testing.T) {
		got, err := s.subsForRooms(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestMongoStore_DelNamedSubs_Integration(t *testing.T) {
	s, db := newTestStore(t)
	seedFixture(t, db)

	subs, err := s.delNamedSubs(context.Background(), 0)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, sub := range subs {
		ids[sub.ID] = true
	}
	assert.Len(t, subs, 3, "s1, s2 and the cross-site s4")
	assert.True(t, ids["s4"])
	assert.False(t, ids["s3"], "a live-named sub is not in the reverse set")
}

func TestMongoStore_RoomsByID_Integration(t *testing.T) {
	s, db := newTestStore(t)
	seedFixture(t, db)

	got, err := s.roomsByID(context.Background(), []string{"r-del", "r-live", "r-absent"})
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, "Del-general", got["r-del"].Name)
	assert.Equal(t, "quarterly", got["r-live"].Name)
	assert.NotContains(t, got, "r-absent", "a missing room is absent, not zero-valued")
}

// End-to-end over a clean fixture: the audit reports SAFE and classifies the
// cross-site row as unverifiable rather than as a false positive.
func TestCollectAndAudit_Safe_Integration(t *testing.T) {
	s, db := newTestStore(t)
	seedFixture(t, db)

	in, err := collect(context.Background(), s, 0, 0, 10)
	require.NoError(t, err)
	rep := audit(&in)

	assert.Equal(t, 2, rep.DelRooms)
	assert.Equal(t, 1, rep.RoomsWithNoSubs)
	assert.Equal(t, 2, rep.SubsScanned)
	assert.Equal(t, 2, rep.Agree)
	assert.Zero(t, rep.Disagree)
	assert.Zero(t, rep.FalsePositive)
	assert.Equal(t, 1, rep.Unverifiable)
	assert.True(t, rep.safeToMoveFilter())
}

// A stale subscription name over a Del- room, and a Del- name over a live room,
// are both surfaced and both block the rewrite.
func TestCollectAndAudit_Unsafe_Integration(t *testing.T) {
	s, db := newTestStore(t)
	seedFixture(t, db)
	seed(t, db, subscriptionsCollection,
		bson.M{"_id": "s5", "name": "general", "roomId": "r-del", "roomType": "dm", "siteId": "site-a"},
		bson.M{"_id": "s6", "name": "Del-quarterly", "roomId": "r-live", "roomType": "channel", "siteId": "site-a"},
	)

	in, err := collect(context.Background(), s, 0, 0, 10)
	require.NoError(t, err)
	rep := audit(&in)

	assert.Equal(t, 1, rep.Disagree)
	assert.Equal(t, 1, rep.FalsePositive)
	assert.False(t, rep.safeToMoveFilter())
	require.Contains(t, rep.ByRoomType, "dm")
	assert.Equal(t, 1, rep.ByRoomType["dm"].Disagree)

	kinds := map[string]bool{}
	for _, m := range rep.Samples {
		kinds[m.Kind] = true
	}
	assert.True(t, kinds["disagree"])
	assert.True(t, kinds["falsePositive"])
}

func TestCollectAndAudit_EmptyDatabase_Integration(t *testing.T) {
	s, _ := newTestStore(t)

	in, err := collect(context.Background(), s, 0, 0, 10)
	require.NoError(t, err)
	rep := audit(&in)

	assert.Zero(t, rep.DelRooms)
	assert.Zero(t, rep.SubsScanned)
	assert.True(t, rep.safeToMoveFilter())
}
