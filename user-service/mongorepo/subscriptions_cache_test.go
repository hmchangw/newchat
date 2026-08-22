//go:build integration

package mongorepo

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/testutil"
)

// listIDs runs AggregateSubscriptions with a wide page and returns the row IDs in order.
func listIDs(t *testing.T, r *SubscriptionRepo, account, listType string) []string {
	t.Helper()
	page, err := r.AggregateSubscriptions(context.Background(), account, listType, false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
	require.NoError(t, err)
	ids := make([]string, 0, len(page.Data))
	for _, sub := range page.Data {
		ids = append(ids, sub.ID)
	}
	return ids
}

func TestAggregateSubscriptions_SortKeysServedFromCache_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-hot", "name": "Hot", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0},
		bson.M{"_id": "r-cold", "name": "Cold", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-time.Hour)},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-hot", "u": bson.M{"_id": "u-cachy", "account": "cachy"}, "roomId": "r-hot",
			"name": "Hot", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "s-cold", "u": bson.M{"_id": "u-cachy", "account": "cachy"}, "roomId": "r-cold",
			"name": "Cold", "roomType": "channel", "siteId": "site-a"},
	)

	require.Equal(t, []string{"s-hot", "s-cold"}, listIDs(t, r, "cachy", "rooms"), "initial order by lastMsgAt desc")

	// Flip the room activity out-of-band: r-cold becomes the most recent room.
	_, err := db.Collection("rooms").UpdateOne(ctx, bson.M{"_id": "r-cold"}, bson.M{"$set": bson.M{"lastMsgAt": t0.Add(time.Hour)}})
	require.NoError(t, err)

	// While the cached keys are still good the list should keep using them —
	// not re-reading room activity per request is the point of the cache.
	assert.Equal(t, []string{"s-hot", "s-cold"}, listIDs(t, r, "cachy", "rooms"),
		"order must be served from the sort-key cache within the TTL, not re-read from rooms")
}

// The list's first read uses Find, not an aggregation, so it can't reuse
// originFilterStage and puts the Teams exclusion in its filter. Nothing else
// covers that exclusion on this path.
func TestAggregateSubscriptions_ExcludesTeamsRooms_Integration(t *testing.T) {
	seedTeams := func(t *testing.T, db *mongo.Database) {
		t.Helper()
		seed(t, db, "rooms",
			bson.M{"_id": "r-native", "name": "Native", "siteId": "site-a", "userCount": 1, "lastMsgAt": time.Now().UTC()},
			bson.M{"_id": "r-teams", "name": "Teams", "siteId": "site-a", "userCount": 1, "lastMsgAt": time.Now().UTC()},
		)
		seed(t, db, "subscriptions",
			bson.M{"_id": "s-native", "u": bson.M{"_id": "u-t", "account": "tuser"}, "roomId": "r-native",
				"name": "Native", "roomType": "channel", "siteId": "site-a"},
			bson.M{"_id": "s-teams", "u": bson.M{"_id": "u-t", "account": "tuser"}, "roomId": "r-teams",
				"name": "Teams", "roomType": "channel", "siteId": "site-a", "origin": model.OriginTeams},
		)
	}

	t.Run("hidden by default", func(t *testing.T) {
		r, db := newTestSubscriptionRepo(t)
		seedTeams(t, db)
		assert.Equal(t, []string{"s-native"}, listIDs(t, r, "tuser", "rooms"))
	})

	t.Run("shown when allowlisted", func(t *testing.T) {
		db := testutil.MongoDB(t, "user-service")
		r := NewSubscriptionRepo(db, 100_000, 15*time.Second, WithShowTeamsAccounts([]string{"tuser"}))
		require.NoError(t, r.EnsureIndexes(context.Background()))
		seedTeams(t, db)
		assert.ElementsMatch(t, []string{"s-native", "s-teams"}, listIDs(t, r, "tuser", "rooms"))
	})
}

func TestAggregateSubscriptions_NegativeCachesMissingRooms_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-here", "name": "Here", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-time.Minute)},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-here", "u": bson.M{"_id": "u-negs", "account": "negs"}, "roomId": "r-here",
			"name": "Here", "roomType": "channel", "siteId": "site-a"},
		// No local room document yet: kept, but with no sort key it lands last.
		bson.M{"_id": "s-late", "u": bson.M{"_id": "u-negs", "account": "negs"}, "roomId": "r-late",
			"name": "Late", "roomType": "channel", "siteId": "site-a"},
	)

	require.Equal(t, []string{"s-here", "s-late"}, listIDs(t, r, "negs", "rooms"), "missing room sorts last")

	// The room document turns up after the first list cached that it was missing.
	// That answer should keep being used until it expires, so a missing room
	// isn't looked up again on every request.
	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r-late", "name": "Late", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(time.Hour)})
	require.NoError(t, err)

	assert.Equal(t, []string{"s-here", "s-late"}, listIDs(t, r, "negs", "rooms"),
		"missing-room resolution must be negative-cached within the TTL")
}

// When the fresh read drops rows — here, subscriptions deleted after their sort
// keys were cached — the page pulls in the next candidates. The client gets a
// full page of rows that exist and a matching HasMore, not a short page whose
// HasMore sends the next request to an offset that skips rows.
func TestAggregateSubscriptions_PageRefillsPastFreshDeletes_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	for i, id := range []string{"a", "b", "c", "d"} {
		seed(t, db, "rooms",
			bson.M{"_id": "r-" + id, "name": "Room" + id, "siteId": "site-a", "userCount": 1,
				"lastMsgAt": t0.Add(-time.Duration(i) * time.Minute)},
		)
		seed(t, db, "subscriptions",
			bson.M{"_id": "s-" + id, "u": bson.M{"_id": "u-refill", "account": "refill"}, "roomId": "r-" + id,
				"name": "Room" + id, "roomType": "channel", "siteId": "site-a"},
		)
	}

	require.Equal(t, []string{"s-a", "s-b", "s-c", "s-d"}, listIDs(t, r, "refill", "rooms"), "warm the cache with all four live")

	// The two rows the first page would serve are deleted elsewhere.
	_, err := db.Collection("subscriptions").DeleteMany(ctx, bson.M{"_id": bson.M{"$in": bson.A{"s-a", "s-b"}}})
	require.NoError(t, err)

	page, err := r.AggregateSubscriptions(ctx, "refill", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 2})
	require.NoError(t, err)
	ids := make([]string, 0, len(page.Data))
	for _, sub := range page.Data {
		ids = append(ids, sub.ID)
	}
	assert.Equal(t, []string{"s-c", "s-d"}, ids, "page must refill from the next live candidates")
	assert.False(t, page.HasMore, "no live rows remain beyond the refilled page")
}

// The withinDays window decides whether a room appears at all, not just where
// it sorts, so it must not hide a room whose latest activity the cache hasn't
// seen: a key outside the window is read again before the row is dropped.
// Slightly stale ordering is fine; a missing room is not.
func TestAggregateSubscriptions_WindowSeesFreshActivityDespiteWarmCache_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()
	days := 7

	seed(t, db, "rooms",
		bson.M{"_id": "r-dormant", "name": "Dormant", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.AddDate(0, 0, -30)},
		bson.M{"_id": "r-active", "name": "Active", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-time.Minute)},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-dormant", "u": bson.M{"_id": "u-windy", "account": "windy"}, "roomId": "r-dormant",
			"name": "Dormant", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "s-active", "u": bson.M{"_id": "u-windy", "account": "windy"}, "roomId": "r-active",
			"name": "Active", "roomType": "channel", "siteId": "site-a"},
	)

	windowedIDs := func() []string {
		t.Helper()
		page, err := r.AggregateSubscriptions(ctx, "windy", "rooms", false, &days, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		ids := make([]string, 0, len(page.Data))
		for _, sub := range page.Data {
			ids = append(ids, sub.ID)
		}
		return ids
	}

	// The first windowed list leaves the dormant room out, caching its old key.
	require.Equal(t, []string{"s-active"}, windowedIDs(), "dormant room outside the window")

	// Then it gets its first message in weeks — exactly when clients re-list. The
	// row must appear despite the cached out-of-window key.
	_, err := db.Collection("rooms").UpdateOne(ctx, bson.M{"_id": "r-dormant"}, bson.M{"$set": bson.M{"lastMsgAt": t0.Add(time.Hour)}})
	require.NoError(t, err)

	assert.Equal(t, []string{"s-dormant", "s-active"}, windowedIDs(),
		"a room entering the window must appear immediately, not after the cache TTL")
}

// Out-of-range page values should behave as the old Mongo paging did: an error
// or a sensible empty result, never a slice-bounds panic. The service layer
// clamps them today, but the repo shouldn't crash on a future caller's input.
func TestAggregateSubscriptions_NonNormalizedPageValues_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-p1", "name": "P1", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0},
		bson.M{"_id": "r-p2", "name": "P2", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-time.Minute)},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-p1", "u": bson.M{"_id": "u-clamp", "account": "clamp"}, "roomId": "r-p1",
			"name": "P1", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "s-p2", "u": bson.M{"_id": "u-clamp", "account": "clamp"}, "roomId": "r-p2",
			"name": "P2", "roomType": "channel", "siteId": "site-a"},
	)

	// A negative offset reads from the start, giving the same page as offset 0.
	page, err := r.AggregateSubscriptions(ctx, "clamp", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: -5, Limit: 1})
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Equal(t, "s-p1", page.Data[0].ID)
	assert.True(t, page.HasMore)

	// A negative limit acts like zero: an empty page, with HasMore still saying
	// there are rows past it, as the old over-read did.
	page, err = r.AggregateSubscriptions(ctx, "clamp", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: -1})
	require.NoError(t, err)
	assert.Empty(t, page.Data)
	assert.True(t, page.HasMore)

	// A limit of MaxInt64 must not overflow the over-read arithmetic: everything
	// comes back and nothing is left over.
	page, err = r.AggregateSubscriptions(ctx, "clamp", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: math.MaxInt64})
	require.NoError(t, err)
	assert.Len(t, page.Data, 2)
	assert.False(t, page.HasMore)
}

// Pins the ordering before the sort moves out of Mongo: rows with nothing to
// sort on — no local room, or neither lastMsgAt nor createdAt — come after every
// dated row, by name among themselves, and a room with no lastMsgAt sorts on
// its createdAt.
func TestAggregateSubscriptions_NullSortKeysLastAndCreatedAtFallback_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	t0 := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-msg", "name": "Msg", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-30 * time.Minute)},
		// A createdAt newer than r-msg's lastMsgAt: the fallback should win.
		bson.M{"_id": "r-fresh", "name": "Fresh", "siteId": "site-a", "userCount": 1, "createdAt": t0},
		// Neither lastMsgAt nor createdAt, so nothing to sort on.
		bson.M{"_id": "r-bare-b", "name": "BareB", "siteId": "site-a", "userCount": 1},
		bson.M{"_id": "r-bare-a", "name": "BareA", "siteId": "site-a", "userCount": 1},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-msg", "u": bson.M{"_id": "u-ord", "account": "ord"}, "roomId": "r-msg",
			"name": "Msg", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "s-fresh", "u": bson.M{"_id": "u-ord", "account": "ord"}, "roomId": "r-fresh",
			"name": "Fresh", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "s-bare-b", "u": bson.M{"_id": "u-ord", "account": "ord"}, "roomId": "r-bare-b",
			"name": "BareB", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "s-bare-a", "u": bson.M{"_id": "u-ord", "account": "ord"}, "roomId": "r-bare-a",
			"name": "BareA", "roomType": "channel", "siteId": "site-a"},
		// Owned by another site: no local room document, so nothing to sort on.
		bson.M{"_id": "s-x", "u": bson.M{"_id": "u-ord", "account": "ord"}, "roomId": "r-x",
			"name": "Across", "roomType": "channel", "siteId": "site-b"},
	)

	assert.Equal(t, []string{"s-fresh", "s-msg", "s-x", "s-bare-a", "s-bare-b"}, listIDs(t, r, "ord", "rooms"),
		"createdAt fallback first (newest), then lastMsgAt, then null keys name-asc")
}

// A subscription that stops matching the filter partway through must not reach
// the page, so the refetch re-applies the filter instead of selecting _ids.
// Calling enrichListRows directly reproduces a real un-favorite's timing.
func TestEnrichListRows_DropsRowsThatNoLongerMatchTheFilter_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-keep", "name": "Keep", "siteId": "site-a", "userCount": 2, "lastMsgAt": t0},
		bson.M{"_id": "r-drop", "name": "Drop", "siteId": "site-a", "userCount": 2, "lastMsgAt": t0},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-keep", "u": bson.M{"_id": "u-amy", "account": "amy"}, "roomId": "r-keep",
			"name": "Keep", "roomType": "channel", "siteId": "site-a", "favorite": true, "_updatedAt": t0},
		bson.M{"_id": "s-drop", "u": bson.M{"_id": "u-amy", "account": "amy"}, "roomId": "r-drop",
			"name": "Drop", "roomType": "channel", "siteId": "site-a", "favorite": true, "_updatedAt": t0},
	)

	// The first read saw both rows as favorites.
	rows := []listRow{
		{sub: subLite{ID: "s-keep", RoomID: "r-keep", RoomType: "channel", Name: "Keep"}, sortAt: &t0},
		{sub: subLite{ID: "s-drop", RoomID: "r-drop", RoomType: "channel", Name: "Drop"}, sortAt: &t0},
	}
	// Then the user un-favorites one before the page is filled in.
	_, err := db.Collection("subscriptions").UpdateOne(ctx,
		bson.M{"_id": "s-drop"}, bson.M{"$set": bson.M{"favorite": false}})
	require.NoError(t, err)

	match := bson.M{"u.account": "amy", "favorite": true, "open": bson.M{"$ne": false}}
	got, err := r.enrichListRows(ctx, match, rows)
	require.NoError(t, err)
	require.Len(t, got, 1, "the un-favorited row must not survive the refetch")
	assert.Equal(t, "s-keep", got[0].ID)
}
