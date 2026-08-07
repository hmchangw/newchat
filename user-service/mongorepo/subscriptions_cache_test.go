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

	"github.com/hmchangw/chat/pkg/mongoutil"
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

	// Within the cache TTL the list must keep serving the cached sort keys —
	// the whole point of the cache is not re-reading room activity per request.
	assert.Equal(t, []string{"s-hot", "s-cold"}, listIDs(t, r, "cachy", "rooms"),
		"order must be served from the sort-key cache within the TTL, not re-read from rooms")
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
		// No local room doc yet — resolves as missing (kept, null sort key, sorts last).
		bson.M{"_id": "s-late", "u": bson.M{"_id": "u-negs", "account": "negs"}, "roomId": "r-late",
			"name": "Late", "roomType": "channel", "siteId": "site-a"},
	)

	require.Equal(t, []string{"s-here", "s-late"}, listIDs(t, r, "negs", "rooms"), "missing room sorts last")

	// The room doc appears after the first list cached its absence. Within the
	// TTL the negative entry must keep answering — no per-request Mongo re-probe
	// for rooms that resolved as missing.
	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r-late", "name": "Late", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(time.Hour)})
	require.NoError(t, err)

	assert.Equal(t, []string{"s-here", "s-late"}, listIDs(t, r, "negs", "rooms"),
		"missing-room resolution must be negative-cached within the TTL")
}

// A soft-delete rename must take effect immediately even when the sort-key
// cache still holds the live name: the page itself is enriched from a fresh
// room read, and that read drops rows whose room turned Del-.
func TestAggregateSubscriptions_DelRenameDropsRowDespiteWarmCache_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-live", "name": "Live", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0},
		bson.M{"_id": "r-keep", "name": "Keep", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-time.Minute)},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-live", "u": bson.M{"_id": "u-deli", "account": "deli"}, "roomId": "r-live",
			"name": "Live", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "s-keep", "u": bson.M{"_id": "u-deli", "account": "deli"}, "roomId": "r-keep",
			"name": "Keep", "roomType": "channel", "siteId": "site-a"},
	)

	require.Equal(t, []string{"s-live", "s-keep"}, listIDs(t, r, "deli", "rooms"), "both rows listed while live")

	_, err := db.Collection("rooms").UpdateOne(ctx, bson.M{"_id": "r-live"}, bson.M{"$set": bson.M{"name": "Del-Live"}})
	require.NoError(t, err)

	assert.Equal(t, []string{"s-keep"}, listIDs(t, r, "deli", "rooms"),
		"soft-deleted room must disappear from the list immediately, cache or not")
}

// A page must be refilled from later live candidates when the fresh read drops
// rows that turned Del- after their sort keys were cached: the client must get
// a full page of live rows and a HasMore computed from the live sequence — not
// a short page with a dangling HasMore that makes the next offset skip rows.
func TestAggregateSubscriptions_PageRefillsPastFreshSoftDeletes_Integration(t *testing.T) {
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

	// The two rows the first page would serve turn soft-deleted out-of-band.
	_, err := db.Collection("rooms").UpdateMany(ctx, bson.M{"_id": bson.M{"$in": bson.A{"r-a", "r-b"}}},
		bson.M{"$set": bson.M{"name": "Del-Gone"}})
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

// The withinDays window decides list MEMBERSHIP, not just position, so it must
// not hide a room whose activity the cache hasn't seen yet: a cache hit that
// fails the window is re-read fresh (demoted to a miss) before the row is
// dropped. Ordering staleness within the TTL stays acceptable — absence does not.
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

	// First windowed list: dormant room is out of the window; its stale sort key
	// is now cached.
	require.Equal(t, []string{"s-active"}, windowedIDs(), "dormant room outside the window")

	// The dormant room receives its first message in weeks — exactly the moment
	// clients re-list. The row must appear despite the warm out-of-window cache entry.
	_, err := db.Collection("rooms").UpdateOne(ctx, bson.M{"_id": "r-dormant"}, bson.M{"$set": bson.M{"lastMsgAt": t0.Add(time.Hour)}})
	require.NoError(t, err)

	assert.Equal(t, []string{"s-dormant", "s-active"}, windowedIDs(),
		"a room entering the window must appear immediately, not after the cache TTL")
}

// Non-normalized page values must degrade the way the old Mongo-side paging
// did (an error or a sane empty result) — never a slice-bounds panic. The
// service layer clamps today, but the repo must not turn a future caller's
// bad input into a process crash.
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

	// Negative offset clamps to 0: same page as offset 0.
	page, err := r.AggregateSubscriptions(ctx, "clamp", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: -5, Limit: 1})
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Equal(t, "s-p1", page.Data[0].ID)
	assert.True(t, page.HasMore)

	// Negative limit clamps to 0: empty page, HasMore mirrors the old
	// over-read-one contract (rows remain beyond the empty page).
	page, err = r.AggregateSubscriptions(ctx, "clamp", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: -1})
	require.NoError(t, err)
	assert.Empty(t, page.Data)
	assert.True(t, page.HasMore)

	// A pathological MaxInt64 limit must not overflow the over-read arithmetic:
	// everything is returned, nothing remains.
	page, err = r.AggregateSubscriptions(ctx, "clamp", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: math.MaxInt64})
	require.NoError(t, err)
	assert.Len(t, page.Data, 2)
	assert.False(t, page.HasMore)
}

// Ordering contract pinned before the sort moves out of Mongo: rows with no
// activity signal (no local room, or a room with neither lastMsgAt nor
// createdAt) sort after every keyed row, name ascending among themselves; a
// room with no lastMsgAt falls back to its createdAt.
func TestAggregateSubscriptions_NullSortKeysLastAndCreatedAtFallback_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	t0 := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-msg", "name": "Msg", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-30 * time.Minute)},
		// newer createdAt than r-msg's lastMsgAt — the fallback must beat it.
		bson.M{"_id": "r-fresh", "name": "Fresh", "siteId": "site-a", "userCount": 1, "createdAt": t0},
		// no lastMsgAt, no createdAt — null key.
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
		// cross-site: no local room doc — null key too.
		bson.M{"_id": "s-x", "u": bson.M{"_id": "u-ord", "account": "ord"}, "roomId": "r-x",
			"name": "Across", "roomType": "channel", "siteId": "site-b"},
	)

	assert.Equal(t, []string{"s-fresh", "s-msg", "s-x", "s-bare-a", "s-bare-b"}, listIDs(t, r, "ord", "rooms"),
		"createdAt fallback first (newest), then lastMsgAt, then null keys name-asc")
}
