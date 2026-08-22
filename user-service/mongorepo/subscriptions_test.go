//go:build integration

package mongorepo

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

func TestAggregateSubscriptions_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()

	now := time.Now().UTC()
	old := now.AddDate(0, 0, -100)
	minSeen := now.AddDate(0, 0, -7)         // distinct from lastMsgAt(now) to prove the baseline reads the right field
	engKey := bytes.Repeat([]byte{0xAB}, 32) // current-slot room secret for r-eng

	// Seed rooms for every local sub that must survive.
	seed(t, db, "rooms",
		bson.M{"_id": "r-eng", "name": "Eng", "siteId": "site-a", "userCount": 5, "appCount": 2,
			"lastMsgId": "m-eng", "lastMsgAt": now, "lastMentionAllAt": now, "minUserLastSeenAt": minSeen,
			"encKey": bson.M{"priv": engKey, "ver": 3}},
		// stale room for the sub-old window row: its lastMsgAt is 100d old.
		bson.M{"_id": "r-eng-old", "name": "EngOld", "siteId": "site-a", "userCount": 1, "lastMsgAt": old},
		bson.M{"_id": "r-dm", "name": "DM-bob", "siteId": "site-a", "userCount": 2,
			"lastMsgId": "m-dm", "lastMsgAt": now},
		// botDM rooms — production always pairs a room with a botDM; missing rooms cause the deleted-filter to drop those subs.
		bson.M{"_id": "r-bot", "name": "helper.bot", "siteId": "site-a", "userCount": 1},
		bson.M{"_id": "r-bot2", "name": "off.bot", "siteId": "site-a", "userCount": 1},
		bson.M{"_id": "r-del", "name": "Del-Old", "siteId": "site-a", "userCount": 3},
		bson.M{"_id": "r-muted", "name": "Muted", "siteId": "site-a", "userCount": 2, "lastMsgAt": now},
		// r-missing intentionally NOT seeded
		// cross-site room is not in the local rooms collection by design
	)

	seed(t, db, "subscriptions",
		// local channel (kept, enriched)
		bson.M{"_id": "sub-eng", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-eng",
			"name": "Eng", "roomType": "channel", "siteId": "site-a", "favorite": true, "_updatedAt": now, "createdAt": now},
		// local dm (kept, enriched)
		bson.M{"_id": "sub-dm", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-dm",
			"name": "bob", "roomType": "dm", "siteId": "site-a", "_updatedAt": now, "createdAt": now},
		// local subscribed botDM (kept for current/apps)
		bson.M{"_id": "sub-bot", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-bot",
			"name": "helper.bot", "roomType": "botDM", "siteId": "site-a", "isSubscribed": true, "_updatedAt": now, "createdAt": now},
		// local unsubscribed botDM (excluded from apps/current)
		bson.M{"_id": "sub-bot-off", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-bot2",
			"name": "off.bot", "roomType": "botDM", "siteId": "site-a", "isSubscribed": false, "_updatedAt": now},
		// local channel whose room is Del-prefixed (DROPPED)
		bson.M{"_id": "sub-del", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-del",
			"name": "Del-Old", "roomType": "channel", "siteId": "site-a", "_updatedAt": now},
		// local channel whose room is missing (DROPPED)
		bson.M{"_id": "sub-missing", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-missing",
			"name": "Gone", "roomType": "channel", "siteId": "site-a", "_updatedAt": now},
		// cross-site channel (KEPT even though no local room doc)
		bson.M{"_id": "sub-xsite", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-xsite",
			"name": "Remote", "roomType": "channel", "siteId": "site-b", "_updatedAt": now},
		// window row: its room r-eng-old is stale (lastMsgAt 100d) while the sub's own
		// _updatedAt is fresh, to prove the window keys on room.lastMsgAt, NOT _updatedAt.
		bson.M{"_id": "sub-old", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-eng-old",
			"name": "EngOld", "roomType": "channel", "siteId": "site-a", "_updatedAt": now},
		// muted local channel — mute suppresses notifications only, not list visibility (KEPT)
		bson.M{"_id": "sub-muted", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-muted",
			"name": "Muted", "roomType": "channel", "siteId": "site-a", "muted": true, "_updatedAt": now, "createdAt": now},
	)

	t.Run("rooms returns dm+channel, drops Del-, keeps missing+cross-site", func(t *testing.T) {
		page, err := r.AggregateSubscriptions(ctx, "alice", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.True(t, got["sub-eng"], "local channel kept")
		assert.True(t, got["sub-dm"], "local dm kept")
		assert.True(t, got["sub-xsite"], "cross-site channel kept")
		assert.True(t, got["sub-muted"], "muted channel kept — mute suppresses notifications only, not list visibility")
		assert.False(t, got["sub-del"], "Del- local room filtered out of the list")
		assert.True(t, got["sub-missing"], "missing local room kept (empty enrichment) — no local room.name to match ^Del-")
		assert.False(t, got["sub-bot"], "botDM excluded from rooms")
	})

	t.Run("local row enriched, cross-site empty", func(t *testing.T) {
		page, err := r.AggregateSubscriptions(ctx, "alice", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		byID := map[string]int{}
		for i, sub := range subs {
			byID[sub.ID] = i
		}
		eng := subs[byID["sub-eng"]]
		assert.Equal(t, 5, eng.UserCount)
		assert.Equal(t, "m-eng", eng.LastMsgID)
		require.NotNil(t, eng.LastMsgAt)
		require.NotNil(t, eng.LastMentionAllAt, "$lookup baseline must carry lastMentionAllAt for degraded-path hasMention")
		require.NotNil(t, eng.MinUserLastSeenAt, "$lookup baseline must carry minUserLastSeenAt")
		assert.WithinDuration(t, minSeen, *eng.MinUserLastSeenAt, time.Second, "baseline minUserLastSeenAt must be the seeded room floor, not lastMsgAt")
		assert.Equal(t, 2, eng.AppCount, "$lookup baseline must carry appCount")
		assert.Equal(t, "Eng", eng.RoomName, "$lookup baseline must carry room canonical name")
		assert.True(t, bytes.Equal(engKey, eng.RoomKeyPriv), "$lookup baseline must carry the room key (encKey.priv)")
		assert.Equal(t, 3, eng.RoomKeyVer, "$lookup baseline must carry the key version (encKey.ver)")
		xsite := subs[byID["sub-xsite"]]
		assert.Equal(t, 0, xsite.UserCount, "cross-site has no local enrichment")
		assert.Empty(t, xsite.LastMsgID)
		assert.Nil(t, xsite.RoomKeyPriv, "cross-site sub carries no local key baseline")
	})

	t.Run("apps returns only subscribed botDMs", func(t *testing.T) {
		page, err := r.AggregateSubscriptions(ctx, "alice", "apps", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.True(t, got["sub-bot"], "subscribed botDM kept")
		assert.False(t, got["sub-bot-off"], "unsubscribed botDM excluded")
		assert.False(t, got["sub-eng"], "channels excluded from apps")
	})

	t.Run("current merges rooms+subscribed botDMs", func(t *testing.T) {
		page, err := r.AggregateSubscriptions(ctx, "alice", "current", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.True(t, got["sub-eng"], "channel in current")
		assert.True(t, got["sub-dm"], "dm in current")
		assert.True(t, got["sub-bot"], "subscribed botDM in current")
		assert.True(t, got["sub-muted"], "muted channel in current — mute suppresses notifications only, not list visibility")
		assert.False(t, got["sub-bot-off"], "unsubscribed botDM excluded from current")
		assert.False(t, got["sub-del"], "Del- local room filtered out of current")
		assert.True(t, got["sub-missing"], "missing local room kept (empty enrichment) — no local room.name to match ^Del-")
	})

	t.Run("rooms window drops rooms stale by lastMsgAt, keeps fresh", func(t *testing.T) {
		within := 30
		page, err := r.AggregateSubscriptions(ctx, "alice", "rooms", false, &within, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.False(t, got["sub-old"], "room stale by lastMsgAt (100d) excluded by the 30-day window even though the sub's _updatedAt is fresh")
		assert.True(t, got["sub-eng"], "room with fresh lastMsgAt kept")
		assert.False(t, got["sub-xsite"], "cross-site room has no local lastMsgAt ⇒ outside the window")
	})

	t.Run("current ignores withinDays — keeps stale rows", func(t *testing.T) {
		within := 30
		page, err := r.AggregateSubscriptions(ctx, "alice", "current", false, &within, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.True(t, got["sub-old"], "current returns the full active set; updatedWithinDays is ignored")
	})

	t.Run("limit caps results", func(t *testing.T) {
		page, err := r.AggregateSubscriptions(ctx, "alice", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 1})
		require.NoError(t, err)
		assert.Len(t, page.Data, 1)
		assert.True(t, page.HasMore, "more rows remain beyond this page of 1")
	})
}

func TestAggregateSubscriptions_SortsByLastMsgAtDesc_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	// The FAVORITE is the OLDER room — proving favorites are NOT pinned in the main
	// list query (any favorite pinning happens post-query, not in Mongo).
	seed(t, db, "rooms",
		bson.M{"_id": "r-new", "name": "New", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0},
		bson.M{"_id": "r-old", "name": "Old", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-time.Hour)},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-old-fav", "u": bson.M{"_id": "u-zoe", "account": "zoe"}, "roomId": "r-old",
			"name": "Old", "roomType": "channel", "siteId": "site-a", "favorite": true, "_updatedAt": t0},
		bson.M{"_id": "s-new", "u": bson.M{"_id": "u-zoe", "account": "zoe"}, "roomId": "r-new",
			"name": "New", "roomType": "channel", "siteId": "site-a", "_updatedAt": t0},
	)

	page, err := r.AggregateSubscriptions(ctx, "zoe", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
	require.NoError(t, err)
	subs := page.Data
	require.Len(t, subs, 2)
	assert.Equal(t, "s-new", subs[0].ID, "newer lastMsgAt sorts first")
	assert.Equal(t, "s-old-fav", subs[1].ID, "favorite is NOT pinned in the non-favorite list query")
}

func TestAggregateSubscriptions_FavoriteFilterAndSelfDMPin_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	seed(t, db, "rooms",
		// self-DM room is the OLDEST, to prove the pin beats recency.
		bson.M{"_id": "r-self", "name": "Me", "siteId": "site-a", "userCount": 1, "lastMsgAt": t0.Add(-time.Hour)},
		bson.M{"_id": "r-fav", "name": "FavCh", "siteId": "site-a", "userCount": 2, "lastMsgAt": t0},
		bson.M{"_id": "r-plain", "name": "Plain", "siteId": "site-a", "userCount": 2, "lastMsgAt": t0},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "s-self", "u": bson.M{"_id": "u-amy", "account": "amy"}, "roomId": "r-self",
			"name": "amy", "roomType": "dm", "siteId": "site-a", "favorite": true, "_updatedAt": t0},
		bson.M{"_id": "s-fav", "u": bson.M{"_id": "u-amy", "account": "amy"}, "roomId": "r-fav",
			"name": "FavCh", "roomType": "channel", "siteId": "site-a", "favorite": true, "_updatedAt": t0},
		// non-favorited — excluded by favorite=true.
		bson.M{"_id": "s-plain", "u": bson.M{"_id": "u-amy", "account": "amy"}, "roomId": "r-plain",
			"name": "Plain", "roomType": "channel", "siteId": "site-a", "favorite": false, "_updatedAt": t0},
	)

	page, err := r.AggregateSubscriptions(ctx, "amy", "current", true, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
	require.NoError(t, err)
	require.Len(t, page.Data, 2, "favorite=true returns only favorited subs")
	assert.False(t, page.HasMore)
	assert.Equal(t, "s-self", page.Data[0].ID, "self-DM pinned first despite its older room")
	assert.Equal(t, "s-fav", page.Data[1].ID)
}

func TestAggregateSubscriptions_Pagination_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	// Five channels with strictly decreasing lastMsgAt ⇒ deterministic order c0..c4.
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("c%d", i)
		seed(t, db, "rooms",
			bson.M{"_id": "room-" + id, "name": "Ch" + id, "siteId": "site-a", "userCount": 1,
				"lastMsgAt": t0.Add(-time.Duration(i) * time.Minute)},
		)
		seed(t, db, "subscriptions",
			bson.M{"_id": id, "u": bson.M{"_id": "u-pat", "account": "pat"}, "roomId": "room-" + id,
				"name": "Ch" + id, "roomType": "channel", "siteId": "site-a", "_updatedAt": t0},
		)
	}

	first, err := r.AggregateSubscriptions(ctx, "pat", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 2})
	require.NoError(t, err)
	require.Len(t, first.Data, 2)
	assert.True(t, first.HasMore, "more pages follow")
	assert.Equal(t, "c0", first.Data[0].ID)
	assert.Equal(t, "c1", first.Data[1].ID)

	second, err := r.AggregateSubscriptions(ctx, "pat", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 2, Limit: 2})
	require.NoError(t, err)
	require.Len(t, second.Data, 2)
	assert.True(t, second.HasMore)
	assert.Equal(t, "c2", second.Data[0].ID)
	assert.Equal(t, "c3", second.Data[1].ID)

	last, err := r.AggregateSubscriptions(ctx, "pat", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 4, Limit: 2})
	require.NoError(t, err)
	require.Len(t, last.Data, 1, "final partial page")
	assert.False(t, last.HasMore, "last page")
	assert.Equal(t, "c4", last.Data[0].ID)
}

func TestAggregateSubscriptions_ExcludesClosed(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r_open", "name": "Open", "siteId": "site-a", "userCount": 1, "lastMsgAt": now},
		bson.M{"_id": "r_closed", "name": "Closed", "siteId": "site-a", "userCount": 1, "lastMsgAt": now},
		bson.M{"_id": "r_missing", "name": "MissingField", "siteId": "site-a", "userCount": 1, "lastMsgAt": now},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "sub-open", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r_open",
			"name": "Open", "roomType": "channel", "siteId": "site-a", "open": true, "_updatedAt": now, "createdAt": now},
		bson.M{"_id": "sub-closed", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r_closed",
			"name": "Closed", "roomType": "channel", "siteId": "site-a", "open": false, "_updatedAt": now, "createdAt": now},
		bson.M{"_id": "sub-missing", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r_missing",
			"name": "MissingField", "roomType": "channel", "siteId": "site-a", "_updatedAt": now, "createdAt": now},
	)

	res, err := r.AggregateSubscriptions(ctx, "alice", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 50})
	require.NoError(t, err)
	roomIDs := map[string]bool{}
	for _, s := range res.Data {
		roomIDs[s.RoomID] = true
	}
	assert.True(t, roomIDs["r_open"], "open:true subscription must be included")
	assert.True(t, roomIDs["r_missing"], "subscription with no open field must be included")
	assert.False(t, roomIDs["r_closed"], "explicitly closed subscription must be excluded")
}

// TestAggregateSubscriptions_CrossSite_Integration exercises the roomsEnrichStages
// pipeline (shared by AggregateSubscriptions/GetDMSubscription/GetSubscriptionByRoomID/
// GetActiveSubscriptions): a locally-hosted room with room.crossSite:true (≥1 remote
// member) must decode as EnrichedSubscription.CrossSite=&true; a room without the flag
// must decode as nil (absent → nil, resolving to global via the fail-safe — NOT false,
// which would assert confirmed same-site). Distinct from the existing "cross-site room has no local doc"
// cases — this room DOES have a local doc, it just has remote members.
func TestAggregateSubscriptions_CrossSite_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-xsite-flag", "name": "Federated", "siteId": "site-a", "userCount": 3, "lastMsgAt": now, "crossSite": true},
		bson.M{"_id": "r-local-only", "name": "Local", "siteId": "site-a", "userCount": 2, "lastMsgAt": now},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "sub-xsite-flag", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-xsite-flag",
			"name": "Federated", "roomType": "channel", "siteId": "site-a", "_updatedAt": now, "createdAt": now},
		bson.M{"_id": "sub-local-only", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-local-only",
			"name": "Local", "roomType": "channel", "siteId": "site-a", "_updatedAt": now, "createdAt": now},
	)

	page, err := r.AggregateSubscriptions(ctx, "alice", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
	require.NoError(t, err)
	byID := map[string]bool{}
	crossSite := map[string]*bool{}
	for _, sub := range page.Data {
		byID[sub.ID] = true
		crossSite[sub.ID] = sub.CrossSite
	}
	require.True(t, byID["sub-xsite-flag"])
	require.True(t, byID["sub-local-only"])
	require.NotNil(t, crossSite["sub-xsite-flag"])
	assert.True(t, *crossSite["sub-xsite-flag"], "room.crossSite:true must decode as EnrichedSubscription.CrossSite=true")
	// A room with no crossSite field at all (unclassified) must decode as nil,
	// never false — coercing it to false would defeat the fail-safe.
	assert.Nil(t, crossSite["sub-local-only"], "room without crossSite must decode as nil (unclassified)")
}

// TestAggregateSubscriptions_HidesCrossSiteTeamsRoom_Integration proves the origin
// filter reads the SUB's own origin, not the enriched $room.origin. A Teams room owned
// by site-a has no local room doc here, so $room.origin is null; only the sub carries
// origin="teams". With showTeamsRoom=false (default) the Teams sub must be hidden while a
// native cross-site sub (no origin, also no local room) stays visible.
func TestAggregateSubscriptions_HidesCrossSiteTeamsRoom_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// No rooms seeded for either roomId → both are remote (no local doc), the cross-site case.
	seed(t, db, "subscriptions",
		bson.M{"_id": "sub-xsite-teams", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-xsite-teams",
			"name": "Migrated", "roomType": "channel", "siteId": "site-a", "origin": "teams", "createdAt": now},
		bson.M{"_id": "sub-xsite-native", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-xsite-native",
			"name": "Native", "roomType": "channel", "siteId": "site-a", "createdAt": now},
	)

	page, err := r.AggregateSubscriptions(ctx, "alice", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
	require.NoError(t, err)
	byID := map[string]bool{}
	for _, sub := range page.Data {
		byID[sub.ID] = true
	}
	assert.False(t, byID["sub-xsite-teams"], "cross-site Teams room must be hidden when showTeamsRoom=false")
	assert.True(t, byID["sub-xsite-native"], "native cross-site room must stay visible")

	n, err := r.CountActiveSubscriptions(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, 1, n, "count must exclude the cross-site Teams room, include the native one")

	// Unread path (GetActiveSubscriptions) must exclude it too.
	subs, err := r.GetActiveSubscriptions(ctx, "alice", 100)
	require.NoError(t, err)
	unreadIDs := map[string]bool{}
	for _, s := range subs {
		unreadIDs[s.ID] = true
	}
	assert.False(t, unreadIDs["sub-xsite-teams"], "unread set must exclude the cross-site Teams room")
	assert.True(t, unreadIDs["sub-xsite-native"], "unread set must include the native cross-site room")
}

// TestFindChannelsByMembers_CrossSite_Integration exercises the SEPARATE roomMatchStages
// pipeline plus the terminal subscriptionProjection — an inclusion-only $project that
// silently drops any field not explicitly whitelisted, so crossSite must be listed there
// too even though roomsEnrichStages already carries it.
func TestFindChannelsByMembers_CrossSite_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "r-fed", "name": "Fed", "siteId": "site-a", "userCount": 2, "createdAt": now, "crossSite": true},
		bson.M{"_id": "r-plain", "name": "Plain", "siteId": "site-a", "userCount": 2, "createdAt": now},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "a-fed", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-fed",
			"name": "Fed", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		bson.M{"_id": "c-fed", "u": bson.M{"_id": "u-carol", "account": "carol"}, "roomId": "r-fed",
			"name": "Fed", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		bson.M{"_id": "a-plain", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-plain",
			"name": "Plain", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		bson.M{"_id": "c-plain", "u": bson.M{"_id": "u-carol", "account": "carol"}, "roomId": "r-plain",
			"name": "Plain", "roomType": "channel", "siteId": "site-a", "createdAt": now},
	)

	page, err := r.FindChannelsByMembers(ctx, "alice", []string{"carol"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
	require.NoError(t, err)
	crossSite := map[string]*bool{}
	for _, sub := range page.Data {
		crossSite[sub.RoomID] = sub.CrossSite
	}
	require.Contains(t, crossSite, "r-fed")
	require.Contains(t, crossSite, "r-plain")
	require.NotNil(t, crossSite["r-fed"])
	assert.True(t, *crossSite["r-fed"], "matched room's crossSite must survive subscriptionProjection's inclusion whitelist")
	assert.Nil(t, crossSite["r-plain"], "room without crossSite must decode as nil (unclassified), never coerced to false")
}

func TestFindChannelsByMembers_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// r-1 createdAt == now, r-2 == now-1h; sort must use room.createdAt DESC, not subscription.createdAt.
	seed(t, db, "rooms",
		bson.M{"_id": "r-1", "name": "Team1", "siteId": "site-a", "userCount": 3, "createdAt": now},
		bson.M{"_id": "r-2", "name": "Team2", "siteId": "site-a", "userCount": 2, "createdAt": now.Add(-time.Hour)},
	)
	// All subscription createdAt values == now so only room.createdAt drives ordering.
	seed(t, db, "subscriptions",
		bson.M{"_id": "a1", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-1",
			"name": "Team1", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		bson.M{"_id": "c1", "u": bson.M{"_id": "u-carol", "account": "carol"}, "roomId": "r-1",
			"name": "Team1", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		bson.M{"_id": "d1", "u": bson.M{"_id": "u-dave", "account": "dave"}, "roomId": "r-1",
			"name": "Team1", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		bson.M{"_id": "a2", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-2",
			"name": "Team2", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		bson.M{"_id": "c2", "u": bson.M{"_id": "u-carol", "account": "carol"}, "roomId": "r-2",
			"name": "Team2", "roomType": "channel", "siteId": "site-a", "createdAt": now},
	)

	t.Run("single member matches both rooms", func(t *testing.T) {
		page, err := r.FindChannelsByMembers(ctx, "alice", []string{"carol"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.RoomID] = true
		}
		assert.True(t, got["r-1"])
		assert.True(t, got["r-2"])
	})

	t.Run("two members match only the room containing both", func(t *testing.T) {
		page, err := r.FindChannelsByMembers(ctx, "alice", []string{"carol", "dave"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		require.Len(t, subs, 1)
		assert.Equal(t, "r-1", subs[0].RoomID)
	})

	t.Run("sorted by room createdAt DESC", func(t *testing.T) {
		page, err := r.FindChannelsByMembers(ctx, "alice", []string{"carol"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		require.Len(t, subs, 2)
		// r-1's room.createdAt == now, r-2's room.createdAt == now-1h → r-1 first.
		assert.Equal(t, "r-1", subs[0].RoomID, "room with newer createdAt sorts first")
		assert.Equal(t, "r-2", subs[1].RoomID)
	})

	t.Run("limit caps the page; hasMore signals more", func(t *testing.T) {
		// alice matches 2 rooms (r-1, r-2); a limit of 1 caps the page to the first
		// (r-1, the room with the newer createdAt under the DESC sort), but Total is 2.
		page, err := r.FindChannelsByMembers(ctx, "alice", []string{"carol"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 1})
		require.NoError(t, err)
		require.Len(t, page.Data, 1)
		assert.True(t, page.HasMore, "more channels remain after this page")
		assert.Equal(t, "r-1", page.Data[0].RoomID)
	})

	t.Run("offset pages through the sorted result", func(t *testing.T) {
		second, err := r.FindChannelsByMembers(ctx, "alice", []string{"carol"}, mongoutil.OffsetPageRequest{Offset: 1, Limit: 1})
		require.NoError(t, err)
		require.Len(t, second.Data, 1)
		assert.False(t, second.HasMore, "last page")
		assert.Equal(t, "r-2", second.Data[0].RoomID, "offset 1 returns the second room (older createdAt)")
	})

	t.Run("field-path-shaped member is treated as a literal, not a path", func(t *testing.T) {
		// "$u.account" must be a literal (no match), not a field path that makes the $all match trivially true.
		page, err := r.FindChannelsByMembers(ctx, "alice", []string{"$u.account"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		assert.Empty(t, page.Data, "$-prefixed member must not bypass the member filter")
	})

	t.Run("soft-deleted and missing-room channels are dropped", func(t *testing.T) {
		// roomMatchStages drops subs whose local room is ^Del- or absent (empty __matchedRoom, $ne: []).
		seed(t, db, "rooms",
			bson.M{"_id": "r-del", "name": "Del-Team", "siteId": "site-a", "userCount": 2, "createdAt": now},
		)
		seed(t, db, "subscriptions",
			// alice+carol both members of a Del- room and of a room with no local doc.
			bson.M{"_id": "a-del", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-del",
				"name": "Del-Team", "roomType": "channel", "siteId": "site-a", "createdAt": now},
			bson.M{"_id": "c-del", "u": bson.M{"_id": "u-carol", "account": "carol"}, "roomId": "r-del",
				"name": "Del-Team", "roomType": "channel", "siteId": "site-a", "createdAt": now},
			bson.M{"_id": "a-miss", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-missing",
				"name": "Gone", "roomType": "channel", "siteId": "site-a", "createdAt": now},
			bson.M{"_id": "c-miss", "u": bson.M{"_id": "u-carol", "account": "carol"}, "roomId": "r-missing",
				"name": "Gone", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		)
		page, err := r.FindChannelsByMembers(ctx, "alice", []string{"carol"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		subs := page.Data
		for _, sub := range subs {
			assert.NotEqual(t, "r-del", sub.RoomID, "Del- room channel must be dropped")
			assert.NotEqual(t, "r-missing", sub.RoomID, "missing-room channel must be dropped")
		}
	})

	t.Run("bot accounts (.bot suffix) are excluded from member matching", func(t *testing.T) {
		// r-3: alice + carol + a bot whose account ends in ".bot" but has NO isBot
		// flag. The suffix filter must exclude the bot regardless of the absent flag
		// (the old isBot-based filter would treat the flagless bot as a real member).
		seed(t, db, "rooms",
			bson.M{"_id": "r-3", "name": "Team3", "siteId": "site-a", "userCount": 3, "createdAt": now},
		)
		seed(t, db, "subscriptions",
			bson.M{"_id": "a3", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "r-3",
				"name": "Team3", "roomType": "channel", "siteId": "site-a", "createdAt": now},
			bson.M{"_id": "c3", "u": bson.M{"_id": "u-carol", "account": "carol"}, "roomId": "r-3",
				"name": "Team3", "roomType": "channel", "siteId": "site-a", "createdAt": now},
			bson.M{"_id": "b3", "u": bson.M{"_id": "u-helper", "account": "helper.bot"}, "roomId": "r-3",
				"name": "Team3", "roomType": "channel", "siteId": "site-a", "createdAt": now},
		)
		// Requesting the bot as a member must NOT match — bots aren't members.
		botPage, err := r.FindChannelsByMembers(ctx, "alice", []string{"helper.bot"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		assert.Empty(t, botPage.Data, "a .bot account must not be a matchable member")

		// The room still matches on its human members (bot ignored, requester counted).
		humanPage, err := r.FindChannelsByMembers(ctx, "alice", []string{"carol"}, mongoutil.OffsetPageRequest{Offset: 0, Limit: 100})
		require.NoError(t, err)
		got := map[string]bool{}
		for _, sub := range humanPage.Data {
			got[sub.RoomID] = true
		}
		assert.True(t, got["r-3"], "room with a bot co-member still matches on human members")
	})
}

func TestGetDMSubscription_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed(t, db, "rooms",
		bson.M{"_id": "dm-bob", "name": "DM-bob", "siteId": "site-a", "userCount": 2, "lastMsgId": "m1", "lastMsgAt": now},
		bson.M{"_id": "dm-rem", "name": "DM-remote", "siteId": "site-a", "userCount": 2},
	)
	seed(t, db, "users",
		bson.M{"_id": "u-bob", "account": "bob", "active": true, "engName": "Bob", "chineseName": "鮑勃"},
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "dm-sub-bob", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "dm-bob",
			"name": "bob", "roomType": "dm", "siteId": "site-a"},
		// cross-site DM counterpart whose room is local but user is remote (no local users doc)
		bson.M{"_id": "dm-sub-rem", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "dm-rem",
			"name": "remoteguy", "roomType": "dm", "siteId": "site-a"},
	)

	t.Run("local counterpart populates HRInfo", func(t *testing.T) {
		dm, err := r.GetDMSubscription(ctx, "alice", "bob")
		require.NoError(t, err)
		require.NotNil(t, dm)
		require.NotNil(t, dm.Subscription)
		require.NotNil(t, dm.HRInfo)
		assert.Equal(t, "bob", dm.HRInfo.Account)
		assert.Equal(t, "鮑勃", dm.HRInfo.Name)
		assert.Equal(t, "Bob", dm.HRInfo.EngName)
		assert.Equal(t, 2, dm.UserCount, "room enrichment applied")
	})

	t.Run("cross-site counterpart yields nil HRInfo", func(t *testing.T) {
		dm, err := r.GetDMSubscription(ctx, "alice", "remoteguy")
		require.NoError(t, err)
		require.NotNil(t, dm)
		assert.Nil(t, dm.HRInfo, "no local users doc → HRInfo nil")
	})

	t.Run("miss yields nil", func(t *testing.T) {
		dm, err := r.GetDMSubscription(ctx, "alice", "nobody")
		require.NoError(t, err)
		assert.Nil(t, dm)
	})
}

func TestGetSubscriptionByRoomID_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()

	seed(t, db, "rooms",
		bson.M{"_id": "ch1", "name": "General", "siteId": "site-a", "userCount": 5, "lastMsgId": "m9"},
		bson.M{"_id": "del1", "name": "Del-Old", "siteId": "site-a", "userCount": 2}, // soft-deleted
	)
	seed(t, db, "subscriptions",
		bson.M{"_id": "sub-ch1", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "ch1",
			"name": "General", "roomType": "channel", "siteId": "site-a"},
		bson.M{"_id": "sub-del", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "del1",
			"name": "Old", "roomType": "channel", "siteId": "site-a"},
		// cross-site sub: no local room doc, must be kept by the deleted-filter.
		bson.M{"_id": "sub-x", "u": bson.M{"_id": "u-alice", "account": "alice"}, "roomId": "rx",
			"name": "Remote", "roomType": "channel", "siteId": "site-b"},
	)

	t.Run("local hit is room-enriched", func(t *testing.T) {
		sub, err := r.GetSubscriptionByRoomID(ctx, "alice", "ch1")
		require.NoError(t, err)
		require.NotNil(t, sub)
		assert.Equal(t, "sub-ch1", sub.ID)
		assert.Equal(t, 5, sub.UserCount, "room enrichment applied")
	})

	t.Run("cross-site sub kept despite no local room", func(t *testing.T) {
		sub, err := r.GetSubscriptionByRoomID(ctx, "alice", "rx")
		require.NoError(t, err)
		require.NotNil(t, sub)
		assert.Equal(t, "sub-x", sub.ID)
	})

	t.Run("soft-deleted local room is kept (room nulled by the service)", func(t *testing.T) {
		sub, err := r.GetSubscriptionByRoomID(ctx, "alice", "del1")
		require.NoError(t, err)
		require.NotNil(t, sub, "Del- room sub is now kept; the service drops the room object")
		assert.Equal(t, "sub-del", sub.ID)
	})

	t.Run("not subscribed yields nil", func(t *testing.T) {
		sub, err := r.GetSubscriptionByRoomID(ctx, "alice", "nope")
		require.NoError(t, err)
		assert.Nil(t, sub)
	})

	t.Run("other account yields nil", func(t *testing.T) {
		sub, err := r.GetSubscriptionByRoomID(ctx, "bob", "ch1")
		require.NoError(t, err)
		assert.Nil(t, sub)
	})
}

func TestCountAndGetActiveSubscriptions_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()

	seed(t, db, "rooms",
		bson.M{"_id": "r-dm", "name": "Bob DM", "siteId": "site-a"},
		bson.M{"_id": "r-ch", "name": "Eng", "siteId": "site-a"},
		bson.M{"_id": "r-noisy", "name": "Noisy", "siteId": "site-a"},
		bson.M{"_id": "r-bot", "name": "helper.bot", "siteId": "site-a"},
		bson.M{"_id": "r-del", "name": "Del-Gone", "siteId": "site-a"}, // soft-deleted
		bson.M{"_id": "r-closed", "name": "Closed", "siteId": "site-a"},
		bson.M{"_id": "r-open", "name": "Opened", "siteId": "site-a"},
	)
	seed(t, db, "subscriptions",
		// active dm
		bson.M{"_id": "a-dm", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "bob", "roomId": "r-dm",
			"roomType": "dm", "siteId": "site-a"},
		// active channel
		bson.M{"_id": "a-ch", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Eng", "roomId": "r-ch",
			"roomType": "channel", "siteId": "site-a"},
		// muted channel (EXCLUDED from count — mute keeps it visible in lists but out of the active/badge count)
		bson.M{"_id": "m-ch", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Noisy", "roomId": "r-noisy",
			"roomType": "channel", "siteId": "site-a", "muted": true},
		// subscribed botDM (included)
		bson.M{"_id": "a-bot", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "helper.bot", "roomId": "r-bot",
			"roomType": "botDM", "siteId": "site-a", "isSubscribed": true},
		// unsubscribed botDM (excluded)
		bson.M{"_id": "u-bot", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "off.bot", "roomId": "r-offbot",
			"roomType": "botDM", "siteId": "site-a", "isSubscribed": false},
		// muted subscribed botDM (excluded — its room r-mutedbot is missing, dropped by the deleted-filter)
		bson.M{"_id": "mu-bot", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "muted.bot", "roomId": "r-mutedbot",
			"roomType": "botDM", "siteId": "site-a", "isSubscribed": true, "muted": true},
		// active by type, but the room is soft-deleted (^Del-) — the rename fanned out
		// onto the sub's own name, which is what the filter now reads.
		bson.M{"_id": "del-ch", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Del-Gone", "roomId": "r-del",
			"roomType": "channel", "siteId": "site-a"},
		// active by type, local room is missing — KEPT (a live sub name passes the filter
		// whether or not a room doc exists)
		bson.M{"_id": "gone-ch", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Vanished", "roomId": "r-missing",
			"roomType": "channel", "siteId": "site-a"},
		// cross-site sub with a live name: no local room doc, kept
		bson.M{"_id": "x-ch", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Remote", "roomId": "rx",
			"roomType": "channel", "siteId": "site-b"},
		// cross-site sub whose room was soft-deleted at its home site: the rename
		// federates onto the local sub name, so it is now excluded. The rooms $lookup
		// could never see this — it had no local room doc to join.
		bson.M{"_id": "x-del-ch", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Del-Remote", "roomId": "rx-del",
			"roomType": "channel", "siteId": "site-b"},
		// closed by the user (open:false) — excluded, matching subscription.list
		bson.M{"_id": "closed-ch", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Closed", "roomId": "r-closed",
			"roomType": "channel", "siteId": "site-a", "open": false},
		// explicitly open — included (guards against an over-broad exclusion)
		bson.M{"_id": "open-ch", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "Opened", "roomId": "r-open",
			"roomType": "channel", "siteId": "site-a", "open": true},
	)

	t.Run("count excludes unsubscribed, muted, and Del- named subs; keeps missing-room and cross-site", func(t *testing.T) {
		n, err := r.CountActiveSubscriptions(ctx, "alice")
		require.NoError(t, err)
		// a-dm, a-ch, a-bot, x-ch, gone-ch, open-ch. Excluded: muted m-ch, closed-ch,
		// unsubscribed u-bot, muted mu-bot, Del--named del-ch and x-del-ch. gone-ch is
		// kept — a live name passes regardless of whether a room doc exists.
		assert.Equal(t, 6, n)
	})

	t.Run("a subscription with no name at all still counts", func(t *testing.T) {
		// $not with a regex literal also matches documents that do not contain the
		// field. That is what preserves the old behavior: when the join found no room,
		// room.name was null and the $not-regex kept the row. Defensive — a nameless
		// sub should not exist — but the filter's semantics rest on it.
		seed(t, db, "subscriptions",
			bson.M{"_id": "nameless-ch", "u": bson.M{"_id": "u-dave", "account": "dave"},
				"roomId": "r-nameless", "roomType": "channel", "siteId": "site-a"})

		n, err := r.CountActiveSubscriptions(ctx, "dave")
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("count reads the sub's own name, so a cross-site Del- room is excluded too", func(t *testing.T) {
		subs, err := r.GetActiveSubscriptions(ctx, "alice", 100)
		require.NoError(t, err)
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.False(t, got["x-del-ch"], "a federated soft-delete must drop the cross-site sub")
		assert.True(t, got["x-ch"], "a live cross-site sub is unaffected")
	})

	t.Run("closed rooms are excluded from both count and get, matching subscription.list", func(t *testing.T) {
		subs, err := r.GetActiveSubscriptions(ctx, "alice", 100)
		require.NoError(t, err)
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.False(t, got["closed-ch"], "open:false must not count")
		assert.True(t, got["open-ch"], "open:true must count")
		assert.True(t, got["a-ch"], "a sub with no open field must count")
	})

	t.Run("get active returns the same set", func(t *testing.T) {
		subs, err := r.GetActiveSubscriptions(ctx, "alice", 100)
		require.NoError(t, err)
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.ID] = true
		}
		assert.True(t, got["a-dm"])
		assert.True(t, got["a-ch"])
		assert.True(t, got["a-bot"])
		assert.True(t, got["x-ch"], "cross-site sub kept despite no local room")
		assert.True(t, got["gone-ch"], "missing local room kept (empty enrichment) — the deleted-filter reads the sub's own name")
		assert.False(t, got["m-ch"], "muted channel excluded from the active/count set")
		assert.False(t, got["u-bot"])
		assert.False(t, got["mu-bot"], "muted botDM excluded by activeSubscriptionFilter before room lookup")
		assert.False(t, got["del-ch"], "a sub carrying the ^Del- name must be filtered out")
	})

	t.Run("limit caps active set exactly", func(t *testing.T) {
		// The deleted-room filter moved into the leading $match, ahead of the cap, so
		// nothing downstream can drop a capped row — the page is exactly the cap.
		subs, err := r.GetActiveSubscriptions(ctx, "alice", 2)
		require.NoError(t, err)
		assert.Len(t, subs, 2)
	})

	t.Run("zero limit does not error (no $limit:0 stage)", func(t *testing.T) {
		// $limit:0 is rejected by MongoDB; the guard must drop the stage so the query returns the uncapped set.
		subs, err := r.GetActiveSubscriptions(ctx, "alice", 0)
		require.NoError(t, err)
		assert.NotEmpty(t, subs)
	})
}

// TestCountUnread_ZeroActive_Integration: no active subs yields count=0 and an empty (non-erroring) active set.
func TestCountUnread_ZeroActive_Integration(t *testing.T) {
	r, _ := newTestSubscriptionRepo(t)
	ctx := context.Background()

	n, err := r.CountActiveSubscriptions(ctx, "nobody")
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	subs, err := r.GetActiveSubscriptions(ctx, "nobody", 0)
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestAppSubscriptionRoundTrip_Integration(t *testing.T) {
	r, db := newTestSubscriptionRepo(t)
	ctx := context.Background()

	seed(t, db, "subscriptions",
		bson.M{"_id": "bot-sub", "u": bson.M{"_id": "u-alice", "account": "alice"}, "name": "helper.bot",
			"roomType": "botDM", "siteId": "site-a", "isSubscribed": false, "muted": false},
	)

	t.Run("get existing", func(t *testing.T) {
		sub, err := r.GetAppSubscription(ctx, "alice", "helper.bot")
		require.NoError(t, err)
		require.NotNil(t, sub)
		assert.False(t, sub.IsSubscribed)
	})

	t.Run("get miss", func(t *testing.T) {
		sub, err := r.GetAppSubscription(ctx, "alice", "ghost.bot")
		require.NoError(t, err)
		assert.Nil(t, sub)
	})

	t.Run("set then re-read", func(t *testing.T) {
		require.NoError(t, r.SetAppSubscribed(ctx, "alice", "helper.bot", true, true))
		sub, err := r.GetAppSubscription(ctx, "alice", "helper.bot")
		require.NoError(t, err)
		require.NotNil(t, sub)
		assert.True(t, sub.IsSubscribed)
		assert.True(t, sub.Muted)
	})
}
