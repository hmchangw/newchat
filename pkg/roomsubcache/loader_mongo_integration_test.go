//go:build integration

package roomsubcache

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

// TestMongoLoader_HistorySharedSince pins the completeness guarantee every
// reader of the shared cache key depends on: the loader maps the nested
// subscription shape into the flat Member, carries Muted through, and converts
// historySharedSince (a Date) to epoch-ms — leaving it nil for full-access
// members. A loader that dropped either field would silently unmute users and
// widen their history windows for whichever service reads the entry next.
func TestMongoLoader_HistorySharedSince(t *testing.T) {
	db := testutil.MongoDB(t, "roomsubcache_loader")
	ctx := context.Background()
	col := db.Collection("subscriptions")

	shared := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	_, err := col.InsertMany(ctx, []any{
		// restricted member: history-shared window set, muted
		model.Subscription{
			ID: "s1", RoomID: "rX", RoomType: model.RoomTypeChannel,
			User:               model.SubscriptionUser{ID: "alice", Account: "alice"},
			Muted:              true,
			HistorySharedSince: &shared,
		},
		// full-access member: no history-shared window (nil), a bot
		model.Subscription{
			ID: "s2", RoomID: "rX", RoomType: model.RoomTypeChannel,
			User: model.SubscriptionUser{ID: "botty", Account: "botty", IsBot: true},
		},
		// different room: must not leak into the rX result
		model.Subscription{
			ID: "s3", RoomID: "rY", RoomType: model.RoomTypeChannel,
			User: model.SubscriptionUser{ID: "carol", Account: "carol"},
		},
	})
	require.NoError(t, err)

	got, err := NewMongoLoader(col, db.Collection("users"))(ctx, "rX")
	require.NoError(t, err)
	require.Len(t, got, 2)

	byAccount := make(map[string]Member, len(got))
	for _, m := range got {
		byAccount[m.Account] = m
	}

	alice, ok := byAccount["alice"]
	require.True(t, ok)
	assert.Equal(t, "alice", alice.ID, "nested u._id maps to flat ID")
	assert.Equal(t, model.RoomTypeChannel, alice.RoomType)
	assert.False(t, alice.IsBot)
	assert.True(t, alice.Muted)
	require.NotNil(t, alice.HistorySharedSince, "restricted member must carry the window")
	assert.Equal(t, shared.UnixMilli(), *alice.HistorySharedSince, "Date converted to epoch-ms")

	botty, ok := byAccount["botty"]
	require.True(t, ok)
	assert.Equal(t, "botty", botty.ID)
	assert.True(t, botty.IsBot, "nested u.isBot maps to flat IsBot")
	assert.Nil(t, botty.HistorySharedSince, "full-access member must have nil HistorySharedSince")
}

// TestMongoLoader_StampsHomeSiteID pins the field the shared key made
// load-bearing. Every service writes this one entry, so the loader must fill
// Member.HomeSiteID — the member's OWN site, read from users.siteId, not the
// room's site on the subscription. Without it a broadcast-worker cold fill
// hands notification-worker an L2 hit whose members carry no home site, and the
// per-site badge RPC is grouped under "" for the whole room.
func TestMongoLoader_StampsHomeSiteID(t *testing.T) {
	db := testutil.MongoDB(t, "roomsubcache_homesite")
	ctx := context.Background()
	subs := db.Collection("subscriptions")
	users := db.Collection("users")

	_, err := subs.InsertMany(ctx, []any{
		// SiteID here is the ROOM's site — deliberately different from both
		// members' home sites, so a loader reading it instead of users.siteId
		// fails this test rather than coincidentally passing.
		model.Subscription{
			ID: "s1", RoomID: "rH", RoomType: model.RoomTypeChannel, SiteID: "site-room",
			User: model.SubscriptionUser{ID: "u-local", Account: "local"},
		},
		model.Subscription{
			ID: "s2", RoomID: "rH", RoomType: model.RoomTypeChannel, SiteID: "site-room",
			User: model.SubscriptionUser{ID: "u-remote", Account: "remote"},
		},
		// No users row for this one — must degrade to empty, not error.
		model.Subscription{
			ID: "s3", RoomID: "rH", RoomType: model.RoomTypeChannel, SiteID: "site-room",
			User: model.SubscriptionUser{ID: "u-ghost", Account: "ghost"},
		},
	})
	require.NoError(t, err)

	_, err = users.InsertMany(ctx, []any{
		bson.M{"_id": "u-local", "account": "local", "siteId": "site-a"},
		bson.M{"_id": "u-remote", "account": "remote", "siteId": "site-b"},
	})
	require.NoError(t, err)

	got, err := NewMongoLoader(subs, users)(ctx, "rH")
	require.NoError(t, err)
	require.Len(t, got, 3)

	byAccount := make(map[string]Member, len(got))
	for _, m := range got {
		byAccount[m.Account] = m
	}

	assert.Equal(t, "site-a", byAccount["local"].HomeSiteID)
	assert.Equal(t, "site-b", byAccount["remote"].HomeSiteID,
		"a remote member's HOME site, not the room's site")
	assert.Empty(t, byAccount["ghost"].HomeSiteID,
		"an account missing from users degrades to empty rather than failing the fill")
}
