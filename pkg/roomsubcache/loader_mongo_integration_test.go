//go:build integration

package roomsubcache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	got, err := NewMongoLoader(col)(ctx, "rX")
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
