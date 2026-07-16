//go:build integration

package mongorepo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

// sub builds a subscriptions doc keyed by (account, roomId) with the given type.
func sub(id, account, roomID string, roomType model.RoomType, subscribed bool) interface{} {
	return model.Subscription{
		ID: id, RoomID: roomID, RoomType: roomType, IsSubscribed: subscribed,
		User: model.SubscriptionUser{Account: account},
	}
}

func TestThreadSubscriptionRepo_ListByAccount(t *testing.T) {
	db := testutil.MongoDB(t, "user_service_threadsubs")
	ctx := context.Background()
	seen := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("thread_subscriptions").InsertMany(ctx, []interface{}{
		model.ThreadSubscription{ID: "1", ThreadRoomID: "tr1", RoomID: "r1", UserAccount: "alice", SiteID: "site-a", LastSeenAt: &seen, HasMention: true},
		model.ThreadSubscription{ID: "2", ThreadRoomID: "tr2", RoomID: "r2", UserAccount: "alice", SiteID: "site-b"},
		model.ThreadSubscription{ID: "3", ThreadRoomID: "tr3", RoomID: "r3", UserAccount: "bob", SiteID: "site-a"},
	})
	require.NoError(t, err)
	// Membership + type source: alice is subscribed to r1 (channel) and r2 (dm).
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		sub("s1", "alice", "r1", model.RoomTypeChannel, false),
		sub("s2", "alice", "r2", model.RoomTypeDM, false),
		sub("s3", "bob", "r3", model.RoomTypeChannel, false),
	})
	require.NoError(t, err)

	repo := NewThreadSubscriptionRepo(db)
	rows, err := repo.ListByAccount(ctx, "alice")
	require.NoError(t, err)
	require.Len(t, rows, 2)

	bySite := map[string]model.ThreadUnreadRow{}
	for _, r := range rows {
		bySite[r.SiteID] = r
	}
	assert.Equal(t, "tr1", bySite["site-a"].ThreadRoomID)
	assert.Equal(t, "r1", bySite["site-a"].RoomID)
	assert.Equal(t, "r2", bySite["site-b"].RoomID)
	assert.Equal(t, model.RoomTypeChannel, bySite["site-a"].RoomType)
	assert.True(t, bySite["site-a"].HasMention)
	require.NotNil(t, bySite["site-a"].LastSeenAt)
	assert.Equal(t, model.RoomTypeDM, bySite["site-b"].RoomType)
	assert.Nil(t, bySite["site-b"].LastSeenAt)

	empty, err := repo.ListByAccount(ctx, "nobody")
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// A thread whose room the account no longer subscribes to (no membership) and an
// unsubscribed-app thread (botDM, isSubscribed=false) are both dropped by the join
// gate; a subscribed-app thread survives.
func TestThreadSubscriptionRepo_ListByAccount_MembershipAndAppGate(t *testing.T) {
	db := testutil.MongoDB(t, "user_service_threadsubs_gate")
	ctx := context.Background()
	_, err := db.Collection("thread_subscriptions").InsertMany(ctx, []interface{}{
		model.ThreadSubscription{ID: "1", ThreadRoomID: "trChan", RoomID: "rChan", UserAccount: "alice", SiteID: "site-a"},
		model.ThreadSubscription{ID: "2", ThreadRoomID: "trGone", RoomID: "rGone", UserAccount: "alice", SiteID: "site-a"}, // no subscription
		model.ThreadSubscription{ID: "3", ThreadRoomID: "trApp", RoomID: "rApp", UserAccount: "alice", SiteID: "site-a"},   // unsubscribed app
		model.ThreadSubscription{ID: "4", ThreadRoomID: "trBot", RoomID: "rBot", UserAccount: "alice", SiteID: "site-a"},   // subscribed app
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		sub("s1", "alice", "rChan", model.RoomTypeChannel, false),
		sub("s3", "alice", "rApp", model.RoomTypeBotDM, false), // unsubscribed → dropped
		sub("s4", "alice", "rBot", model.RoomTypeBotDM, true),  // subscribed → kept
		// rGone has no subscription for alice → dropped.
	})
	require.NoError(t, err)

	rows, err := NewThreadSubscriptionRepo(db).ListByAccount(ctx, "alice")
	require.NoError(t, err)

	byThread := map[string]model.ThreadUnreadRow{}
	for _, r := range rows {
		byThread[r.ThreadRoomID] = r
	}
	require.Len(t, rows, 2)
	assert.Contains(t, byThread, "trChan")
	assert.Contains(t, byThread, "trBot")
	assert.Equal(t, model.RoomTypeBotDM, byThread["trBot"].RoomType)
	assert.NotContains(t, byThread, "trGone") // no membership
	assert.NotContains(t, byThread, "trApp")  // unsubscribed app
}

// ListByAccount returns only the newest maxThreadSubscriptions, newest-first, and
// the membership/app gate is applied BEFORE the limit so gated-out threads do not
// consume a slot.
func TestThreadSubscriptionRepo_ListByAccount_NewestCappedAndOrdered(t *testing.T) {
	db := testutil.MongoDB(t, "user_service_threadsubs_cap")
	ctx := context.Background()

	const total = maxThreadSubscriptions + 1 // one over the cap
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	docs := make([]interface{}, 0, total)
	subs := make([]interface{}, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("%04d", i) // zero-padded id == creation rank (higher i = newer)
		roomID := "room-" + id
		docs = append(docs, model.ThreadSubscription{
			ID: id, ThreadRoomID: id, RoomID: roomID, UserAccount: "carol", SiteID: "site-a",
			CreatedAt: base.Add(time.Duration(i) * time.Minute),
		})
		subs = append(subs, sub("sub-"+id, "carol", roomID, model.RoomTypeChannel, false))
	}
	_, err := db.Collection("thread_subscriptions").InsertMany(ctx, docs)
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, subs)
	require.NoError(t, err)

	rows, err := NewThreadSubscriptionRepo(db).ListByAccount(ctx, "carol")
	require.NoError(t, err)

	// Capped at the newest maxThreadSubscriptions.
	require.Len(t, rows, maxThreadSubscriptions)
	// Newest-first by createdAt: highest id (latest created) first, strictly descending.
	assert.Equal(t, fmt.Sprintf("%04d", total-1), rows[0].ThreadRoomID)
	for i := 1; i < len(rows); i++ {
		assert.Greater(t, rows[i-1].ThreadRoomID, rows[i].ThreadRoomID)
	}
	// The oldest sub (earliest createdAt) is dropped by the limit.
	for _, r := range rows {
		assert.NotEqual(t, "0000", r.ThreadRoomID)
	}
}
