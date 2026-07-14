//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
)

// insertBotDMSubscription seeds a botDM room subscription with an explicit
// isSubscribed flag, mirroring how user-service's SetAppSubscribed persists the
// soft-unsubscribe toggle ($set isSubscribed:false, row retained). Raw bson, not
// model.Subscription, because the struct's `omitempty` would drop a false
// isSubscribed — and explicit false is exactly the production state under test.
func insertBotDMSubscription(t *testing.T, db *mongo.Database, account, roomID string, subscribed bool) {
	t.Helper()
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), bson.M{
		"_id":          account + ":" + roomID,
		"u":            bson.M{"account": account},
		"roomId":       roomID,
		"siteId":       "site-a",
		"roomType":     string(model.RoomTypeBotDM),
		"isSubscribed": subscribed,
	})
	require.NoError(t, err)
}

// insertNamedSubscription seeds a room subscription carrying the per-subscriber
// display Name and roomType, mirroring how room-worker's newSub persists them
// (channel: room name; dm/botDM: counterpart account / app name).
func insertNamedSubscription(t *testing.T, db *mongo.Database, account, roomID, name string, roomType model.RoomType) {
	t.Helper()
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), model.Subscription{
		ID:       account + ":" + roomID,
		User:     model.SubscriptionUser{Account: account},
		RoomID:   roomID,
		SiteID:   "site-a",
		Name:     name,
		RoomType: roomType,
	})
	require.NoError(t, err)
}

// roomName and roomType are filled from the user's own subscription, not a room
// document: dm/botDM rooms carry an empty room name (the display name is
// per-subscriber). No rooms collection is seeded — the pipeline must resolve both
// fields without it.
func TestThreadSubscriptionRepo_ListUserThreadSubscriptions_RoomNameFromSubscription(t *testing.T) {
	db := setupMongo(t)
	repo := NewThreadSubscriptionRepo(db)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// A DM thread with no rooms document at all; alice's subscription names the
	// counterpart (bob) and carries the room type.
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-dm", RoomID: "dm1", ParentMessageID: "p-dm", LastMsgID: "m-dm", SiteID: "site-a", LastMsgAt: base.Add(time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-dm", ThreadRoomID: "tr-dm", RoomID: "dm1", ParentMessageID: "p-dm", UserAccount: "alice", SiteID: "site-a", CreatedAt: base, UpdatedAt: base})
	insertNamedSubscription(t, db, "alice", "dm1", "bob", model.RoomTypeDM)

	rows, _, err := repo.ListUserThreadSubscriptions(ctx, "alice", nil, "", 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "bob", rows[0].RoomName, "DM roomName must come from the subscription Name, not the empty room name")
	assert.Equal(t, model.RoomTypeDM, rows[0].RoomType, "DM roomType must come from the subscription, with no rooms lookup")
}

// An unsubscribed app's botDM thread must not appear in the inbox, while a
// still-subscribed app's botDM thread does. botDM unsubscribe is a soft toggle
// (isSubscribed=false) that leaves the subscription row in place — unlike a room
// leave, which purges it — so the membership join must gate botDM rows on
// isSubscribed.
func TestThreadSubscriptionRepo_ListUserThreadSubscriptions_UnsubscribedApp(t *testing.T) {
	db := setupMongo(t)
	repo := NewThreadSubscriptionRepo(db)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Two botDM rooms, each hosting one thread alice follows. bot-off has the newer
	// activity, so a missing filter would surface it first.
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-on", RoomID: "bot-on", ParentMessageID: "p-on", LastMsgID: "m-on", SiteID: "site-a", LastMsgAt: base.Add(3 * time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-off", RoomID: "bot-off", ParentMessageID: "p-off", LastMsgID: "m-off", SiteID: "site-a", LastMsgAt: base.Add(5 * time.Hour), CreatedAt: base, UpdatedAt: base})

	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-on", ThreadRoomID: "tr-on", RoomID: "bot-on", ParentMessageID: "p-on", UserAccount: "alice", SiteID: "site-a", CreatedAt: base, UpdatedAt: base})
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-off", ThreadRoomID: "tr-off", RoomID: "bot-off", ParentMessageID: "p-off", UserAccount: "alice", SiteID: "site-a", CreatedAt: base, UpdatedAt: base})

	// alice still subscribes to bot-on; she has unsubscribed from bot-off.
	insertBotDMSubscription(t, db, "alice", "bot-on", true)
	insertBotDMSubscription(t, db, "alice", "bot-off", false)

	rows, hasMore, err := repo.ListUserThreadSubscriptions(ctx, "alice", nil, "", 10)
	require.NoError(t, err)
	assert.False(t, hasMore)
	require.Len(t, rows, 1)
	assert.Equal(t, "tr-on", rows[0].ThreadRoomID, "unsubscribed-app thread must be filtered out")
}

func TestThreadSubscriptionRepo_ListUserThreadSubscriptions(t *testing.T) {
	db := setupMongo(t)
	repo := NewThreadSubscriptionRepo(db)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Three threads alice follows, plus one for bob and one orphan (no thread_room).
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-1", RoomID: "r1", ParentMessageID: "p1", LastMsgID: "m1", SiteID: "site-a", LastMsgAt: base.Add(5 * time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-2", RoomID: "r2", ParentMessageID: "p2", LastMsgID: "m2", SiteID: "site-a", LastMsgAt: base.Add(3 * time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-3", RoomID: "r1", ParentMessageID: "p3", LastMsgID: "m3", SiteID: "site-a", LastMsgAt: base.Add(1 * time.Hour), CreatedAt: base, UpdatedAt: base})

	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-1", ThreadRoomID: "tr-1", RoomID: "r1", ParentMessageID: "p1", UserAccount: "alice", SiteID: "site-a", LastSeenAt: timePtr(base.Add(2 * time.Hour)), HasMention: true, CreatedAt: base, UpdatedAt: base})
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-2", ThreadRoomID: "tr-2", RoomID: "r2", ParentMessageID: "p2", UserAccount: "alice", SiteID: "site-a", LastSeenAt: nil, CreatedAt: base, UpdatedAt: base})
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-3", ThreadRoomID: "tr-3", RoomID: "r1", ParentMessageID: "p3", UserAccount: "alice", SiteID: "site-a", LastSeenAt: timePtr(base.Add(1 * time.Hour)), CreatedAt: base, UpdatedAt: base})
	// bob's sub — must never appear for alice.
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-bob", ThreadRoomID: "tr-1", RoomID: "r1", ParentMessageID: "p1", UserAccount: "bob", SiteID: "site-a", CreatedAt: base, UpdatedAt: base})
	// orphan sub — thread_room missing, $unwind drops it.
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-orphan", ThreadRoomID: "tr-gone", RoomID: "r1", ParentMessageID: "p9", UserAccount: "alice", SiteID: "site-a", CreatedAt: base, UpdatedAt: base})
	// left-room sub — alice holds a thread subscription in r-left (e.g. left the
	// room) but has no room subscription there, so the membership join drops it.
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-left", RoomID: "r-left", ParentMessageID: "p-left", LastMsgID: "m-left", SiteID: "site-a", LastMsgAt: base.Add(9 * time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-left", ThreadRoomID: "tr-left", RoomID: "r-left", ParentMessageID: "p-left", UserAccount: "alice", SiteID: "site-a", CreatedAt: base, UpdatedAt: base})

	// alice's room subscriptions — the membership $lookup keeps only threads whose
	// room she is still subscribed to. She is in r1 and r2, but not r-left. The
	// subscription is also where roomName/roomType come from: r1 carries its channel
	// name and type; r2 is left nameless/typeless to exercise the empty pass-through.
	insertNamedSubscription(t, db, "alice", "r1", "general", model.RoomTypeChannel)
	insertSubscription(t, db, "alice", "r2")

	// Page 1, limit 2: newest two (tr-1 5h, tr-2 3h), hasMore true.
	rows, hasMore, err := repo.ListUserThreadSubscriptions(ctx, "alice", nil, "", 2)
	require.NoError(t, err)
	assert.True(t, hasMore)
	require.Len(t, rows, 2)
	assert.Equal(t, "tr-1", rows[0].ThreadRoomID)
	assert.Equal(t, "tr-2", rows[1].ThreadRoomID)

	// Joined + own fields land on the row.
	assert.Equal(t, "r1", rows[0].RoomID)
	assert.Equal(t, "site-a", rows[0].SiteID)
	assert.Equal(t, "p1", rows[0].ParentMessageID)
	assert.Equal(t, "m1", rows[0].LastMsgID)
	assert.True(t, rows[0].HasMention)
	require.NotNil(t, rows[0].LastSeenAt)
	assert.WithinDuration(t, base.Add(2*time.Hour), *rows[0].LastSeenAt, time.Second)
	assert.Nil(t, rows[1].LastSeenAt) // never-seen thread

	// Room name/type ride in on the membership subscription; r2's sub is nameless ⇒ empty.
	assert.Equal(t, "general", rows[0].RoomName)
	assert.Equal(t, model.RoomTypeChannel, rows[0].RoomType)
	assert.Empty(t, rows[1].RoomName)
	assert.Empty(t, string(rows[1].RoomType))

	// Page 2 from the cursor at row[1]: only tr-3 remains, hasMore false.
	last := rows[1]
	rows2, hasMore2, err := repo.ListUserThreadSubscriptions(ctx, "alice", &last.LastMsgAt, last.ThreadRoomID, 2)
	require.NoError(t, err)
	assert.False(t, hasMore2)
	require.Len(t, rows2, 1)
	assert.Equal(t, "tr-3", rows2[0].ThreadRoomID)

	// Unknown account → empty.
	none, hasMoreNone, err := repo.ListUserThreadSubscriptions(ctx, "nobody", nil, "", 10)
	require.NoError(t, err)
	assert.False(t, hasMoreNone)
	assert.Empty(t, none)

	// Membership filter: tr-left has the newest activity (9h) but lives in r-left,
	// where alice holds no room subscription, so it never appears.
	all, _, err := repo.ListUserThreadSubscriptions(ctx, "alice", nil, "", 10)
	require.NoError(t, err)
	require.Len(t, all, 3) // tr-1, tr-2, tr-3 — not tr-left, not the orphan
	for _, r := range all {
		assert.NotEqual(t, "tr-left", r.ThreadRoomID, "thread in a left room must be filtered")
	}
}

// Equal lastMsgAt across threads is fully ordered by threadRoomId DESC, so a
// page boundary neither skips nor duplicates.
func TestThreadSubscriptionRepo_ListUserThreadSubscriptions_Tiebreak(t *testing.T) {
	db := setupMongo(t)
	repo := NewThreadSubscriptionRepo(db)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	same := base.Add(5 * time.Hour)

	for _, id := range []string{"tr-a", "tr-b", "tr-c"} {
		insertThreadRoom(t, db, model.ThreadRoom{ID: id, RoomID: "r1", ParentMessageID: "p-" + id, SiteID: "site-a", LastMsgAt: same, CreatedAt: base, UpdatedAt: base})
		insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-" + id, ThreadRoomID: id, RoomID: "r1", ParentMessageID: "p-" + id, UserAccount: "alice", SiteID: "site-a", CreatedAt: base, UpdatedAt: base})
	}
	insertSubscription(t, db, "alice", "r1") // membership for the room all threads live in

	// threadRoomId DESC ⇒ tr-c, tr-b, tr-a.
	rows, hasMore, err := repo.ListUserThreadSubscriptions(ctx, "alice", nil, "", 2)
	require.NoError(t, err)
	assert.True(t, hasMore)
	require.Len(t, rows, 2)
	assert.Equal(t, "tr-c", rows[0].ThreadRoomID)
	assert.Equal(t, "tr-b", rows[1].ThreadRoomID)

	rows2, hasMore2, err := repo.ListUserThreadSubscriptions(ctx, "alice", &rows[1].LastMsgAt, rows[1].ThreadRoomID, 2)
	require.NoError(t, err)
	assert.False(t, hasMore2)
	require.Len(t, rows2, 1)
	assert.Equal(t, "tr-a", rows2[0].ThreadRoomID)
}
