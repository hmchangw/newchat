//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

func insertThreadRoom(t *testing.T, db *mongo.Database, tr model.ThreadRoom) {
	t.Helper()
	_, err := db.Collection("thread_rooms").InsertOne(context.Background(), tr)
	require.NoError(t, err)
}

func insertThreadSubscription(t *testing.T, db *mongo.Database, ts model.ThreadSubscription) {
	t.Helper()
	_, err := db.Collection("thread_subscriptions").InsertOne(context.Background(), ts)
	require.NoError(t, err)
}

// insertSubscription seeds a minimal room subscription so the thread-list
// membership $lookup treats account as still a member of roomID.
func insertSubscription(t *testing.T, db *mongo.Database, account, roomID string) {
	t.Helper()
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), model.Subscription{
		ID:     account + ":" + roomID,
		User:   model.SubscriptionUser{Account: account},
		RoomID: roomID,
		SiteID: "site-a",
	})
	require.NoError(t, err)
}

func timePtr(t time.Time) *time.Time { return &t }

func TestThreadRoomRepo_GetThreadRooms(t *testing.T) {
	db := setupMongo(t)
	repo := NewThreadRoomRepo(db)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// 4 rooms in r1: tr-old, tr-1, tr-2, tr-3 (sorted by lastMsgAt desc: tr-old first)
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-old", RoomID: "r1", ParentMessageID: "p0", ThreadParentCreatedAt: base.Add(-1 * time.Hour), LastMsgAt: base.Add(5 * time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-1", RoomID: "r1", ParentMessageID: "p1", ThreadParentCreatedAt: base.Add(1 * time.Hour), LastMsgAt: base.Add(3 * time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-2", RoomID: "r1", ParentMessageID: "p2", ThreadParentCreatedAt: base.Add(2 * time.Hour), LastMsgAt: base.Add(1 * time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-3", RoomID: "r1", ParentMessageID: "p3", ThreadParentCreatedAt: base.Add(3 * time.Hour), LastMsgAt: base.Add(2 * time.Hour), CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-other", RoomID: "r2", ParentMessageID: "p4", ThreadParentCreatedAt: base.Add(1 * time.Hour), LastMsgAt: base.Add(4 * time.Hour), CreatedAt: base, UpdatedAt: base})

	// Page 1: limit 2 — gets tr-old (5h) and tr-1 (3h); total is 4
	page, err := repo.GetThreadRooms(ctx, "r1", nil, mongoutil.NewOffsetPageRequest(0, 2))
	require.NoError(t, err)
	assert.Equal(t, int64(4), page.Total)
	require.Len(t, page.Data, 2)
	assert.Equal(t, "tr-old", page.Data[0].ID)
	assert.Equal(t, "tr-1", page.Data[1].ID)

	// Page 2: offset 2, limit 2 — gets tr-3 (2h) and tr-2 (1h); total still 4
	page2, err := repo.GetThreadRooms(ctx, "r1", nil, mongoutil.NewOffsetPageRequest(2, 2))
	require.NoError(t, err)
	assert.Equal(t, int64(4), page2.Total)
	require.Len(t, page2.Data, 2)

	// accessSince = base: excludes tr-old (threadParentCreatedAt = base-1h < base)
	afterBase := base
	page3, err := repo.GetThreadRooms(ctx, "r1", &afterBase, mongoutil.NewOffsetPageRequest(0, 10))
	require.NoError(t, err)
	assert.Equal(t, int64(3), page3.Total)
	for _, tr := range page3.Data {
		assert.NotEqual(t, "tr-old", tr.ID)
	}
}

func TestThreadRoomRepo_GetFollowingThreadRooms(t *testing.T) {
	db := setupMongo(t)
	repo := NewThreadRoomRepo(db)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-1", RoomID: "r1", ParentMessageID: "p1", ThreadParentCreatedAt: base.Add(1 * time.Hour), LastMsgAt: base.Add(2 * time.Hour), ReplyAccounts: []string{"alice", "bob"}, CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-2", RoomID: "r1", ParentMessageID: "p2", ThreadParentCreatedAt: base.Add(2 * time.Hour), LastMsgAt: base.Add(1 * time.Hour), ReplyAccounts: []string{"bob"}, CreatedAt: base, UpdatedAt: base})
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-3", RoomID: "r1", ParentMessageID: "p3", ThreadParentCreatedAt: base.Add(3 * time.Hour), LastMsgAt: base.Add(3 * time.Hour), ReplyAccounts: []string{"alice"}, CreatedAt: base, UpdatedAt: base})

	page, err := repo.GetFollowingThreadRooms(ctx, "r1", "alice", nil, mongoutil.NewOffsetPageRequest(0, 10))
	require.NoError(t, err)
	assert.Equal(t, int64(2), page.Total)
	require.Len(t, page.Data, 2)
	assert.Equal(t, "tr-3", page.Data[0].ID) // lastMsgAt 3h > 2h
	assert.Equal(t, "tr-1", page.Data[1].ID)
}

func TestThreadRoomRepo_GetUnreadThreadRooms(t *testing.T) {
	db := setupMongo(t)
	repo := NewThreadRoomRepo(db)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// tr-1: alice has sub, lastMsgAt (5h) > lastSeenAt (3h) → unread
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-1", RoomID: "r1", ParentMessageID: "p1",
		ThreadParentCreatedAt: base.Add(1 * time.Hour), LastMsgAt: base.Add(5 * time.Hour),
		CreatedAt: base, UpdatedAt: base})
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-1", ThreadRoomID: "tr-1",
		RoomID: "r1", ParentMessageID: "p1", UserID: "u1", UserAccount: "alice",
		SiteID: "site-local", LastSeenAt: timePtr(base.Add(3 * time.Hour)),
		CreatedAt: base, UpdatedAt: base})

	// tr-2: alice has sub, lastMsgAt (2h) < lastSeenAt (4h) → read, excluded
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-2", RoomID: "r1", ParentMessageID: "p2",
		ThreadParentCreatedAt: base.Add(2 * time.Hour), LastMsgAt: base.Add(2 * time.Hour),
		CreatedAt: base, UpdatedAt: base})
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-2", ThreadRoomID: "tr-2",
		RoomID: "r1", ParentMessageID: "p2", UserID: "u1", UserAccount: "alice",
		SiteID: "site-local", LastSeenAt: timePtr(base.Add(4 * time.Hour)),
		CreatedAt: base, UpdatedAt: base})

	// tr-3: alice has sub, nil lastSeenAt → always unread
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-3", RoomID: "r1", ParentMessageID: "p3",
		ThreadParentCreatedAt: base.Add(3 * time.Hour), LastMsgAt: base.Add(3 * time.Hour),
		CreatedAt: base, UpdatedAt: base})
	insertThreadSubscription(t, db, model.ThreadSubscription{ID: "ts-3", ThreadRoomID: "tr-3",
		RoomID: "r1", ParentMessageID: "p3", UserID: "u1", UserAccount: "alice",
		SiteID: "site-local", LastSeenAt: nil, CreatedAt: base, UpdatedAt: base})

	// tr-4: alice has NO sub → excluded
	insertThreadRoom(t, db, model.ThreadRoom{ID: "tr-4", RoomID: "r1", ParentMessageID: "p4",
		ThreadParentCreatedAt: base.Add(4 * time.Hour), LastMsgAt: base.Add(4 * time.Hour),
		CreatedAt: base, UpdatedAt: base})

	page, err := repo.GetUnreadThreadRooms(ctx, "r1", "alice", nil, mongoutil.NewOffsetPageRequest(0, 10))
	require.NoError(t, err)
	// tr-1 (unread) and tr-3 (nil lastSeenAt); sorted lastMsgAt desc: tr-1 (5h) first
	assert.Equal(t, int64(2), page.Total)
	require.Len(t, page.Data, 2)
	assert.Equal(t, "tr-1", page.Data[0].ID)
	assert.Equal(t, "tr-3", page.Data[1].ID)

	// accessSince excludes tr-1 (threadParentCreatedAt = 1h < 2h threshold)
	since := base.Add(2 * time.Hour)
	page2, err := repo.GetUnreadThreadRooms(ctx, "r1", "alice", &since, mongoutil.NewOffsetPageRequest(0, 10))
	require.NoError(t, err)
	assert.Equal(t, int64(1), page2.Total)
	assert.Equal(t, "tr-3", page2.Data[0].ID)

	// user with no subscriptions → zero results
	pageNone, err := repo.GetUnreadThreadRooms(ctx, "r1", "nobody", nil, mongoutil.NewOffsetPageRequest(0, 10))
	require.NoError(t, err)
	assert.Equal(t, int64(0), pageNone.Total)
	assert.Empty(t, pageNone.Data)
}

func TestThreadRoomRepo_GetMinThreadUserLastSeenAt(t *testing.T) {
	db := setupMongo(t)
	repo := NewThreadRoomRepo(db)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	floorTime := base.Add(30 * time.Minute)

	// Thread room with minUserLastSeenAt set.
	insertThreadRoom(t, db, model.ThreadRoom{
		ID: "tr-floor", RoomID: "r1", ParentMessageID: "p1",
		LastMsgAt:         base,
		CreatedAt:         base,
		UpdatedAt:         base,
		MinUserLastSeenAt: &floorTime,
	})

	// Thread room without the field.
	insertThreadRoom(t, db, model.ThreadRoom{
		ID: "tr-no-floor", RoomID: "r1", ParentMessageID: "p2",
		LastMsgAt: base, CreatedAt: base, UpdatedAt: base,
	})

	got, err := repo.GetMinThreadUserLastSeenAt(ctx, "tr-floor")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.WithinDuration(t, floorTime, *got, time.Second)

	// Field unset → nil.
	got, err = repo.GetMinThreadUserLastSeenAt(ctx, "tr-no-floor")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Missing document → nil.
	got, err = repo.GetMinThreadUserLastSeenAt(ctx, "tr-missing")
	require.NoError(t, err)
	assert.Nil(t, got)
}
