//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func newTestStore(t *testing.T) *storeMongo {
	t.Helper()
	st := newStoreMongo(testutil.MongoDB(t, "botroomsvc"))
	require.NoError(t, st.EnsureIndexes(context.Background()))
	return st
}

func newSub(roomID, userID, account string) *Subscription {
	return &Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, UserID: userID, Account: account,
		SiteID: "site-a", CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
}

// -------------------------------------------------------------------------
// EnsureIndexes
// -------------------------------------------------------------------------

func TestIntegration_EnsureIndexes_SubscriptionKeys(t *testing.T) {
	st := newTestStore(t)
	// Re-run: index creation must stay idempotent across restarts.
	require.NoError(t, st.EnsureIndexes(context.Background()))

	specs := testutil.IndexSpecs(t, st.subs)
	require.Contains(t, specs, "roomId:1,u._id:1")
	assert.False(t, specs["roomId:1,u._id:1"])
	// {roomId,u.account} unique is owned by room-service now, not created here.
	assert.Len(t, specs, 2, "_id plus the {roomId,u._id} index this service owns")
}

// -------------------------------------------------------------------------
// UpsertSubscription
// -------------------------------------------------------------------------

func TestIntegration_UpsertSubscription_CreatesThenNoOps(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	created, err := st.UpsertSubscription(ctx, newSub("room-1", "user-1", "alice"))
	require.NoError(t, err)
	assert.True(t, created, "first upsert inserts")

	created, err = st.UpsertSubscription(ctx, newSub("room-1", "user-1", "alice"))
	require.NoError(t, err)
	assert.False(t, created, "re-executing the same upsert is a no-op")

	n, err := st.subs.CountDocuments(ctx, bson.M{"roomId": "room-1"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

func TestIntegration_UpsertSubscription_DuplicateAccountIsNotAnError(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	// subscriptions.{roomId,u.account} unique is owned by room-service; create it
	// here so the duplicate-account dedup path is exercised.
	_, err := st.subs.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "roomId", Value: 1}, {Key: "u.account", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	created, err := st.UpsertSubscription(ctx, newSub("room-1", "user-1", "alice"))
	require.NoError(t, err)
	require.True(t, created)

	created, err = st.UpsertSubscription(ctx, newSub("room-1", "user-2", "alice"))
	require.NoError(t, err, "duplicate account must not surface as a write error")
	assert.False(t, created, "the account already holds a subscription")

	n, err := st.subs.CountDocuments(ctx, bson.M{"roomId": "room-1"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "the unique index still admits exactly one row per account")
}

func TestIntegration_UpsertSubscription_DistinctAccountsCoexist(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for _, acct := range []string{"alice", "bob"} {
		created, err := st.UpsertSubscription(ctx, newSub("room-1", "id-"+acct, acct))
		require.NoError(t, err)
		assert.True(t, created)
	}

	accounts, err := st.ListRoomMemberAccounts(ctx, "room-1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice", "bob"}, accounts)
}

// -------------------------------------------------------------------------
// DeleteSubscription
// -------------------------------------------------------------------------

func TestIntegration_DeleteSubscription_ByUserID(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	_, err := st.UpsertSubscription(ctx, newSub("room-1", "user-1", "alice"))
	require.NoError(t, err)

	deleted, err := st.DeleteSubscription(ctx, "room-1", "user-1")
	require.NoError(t, err)
	assert.True(t, deleted)

	deleted, err = st.DeleteSubscription(ctx, "room-1", "user-1")
	require.NoError(t, err)
	assert.False(t, deleted, "duplicate remove is a no-op")
}

func TestIntegration_DeleteSubscription_LeavesOtherRooms(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for _, room := range []string{"room-1", "room-2"} {
		_, err := st.UpsertSubscription(ctx, newSub(room, "user-1", "alice"))
		require.NoError(t, err)
	}

	deleted, err := st.DeleteSubscription(ctx, "room-1", "user-1")
	require.NoError(t, err)
	require.True(t, deleted)

	accounts, err := st.ListRoomMemberAccounts(ctx, "room-2")
	require.NoError(t, err)
	assert.Equal(t, []string{"alice"}, accounts)
}
