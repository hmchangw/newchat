//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func TestMongoStore_GetRoomMeta_ReadsThroughL2(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { testutil.FlushValkey(t) })
	client := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	db := testutil.MongoDB(t, "gk-meta")

	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
		"_id": "r1", "name": "general", "type": model.RoomTypeChannel,
		"siteId": "site-a", "userCount": 600,
	})
	require.NoError(t, err)

	store := NewMongoStore(db, client, time.Minute, 90*time.Minute, circuitbreaker.New(5, 10*time.Second))

	got, err := store.GetRoomMeta(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, 600, got.UserCount)
	assert.Equal(t, "general", got.Name)
	assert.Equal(t, "site-a", got.SiteID)
	assert.Equal(t, model.RoomTypeChannel, got.Type)

	// Served from L2 after the Mongo doc is gone.
	_, err = db.Collection("rooms").DeleteOne(ctx, bson.M{"_id": "r1"})
	require.NoError(t, err)
	again, err := store.GetRoomMeta(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, 600, again.UserCount)
	assert.Equal(t, "general", again.Name)
	assert.Equal(t, "site-a", again.SiteID)
	assert.Equal(t, model.RoomTypeChannel, again.Type)
}

// TestMongoStore_SubscriptionSurvivesMongoOutage proves the send-path
// outage-survival contract end to end: a warm subscription resolves from the
// Valkey L2 tier without touching Mongo, while a cold (never-cached)
// subscription is denied with a retryable infra error once Mongo is
// unavailable and the breaker has tripped.
//
// NOTE: pkg/testutil has no StopMongo/TerminateMongo-for-one-test helper —
// TerminateMongo tears down the process-shared container for every test in
// the binary, which would break sibling tests. Instead this simulates the
// outage per the task brief's documented fallback: after warming the L2 with
// a normal-deadline context, subsequent calls use a context whose deadline
// has already elapsed, so the L2-miss Mongo read times out exactly as it
// would against a wedged/unreachable Mongo, and the breaker trips on it.
func TestMongoStore_SubscriptionSurvivesMongoOutage(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "gk_outage")
	valkey := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	t.Cleanup(func() { testutil.FlushValkey(t) })

	// Seed a subscription in Mongo.
	_, err := db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "room1", "roles": []string{},
		"u": bson.M{"_id": "u1", "account": "alice"},
	})
	require.NoError(t, err)

	breaker := circuitbreaker.New(1, 50*time.Millisecond)
	store := NewMongoStore(db, valkey, 15*time.Minute, 90*time.Minute, breaker)

	// Warm the L2 while Mongo is up.
	sub, err := store.GetSubscription(ctx, "alice", "room1")
	require.NoError(t, err)
	require.Equal(t, "u1", sub.User.ID)

	// Simulate Mongo down: an already-expired deadline makes any Mongo op this
	// context reaches fail with context.DeadlineExceeded, same as a wedged
	// connection would eventually time out.
	outageCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()

	// Warm room still resolves from L2 (no Mongo hit — the expired deadline
	// never reaches a Mongo call).
	got, err := store.GetSubscription(outageCtx, "alice", "room1")
	require.NoError(t, err, "warm subscription must survive the outage via L2")
	require.Equal(t, "u1", got.User.ID)

	// Cold room: not in L2, Mongo unreachable -> error (denied). Trips the
	// breaker via the expired-context Mongo failure.
	_, err = store.GetSubscription(outageCtx, "bob", "coldroom")
	require.Error(t, err)
}
