//go:build integration

package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subauthcache"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// These integration tests run in CI (Docker required for the Valkey/Mongo
// testcontainers); they are excluded from the default unit run by the
// integration build tag.

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

	store := NewMongoStore(db, client, time.Minute, 90*time.Minute,
		circuitbreaker.New(5, 10*time.Second), circuitbreaker.New(5, 10*time.Second))

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

// TestSubauthReadThrough_SurvivesMongoOutage_ViaWarmedL2 proves the send-path
// outage-survival contract against a REAL Valkey L2: a warm subscription
// resolves from L2 without consulting the (unavailable) Mongo loader, while a
// cold (never-cached) subscription drives the loader, errors, trips the breaker,
// and is denied.
//
// The outage is simulated soundly — a HEALTHY context throughout, with the
// Mongo unavailability modeled by a spy Loader that deterministically errors
// (exactly the shape production wires: breaker.Do around subauthcache.FetchFromMongo).
// This avoids the unsound "expired shared context" pattern, which also kills the
// Valkey L2 read and so never actually exercises the L2-through-outage path.
func TestSubauthReadThrough_SurvivesMongoOutage_ViaWarmedL2(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { testutil.FlushValkey(t) })
	valkey := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))

	const (
		roomID  = "room1"
		account = "alice"
		ttl     = 90 * time.Minute
	)
	rec := cachemetrics.For("subauth", "l2")

	// Warm the real Valkey L2 with a healthy loader (Mongo up).
	warmAuth := subauthcache.SubAuth{ID: "u1", Account: account}
	warmTier := subauthcache.NewTierWithLoader(valkey, ttl, rec,
		func(context.Context, string, string) (subauthcache.SubAuth, bool, error) {
			return warmAuth, true, nil
		})
	_, subscribed, err := warmTier.Resolve(ctx, roomID, account)
	require.NoError(t, err)
	require.True(t, subscribed)

	// Simulate Mongo down: a spy loader wrapped in the breaker, exactly as the
	// store wires it, that deterministically errors. Context stays healthy so the
	// Valkey read is unaffected — only the Mongo loader is "down".
	breaker := circuitbreaker.New(1, 10*time.Second)
	var loaderCalls atomic.Int32
	outageLoader := func(context.Context, string, string) (subauthcache.SubAuth, bool, error) {
		var auth subauthcache.SubAuth
		var sub bool
		err := breaker.Do(func() error {
			loaderCalls.Add(1)
			return errors.New("mongo unreachable")
		})
		return auth, sub, err
	}

	outageTier := subauthcache.NewTierWithLoader(valkey, ttl, rec, outageLoader)

	// Warm key: L2 hit, loader NOT consulted, success.
	got, subscribed, err := outageTier.Resolve(ctx, roomID, account)
	require.NoError(t, err, "warm subscription must survive the outage via L2")
	require.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, int32(0), loaderCalls.Load(), "warm L2 hit must not consult the Mongo loader")

	// Cold key: L2 miss, loader invoked, errors, breaker trips, denied.
	_, _, err = outageTier.Resolve(ctx, "coldroom", "bob")
	require.Error(t, err, "cold subscription must be denied once Mongo is unavailable")
	assert.GreaterOrEqual(t, loaderCalls.Load(), int32(1), "cold L2 miss must consult the loader")
	assert.Equal(t, circuitbreaker.StateOpen, breaker.State(), "the cold-miss loader failure must trip the breaker")
}
