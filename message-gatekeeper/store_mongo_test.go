package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// TestNewMongoStore_UsesIndependentBreakers guards the outage-survival wiring:
// the subscription and room-meta reads must ride SEPARATE circuit breakers so a
// warm room-meta L2 hit (success) can't reset the subscription breaker's failure
// count. With a single shared breaker, meta successes would keep the breaker
// Closed and cold subscription misses would never fast-fail during a Mongo outage.
func TestNewMongoStore_UsesIndependentBreakers(t *testing.T) {
	// mongo.Connect does not dial in the v2 driver, so this yields a usable
	// *mongo.Database without a live Mongo — enough to exercise the real
	// constructor and its breaker wiring.
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	db := client.Database("gatekeeper_test")

	subBreaker := circuitbreaker.New(2, time.Minute)
	metaBreaker := circuitbreaker.New(2, time.Minute)
	s := NewMongoStore(db, nil, 0, time.Minute, subBreaker, metaBreaker)

	require.NotSame(t, s.subBreaker, s.metaBreaker, "sub and meta breakers must be distinct instances")
	require.Same(t, subBreaker, s.subBreaker)
	require.Same(t, metaBreaker, s.metaBreaker)

	// Trip the subscription breaker with consecutive failures.
	boom := errors.New("mongo down")
	for i := 0; i < 2; i++ {
		_ = s.subBreaker.Do(func() error { return boom })
	}
	require.Equal(t, circuitbreaker.StateOpen, s.subBreaker.State())

	// Room-meta successes on the meta breaker must NOT reset the sub breaker.
	for i := 0; i < 5; i++ {
		require.NoError(t, s.metaBreaker.Do(func() error { return nil }))
	}
	assert.Equal(t, circuitbreaker.StateOpen, s.subBreaker.State(),
		"room-meta successes must not reset the subscription breaker")

	// The subscription path still fast-fails without invoking its loader.
	err = s.subBreaker.Do(func() error {
		t.Fatal("sub loader must not run while the breaker is open")
		return nil
	})
	assert.ErrorIs(t, err, circuitbreaker.ErrOpen)
}
