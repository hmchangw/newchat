package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// A room that does not exist is a healthy answer from a healthy Mongo. Spending
// the breaker's failure budget on it would let a handful of requests for deleted
// or mistyped rooms open the breaker and degrade every other room's meta read.
func TestMetaBreaker_MissingRoomDoesNotTripBreaker(t *testing.T) {
	metaBreaker := circuitbreaker.New(2, time.Minute,
		circuitbreaker.WithFailurePredicate(MetaBreakerFailure))

	for range 5 {
		err := metaBreaker.Do(func() error {
			return fmt.Errorf("fetch room meta: %w", mongo.ErrNoDocuments)
		})
		require.ErrorIs(t, err, mongo.ErrNoDocuments, "the not-found error must still reach the caller")
	}
	assert.Equal(t, circuitbreaker.StateClosed, metaBreaker.State(),
		"missing rooms must not count as Mongo failures")
}

// Genuine infrastructure errors must still open the breaker.
func TestMetaBreaker_InfraErrorTripsBreaker(t *testing.T) {
	metaBreaker := circuitbreaker.New(2, time.Minute,
		circuitbreaker.WithFailurePredicate(MetaBreakerFailure))

	boom := errors.New("connection refused")
	for range 2 {
		require.ErrorIs(t, metaBreaker.Do(func() error { return boom }), boom)
	}
	require.Equal(t, circuitbreaker.StateOpen, metaBreaker.State())

	// Once open, the fetch must not run at all.
	err := metaBreaker.Do(func() error {
		t.Fatal("meta fetch must not run while the breaker is open")
		return nil
	})
	assert.ErrorIs(t, err, circuitbreaker.ErrOpen)
}

// The subscription and room-meta reads must ride SEPARATE circuit breakers: with
// one shared breaker, warm room-meta L2 hits keep reporting success and hold it
// closed, so cold subscription misses would never start failing fast during an
// outage. Asserted through the store's own methods rather than its fields, so
// this checks the wiring rather than the struct layout.
func TestNewMongoStore_UsesIndependentBreakers(t *testing.T) {
	// mongo.Connect does not dial in the v2 driver, so this yields a usable
	// *mongo.Database without a live Mongo. Both breakers are open for the reads
	// below, so no query is ever issued.
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	db := client.Database("gatekeeper_test")

	subBreaker := circuitbreaker.New(1, time.Minute)
	metaBreaker := circuitbreaker.New(1, time.Minute)
	s := NewMongoStore(db, nil, 0, time.Minute, subBreaker, metaBreaker)

	// Trip only the subscription breaker.
	boom := errors.New("mongo down")
	require.ErrorIs(t, subBreaker.Do(func() error { return boom }), boom)
	require.Equal(t, circuitbreaker.StateOpen, subBreaker.State())

	// The subscription read now fast-fails, proving it rides subBreaker.
	_, err = s.GetSubscription(context.Background(), "alice", "room1")
	require.ErrorIs(t, err, circuitbreaker.ErrOpen)

	// Room-meta successes must not reset the subscription breaker.
	for range 5 {
		require.NoError(t, metaBreaker.Do(func() error { return nil }))
	}
	assert.Equal(t, circuitbreaker.StateOpen, subBreaker.State(),
		"room-meta successes must not reset the subscription breaker")

	// And the room-meta read rides metaBreaker, not subBreaker: it is still
	// closed here, so an open subscription breaker must not fence it.
	require.ErrorIs(t, metaBreaker.Do(func() error { return boom }), boom)
	require.Equal(t, circuitbreaker.StateOpen, metaBreaker.State())
	_, err = s.GetRoomMeta(context.Background(), "room1")
	assert.ErrorIs(t, err, circuitbreaker.ErrOpen)
}
