package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// newUndialedStore builds a store over a *mongo.Database that was never dialed.
// mongo.Connect does not dial in the v2 driver, so every read below either
// fast-fails at the breaker (the behaviour under test) or would go looking for a
// server that is not there.
func newUndialedStore(t *testing.T, breaker *circuitbreaker.Breaker) *mongoStore {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	db := client.Database("broadcast_worker_test")
	return NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"),
		db.Collection("thread_rooms"), db.Collection("users"), nil, time.Minute, time.Minute, breaker)
}

// main.go wires one breaker and documents it as covering "every Mongo call site
// in this service". These three reads are on the edit and hidden-thread-reply
// paths; unfenced, each burns a full server-selection timeout per attempt for
// the whole OutageRetryBudget (MaxDeliver=40 ≈ 72 min) and never reports the
// failure, so the breaker stays closed for exactly the paths that are stalling.
func TestMongoStore_ReadsAreFencedByTheBreaker(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *mongoStore) error
	}{
		{"GetRoom", func(ctx context.Context, s *mongoStore) error {
			_, err := s.GetRoom(ctx, "room1")
			return err
		}},
		{"GetThreadFollowers", func(ctx context.Context, s *mongoStore) error {
			_, err := s.GetThreadFollowers(ctx, "msg1")
			return err
		}},
		{"GetHistorySharedSince", func(ctx context.Context, s *mongoStore) error {
			_, err := s.GetHistorySharedSince(ctx, "room1", []string{"alice"})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			breaker := circuitbreaker.New(1, time.Minute,
				circuitbreaker.WithFailurePredicate(MongoBreakerFailure))
			s := newUndialedStore(t, breaker)

			boom := errors.New("mongo down")
			require.ErrorIs(t, breaker.Do(func() error { return boom }), boom)
			require.Equal(t, circuitbreaker.StateOpen, breaker.State())

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			assert.ErrorIs(t, tt.call(ctx, s), circuitbreaker.ErrOpen)
		})
	}
}

// Fencing must not turn a healthy absence into breaker pressure. A thread with
// no followers is the common case for a first reply, and a room-less lookup is
// what a deleted room returns; neither is evidence Mongo is unwell.
func TestMongoStore_FencedReadsDoNotSpendBudgetOnAbsence(t *testing.T) {
	breaker := circuitbreaker.New(3, time.Minute,
		circuitbreaker.WithFailurePredicate(MongoBreakerFailure))

	for range 10 {
		require.ErrorIs(t, breaker.Do(func() error { return mongo.ErrNoDocuments }), mongo.ErrNoDocuments)
	}
	assert.Equal(t, circuitbreaker.StateClosed, breaker.State(),
		"a missing document is an answer from a working Mongo")
}
