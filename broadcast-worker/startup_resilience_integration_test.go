//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestStartup_ReachesServingStateWithMongoUnreachable walks the service's real
// Mongo boot sequence with MongoDB down: connect, build the store, run the
// index step. Before this change each of those steps killed the process; all
// three must now complete so the worker can go on to consume from JetStream.
func TestStartup_ReachesServingStateWithMongoUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Gate 1: connect must not fail.
	client, err := mongoutil.Connect(ctx, testutil.UnreachableMongoURI(t), "", "", mongoutil.WithLazyConnect())
	require.NoError(t, err, "service must connect lazily while MongoDB is unreachable")
	t.Cleanup(func() { mongoutil.Disconnect(context.Background(), client) })

	db := client.Database("broadcast_worker_startup_test")
	store := NewMongoStore(
		db.Collection("rooms"),
		db.Collection("subscriptions"),
		db.Collection("thread_rooms"),
		nil, // no Valkey L2 tier
		time.Minute,
	)
	require.NotNil(t, store, "store must be constructible with MongoDB unreachable")

	// Gate 2: the index step must warn and continue, not exit.
	require.NotPanics(t, func() {
		mongoutil.EnsureIndexesBestEffort(ctx, "broadcast-worker store", store.EnsureIndexes)
	}, "failed index creation must not stop startup")

	// Serving state: the store answers requests with an error rather than
	// hanging, so the consumer loop can Nak and retry instead of wedging.
	opCtx, opCancel := context.WithTimeout(ctx, 5*time.Second)
	defer opCancel()
	start := time.Now()
	_, err = store.GetRoom(opCtx, "room-1")
	elapsed := time.Since(start)

	assert.Error(t, err, "reads must fail while MongoDB is unreachable")
	assert.Less(t, elapsed, 8*time.Second, "reads must fail bounded, not hang")
}
