//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

// unreachableMongoURI points at a local port with nothing listening — what a
// pod restarting into a MongoDB outage actually sees.
func unreachableMongoURI(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return fmt.Sprintf("mongodb://127.0.0.1:%d/?directConnection=true", port)
}

// TestStartup_ReachesServingStateWithMongoUnreachable walks the service's real
// Mongo boot sequence with MongoDB down: connect, build the store, run the
// index step. Before this change each of those steps killed the process; all
// three must now complete so the worker can go on to consume from JetStream.
func TestStartup_ReachesServingStateWithMongoUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Gate 1: connect must not fail.
	client, err := mongoutil.Connect(ctx, unreachableMongoURI(t), "", "", mongoutil.WithLazyConnect())
	require.NoError(t, err, "service must connect lazily while MongoDB is unreachable")
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

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
	indexCtx, indexCancel := context.WithTimeout(ctx, 10*time.Second)
	defer indexCancel()
	require.NotPanics(t, func() {
		mongoutil.EnsureIndexesBestEffort(indexCtx, "broadcast-worker store", store.EnsureIndexes)
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

// TestStartup_IndexStepIsFatalNowhere guards the regression that matters: the
// index step must stay non-fatal even when it fails repeatedly, since a
// restarted pod runs it on every boot during an outage.
func TestStartup_IndexStepIsFatalNowhere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongoutil.Connect(ctx, unreachableMongoURI(t), "", "", mongoutil.WithLazyConnect())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	db := client.Database("broadcast_worker_startup_test")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"),
		db.Collection("thread_rooms"), nil, time.Minute)

	for i := range 3 {
		stepCtx, stepCancel := context.WithTimeout(ctx, 3*time.Second)
		require.NotPanics(t, func() {
			mongoutil.EnsureIndexesBestEffort(stepCtx, "broadcast-worker store", store.EnsureIndexes)
		}, "boot %d must survive a failing index step", i+1)
		stepCancel()
	}
}
