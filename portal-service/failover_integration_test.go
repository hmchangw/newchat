//go:build integration

package main

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoFailoverStore_GetDefaultsHealthy(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	got, err := store.Get(ctx, "site-unknown")
	require.NoError(t, err)
	assert.Equal(t, StatusHealthy, got.Status)
	assert.Equal(t, int64(0), got.Version)
	assert.Equal(t, "site-unknown", got.SiteID)
}

func TestMongoFailoverStore_TransitionInsertThenUpdate(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	// First transition inserts version 1.
	v1 := FailoverState{SiteID: "site-a", Status: StatusFailedOver, Operator: "jane", Reason: "down", Since: 100, Version: 1, Timestamp: 100}
	require.NoError(t, store.Transition(ctx, v1))

	got, err := store.Get(ctx, "site-a")
	require.NoError(t, err)
	assert.Equal(t, StatusFailedOver, got.Status)
	assert.Equal(t, int64(1), got.Version)

	// Second transition CAS-updates to version 2.
	v2 := FailoverState{SiteID: "site-a", Status: StatusFailingBack, Operator: "jane", Reason: "draining", Since: 200, Version: 2, Timestamp: 200}
	require.NoError(t, store.Transition(ctx, v2))
	got, err = store.Get(ctx, "site-a")
	require.NoError(t, err)
	assert.Equal(t, StatusFailingBack, got.Status)
	assert.Equal(t, int64(2), got.Version)
}

func TestMongoFailoverStore_TransitionStaleVersionConflicts(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	require.NoError(t, store.Transition(ctx, FailoverState{SiteID: "site-b", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))
	// Now at version 1. A second "version 1" insert (stale) must conflict.
	err := store.Transition(ctx, FailoverState{SiteID: "site-b", Status: StatusFailedOver, Version: 1, Since: 2, Timestamp: 2})
	assert.ErrorIs(t, err, errFailoverVersionConflict)

	// A version-3 update (skipping 2) finds no version-2 doc -> conflict.
	err = store.Transition(ctx, FailoverState{SiteID: "site-b", Status: StatusHealthy, Version: 3, Since: 3, Timestamp: 3})
	assert.ErrorIs(t, err, errFailoverVersionConflict)
}

func TestMongoFailoverStore_ConcurrentCASOneWinner(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	// Seed version 1.
	require.NoError(t, store.Transition(ctx, FailoverState{SiteID: "site-c", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))

	// Two goroutines both try to move version 1 -> 2. Exactly one wins.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = store.Transition(ctx, FailoverState{SiteID: "site-c", Status: StatusHealthy, Version: 2, Since: 2, Timestamp: 2})
		}(i)
	}
	wg.Wait()

	conflicts := 0
	oks := 0
	for _, e := range errs {
		switch {
		case e == nil:
			oks++
		case assert.ErrorIs(t, e, errFailoverVersionConflict):
			conflicts++
		}
	}
	assert.Equal(t, 1, oks, "exactly one writer wins")
	assert.Equal(t, 1, conflicts, "exactly one writer conflicts")
}

func TestMongoFailoverStore_List(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	require.NoError(t, store.Transition(ctx, FailoverState{SiteID: "site-x", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))
	require.NoError(t, store.Transition(ctx, FailoverState{SiteID: "site-y", Status: StatusHealthy, Version: 1, Since: 1, Timestamp: 1}))

	all, err := store.List(ctx)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, s := range all {
		ids[s.SiteID] = true
	}
	assert.True(t, ids["site-x"])
	assert.True(t, ids["site-y"])
}
