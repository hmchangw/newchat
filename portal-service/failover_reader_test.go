package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
)

func TestFailoverReader_MissThenCacheHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	// Store is consulted exactly once; the second call is served from cache.
	store.EXPECT().Get(gomock.Any(), "site-a").
		Return(FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1}, nil).
		Times(1)

	r := newFailoverReader(store, time.Minute)
	now := time.UnixMilli(1000)
	r.now = func() time.Time { return now }

	assert.Equal(t, ServingBackup, r.ServingTarget(context.Background(), "site-a"))
	assert.Equal(t, ServingBackup, r.ServingTarget(context.Background(), "site-a"))
}

func TestFailoverReader_RefreshesAfterTTL(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	gomock.InOrder(
		store.EXPECT().Get(gomock.Any(), "site-a").
			Return(FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1}, nil),
		store.EXPECT().Get(gomock.Any(), "site-a").
			Return(FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 2}, nil),
	)

	r := newFailoverReader(store, 5*time.Second)
	now := time.UnixMilli(1000)
	r.now = func() time.Time { return now }

	assert.Equal(t, ServingBackup, r.ServingTarget(context.Background(), "site-a"))
	now = now.Add(6 * time.Second) // past TTL
	assert.Equal(t, ServingHome, r.ServingTarget(context.Background(), "site-a"))
}

func TestFailoverReader_StoreErrorFailsSafeHomeUncached(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	// Error path is not cached: both calls hit the store.
	store.EXPECT().Get(gomock.Any(), "site-a").Return(FailoverState{}, errors.New("mongo down")).Times(2)

	r := newFailoverReader(store, time.Minute)
	r.now = func() time.Time { return time.UnixMilli(1000) }

	assert.Equal(t, ServingHome, r.ServingTarget(context.Background(), "site-a"))
	assert.Equal(t, ServingHome, r.ServingTarget(context.Background(), "site-a"))
}

// Two concurrent misses can finish out of order. The slower read carries the
// older version and must neither overwrite the newer cached target nor return
// its own stale answer — otherwise routing is stale for a whole TTL.
func TestFailoverReader_StaleReadDoesNotOverwriteNewer(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)

	started := make(chan struct{})
	release := make(chan struct{})
	gomock.InOrder(
		// Slow reader: sees version 1 (failed over), returns last.
		store.EXPECT().Get(gomock.Any(), "site-a").DoAndReturn(
			func(context.Context, string) (FailoverState, error) {
				close(started)
				<-release
				return FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1}, nil
			}),
		// Fast reader: sees version 2 (healthy) and caches it first.
		store.EXPECT().Get(gomock.Any(), "site-a").DoAndReturn(
			func(context.Context, string) (FailoverState, error) {
				return FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 2}, nil
			}),
	)

	r := newFailoverReader(store, time.Minute)
	r.now = func() time.Time { return time.UnixMilli(1000) }

	slowResult := make(chan ServingTarget, 1)
	go func() { slowResult <- r.ServingTarget(context.Background(), "site-a") }()

	<-started
	assert.Equal(t, ServingHome, r.ServingTarget(context.Background(), "site-a"))
	close(release)

	assert.Equal(t, ServingHome, <-slowResult, "stale read must yield to the newer cached target")
	// Cache still holds version 2: no further store call.
	assert.Equal(t, ServingHome, r.ServingTarget(context.Background(), "site-a"))
}
