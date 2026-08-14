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
