package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	alice = employee{
		Account:    "alice",
		EmployeeID: "E001",
		SiteID:     "site-a",
	}
	bob = employee{
		Account:    "bob",
		EmployeeID: "E002",
		SiteID:     "site-b",
	}
)

func TestDirectoryCache_EmptyUntilLoaded(t *testing.T) {
	cache := newDirectoryCache()

	assert.False(t, cache.Ready())
	_, ok := cache.Get("alice")
	assert.False(t, ok)
}

func TestDirectoryCache_Load(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().ListEmployees(gomock.Any()).Return([]employee{alice, bob}, nil)

	cache := newDirectoryCache()
	require.NoError(t, cache.Load(context.Background(), store))

	assert.True(t, cache.Ready())
	got, ok := cache.Get("alice")
	require.True(t, ok)
	assert.Equal(t, alice, got)
	got, ok = cache.Get("bob")
	require.True(t, ok)
	assert.Equal(t, bob, got)
	_, ok = cache.Get("mallory")
	assert.False(t, ok)
}

func TestDirectoryCache_LoadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().ListEmployees(gomock.Any()).Return(nil, errors.New("mongo down"))

	cache := newDirectoryCache()
	require.Error(t, cache.Load(context.Background(), store))
	assert.False(t, cache.Ready())
}

func TestDirectoryCache_LoadErrorKeepsPreviousEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListEmployees(gomock.Any()).Return([]employee{alice}, nil),
		store.EXPECT().ListEmployees(gomock.Any()).Return(nil, errors.New("mongo down")),
	)

	cache := newDirectoryCache()
	require.NoError(t, cache.Load(context.Background(), store))
	require.Error(t, cache.Load(context.Background(), store))

	assert.True(t, cache.Ready(), "a failed refresh must keep serving the previous data")
	_, ok := cache.Get("alice")
	assert.True(t, ok)
}

func TestDirectoryCache_EmptyLoadIsNotReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().ListEmployees(gomock.Any()).Return([]employee{}, nil)

	cache := newDirectoryCache()
	require.NoError(t, cache.Load(context.Background(), store))

	assert.False(t, cache.Ready(), "an empty directory must not report ready")
}

func TestDirectoryCache_EmptyRefreshAfterReadyKeepsPrevious(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListEmployees(gomock.Any()).Return([]employee{alice}, nil),
		// The daily HR cron rewrites hr_employee; a refresh racing it can see zero rows.
		store.EXPECT().ListEmployees(gomock.Any()).Return([]employee{}, nil),
	)

	cache := newDirectoryCache()
	require.NoError(t, cache.Load(context.Background(), store))
	require.Error(t, cache.Load(context.Background(), store),
		"an empty snapshot after a successful load is a failed refresh")

	assert.True(t, cache.Ready(), "the previous snapshot must keep serving")
	_, ok := cache.Get("alice")
	assert.True(t, ok)
}

func TestDirectoryCache_DuplicateAccountSkippedKeepsRest(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	dupAlice := alice
	dupAlice.SiteID = "site-b"
	// The first occurrence wins; the later duplicate row is skipped (with a
	// warning) rather than rejecting the whole snapshot.
	store.EXPECT().ListEmployees(gomock.Any()).Return([]employee{alice, bob, dupAlice}, nil)

	cache := newDirectoryCache()
	require.NoError(t, cache.Load(context.Background(), store),
		"a duplicate row must be skipped, not reject the whole snapshot")

	assert.True(t, cache.Ready())
	got, ok := cache.Get("alice")
	require.True(t, ok)
	assert.Equal(t, alice, got, "the first occurrence wins")
	_, ok = cache.Get("bob")
	assert.True(t, ok, "non-duplicate rows are still published")
}

func TestDirectoryCache_DuplicateAccountAtStartupReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().ListEmployees(gomock.Any()).Return([]employee{alice, alice}, nil)

	cache := newDirectoryCache()
	require.NoError(t, cache.Load(context.Background(), store))

	assert.True(t, cache.Ready(), "a duplicate at startup is skipped, not fatal")
	got, ok := cache.Get("alice")
	require.True(t, ok)
	assert.Equal(t, alice, got)
}

// TestDirectoryCache_ConcurrentReadDuringLoad guards the lock-free read path:
// many readers hit Get/Ready while the writer swaps the snapshot. The race
// detector fails this if the swap is not atomic against concurrent reads.
func TestDirectoryCache_ConcurrentReadDuringLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().ListEmployees(gomock.Any()).Return([]employee{alice, bob}, nil).AnyTimes()

	cache := newDirectoryCache()
	require.NoError(t, cache.Load(context.Background(), store))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					cache.Get("alice")
					cache.Ready()
				}
			}
		}()
	}
	for range 50 {
		require.NoError(t, cache.Load(context.Background(), store))
	}
	close(stop)
	wg.Wait()
}

func TestDirectoryCache_RefreshLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	loads := make(chan struct{}, 2)
	gomock.InOrder(
		// First attempt fails — the loop must retry on the short interval.
		store.EXPECT().ListEmployees(gomock.Any()).DoAndReturn(func(context.Context) ([]employee, error) {
			loads <- struct{}{}
			return nil, errors.New("mongo down")
		}),
		store.EXPECT().ListEmployees(gomock.Any()).DoAndReturn(func(context.Context) ([]employee, error) {
			loads <- struct{}{}
			return []employee{alice}, nil
		}),
	)

	cache := newDirectoryCache()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		cache.RefreshLoop(ctx, store, time.Hour, time.Millisecond)
	}()

	<-loads
	<-loads
	require.Eventually(t, cache.Ready, time.Second, time.Millisecond)
	_, ok := cache.Get("alice")
	assert.True(t, ok)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RefreshLoop did not stop on context cancel")
	}
}
