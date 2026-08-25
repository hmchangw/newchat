package preview

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachedAppNameLookup_ServesRepeatsFromCache(t *testing.T) {
	var calls atomic.Int64
	lookup := CachedAppNameLookup(func(context.Context, string) (string, error) {
		calls.Add(1)
		return "Weather Bot", nil
	})

	for range 5 {
		name, err := lookup(context.Background(), "bot-1")
		require.NoError(t, err)
		assert.Equal(t, "Weather Bot", name)
	}
	assert.Equal(t, int64(1), calls.Load(), "a bot's app name must be read once, not once per message")
}

// A bot account with no app row is a stable answer. Left uncached, one misconfigured bot
// re-reads on every message it sends — the case the cache exists for.
func TestCachedAppNameLookup_CachesTheAbsentApp(t *testing.T) {
	var calls atomic.Int64
	lookup := CachedAppNameLookup(func(context.Context, string) (string, error) {
		calls.Add(1)
		return "", nil
	})

	for range 3 {
		name, err := lookup(context.Background(), "bot-nomatch")
		require.NoError(t, err)
		assert.Empty(t, name)
	}
	assert.Equal(t, int64(1), calls.Load(), "a negative result is an answer, not a reason to re-read")
}

// An error says nothing about the app; caching it would pin a transient failure for the
// whole TTL, on a path whose whole point is to degrade for one message only.
func TestCachedAppNameLookup_DoesNotCacheErrors(t *testing.T) {
	var calls atomic.Int64
	failing := errors.New("mongo down")
	lookup := CachedAppNameLookup(func(context.Context, string) (string, error) {
		if calls.Add(1) == 1 {
			return "", failing
		}
		return "Recovered Bot", nil
	})

	_, err := lookup(context.Background(), "bot-1")
	require.ErrorIs(t, err, failing)

	name, err := lookup(context.Background(), "bot-1")
	require.NoError(t, err)
	assert.Equal(t, "Recovered Bot", name, "the next message must retry, not inherit the failure")
}

// A cold key under a burst is exactly when the uncached read hurt most.
func TestCachedAppNameLookup_CollapsesConcurrentMisses(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	lookup := CachedAppNameLookup(func(context.Context, string) (string, error) {
		calls.Add(1)
		<-release
		return "Weather Bot", nil
	})

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name, err := lookup(context.Background(), "bot-1")
			assert.NoError(t, err)
			assert.Equal(t, "Weather Bot", name)
		}()
	}
	close(release)
	wg.Wait()
	assert.Equal(t, int64(1), calls.Load(), "concurrent misses on one key must collapse to one read")
}

func TestCachedAppNameLookup_NilInnerStaysNil(t *testing.T) {
	assert.Nil(t, CachedAppNameLookup(nil), "no lookup wrapped is still no lookup")
}

// The outer cache check and sf.Do are separate steps, so a caller can miss the check
// while a load is in flight and then reach Do after that load completed and released the
// key — arriving as a NEW leader for an answer already cached. That interleaving sits
// between two adjacent statements and cannot be driven from outside, so this pins the
// property the leader itself must hold: entering the load with a warm cache reads nothing.
func TestLoadAppName_RechecksTheCacheBeforeReading(t *testing.T) {
	var calls atomic.Int64
	inner := func(context.Context, string) (string, error) {
		calls.Add(1)
		return "Weather Bot", nil
	}
	cache := lru.NewLRU[string, string](appNameCacheSize, nil, appNameCacheTTL)

	// The first leader populates the cache, as the real one does.
	got, err := loadAppName(context.Background(), cache, inner, "bot-1")
	require.NoError(t, err)
	assert.Equal(t, "Weather Bot", got)

	// The post-completion leader must serve from that, not read again.
	got, err = loadAppName(context.Background(), cache, inner, "bot-1")
	require.NoError(t, err)
	assert.Equal(t, "Weather Bot", got)
	assert.Equal(t, int64(1), calls.Load(),
		"a leader arriving after the load completed must serve the cached answer, not re-read")
}

// A negative is a cached answer like any other, so the recheck must honour it too —
// otherwise the misconfigured-bot case the cache exists for still re-reads.
func TestLoadAppName_RechecksHonourTheCachedNegative(t *testing.T) {
	var calls atomic.Int64
	inner := func(context.Context, string) (string, error) {
		calls.Add(1)
		return "", nil
	}
	cache := lru.NewLRU[string, string](appNameCacheSize, nil, appNameCacheTTL)

	for range 2 {
		got, err := loadAppName(context.Background(), cache, inner, "bot-nomatch")
		require.NoError(t, err)
		assert.Empty(t, got)
	}
	assert.Equal(t, int64(1), calls.Load(), "an empty name is an answer the recheck must reuse")
}

// An error leaves nothing cached, so the next leader must genuinely retry.
func TestLoadAppName_ErrorLeavesNothingForTheRecheck(t *testing.T) {
	var calls atomic.Int64
	failing := errors.New("mongo down")
	inner := func(context.Context, string) (string, error) {
		if calls.Add(1) == 1 {
			return "", failing
		}
		return "Recovered Bot", nil
	}
	cache := lru.NewLRU[string, string](appNameCacheSize, nil, appNameCacheTTL)

	_, err := loadAppName(context.Background(), cache, inner, "bot-1")
	require.ErrorIs(t, err, failing)

	got, err := loadAppName(context.Background(), cache, inner, "bot-1")
	require.NoError(t, err)
	assert.Equal(t, "Recovered Bot", got, "a failed load must not poison the recheck")
}
