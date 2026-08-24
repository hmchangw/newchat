package preview

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

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
