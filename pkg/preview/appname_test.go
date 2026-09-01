package preview

import (
	"context"
	"errors"
	"strings"
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

// The batch half exists so a page of many bots costs one read, not one per bot.
func TestAppNameCache_NamesReadsOnceForRepeats(t *testing.T) {
	var calls atomic.Int64
	c := NewAppNameCache(nil, func(_ context.Context, accounts []string) (map[string]string, error) {
		calls.Add(1)
		return map[string]string{"a.bot": "Alpha", "b.bot": "Beta"}, nil
	})

	for range 3 {
		got, err := c.Names(context.Background(), []string{"a.bot", "b.bot"})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"a.bot": "Alpha", "b.bot": "Beta"}, got)
	}
	assert.Equal(t, int64(1), calls.Load())
}

// The point of caching a batch: a warm account is never re-queried, so only the
// genuinely unknown accounts reach Mongo.
func TestAppNameCache_NamesQueriesOnlyTheMisses(t *testing.T) {
	var asked [][]string
	c := NewAppNameCache(nil, func(_ context.Context, accounts []string) (map[string]string, error) {
		asked = append(asked, append([]string(nil), accounts...))
		out := map[string]string{}
		for _, a := range accounts {
			out[a] = strings.ToUpper(a)
		}
		return out, nil
	})

	_, err := c.Names(context.Background(), []string{"a.bot", "b.bot"})
	require.NoError(t, err)
	got, err := c.Names(context.Background(), []string{"a.bot", "b.bot", "c.bot"})
	require.NoError(t, err)

	assert.Equal(t, map[string]string{"a.bot": "A.BOT", "b.bot": "B.BOT", "c.bot": "C.BOT"}, got)
	require.Len(t, asked, 2)
	assert.ElementsMatch(t, []string{"a.bot", "b.bot"}, asked[0])
	assert.Equal(t, []string{"c.bot"}, asked[1], "a warm account must not be re-queried")
}

// An account absent from the batch result has no app — a stable answer, cached as
// the empty name it is, exactly as the single-account half does.
func TestAppNameCache_NamesCachesTheAbsentApp(t *testing.T) {
	var calls atomic.Int64
	c := NewAppNameCache(nil, func(context.Context, []string) (map[string]string, error) {
		calls.Add(1)
		return map[string]string{}, nil
	})

	for range 3 {
		got, err := c.Names(context.Background(), []string{"nomatch.bot"})
		require.NoError(t, err)
		assert.Empty(t, got)
	}
	assert.Equal(t, int64(1), calls.Load())
}

// One cache serves both halves, so a name either one resolves is free to the other.
func TestAppNameCache_BothHalvesShareOneCache(t *testing.T) {
	var oneCalls, manyCalls atomic.Int64
	var asked [][]string
	c := NewAppNameCache(
		func(context.Context, string) (string, error) {
			oneCalls.Add(1)
			return "Alpha", nil
		},
		func(_ context.Context, accounts []string) (map[string]string, error) {
			manyCalls.Add(1)
			asked = append(asked, append([]string(nil), accounts...))
			return map[string]string{"b.bot": "Beta"}, nil
		},
	)

	// Single warms a.bot; the batch must then ask only for b.bot.
	name, err := c.Name(context.Background(), "a.bot")
	require.NoError(t, err)
	assert.Equal(t, "Alpha", name)

	got, err := c.Names(context.Background(), []string{"a.bot", "b.bot"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a.bot": "Alpha", "b.bot": "Beta"}, got)
	require.Len(t, asked, 1)
	assert.Equal(t, []string{"b.bot"}, asked[0])

	// And the reverse: b.bot came from the batch, so the single half is free.
	name, err = c.Name(context.Background(), "b.bot")
	require.NoError(t, err)
	assert.Equal(t, "Beta", name)
	assert.Equal(t, int64(1), oneCalls.Load())
	assert.Equal(t, int64(1), manyCalls.Load())
}

// A failed batch still hands back what the cache already knew, and caches nothing
// from the failure — the next call retries the misses.
func TestAppNameCache_NamesErrorKeepsCacheHitsAndRetries(t *testing.T) {
	var calls atomic.Int64
	failing := errors.New("mongo down")
	c := NewAppNameCache(nil, func(_ context.Context, accounts []string) (map[string]string, error) {
		if calls.Add(1) == 1 {
			return map[string]string{"a.bot": "Alpha"}, nil
		}
		return nil, failing
	})

	_, err := c.Names(context.Background(), []string{"a.bot"})
	require.NoError(t, err)

	got, err := c.Names(context.Background(), []string{"a.bot", "b.bot"})
	require.ErrorIs(t, err, failing)
	assert.Equal(t, map[string]string{"a.bot": "Alpha"}, got, "cache hits survive a failed fetch")

	// Nothing about b.bot was learned, so it must be asked again.
	_, err = c.Names(context.Background(), []string{"b.bot"})
	require.ErrorIs(t, err, failing)
	assert.Equal(t, int64(3), calls.Load())
}

func TestAppNameCache_NamesEmptyInputReadsNothing(t *testing.T) {
	var calls atomic.Int64
	c := NewAppNameCache(nil, func(context.Context, []string) (map[string]string, error) {
		calls.Add(1)
		return nil, nil
	})

	got, err := c.Names(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Zero(t, calls.Load())
}

// A nil inner half is a store that was never wired: it resolves nothing rather
// than panicking, so callers degrade to the composed name.
func TestAppNameCache_NilInnersDegrade(t *testing.T) {
	c := NewAppNameCache(nil, nil)

	name, err := c.Name(context.Background(), "a.bot")
	require.NoError(t, err)
	assert.Empty(t, name)

	got, err := c.Names(context.Background(), []string{"a.bot"})
	require.NoError(t, err)
	assert.Empty(t, got)
}
