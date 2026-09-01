package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingLoad is a batch loader that answers from a fixed table and records
// the exact key slice of every call, so a test can assert that a cache hit
// removed a key from the query rather than merely returning the right value.
type recordingLoad struct {
	mu    sync.Mutex
	table map[string]HRUser
	err   error
	calls [][]string
}

func (r *recordingLoad) load(_ context.Context, keys []string) (map[string]HRUser, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, append([]string(nil), keys...))
	if r.err != nil {
		return nil, r.err
	}
	out := map[string]HRUser{}
	for _, k := range keys {
		if v, ok := r.table[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func (r *recordingLoad) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// countingRecorder counts cache outcomes; it satisfies cacheRecorder.
type countingRecorder struct {
	mu                     sync.Mutex
	hits, misses, errCount int
}

func (c *countingRecorder) Hit(context.Context)   { c.mu.Lock(); c.hits++; c.mu.Unlock() }
func (c *countingRecorder) Miss(context.Context)  { c.mu.Lock(); c.misses++; c.mu.Unlock() }
func (c *countingRecorder) Error(context.Context) { c.mu.Lock(); c.errCount++; c.mu.Unlock() }

func hrUser(acct string) HRUser {
	return HRUser{Account: acct, EngName: acct + " Eng", ChineseName: acct + " 中"}
}

func TestNewEntryLRU_DisabledOnNonPositive(t *testing.T) {
	tests := []struct {
		name string
		size int
		ttl  time.Duration
	}{
		{"zero size", 0, time.Minute},
		{"negative size", -1, time.Minute},
		{"zero ttl", 10, 0},
		{"negative ttl", 10, -time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Nil(t, newEntryLRU[HRUser](tc.size, tc.ttl))
		})
	}
}

func TestNewEntryLRU_EnabledOnPositive(t *testing.T) {
	assert.NotNil(t, newEntryLRU[HRUser](8, time.Minute))
}

func TestLookupCached_ColdMissLoadsAndCaches(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice")}}
	c := newEntryLRU[HRUser](8, time.Minute)
	rec := &countingRecorder{}

	got, err := lookupCached(context.Background(), c, rec, []string{"alice"}, loader.load)

	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice")}, got)
	assert.Equal(t, [][]string{{"alice"}}, loader.calls)
	assert.Equal(t, 1, rec.misses)
	assert.Equal(t, 0, rec.hits)
}

func TestLookupCached_WarmHitSkipsLoad(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice")}}
	c := newEntryLRU[HRUser](8, time.Minute)
	rec := &countingRecorder{}
	_, err := lookupCached(context.Background(), c, rec, []string{"alice"}, loader.load)
	require.NoError(t, err)

	got, err := lookupCached(context.Background(), c, rec, []string{"alice"}, loader.load)

	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice")}, got)
	assert.Equal(t, 1, loader.callCount(), "a fully cached batch must not query at all")
	assert.Equal(t, 1, rec.hits)
	assert.Equal(t, 1, rec.misses)
}

func TestLookupCached_PartialHitQueriesOnlyMissingKeys(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice"), "bob": hrUser("bob")}}
	c := newEntryLRU[HRUser](8, time.Minute)
	rec := &countingRecorder{}
	_, err := lookupCached(context.Background(), c, rec, []string{"alice"}, loader.load)
	require.NoError(t, err)

	got, err := lookupCached(context.Background(), c, rec, []string{"alice", "bob"}, loader.load)

	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice"), "bob": hrUser("bob")}, got,
		"the merged result must carry the cached key as well as the loaded one")
	require.Len(t, loader.calls, 2)
	assert.Equal(t, []string{"bob"}, loader.calls[1], "alice was cached and must not be re-queried")
}

// A key with no row is cached as a tombstone: without this, one departed user
// in a room would force a Mongo query on every search that room ever appears in.
func TestLookupCached_CachesMissesAsTombstones(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{}}
	c := newEntryLRU[HRUser](8, time.Minute)
	rec := &countingRecorder{}
	first, err := lookupCached(context.Background(), c, rec, []string{"ghost"}, loader.load)
	require.NoError(t, err)
	assert.Empty(t, first)

	second, err := lookupCached(context.Background(), c, rec, []string{"ghost"}, loader.load)

	require.NoError(t, err)
	assert.Empty(t, second, "a tombstone must resolve to absence, not a zero-value row")
	assert.NotContains(t, second, "ghost")
	assert.Equal(t, 1, loader.callCount())
}

func TestLookupCached_DeduplicatesRepeatedKeys(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice")}}
	c := newEntryLRU[HRUser](8, time.Minute)

	got, err := lookupCached(context.Background(), c, &countingRecorder{}, []string{"alice", "alice", "alice"}, loader.load)

	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice")}, got)
	require.Len(t, loader.calls, 1)
	assert.Equal(t, []string{"alice"}, loader.calls[0])
}

func TestLookupCached_EmptyKeysSkipsLoad(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice")}}
	c := newEntryLRU[HRUser](8, time.Minute)

	got, err := lookupCached(context.Background(), c, &countingRecorder{}, nil, loader.load)

	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 0, loader.callCount())
}

// A load failure must not poison the cache: the next call retries rather than
// serving a tombstone minted from an outage.
func TestLookupCached_ErrorPropagatesAndIsNotCached(t *testing.T) {
	sentinel := errors.New("mongo down")
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice")}, err: sentinel}
	c := newEntryLRU[HRUser](8, time.Minute)

	got, err := lookupCached(context.Background(), c, &countingRecorder{}, []string{"alice"}, loader.load)

	require.ErrorIs(t, err, sentinel)
	assert.Nil(t, got)

	loader.err = nil
	got, err = lookupCached(context.Background(), c, &countingRecorder{}, []string{"alice"}, loader.load)
	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice")}, got)
	assert.Equal(t, 2, loader.callCount())
}

func TestLookupCached_NilCacheAlwaysLoads(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice")}}
	rec := &countingRecorder{}

	for i := 0; i < 3; i++ {
		got, err := lookupCached[HRUser](context.Background(), nil, rec, []string{"alice"}, loader.load)
		require.NoError(t, err)
		assert.Equal(t, map[string]HRUser{"alice": hrUser("alice")}, got)
	}

	assert.Equal(t, 3, loader.callCount(), "a disabled cache must not change behaviour, only cost")
	assert.Equal(t, 0, rec.hits, "a disabled cache records nothing — its hit rate is not 0%, it is undefined")
	assert.Equal(t, 0, rec.misses)
}

func TestLookupCached_EntryExpiresAfterTTL(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice")}}
	c := newEntryLRU[HRUser](8, 30*time.Millisecond)
	_, err := lookupCached(context.Background(), c, &countingRecorder{}, []string{"alice"}, loader.load)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		_, ok := c.Get("alice")
		return !ok
	}, 2*time.Second, 5*time.Millisecond, "entry must expire once its TTL elapses")

	got, err := lookupCached(context.Background(), c, &countingRecorder{}, []string{"alice"}, loader.load)

	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice")}, got)
	assert.Equal(t, 2, loader.callCount(), "an expired key must be re-queried")
}

func TestLookupCached_EvictsLeastRecentlyUsedOverSize(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"a": hrUser("a"), "b": hrUser("b"), "c": hrUser("c")}}
	c := newEntryLRU[HRUser](2, time.Minute)
	rec := &countingRecorder{}
	_, err := lookupCached(context.Background(), c, rec, []string{"a", "b"}, loader.load)
	require.NoError(t, err)

	// Touch "b" so "a" is the least recently used, then add "c" to overflow.
	_, err = lookupCached(context.Background(), c, rec, []string{"b"}, loader.load)
	require.NoError(t, err)
	_, err = lookupCached(context.Background(), c, rec, []string{"c"}, loader.load)
	require.NoError(t, err)

	assert.Equal(t, 2, c.Len())
	_, aStillCached := c.Get("a")
	assert.False(t, aStillCached, "the least recently used key is the one evicted")
	_, bStillCached := c.Get("b")
	assert.True(t, bStillCached)
}

func TestLookupCached_ConcurrentCallersAreRaceFree(t *testing.T) {
	loader := &recordingLoad{table: map[string]HRUser{"alice": hrUser("alice"), "bob": hrUser("bob")}}
	c := newEntryLRU[HRUser](8, time.Minute)
	rec := &countingRecorder{}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := lookupCached(context.Background(), c, rec, []string{"alice", "bob"}, loader.load)
			assert.NoError(t, err)
			assert.Len(t, got, 2)
		}()
	}
	wg.Wait()

	assert.GreaterOrEqual(t, loader.callCount(), 1)
}
