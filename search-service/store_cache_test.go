package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
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

// cachedTestMongo is a MongoStore that answers keyed lookups from fixed tables
// and records the exact keys of each call. It differs from handler_test.go's
// fakeMongo, which returns its whole table regardless of the keys asked for —
// that shape cannot show whether the cache narrowed the query.
type cachedTestMongo struct {
	mu sync.Mutex

	users    map[string]HRUser
	apps     map[string]AppRef
	usersErr error
	appsErr  error

	userCalls [][]string
	appCalls  [][]string
	subCalls  int
	appSearch int
}

func (f *cachedTestMongo) UsersByAccounts(_ context.Context, accounts []string) (map[string]HRUser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.userCalls = append(f.userCalls, append([]string(nil), accounts...))
	if f.usersErr != nil {
		return nil, f.usersErr
	}
	out := map[string]HRUser{}
	for _, a := range accounts {
		if v, ok := f.users[a]; ok {
			out[a] = v
		}
	}
	return out, nil
}

func (f *cachedTestMongo) AppsByAssistantNames(_ context.Context, bots []string) (map[string]AppRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appCalls = append(f.appCalls, append([]string(nil), bots...))
	if f.appsErr != nil {
		return nil, f.appsErr
	}
	out := map[string]AppRef{}
	for _, b := range bots {
		if v, ok := f.apps[b]; ok {
			out[b] = v
		}
	}
	return out, nil
}

func (f *cachedTestMongo) SubscriptionsByRoomIDs(_ context.Context, _ string, _ []string) (map[string]SubscriptionMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subCalls++
	return map[string]SubscriptionMeta{"r1": {RoomType: model.RoomTypeDM, Name: "bob"}}, nil
}

func (f *cachedTestMongo) SearchAppsByName(_ context.Context, _, _ string, _ *bool, _, _ int) ([]model.App, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appSearch++
	return []model.App{{ID: "app-1"}}, nil
}

func enabledCacheConfig() cacheConfig {
	return cacheConfig{HRSize: 64, HRTTL: time.Minute, AppSize: 64, AppTTL: time.Minute}
}

func TestNewCachedMongoStore_ReturnsInnerWhenFullyDisabled(t *testing.T) {
	inner := &cachedTestMongo{}

	got := newCachedMongoStore(inner, cacheConfig{})

	assert.Same(t, inner, got, "with both tiers off there is nothing to decorate")
}

func TestNewCachedMongoStore_WrapsWhenEitherTierEnabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  cacheConfig
	}{
		{"hr only", cacheConfig{HRSize: 8, HRTTL: time.Minute}},
		{"app only", cacheConfig{AppSize: 8, AppTTL: time.Minute}},
		{"both", enabledCacheConfig()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inner := &cachedTestMongo{}

			got := newCachedMongoStore(inner, tc.cfg)

			assert.NotSame(t, inner, got)
		})
	}
}

func TestCachedMongoStore_UsersServedFromCacheOnSecondCall(t *testing.T) {
	inner := &cachedTestMongo{users: map[string]HRUser{"alice": hrUser("alice"), "bob": hrUser("bob")}}
	s := newCachedMongoStore(inner, enabledCacheConfig())

	first, err := s.UsersByAccounts(context.Background(), []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice")}, first)

	second, err := s.UsersByAccounts(context.Background(), []string{"alice", "bob"})

	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice"), "bob": hrUser("bob")}, second)
	require.Len(t, inner.userCalls, 2)
	assert.Equal(t, []string{"alice"}, inner.userCalls[0])
	assert.Equal(t, []string{"bob"}, inner.userCalls[1])
}

func TestCachedMongoStore_AppsServedFromCacheOnSecondCall(t *testing.T) {
	weather := AppRef{ID: "app-1", Name: "Weather", AssistantName: "weather.bot"}
	inner := &cachedTestMongo{apps: map[string]AppRef{"weather.bot": weather}}
	s := newCachedMongoStore(inner, enabledCacheConfig())

	_, err := s.AppsByAssistantNames(context.Background(), []string{"weather.bot"})
	require.NoError(t, err)

	got, err := s.AppsByAssistantNames(context.Background(), []string{"weather.bot"})

	require.NoError(t, err)
	assert.Equal(t, map[string]AppRef{"weather.bot": weather}, got)
	assert.Len(t, inner.appCalls, 1, "a fully cached batch must not query")
}

func TestCachedMongoStore_UsersErrorIsWrappedAndNotCached(t *testing.T) {
	sentinel := errors.New("mongo down")
	inner := &cachedTestMongo{users: map[string]HRUser{"alice": hrUser("alice")}, usersErr: sentinel}
	s := newCachedMongoStore(inner, enabledCacheConfig())

	got, err := s.UsersByAccounts(context.Background(), []string{"alice"})

	require.Error(t, err)
	assert.Nil(t, got)
	assert.ErrorIs(t, err, sentinel)
	assert.Contains(t, err.Error(), "load uncached users")

	inner.usersErr = nil
	got, err = s.UsersByAccounts(context.Background(), []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, map[string]HRUser{"alice": hrUser("alice")}, got)
}

func TestCachedMongoStore_AppsErrorIsWrappedAndNotCached(t *testing.T) {
	sentinel := errors.New("mongo down")
	inner := &cachedTestMongo{apps: map[string]AppRef{"weather.bot": {ID: "app-1"}}, appsErr: sentinel}
	s := newCachedMongoStore(inner, enabledCacheConfig())

	got, err := s.AppsByAssistantNames(context.Background(), []string{"weather.bot"})

	require.Error(t, err)
	assert.Nil(t, got)
	assert.ErrorIs(t, err, sentinel)
	assert.Contains(t, err.Error(), "load uncached apps")

	inner.appsErr = nil
	got, err = s.AppsByAssistantNames(context.Background(), []string{"weather.bot"})
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

// Subscriptions are per-caller and carry the volatile isSubscribed flag, and
// SearchAppsByName is a paged text query. Neither is cacheable by account key,
// so both must reach the inner store on every call.
func TestCachedMongoStore_UncachedMethodsPassThroughEveryTime(t *testing.T) {
	inner := &cachedTestMongo{}
	s := newCachedMongoStore(inner, enabledCacheConfig())

	for i := 0; i < 3; i++ {
		subs, err := s.SubscriptionsByRoomIDs(context.Background(), "alice", []string{"r1"})
		require.NoError(t, err)
		assert.Len(t, subs, 1)

		apps, err := s.SearchAppsByName(context.Background(), "weath", "alice", nil, 0, 10)
		require.NoError(t, err)
		assert.Len(t, apps, 1)
	}

	assert.Equal(t, 3, inner.subCalls)
	assert.Equal(t, 3, inner.appSearch)
}

// One disabled tier must not disable the other.
func TestCachedMongoStore_DisabledTierStillPassesThrough(t *testing.T) {
	inner := &cachedTestMongo{
		users: map[string]HRUser{"alice": hrUser("alice")},
		apps:  map[string]AppRef{"weather.bot": {ID: "app-1"}},
	}
	s := newCachedMongoStore(inner, cacheConfig{AppSize: 8, AppTTL: time.Minute})

	for i := 0; i < 2; i++ {
		_, err := s.UsersByAccounts(context.Background(), []string{"alice"})
		require.NoError(t, err)
		_, err = s.AppsByAssistantNames(context.Background(), []string{"weather.bot"})
		require.NoError(t, err)
	}

	assert.Len(t, inner.userCalls, 2, "the HR tier is off, so every call queries")
	assert.Len(t, inner.appCalls, 1, "the app tier is on and must still cache")
}

// The decorator satisfies the interface enrich.go consumes, so the cache is
// invisible to the enrichment path.
func TestCachedMongoStore_SatisfiesMongoStore(t *testing.T) {
	var _ MongoStore = (*cachedMongoStore)(nil) // the decorator must satisfy the interface enrich.go consumes
}
