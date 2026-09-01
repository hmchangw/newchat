# search-service HR/App LRU Caches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `search.messages` enrichment from re-reading the same HR users and bot apps out of MongoDB on every search, by fronting the two account-keyed lookups with pod-local LRU+TTL caches.

**Architecture:** A `cachedMongoStore` decorator implementing the existing `MongoStore` interface wraps `*mongoStore` in `main.go`. It caches `UsersByAccounts` and `AppsByAssistantNames` per key with `github.com/hashicorp/golang-lru/v2/expirable`, splitting each batch into cached keys and a remainder forwarded as one Mongo `$in`. `SubscriptionsByRoomIDs` and `SearchAppsByName` pass straight through via struct embedding. `enrich.go`, `handler.go` and the `MongoStore` interface are untouched.

**Tech Stack:** Go 1.25, `github.com/hashicorp/golang-lru/v2/expirable` (already in `go.mod`), `pkg/cachemetrics`, `pkg/valkeyutil` (for the shared `CacheRecorder` interface), `caarlos0/env`, testify.

**Spec:** No separate spec document. Brainstorming classified this as a **bounded** change (a cache layer over two existing lookups in one service), so the design lives in the "Design" section below and this plan argues from it.

---

## Design

### What is cached

| `MongoStore` method | Policy | Why |
|---|---|---|
| `UsersByAccounts` | per-account `HRUser`, LRU+TTL | Account-keyed, highly repeatable across searches; HR names change rarely |
| `AppsByAssistantNames` | per-bot-account `AppRef`, LRU+TTL | Same shape; app names change only on rename |
| `SubscriptionsByRoomIDs` | pass through | Per-caller AND per-room, and `isSubscribed` is volatile |
| `SearchAppsByName` | pass through | A paged text query, not a keyed lookup |

### Semantics

- **Batch-aware.** Each call partitions the requested keys into cached and uncached, issues one Mongo query for the uncached remainder only, then merges. All keys cached ⇒ zero round-trips.
- **Negative caching.** A key the store did not return is cached as a tombstone at the same TTL, so a permanently-absent account (departed user, misconfigured bot) cannot force a query on every search. Same policy as `pkg/preview/appname.go`. Cost: a newly-created user/app is invisible to enrichment for up to one TTL, where enrichment falls back to the raw account name — a degraded label, never a failed search.
- **Errors are never cached** and propagate as today; `enrich.go` logs and degrades.
- **Disable switch.** A non-positive size or TTL disables that cache; with both disabled the constructor returns the inner store unwrapped.
- **No singleflight**, unlike `pkg/userstore`. These loads are already batched per request against indexed `$in` lookups, so concurrent misses cost one extra cheap query each rather than a stampede on one hot key. YAGNI until measured.

### Config

Four knobs on `SearchConfig` (`envPrefix:"SEARCH_"`). These are pod-local caches, not a shared Valkey key, so they are declared in the service rather than a `pkg/` `TTLConfig` — no two services can disagree about an entry only one process can see.

| Env var | Default |
|---|---|
| `SEARCH_HR_CACHE_SIZE` | `130000` |
| `SEARCH_HR_CACHE_TTL` | `24h` |
| `SEARCH_APP_CACHE_SIZE` | `1000` |
| `SEARCH_APP_CACHE_TTL` | `24h` |

None of these names collide with `ESConfig`, which shares the `SEARCH_` prefix (`URL`, `BACKEND`, `USERNAME`, `PASSWORD`, `TLS_SKIP_VERIFY`).

### Metrics

`cachemetrics.For("search_hr", "l1")` and `cachemetrics.For("search_app", "l1")`, recorded once per key so `cache_hits_total` reads as "keys served without touching Mongo".

### Out of scope

`search.rooms` serves `roomId/name/roomType/siteId` straight off the spotlight ES index and makes no Mongo call, so there is nothing there to cache today. Because the cache sits in the store layer rather than in `enrich.go`, room search inherits it automatically if it ever starts enriching.

No wire schema changes ⇒ **no `docs/client-api.md` update** in this PR.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root. `search-service` is a flat `package main` directory.
- **No new third-party dependencies.** `github.com/hashicorp/golang-lru/v2 v2.0.7` is already required.
- Run everything through `make` — never raw `go` commands.
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement.
- Lint and tests are enforced by a pre-commit hook. `make sast` is a blocking CI gate.
- `log/slog` only, structured key-value fields; never log tokens or message bodies.
- Wrap errors with context describing what the current function was doing: `fmt.Errorf("short description: %w", err)`. Never return bare `err`.
- Test files stay in `package main` alongside the code; helpers live only in `_test.go`.
- Minimum 80% coverage; target 90%+ for this new code.
- `expirable.LRU` in v2.0.7 has **no `Close()`** — its cleanup goroutine runs for the process lifetime. Construct each cache exactly once, in `main.go`. This matches `pkg/userstore` and `pkg/roommetacache`.

---

### Task 1: Config knobs

**Files:**
- Modify: `search-service/main.go` (the `SearchConfig` struct, ~line 66-73)
- Test: `search-service/config_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `SearchConfig.HRCacheSize int`, `SearchConfig.HRCacheTTL time.Duration`, `SearchConfig.AppCacheSize int`, `SearchConfig.AppCacheTTL time.Duration`, read from `SEARCH_HR_CACHE_SIZE`, `SEARCH_HR_CACHE_TTL`, `SEARCH_APP_CACHE_SIZE`, `SEARCH_APP_CACHE_TTL`. Task 4 passes these into `newCachedMongoStore`.

- [ ] **Step 1: Write the failing test**

Append to `search-service/config_test.go` (the file already has a `setRequiredSearchEnv(t)` helper that seeds every required var — reuse it, do not redefine it):

```go
// The four cache knobs are the operator's only control over enrichment
// staleness and pod memory, so both the name and the default are pinned.
func TestConfig_EnrichmentCacheKnobs(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		setRequiredSearchEnv(t)
		for _, k := range []string{"SEARCH_HR_CACHE_SIZE", "SEARCH_HR_CACHE_TTL", "SEARCH_APP_CACHE_SIZE", "SEARCH_APP_CACHE_TTL"} {
			require.NoError(t, os.Unsetenv(k))
		}

		cfg, err := env.ParseAs[Config]()

		require.NoError(t, err)
		assert.Equal(t, 8192, cfg.Search.HRCacheSize)
		assert.Equal(t, 5*time.Minute, cfg.Search.HRCacheTTL)
		assert.Equal(t, 1024, cfg.Search.AppCacheSize)
		assert.Equal(t, 5*time.Minute, cfg.Search.AppCacheTTL)
	})

	t.Run("overrides", func(t *testing.T) {
		setRequiredSearchEnv(t)
		t.Setenv("SEARCH_HR_CACHE_SIZE", "16")
		t.Setenv("SEARCH_HR_CACHE_TTL", "90s")
		t.Setenv("SEARCH_APP_CACHE_SIZE", "8")
		t.Setenv("SEARCH_APP_CACHE_TTL", "30s")

		cfg, err := env.ParseAs[Config]()

		require.NoError(t, err)
		assert.Equal(t, 16, cfg.Search.HRCacheSize)
		assert.Equal(t, 90*time.Second, cfg.Search.HRCacheTTL)
		assert.Equal(t, 8, cfg.Search.AppCacheSize)
		assert.Equal(t, 30*time.Second, cfg.Search.AppCacheTTL)
	})

	// Zero is the documented disable switch, not a parse error — Task 3's
	// constructor turns it into a pass-through store.
	t.Run("zero disables", func(t *testing.T) {
		setRequiredSearchEnv(t)
		t.Setenv("SEARCH_HR_CACHE_SIZE", "0")
		t.Setenv("SEARCH_APP_CACHE_TTL", "0s")

		cfg, err := env.ParseAs[Config]()

		require.NoError(t, err)
		assert.Equal(t, 0, cfg.Search.HRCacheSize)
		assert.Equal(t, time.Duration(0), cfg.Search.AppCacheTTL)
	})
}
```

Add `"time"` to the import block of `config_test.go` if it is not already there.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-service`
Expected: FAIL — compile error `cfg.Search.HRCacheSize undefined (type SearchConfig has no field or method HRCacheSize)`.

- [ ] **Step 3: Write minimal implementation**

In `search-service/main.go`, add the four fields to `SearchConfig`, keeping the existing tag alignment style:

```go
type SearchConfig struct {
	DocCounts               int           `env:"DOC_COUNTS"                 envDefault:"25"`
	MaxDocCounts            int           `env:"MAX_DOC_COUNTS"             envDefault:"100"`
	RestrictedRoomsCacheTTL time.Duration `env:"RESTRICTED_ROOMS_CACHE_TTL" envDefault:"5m"`
	RecentWindow            time.Duration `env:"RECENT_WINDOW"              envDefault:"8760h"`
	RequestTimeout          time.Duration `env:"REQUEST_TIMEOUT"            envDefault:"10s"`
	HealthAddr              string        `env:"HEALTH_ADDR"                envDefault:":9090"`
	// HR/App cache knobs size the pod-local L1 caches fronting the enrichment
	// lookups in enrich.go. A non-positive size or TTL disables that cache.
	// The TTL is the worst-case staleness of an HR name or an app name in a
	// search result, and the worst-case delay before a newly-created user or
	// app stops rendering as a bare account name.
	HRCacheSize  int           `env:"HR_CACHE_SIZE"              envDefault:"8192"`
	HRCacheTTL   time.Duration `env:"HR_CACHE_TTL"               envDefault:"5m"`
	AppCacheSize int           `env:"APP_CACHE_SIZE"             envDefault:"1024"`
	AppCacheTTL  time.Duration `env:"APP_CACHE_TTL"              envDefault:"5m"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=search-service`
Expected: PASS, including `TestConfig_EnrichmentCacheKnobs`.

- [ ] **Step 5: Commit**

```bash
git add search-service/main.go search-service/config_test.go
git commit -m "feat(search-service): add HR/App enrichment cache config knobs"
```

---

### Task 2: Cache mechanics — `cacheEntry`, `newEntryLRU`, `lookupCached`

**Files:**
- Create: `search-service/store_cache.go`
- Test: `search-service/store_cache_test.go`

**Interfaces:**
- Consumes: `HRUser` and `AppRef` from `search-service/store.go`.
- Produces, for Task 3:
  - `type cacheEntry[T any] struct { val T; found bool }`
  - `func newEntryLRU[T any](size int, ttl time.Duration) *lru.LRU[string, cacheEntry[T]]` — returns `nil` when disabled.
  - `type cacheRecorder = valkeyutil.CacheRecorder` — the `Hit/Miss/Error(ctx)` interface.
  - `func lookupCached[T any](ctx context.Context, c *lru.LRU[string, cacheEntry[T]], rec cacheRecorder, keys []string, load func(context.Context, []string) (map[string]T, error)) (map[string]T, error)`

This task delivers only the cache mechanics, tested directly. Task 3 mounts them on the store.

- [ ] **Step 1: Write the failing test**

Create `search-service/store_cache_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-service`
Expected: FAIL — compile errors `undefined: newEntryLRU`, `undefined: lookupCached`.

- [ ] **Step 3: Write minimal implementation**

Create `search-service/store_cache.go`:

```go
package main

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// cacheRecorder records the outcome of an L1 lookup. An alias of
// valkeyutil.CacheRecorder: every cache tier in this repo records against one
// interface, and cachemetrics.Recorder satisfies it.
type cacheRecorder = valkeyutil.CacheRecorder

// cacheEntry is one cached lookup outcome. found=false is a tombstone — "this key
// has no row" is a stable answer, and not caching it would leave a single
// departed account or misconfigured bot re-querying on every search it appears
// in, which is the exact case the cache exists for.
type cacheEntry[T any] struct {
	val   T
	found bool
}

// newEntryLRU builds the LRU for one cached tier, or returns nil when the tier
// is disabled by a non-positive size or TTL. Callers treat a nil cache as an
// unconditional miss, so disabling a tier costs performance and nothing else.
//
// expirable.LRU has no Close in v2.0.7 and its reaper goroutine runs for the
// process lifetime, so build these once at startup — never per request.
func newEntryLRU[T any](size int, ttl time.Duration) *lru.LRU[string, cacheEntry[T]] {
	if size <= 0 || ttl <= 0 {
		return nil
	}
	return lru.NewLRU[string, cacheEntry[T]](size, nil, ttl)
}

// lookupCached serves what it can from c, forwards the remaining keys to load
// as a single batch, and caches every answer including absences. Duplicate
// keys are collapsed. A load error is returned unwrapped and nothing is
// cached, so an outage cannot mint tombstones.
func lookupCached[T any](
	ctx context.Context,
	c *lru.LRU[string, cacheEntry[T]],
	rec cacheRecorder,
	keys []string,
	load func(context.Context, []string) (map[string]T, error),
) (map[string]T, error) {
	if c == nil {
		return load(ctx, keys)
	}

	out := make(map[string]T, len(keys))
	seen := make(map[string]struct{}, len(keys))
	var missing []string
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		if e, ok := c.Get(k); ok {
			rec.Hit(ctx)
			if e.found {
				out[k] = e.val
			}
			continue
		}
		rec.Miss(ctx)
		missing = append(missing, k)
	}
	if len(missing) == 0 {
		return out, nil
	}

	loaded, err := load(ctx, missing)
	if err != nil {
		return nil, err
	}
	for _, k := range missing {
		v, found := loaded[k]
		c.Add(k, cacheEntry[T]{val: v, found: found})
		if found {
			out[k] = v
		}
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=search-service`
Expected: PASS — all `TestLookupCached_*` and `TestNewEntryLRU_*` cases green, no race warnings.

- [ ] **Step 5: Lint and commit**

```bash
make fmt
make lint
git add search-service/store_cache.go search-service/store_cache_test.go
git commit -m "feat(search-service): add batch-aware LRU+TTL lookup helper with negative caching"
```

---

### Task 3: `cachedMongoStore` decorator

**Files:**
- Modify: `search-service/store_cache.go`
- Test: `search-service/store_cache_test.go`

**Interfaces:**
- Consumes: `cacheEntry`, `newEntryLRU`, `lookupCached`, `cacheRecorder` (Task 2); `MongoStore`, `HRUser`, `AppRef` from `store.go`.
- Produces, for Task 4:
  - `type cacheConfig struct { HRSize int; HRTTL time.Duration; AppSize int; AppTTL time.Duration }`
  - `func newCachedMongoStore(inner MongoStore, cfg cacheConfig) MongoStore` — returns `inner` unchanged when both tiers are disabled.

- [ ] **Step 1: Write the failing test**

Append to `search-service/store_cache_test.go`:

```go
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
	var _ MongoStore = newCachedMongoStore(&cachedTestMongo{}, enabledCacheConfig())
}
```

Add `"github.com/hmchangw/chat/pkg/model"` to the imports of `store_cache_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-service`
Expected: FAIL — compile errors `undefined: cacheConfig`, `undefined: newCachedMongoStore`.

- [ ] **Step 3: Write minimal implementation**

Append to `search-service/store_cache.go`, and add `"fmt"` plus `"github.com/hmchangw/chat/pkg/cachemetrics"` to its imports:

```go
// cacheConfig sizes the two enrichment tiers. A non-positive size or TTL
// disables that tier.
type cacheConfig struct {
	HRSize  int
	HRTTL   time.Duration
	AppSize int
	AppTTL  time.Duration
}

// cachedMongoStore fronts the two account-keyed enrichment lookups with
// pod-local LRU+TTL caches. The embedded MongoStore carries the rest
// unchanged: SubscriptionsByRoomIDs is per-caller and its isSubscribed flag is
// volatile, and SearchAppsByName is a paged text query, so neither is
// cacheable by key.
//
// Entries are pod-local and TTL-bounced, so a rename is visible to one pod up
// to a TTL after another — enrichment renders a display name, not a decision
// input, so a brief disagreement costs a stale label and nothing else.
type cachedMongoStore struct {
	MongoStore
	users   *lru.LRU[string, cacheEntry[HRUser]]
	apps    *lru.LRU[string, cacheEntry[AppRef]]
	userMet cacheRecorder
	appMet  cacheRecorder
}

// newCachedMongoStore wraps inner, or returns it untouched when both tiers are
// disabled — an operator turning the cache off gets the original store, not a
// decorator that forwards every call.
func newCachedMongoStore(inner MongoStore, cfg cacheConfig) MongoStore {
	users := newEntryLRU[HRUser](cfg.HRSize, cfg.HRTTL)
	apps := newEntryLRU[AppRef](cfg.AppSize, cfg.AppTTL)
	if users == nil && apps == nil {
		return inner
	}
	return &cachedMongoStore{
		MongoStore: inner,
		users:      users,
		apps:       apps,
		userMet:    cachemetrics.For("search_hr", "l1"),
		appMet:     cachemetrics.For("search_app", "l1"),
	}
}

// UsersByAccounts serves cached accounts and queries only the remainder.
func (s *cachedMongoStore) UsersByAccounts(ctx context.Context, accounts []string) (map[string]HRUser, error) {
	out, err := lookupCached(ctx, s.users, s.userMet, accounts, s.MongoStore.UsersByAccounts)
	if err != nil {
		return nil, fmt.Errorf("load uncached users: %w", err)
	}
	return out, nil
}

// AppsByAssistantNames serves cached bot accounts and queries only the remainder.
func (s *cachedMongoStore) AppsByAssistantNames(ctx context.Context, botAccounts []string) (map[string]AppRef, error) {
	out, err := lookupCached(ctx, s.apps, s.appMet, botAccounts, s.MongoStore.AppsByAssistantNames)
	if err != nil {
		return nil, fmt.Errorf("load uncached apps: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=search-service`
Expected: PASS — every `TestCachedMongoStore_*` and `TestNewCachedMongoStore_*` case green.

- [ ] **Step 5: Check coverage of the new file**

```bash
go test -race -coverprofile=/tmp/search-cover.out ./search-service/... >/dev/null && \
  go tool cover -func=/tmp/search-cover.out | grep store_cache.go
```

Expected: every function in `store_cache.go` at or above 80%; `lookupCached`, `newCachedMongoStore`, `UsersByAccounts` and `AppsByAssistantNames` at 100%. If any line is uncovered, add the case that exercises it before committing.

- [ ] **Step 6: Lint and commit**

```bash
make fmt
make lint
git add search-service/store_cache.go search-service/store_cache_test.go
git commit -m "feat(search-service): cache HR and app enrichment lookups behind MongoStore"
```

---

### Task 4: Wire into `main.go` and local compose

**Files:**
- Modify: `search-service/main.go` (the store wiring block, ~lines 224-247)
- Modify: `search-service/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: `SearchConfig.HRCacheSize/HRCacheTTL/AppCacheSize/AppCacheTTL` (Task 1); `cacheConfig`, `newCachedMongoStore` (Task 3).
- Produces: nothing downstream — this is the terminal task.

- [ ] **Step 1: Wire the decorator**

In `search-service/main.go`, `ensureIndexes` must keep running against the **concrete** `mongoStore` before wrapping (the decorator's interface has no such method). Replace the block that today reads:

```go
	ensureCancel()
	handler := newHandler(store, mongoStore, usersClient, cache, &handlerConfig{
```

with:

```go
	ensureCancel()

	// The cache fronts only the account-keyed enrichment lookups; enrich.go
	// sees the same MongoStore either way. Built once here — expirable.LRU's
	// reaper goroutine lives for the process.
	cachedMongo := newCachedMongoStore(mongoStore, cacheConfig{
		HRSize:  cfg.Search.HRCacheSize,
		HRTTL:   cfg.Search.HRCacheTTL,
		AppSize: cfg.Search.AppCacheSize,
		AppTTL:  cfg.Search.AppCacheTTL,
	})
	slog.Info("enrichment caches configured",
		"hr_size", cfg.Search.HRCacheSize, "hr_ttl", cfg.Search.HRCacheTTL,
		"app_size", cfg.Search.AppCacheSize, "app_ttl", cfg.Search.AppCacheTTL)

	handler := newHandler(store, cachedMongo, usersClient, cache, &handlerConfig{
```

Leave the rest of the `newHandler(...)` argument list exactly as it is.

- [ ] **Step 2: Verify it compiles and the suite is still green**

Run: `make build SERVICE=search-service && make test SERVICE=search-service`
Expected: build succeeds; all tests PASS.

- [ ] **Step 3: Expose the knobs in local compose**

In `search-service/deploy/docker-compose.yml`, after the `SEARCH_REQUEST_TIMEOUT` line in the service `environment:` block, add:

```yaml
      - SEARCH_HR_CACHE_SIZE=${SEARCH_HR_CACHE_SIZE:-8192}
      - SEARCH_HR_CACHE_TTL=${SEARCH_HR_CACHE_TTL:-5m}
      - SEARCH_APP_CACHE_SIZE=${SEARCH_APP_CACHE_SIZE:-1024}
      - SEARCH_APP_CACHE_TTL=${SEARCH_APP_CACHE_TTL:-5m}
```

- [ ] **Step 4: Run the full gate**

```bash
make fmt
make lint
make test SERVICE=search-service
make sast
```

Expected: all clean. `make sast` must report no medium-or-above finding attributable to this change.

- [ ] **Step 5: Commit and push**

```bash
git add search-service/main.go search-service/deploy/docker-compose.yml
git commit -m "feat(search-service): wire HR/App enrichment caches into startup"
git push -u origin claude/search-service-lru-caches-sqckz4
```

---

## Verification

Run before declaring the work done — evidence, not assertion:

```bash
make fmt
make lint
make test SERVICE=search-service
make test
make sast
```

Integration tests (`make test-integration SERVICE=search-service`) need Docker and are unaffected by this change — the decorator sits above the store, and `integration_enrich_test.go` drives the concrete `mongoStore` directly.

**Docs:** no `docs/client-api.md` change. No handler registration moved, and no request/response or event struct in `pkg/model` gained, lost, renamed or retyped a field — the caches are entirely internal to how `search-service` sources data it already returns.
