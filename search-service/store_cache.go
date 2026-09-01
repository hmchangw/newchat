package main

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/hmchangw/chat/pkg/cachemetrics"
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

// minCacheTTL is the smallest TTL a tier may be configured with. expirable.LRU
// derives its reaper interval as ttl/100 and calls time.NewTicker with it, so
// any TTL under 100ns yields NewTicker(0), which panics in a goroutine nothing
// recovers — an operator typo would take the process down. The floor is set far
// above that boundary because a sub-second reaper tick is pathological anyway.
// Config validation rejects violations at startup; see SearchConfig.Validate.
const minCacheTTL = time.Second

// newEntryLRU builds the LRU for one cached tier, or returns nil when the tier
// is disabled by a non-positive size or TTL. A nil cache bypasses the tier
// entirely in lookupCached and records nothing, so disabling a tier costs
// performance and nothing else.
//
// A positive TTL below minCacheTTL never reaches here — SearchConfig.Validate
// fails startup first, so this cannot silently clamp a misconfiguration.
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
		missing = append(missing, k)
	}
	if len(missing) == 0 {
		return out, nil
	}

	// Miss vs Error is decided by the load, not by the cache lookup: a key that
	// fell through is only a clean absence once the backing store answered.
	// pkg/userstore and pkg/roommetacache record the same way, so one Grafana
	// panel reads every tier alike — a Mongo outage must surface as errors here
	// too, not as a tier reporting a 0% hit rate and no errors at all.
	loaded, err := load(ctx, missing)
	if err != nil {
		for range missing {
			rec.Error(ctx)
		}
		return nil, err
	}
	for range missing {
		rec.Miss(ctx)
	}
	for _, k := range missing {
		v, found := loaded[k]
		if !found {
			// Our load started before a concurrent one that has since cached a
			// positive value — the row was created while our query was in
			// flight. Writing this tombstone over it would render a bare
			// account name for a whole TTL (a day at the default) even though
			// this pod has already seen the row. Outside that race the key is
			// absent or a tombstone, so this check no-ops.
			if e, live := c.Get(k); live && e.found {
				continue
			}
		}
		c.Add(k, cacheEntry[T]{val: v, found: found})
		if found {
			out[k] = v
		}
	}
	return out, nil
}

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
// Entries are pod-local and TTL-bounded, so a rename is visible to one pod up
// to a TTL after another — enrichment renders a display name, not a decision
// input, so a brief disagreement costs a stale label and nothing else.
type cachedMongoStore struct {
	MongoStore
	users   *lru.LRU[string, cacheEntry[HRUser]]
	apps    *lru.LRU[string, cacheEntry[AppRef]]
	userMet cacheRecorder
	appMet  cacheRecorder
}

// The decorator satisfies the interface enrich.go consumes, so the cache is
// invisible to the enrichment path. Checked at package scope rather than a
// test: embedding MongoStore already satisfies this unconditionally, so the
// value here is compile-time documentation, not a test that can fail.
var _ MongoStore = (*cachedMongoStore)(nil)

// newCachedMongoStore wraps inner, or returns it untouched when both tiers are
// disabled — an operator turning the cache off gets the original store, not a
// decorator that forwards every call.
//
// A nil inner is passed through as nil rather than wrapped: enrich.go treats
// a nil MongoStore as "enrichment disabled" (h.mongo == nil), and wrapping it
// would produce a non-nil MongoStore whose embedded nil interface panics on
// the first call — defeating that check silently.
func newCachedMongoStore(inner MongoStore, cfg cacheConfig) MongoStore {
	if inner == nil {
		return nil
	}
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
