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

// newEntryLRU builds the LRU for one cached tier, or returns nil when the tier
// is disabled by a non-positive size or TTL. A nil cache bypasses the tier
// entirely in lookupCached and records nothing, so disabling a tier costs
// performance and nothing else.
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
