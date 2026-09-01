package preview

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"
)

// An app's display name changes about as often as the app is renamed, so the TTL is
// generous: a few minutes of a stale name in a room-list row is not a correctness
// concern, and the read it replaces sits on the message fan-out path.
const (
	appNameCacheSize = 1024
	appNameCacheTTL  = 5 * time.Minute
)

// CachedAppNameLookup wraps inner with a bounded TTL cache and singleflight, so repeated
// messages from one bot cost one read per TTL rather than one per message, and a cold key
// under concurrent traffic collapses to one read instead of one per goroutine (#366).
//
// A miss is cached as the empty name it is: "this bot account has no app row" is a stable
// answer, and not caching it would leave one misconfigured bot re-reading on every message
// it ever sends — the exact case the cache exists for.
//
// Errors are not cached, and the load runs on the caller's context so it stays inside the
// seal's budget. A cancelled leader fails its followers too; they are all under that same
// budget, and the caller degrades to the composed display name for the message either way.
func CachedAppNameLookup(inner AppNameLookup) AppNameLookup {
	if inner == nil {
		return nil
	}
	return NewAppNameCache(inner, nil).Name
}

// AppNameCache resolves bot app names one at a time or many at once over a SINGLE
// shared cache, so a name either half fetches is free to the other.
//
// Both halves degrade rather than fail: a nil inner resolves nothing, and callers
// fall back to the composed display name.
type AppNameCache struct {
	cache *lru.LRU[string, string]
	sf    singleflight.Group
	one   AppNameLookup
	many  AppNamesLookup
}

// NewAppNameCache builds the shared cache. Either inner may be nil when a caller
// only needs the other half.
func NewAppNameCache(one AppNameLookup, many AppNamesLookup) *AppNameCache {
	return &AppNameCache{
		cache: lru.NewLRU[string, string](appNameCacheSize, nil, appNameCacheTTL),
		one:   one,
		many:  many,
	}
}

// Name resolves one bot account, collapsing concurrent cold reads of the same key.
func (c *AppNameCache) Name(ctx context.Context, botAccount string) (string, error) {
	if c.one == nil {
		return "", nil
	}
	if name, ok := c.cache.Get(botAccount); ok {
		return name, nil
	}
	v, err, _ := c.sf.Do(botAccount, func() (any, error) {
		return loadAppName(ctx, c.cache, c.one, botAccount)
	})
	if err != nil {
		return "", err
	}
	name, _ := v.(string)
	return name, nil
}

// Names resolves many bot accounts, reading ONLY the ones the cache cannot answer —
// so a page of warm bots costs nothing and a page of cold ones costs one read.
//
// On a failed fetch it returns the cache hits it already gathered alongside the
// error: a partial answer renders more names than none, and the caller degrades the
// rest to their composed names either way. Nothing from a failure is cached, so the
// next call retries those accounts.
//
// Deliberately not singleflighted: the key is a set rather than an account, and this
// half serves page reads rather than the per-message fan-out the single half sits on.
// Concurrent pages sharing a cold bot cost one read each, then warm the cache.
func (c *AppNameCache) Names(ctx context.Context, botAccounts []string) (map[string]string, error) {
	names := make(map[string]string, len(botAccounts))
	if len(botAccounts) == 0 || c.many == nil {
		return names, nil
	}

	var misses []string
	for _, account := range botAccounts {
		name, ok := c.cache.Get(account)
		if !ok {
			misses = append(misses, account)
			continue
		}
		if name != "" {
			names[account] = name
		}
	}
	if len(misses) == 0 {
		return names, nil
	}

	fetched, err := c.many(ctx, misses)
	if err != nil {
		return names, err
	}
	for _, account := range misses {
		// An account absent from the result has no app — a stable answer, cached as
		// the empty name it is so one misconfigured bot stops re-reading.
		name := fetched[account]
		c.cache.Add(account, name)
		if name != "" {
			names[account] = name
		}
	}
	return names, nil
}

// loadAppName is the singleflight leader's body, split out so the recheck below is
// testable without driving an interleaving from outside.
//
// It rechecks the cache because the caller's check and its sf.Do are separate steps: a
// caller can miss the outer check while a load is in flight, then reach Do after that
// load finished and released the key, which makes it a NEW leader for an answer already
// cached. Without this it would read again — the redundant read the cache exists to stop.
func loadAppName(ctx context.Context, cache *lru.LRU[string, string], inner AppNameLookup, botAccount string) (any, error) {
	if name, ok := cache.Get(botAccount); ok {
		return name, nil
	}
	name, err := inner(ctx, botAccount)
	if err != nil {
		return "", err
	}
	cache.Add(botAccount, name)
	return name, nil
}
