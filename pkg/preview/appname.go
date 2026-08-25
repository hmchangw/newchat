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
	cache := lru.NewLRU[string, string](appNameCacheSize, nil, appNameCacheTTL)
	var sf singleflight.Group

	return func(ctx context.Context, botAccount string) (string, error) {
		if name, ok := cache.Get(botAccount); ok {
			return name, nil
		}
		v, err, _ := sf.Do(botAccount, func() (any, error) {
			return loadAppName(ctx, cache, inner, botAccount)
		})
		if err != nil {
			return "", err
		}
		name, _ := v.(string)
		return name, nil
	}
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
