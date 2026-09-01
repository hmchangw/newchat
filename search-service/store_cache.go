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
