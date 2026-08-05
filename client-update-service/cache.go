package main

import (
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"
)

// cachedBlob is a fully-read artifact held in memory for fast re-serving.
type cachedBlob struct {
	body        []byte
	contentType string
}

// blobCache fronts version downloads with a size+TTL-bounded LRU and singleflight.
// A nil lru means the cache is disabled (get always misses, add/remove no-op).
//
// gen is an invalidation generation bumped on every remove. loadCacheable captures
// it before opening the object and refuses to store the loaded blob if it changed
// meanwhile — so an upload's Put+remove that races an in-flight download fill can
// never restore the pre-upload artifact into the cache.
type blobCache struct {
	lru            *lru.LRU[string, cachedBlob]
	sf             singleflight.Group
	maxObjectBytes int64
	gen            atomic.Uint64
}

// enabled reports whether caching is on. When off, callers must not buffer objects.
func (c *blobCache) enabled() bool { return c.lru != nil }

// newBlobCache builds a cache bounded to maxEntries LRU slots and a per-entry ttl.
// Objects larger than maxObjectBytes are never stored. maxEntries<=0 or ttl<=0
// disables caching entirely.
func newBlobCache(maxEntries int, ttl time.Duration, maxObjectBytes int64) *blobCache {
	c := &blobCache{maxObjectBytes: maxObjectBytes}
	if maxEntries > 0 && ttl > 0 {
		c.lru = lru.NewLRU[string, cachedBlob](maxEntries, nil, ttl)
	}
	return c
}

func (c *blobCache) get(key string) (cachedBlob, bool) {
	if c.lru == nil {
		return cachedBlob{}, false
	}
	return c.lru.Get(key)
}

func (c *blobCache) add(key string, b cachedBlob) {
	if c.lru == nil || int64(len(b.body)) > c.maxObjectBytes {
		return
	}
	c.lru.Add(key, b)
}

func (c *blobCache) remove(key string) {
	// Bump the generation even when disabled is cheap and keeps loadCacheable's
	// invalidation check correct regardless of cache state.
	c.gen.Add(1)
	if c.lru == nil {
		return
	}
	c.lru.Remove(key)
}

// loadCacheable collapses concurrent misses for key via singleflight. loader opens
// the object and returns (blob, cacheable): a cacheable blob is stored and shared
// with all waiters; a non-cacheable result (object over maxObjectBytes) is returned
// but never stored, so its callers fall back to direct streaming.
func (c *blobCache) loadCacheable(key string, loader func() (cachedBlob, bool, error)) (cachedBlob, bool, error) {
	type result struct {
		blob      cachedBlob
		cacheable bool
	}
	genBefore := c.gen.Load()
	v, err, _ := c.sf.Do(key, func() (any, error) {
		if b, ok := c.get(key); ok {
			return result{blob: b, cacheable: true}, nil
		}
		blob, cacheable, err := loader()
		if err != nil {
			return nil, err
		}
		// Skip the store if an invalidation (upload) happened while we were
		// loading — otherwise a stale body could be revived until TTL expiry.
		if cacheable && c.gen.Load() == genBefore {
			c.add(key, blob)
		}
		return result{blob: blob, cacheable: cacheable}, nil
	})
	if err != nil {
		return cachedBlob{}, false, err
	}
	r := v.(result)
	return r.blob, r.cacheable, nil
}
