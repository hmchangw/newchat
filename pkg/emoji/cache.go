package emoji

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"
)

// CachedLookup wraps a CustomEmojiLookup with a process-local LRU+TTL cache
// keyed by (siteID, shortcode). Negative results are cached too. Admin
// add/delete becomes visible at most TTL after the change (no active
// invalidation). Mirrors pkg/roommetacache.
type CachedLookup struct {
	inner CustomEmojiLookup
	lru   *lru.LRU[cacheKey, bool]
	sf    singleflight.Group

	hits     atomic.Uint64
	misses   atomic.Uint64
	loadErrs atomic.Uint64
}

type cacheKey struct {
	siteID    string
	shortcode string
}

// String returns the canonical flat form for the singleflight dedup key;
// `\x00` separator is collision-free for ASCII siteIDs and validated shortcodes.
func (k cacheKey) String() string {
	return k.siteID + "\x00" + k.shortcode
}

// CacheStats is a snapshot of a CachedLookup's counters.
type CacheStats struct {
	Hits, Misses, LoadErrors uint64
	Size                     int
}

// NewCachedLookup wraps inner with an LRU+TTL cache; size, ttl, inner all required.
func NewCachedLookup(inner CustomEmojiLookup, size int, ttl time.Duration) (*CachedLookup, error) {
	if inner == nil {
		return nil, fmt.Errorf("emoji cached lookup: inner must not be nil")
	}
	if size <= 0 {
		return nil, fmt.Errorf("emoji cached lookup: size must be positive, got %d", size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("emoji cached lookup: ttl must be positive, got %v", ttl)
	}
	return &CachedLookup{
		inner: inner,
		lru:   lru.NewLRU[cacheKey, bool](size, nil, ttl),
	}, nil
}

// CustomEmojiExists serves from cache; inner errors are not cached.
func (c *CachedLookup) CustomEmojiExists(ctx context.Context, siteID, shortcode string) (bool, error) {
	k := cacheKey{siteID: siteID, shortcode: shortcode}
	if v, ok := c.lru.Get(k); ok {
		c.hits.Add(1)
		return v, nil
	}
	c.misses.Add(1)

	v, err, _ := c.sf.Do(k.String(), func() (interface{}, error) {
		if cached, ok := c.lru.Get(k); ok {
			return cached, nil
		}
		exists, err := c.inner.CustomEmojiExists(ctx, siteID, shortcode)
		if err != nil {
			return false, err
		}
		c.lru.Add(k, exists)
		return exists, nil
	})
	if err != nil {
		c.loadErrs.Add(1)
		return false, fmt.Errorf("custom emoji lookup %q for site %q: %w", shortcode, siteID, err)
	}
	return v.(bool), nil
}

// Stats returns a snapshot of the cache's counters.
func (c *CachedLookup) Stats() CacheStats {
	return CacheStats{
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		LoadErrors: c.loadErrs.Load(),
		Size:       c.lru.Len(),
	}
}

// Invalidate removes the cached entry for (siteID, shortcode); safe on a miss.
func (c *CachedLookup) Invalidate(siteID, shortcode string) {
	c.lru.Remove(cacheKey{siteID: siteID, shortcode: shortcode})
}
