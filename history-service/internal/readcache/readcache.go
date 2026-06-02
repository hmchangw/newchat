// Package readcache provides process-local LRU+TTL caches for the per-request
// MongoDB lookups on the history-service read hot path: the subscription
// access check, room times (lastMsgAt/createdAt), and minUserLastSeenAt.
//
// Freshness is TTL-bounded; there is no active invalidation. Following the
// message-gatekeeper precedent, only positive subscriptions are cached —
// "not subscribed" results and errors are never stored, so revoked access
// goes stale by at most the subscription TTL and the negative path stays
// always-fresh. Loads are deduped with singleflight.
package readcache

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"

	pkgmodel "github.com/hmchangw/chat/pkg/model"
)

// Stats is a snapshot of a cache's counters.
type Stats struct {
	Hits, Misses uint64
	Size         int
}

// ttlCache is an LRU+TTL cache whose misses are deduped via singleflight.
type ttlCache[V any] struct {
	lru    *lru.LRU[string, V]
	sf     singleflight.Group
	hits   atomic.Uint64
	misses atomic.Uint64
}

func newTTLCache[V any](size int, ttl time.Duration) (*ttlCache[V], error) {
	if size <= 0 {
		return nil, fmt.Errorf("readcache: size must be positive, got %d", size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("readcache: ttl must be positive, got %v", ttl)
	}
	return &ttlCache[V]{lru: lru.NewLRU[string, V](size, nil, ttl)}, nil
}

// getOrLoad returns the cached value for key, or invokes load on miss. load
// returns (value, store, err): when store is false the value is returned to
// the caller but not cached (used for negative results); when err is non-nil
// nothing is cached and the error is returned.
func (c *ttlCache[V]) getOrLoad(ctx context.Context, key string, load func(context.Context) (V, bool, error)) (V, error) {
	if v, ok := c.lru.Get(key); ok {
		c.hits.Add(1)
		return v, nil
	}
	c.misses.Add(1)

	v, err, _ := c.sf.Do(key, func() (any, error) {
		// Re-check under singleflight in case a sibling populated the entry.
		if cached, ok := c.lru.Get(key); ok {
			return cached, nil
		}
		val, store, err := load(ctx)
		if err != nil {
			return val, err
		}
		if store {
			c.lru.Add(key, val)
		}
		return val, nil
	})
	if err != nil {
		var zero V
		return zero, err
	}
	return v.(V), nil
}

func (c *ttlCache[V]) stats() Stats {
	return Stats{Hits: c.hits.Load(), Misses: c.misses.Load(), Size: c.lru.Len()}
}

// SubscriptionSource is the subscription read the cache fronts.
type SubscriptionSource interface {
	GetHistorySharedSince(ctx context.Context, account, roomID string) (*time.Time, bool, error)
	GetSubscription(ctx context.Context, account, roomID string) (*pkgmodel.Subscription, error)
}

type subEntry struct {
	sharedSince *time.Time
	subscribed  bool
}

// SubscriptionCache caches positive subscription access checks.
type SubscriptionCache struct {
	inner SubscriptionSource
	cache *ttlCache[subEntry]
}

// NewSubscriptionCache wraps inner with an LRU+TTL cache of size entries and
// the given TTL. size and ttl must be positive.
func NewSubscriptionCache(inner SubscriptionSource, size int, ttl time.Duration) (*SubscriptionCache, error) {
	cache, err := newTTLCache[subEntry](size, ttl)
	if err != nil {
		return nil, err
	}
	return &SubscriptionCache{inner: inner, cache: cache}, nil
}

// GetHistorySharedSince serves the access check from cache, loading on miss.
// Only subscribed=true results are cached; not-subscribed and errors are not.
func (c *SubscriptionCache) GetHistorySharedSince(ctx context.Context, account, roomID string) (*time.Time, bool, error) {
	key := account + "\x00" + roomID
	entry, err := c.cache.getOrLoad(ctx, key, func(ctx context.Context) (subEntry, bool, error) {
		ss, subscribed, err := c.inner.GetHistorySharedSince(ctx, account, roomID)
		if err != nil {
			return subEntry{}, false, err
		}
		return subEntry{sharedSince: ss, subscribed: subscribed}, subscribed, nil
	})
	if err != nil {
		return nil, false, err
	}
	return entry.sharedSince, entry.subscribed, nil
}

// Stats returns the subscription cache counters.
func (c *SubscriptionCache) Stats() Stats { return c.cache.stats() }

// GetSubscription bypasses the access-window cache and delegates to the
// underlying source. Pin/unpin paths need the full subscription doc (roles,
// account) which we don't cache.
func (c *SubscriptionCache) GetSubscription(ctx context.Context, account, roomID string) (*pkgmodel.Subscription, error) {
	return c.inner.GetSubscription(ctx, account, roomID)
}

// RoomSource is the room metadata reads the cache fronts.
type RoomSource interface {
	GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error)
	GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error)
	GetRoomUserCount(ctx context.Context, roomID string) (int, error)
}

type roomTimes struct {
	lastMsgAt time.Time
	createdAt time.Time
}

// RoomCache caches room times and minUserLastSeenAt. lastMsgAt advances on
// every new message, so the configured TTL bounds how stale the walk ceiling
// can be; pair a short TTL with client-supplied room hints.
type RoomCache struct {
	inner   RoomSource
	times   *ttlCache[roomTimes]
	minSeen *ttlCache[*time.Time]
}

// NewRoomCache wraps inner with LRU+TTL caches for room times and
// minUserLastSeenAt. size and ttl must be positive.
func NewRoomCache(inner RoomSource, size int, ttl time.Duration) (*RoomCache, error) {
	times, err := newTTLCache[roomTimes](size, ttl)
	if err != nil {
		return nil, err
	}
	minSeen, err := newTTLCache[*time.Time](size, ttl)
	if err != nil {
		return nil, err
	}
	return &RoomCache{inner: inner, times: times, minSeen: minSeen}, nil
}

// GetRoomTimes serves room times from cache, loading on miss. Errors are not cached.
func (c *RoomCache) GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error) {
	rt, err := c.times.getOrLoad(ctx, roomID, func(ctx context.Context) (roomTimes, bool, error) {
		l, cr, err := c.inner.GetRoomTimes(ctx, roomID)
		if err != nil {
			return roomTimes{}, false, err
		}
		return roomTimes{lastMsgAt: l, createdAt: cr}, true, nil
	})
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return rt.lastMsgAt, rt.createdAt, nil
}

// GetMinUserLastSeenAt serves minUserLastSeenAt from cache, loading on miss. A
// nil result is a valid, cacheable value; errors are not cached.
func (c *RoomCache) GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error) {
	return c.minSeen.getOrLoad(ctx, roomID, func(ctx context.Context) (*time.Time, bool, error) {
		t, err := c.inner.GetMinUserLastSeenAt(ctx, roomID)
		if err != nil {
			return nil, false, err
		}
		return t, true, nil
	})
}

// Stats returns the room-times and min-last-seen cache counters.
func (c *RoomCache) Stats() (times, minSeen Stats) {
	return c.times.stats(), c.minSeen.stats()
}

// GetRoomUserCount bypasses the cache and delegates to the source. The
// large-room pin check needs the live member count, not a cached one.
func (c *RoomCache) GetRoomUserCount(ctx context.Context, roomID string) (int, error) {
	return c.inner.GetRoomUserCount(ctx, roomID)
}
