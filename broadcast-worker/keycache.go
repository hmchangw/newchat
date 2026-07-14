package main

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// cacheRecorder records the outcome of a cache lookup. cachemetrics.Recorder
// satisfies it; tests substitute a spy.
type cacheRecorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

// CachedKeyProvider wraps a RoomKeyProvider with a bounded in-process
// LRU+TTL cache. Concurrent misses on the same roomID are coalesced via
// singleflight so a hot room never triggers more than one in-flight inner
// fetch. The backing store is a size-capped expirable LRU (the same
// pattern as pkg/roommetacache), so memory is bounded by the configured
// size regardless of how many distinct rooms the worker sees over its
// lifetime — cold rooms are evicted by capacity or TTL rather than leaking.
//
// Staleness contract: after a key rotation, this cache keeps returning the
// previous version for up to TTL. That is safe only while TTL stays below
// the roomkeystore grace period — clients still hold the previous key
// during the grace window and can decrypt either version. main enforces
// this with keyCacheTTLSafe before wiring the cache.
//
// Negative results (nil, nil — meaning the room has no provisioned key)
// are deliberately not cached, so a freshly-provisioned key is picked up
// on the next call rather than being shadowed for up to TTL.
//
// No Valkey L2 is layered beneath this cache (unlike pkg/roommetacache).
// Two reasons: the key is the single source of truth in the room's Mongo
// document (#285), so a second key store would reintroduce the cross-store
// drift that migration removed; and a stale key is not cosmetic like stale
// room metadata — it fails decryption, so the fail-open, best-effort
// invalidation an L2 tier relies on is unsafe for key material. Cold-start
// repopulation is cheap regardless: keys load lazily as rooms are first
// seen, each a singleflight-coalesced _id point-read of a ~40-byte field.
type CachedKeyProvider struct {
	inner RoomKeyProvider
	lru   *lru.LRU[string, *roomkeystore.VersionedKeyPair]
	sf    singleflight.Group

	metrics cacheRecorder
}

// NewCachedKeyProvider returns a cache wrapping inner with the given
// capacity and TTL. size and ttl must both be positive; main wires the
// cache only when both are configured > 0.
func NewCachedKeyProvider(inner RoomKeyProvider, size int, ttl time.Duration) *CachedKeyProvider {
	return &CachedKeyProvider{
		inner:   inner,
		lru:     lru.NewLRU[string, *roomkeystore.VersionedKeyPair](size, nil, ttl),
		metrics: cachemetrics.For("roomkey", "l1"),
	}
}

// Get returns the room's current key, serving from cache when fresh and
// falling through to the inner store on miss or expiry.
//
// The shared fetch detaches from the caller's cancellation so that a
// per-caller cancel does not abort the in-flight inner Get for other
// callers waiting on the same room. Each caller still observes its own
// ctx.Done() via the select below and can give up independently.
func (c *CachedKeyProvider) Get(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error) {
	if key, ok := c.lru.Get(roomID); ok {
		c.metrics.Hit(ctx)
		return key, nil
	}

	resCh := c.sf.DoChan(roomID, func() (any, error) {
		// Re-check under the singleflight gate: another flight may have
		// just populated the cache for this room between our initial
		// miss and this point.
		if key, ok := c.lru.Get(roomID); ok {
			return key, nil
		}
		// WithoutCancel preserves trace/log values but strips cancellation
		// so one caller's ctx cancel cannot poison the herd.
		fetchCtx := context.WithoutCancel(ctx)
		key, err := c.inner.Get(fetchCtx, roomID)
		if err != nil {
			return nil, fmt.Errorf("get room key for %q: %w", roomID, err)
		}
		if key == nil {
			return nil, nil
		}
		c.lru.Add(roomID, key)
		return key, nil
	})

	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			return nil, res.Err
		}
		// A nil key (room has no provisioned key) is a clean, successful
		// fall-through — count it as a miss, not an error.
		c.metrics.Miss(ctx)
		if res.Val == nil {
			return nil, nil
		}
		return res.Val.(*roomkeystore.VersionedKeyPair), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return nil, ctx.Err()
	}
}

// keyCacheTTLSafe reports whether a cache TTL is safe to use given the key
// rotation grace period. Serving a cached key for longer than the grace
// period risks handing out a version the store has already retired from its
// previous-key slot, which clients can no longer decrypt. The cache TTL
// must therefore stay strictly below the grace period (and be positive).
func keyCacheTTLSafe(ttl, grace time.Duration) bool {
	return ttl > 0 && ttl < grace
}
