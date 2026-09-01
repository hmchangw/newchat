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
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/pkg/cachemetrics"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
)

// fetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const fetchTimeout = 10 * time.Second

// Recorder records the outcome of a cache lookup. cachemetrics.Recorder
// satisfies it; tests substitute a spy.
type Recorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

// ttlCache is an LRU+TTL cache whose misses are deduped via singleflight.
type ttlCache[V any] struct {
	lru     *lru.LRU[string, V]
	sf      singleflight.Group
	metrics Recorder
}

// newTTLCache builds an LRU+TTL cache. rec records hit/miss/error outcomes;
// pass a cachemetrics.For(name, "l1") so each cache reports its own series.
func newTTLCache[V any](size int, ttl time.Duration, rec Recorder) (*ttlCache[V], error) {
	if size <= 0 {
		return nil, fmt.Errorf("readcache: size must be positive, got %d", size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("readcache: ttl must be positive, got %v", ttl)
	}
	return &ttlCache[V]{lru: lru.NewLRU[string, V](size, nil, ttl), metrics: rec}, nil
}

// getOrLoad returns the cached value for key, or invokes load on miss. load
// returns (value, store, err): when store is false the value is returned to
// the caller but not cached (used for negative results); when err is non-nil
// nothing is cached and the error is returned.
// If ctx is canceled before the shared load finishes, getOrLoad returns ctx.Err() immediately
// while the load continues detached in the background, bounded by fetchTimeout.
// remove drops key if present; absent keys are a no-op.
func (c *ttlCache[V]) remove(key string) { c.lru.Remove(key) }

func (c *ttlCache[V]) getOrLoad(ctx context.Context, key string, load func(context.Context) (V, bool, error)) (V, error) {
	if v, ok := c.lru.Get(key); ok {
		c.metrics.Hit(ctx)
		return v, nil
	}

	resCh := c.sf.DoChan(key, func() (any, error) {
		// Re-check under singleflight in case a sibling populated the entry.
		if cached, ok := c.lru.Get(key); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		val, store, err := load(fetchCtx)
		if err != nil {
			return val, err
		}
		if store {
			c.lru.Add(key, val)
		}
		return val, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			var zero V
			return zero, res.Err
		}
		c.metrics.Miss(ctx)
		return res.Val.(V), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		var zero V
		return zero, ctx.Err()
	}
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

// SubscriptionCache caches positive subscription access checks (L1) in front of
// a SubscriptionSource. The shared Valkey L2 and its circuit breaker live inside
// that source, not here — history-service's base source already resolves through
// subauthcache.ReadThrough — so this type stays a plain L1 with one loader path.
type SubscriptionCache struct {
	inner SubscriptionSource
	cache *ttlCache[subEntry]
}

// NewSubscriptionCache wraps inner with an LRU+TTL cache of size entries and
// the given TTL. size and ttl must be positive.
func NewSubscriptionCache(inner SubscriptionSource, size int, ttl time.Duration) (*SubscriptionCache, error) {
	cache, err := newTTLCache[subEntry](size, ttl, cachemetrics.For("history_sub", "l1"))
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

// GetSubscription bypasses the access-window cache and delegates to the source, which
// projects (mongorepo.subscriptionReadProjection) — read a new field, widen that first.
func (c *SubscriptionCache) GetSubscription(ctx context.Context, account, roomID string) (*pkgmodel.Subscription, error) {
	return c.inner.GetSubscription(ctx, account, roomID)
}

// RoomSource is the room metadata reads the cache fronts.
type RoomSource interface {
	GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error)
	GetRoomTimesByIDs(ctx context.Context, ids []string) (map[string]mongorepo.RoomTimes, error)
	GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error)
	GetRoomUserCount(ctx context.Context, roomID string) (int, error)
	SetPreviewMessage(ctx context.Context, roomID string, pvw pkgmodel.PreviewMessage, forMsgID string, asOf int64) error
	UpdatePreviewBody(ctx context.Context, roomID string, pvw pkgmodel.PreviewMessage, forMsgID string, asOf int64) (bool, error)
	ClearPreview(ctx context.Context, roomID string, asOf int64) (bool, error)
	InvalidatePreviewKey(ctx context.Context, roomID, msgID string, asOf int64) error
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
	times, err := newTTLCache[roomTimes](size, ttl, cachemetrics.For("history_room_times", "l1"))
	if err != nil {
		return nil, err
	}
	minSeen, err := newTTLCache[*time.Time](size, ttl, cachemetrics.For("history_room_min_seen", "l1"))
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

// GetRoomUserCount bypasses the cache and delegates to the source. The
// large-room pin check needs the live member count, not a cached one.
func (c *RoomCache) GetRoomUserCount(ctx context.Context, roomID string) (int, error) {
	return c.inner.GetRoomUserCount(ctx, roomID)
}

// GetRoomTimesByIDs bypasses the per-key cache and delegates to the source.
// It is called at most once per RoomsGet request — not a hot single-room path —
// so there is no per-room caching benefit to justify the bookkeeping. Caching it
// would also be wrong now that it carries the stored preview: the freshness check
// is an identity comparison against a lastMsgId that must be read live.
func (c *RoomCache) GetRoomTimesByIDs(ctx context.Context, ids []string) (map[string]mongorepo.RoomTimes, error) {
	return c.inner.GetRoomTimesByIDs(ctx, ids)
}

// SetPreviewMessage bypasses the cache and delegates to the source — a write,
// not a read this cache fronts.
//
//nolint:gocritic // hugeParam: pvw's by-value shape is the RoomSource contract this passes through unchanged.
func (c *RoomCache) SetPreviewMessage(ctx context.Context, roomID string, pvw pkgmodel.PreviewMessage, forMsgID string, asOf int64) error {
	return c.inner.SetPreviewMessage(ctx, roomID, pvw, forMsgID, asOf)
}

// UpdatePreviewBody bypasses the cache and delegates to the source — a write,
// not a read this cache fronts.
//
//nolint:gocritic // hugeParam: pvw's by-value shape is the RoomSource contract this passes through unchanged.
func (c *RoomCache) UpdatePreviewBody(ctx context.Context, roomID string, pvw pkgmodel.PreviewMessage, forMsgID string, asOf int64) (bool, error) {
	return c.inner.UpdatePreviewBody(ctx, roomID, pvw, forMsgID, asOf)
}

// ClearPreview bypasses the cache and delegates to the source — a write, not a
// read this cache fronts.
func (c *RoomCache) ClearPreview(ctx context.Context, roomID string, asOf int64) (bool, error) {
	return c.inner.ClearPreview(ctx, roomID, asOf)
}

// InvalidatePreviewKey bypasses the cache and delegates to the source — a write,
// not a read this cache fronts.
func (c *RoomCache) InvalidatePreviewKey(ctx context.Context, roomID, msgID string, asOf int64) error {
	return c.inner.InvalidatePreviewKey(ctx, roomID, msgID, asOf)
}

// previewEntry is the cached resolved room preview. found=false is never stored
// (positives-only), so an empty room / read miss re-resolves each time — cheap
// after the single-query preview walk, and it keeps stale "no preview" out of
// the cache.
type previewEntry struct {
	preview pkgmodel.PreviewMessage
	found   bool
}

// PreviewCache caches the resolved last-eligible preview message per room.
// lastMsgAt advances on every message, so the configured TTL bounds how stale a
// preview can be; the room list also carries lastMsgAt and clients update open
// rooms from real-time delivery. Positives-only, singleflight-deduped.
type PreviewCache struct {
	cache *ttlCache[previewEntry]
}

// Invalidate drops roomID's cached preview. A mutation changes what the room previews,
// and the entry it would otherwise keep serving describes the message that changed --
// including one that was just deleted. Local only: a sibling replica's copy lives out
// its TTL, which is why #292 wants revision-keyed identity rather than eviction.
func (c *PreviewCache) Invalidate(roomID string) {
	c.cache.remove(roomID)
}

// NewPreviewCache builds a preview cache of size entries with the given TTL.
// size and ttl must be positive.
func NewPreviewCache(size int, ttl time.Duration) (*PreviewCache, error) {
	c, err := newTTLCache[previewEntry](size, ttl, cachemetrics.For("history_room_preview", "l1"))
	if err != nil {
		return nil, fmt.Errorf("create preview cache: %w", err)
	}
	return &PreviewCache{cache: c}, nil
}

// Get returns the cached preview for roomID or invokes load on miss. Only a
// found preview is cached; a not-found result (empty room) and errors are
// returned to the caller but not stored.
func (c *PreviewCache) Get(ctx context.Context, roomID string, load func(context.Context) (pkgmodel.PreviewMessage, bool, error)) (pkgmodel.PreviewMessage, bool, error) {
	entry, err := c.cache.getOrLoad(ctx, roomID, func(ctx context.Context) (previewEntry, bool, error) {
		p, found, err := load(ctx)
		if err != nil {
			return previewEntry{}, false, err
		}
		return previewEntry{preview: p, found: found}, found, nil
	})
	if err != nil {
		return pkgmodel.PreviewMessage{}, false, err
	}
	return entry.preview, entry.found, nil
}
