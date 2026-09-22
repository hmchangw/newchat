// Package readcache provides process-local LRU+TTL caches for the per-request
// MongoDB lookups on the history-service read hot path: the subscription
// access check, room times (lastMsgAt/createdAt), and minUserLastSeenAt.
//
// Freshness is TTL-bounded; the subscription access cache is additionally
// evicted on membership change (SubscriptionCache.Evict, driven by the
// subscription.update subscription in cmd) so revoked or narrowed access takes
// effect at once rather than after the TTL. Following the message-gatekeeper
// precedent, only positive subscriptions are cached — "not subscribed" results
// and errors are never stored, so the negative path stays always-fresh. Loads
// are deduped with singleflight.
package readcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

// defaultMaxInflight bounds concurrent source loads per cache when a caller
// gives no explicit cap. A burst of distinct-key misses would otherwise spawn a
// goroutine + fetch context each, so this caps the fan-out; 0 disables it.
const defaultMaxInflight = 256

// loadHandle carries the cancel of an in-flight source load so a concurrent
// remove/purge can cancel the now-superseded load instead of letting it run to
// fetchTimeout. Boxed so a completing load deletes only its own entry.
type loadHandle struct{ cancel context.CancelFunc }

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
	// mu guards gen and the check-then-Add in getOrLoad so an eviction cannot
	// interleave between the generation check and the store. gen bumps on every
	// remove; a load that raced an eviction sees the changed gen and skips its
	// now-stale store. Global, not per-key: an eviction of any key skips any load
	// in flight at that moment — correct, and at membership-change rates the rare
	// extra reload is cheaper than tracking a per-key counter.
	// ponytail: global generation, per-key if evict rate ever dwarfs read misses.
	mu       sync.Mutex
	gen      uint64
	inflight map[string]*loadHandle // key → cancel of the in-flight load, guarded by mu

	// sem bounds concurrent source loads; nil disables the bound.
	sem chan struct{}
	// valid gates cache use. Flipped false by suspend (NATS disconnect) so reads
	// fall through to a fresh source lookup instead of serving a possibly-stale
	// entry, and true again by resume only after the purge has cleared the cache.
	valid atomic.Bool
}

// newTTLCache builds an LRU+TTL cache. rec records hit/miss/error outcomes;
// pass a cachemetrics.For(name, "l1") so each cache reports its own series.
// maxInflight caps concurrent source loads (<=0 → defaultMaxInflight; a nil sem
// disables the cap only via the sentinel checked below).
func newTTLCache[V any](size int, ttl time.Duration, maxInflight int, rec Recorder) (*ttlCache[V], error) {
	if size <= 0 {
		return nil, fmt.Errorf("readcache: size must be positive, got %d", size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("readcache: ttl must be positive, got %v", ttl)
	}
	c := &ttlCache[V]{
		lru:      lru.NewLRU[string, V](size, nil, ttl),
		metrics:  rec,
		inflight: make(map[string]*loadHandle),
	}
	if maxInflight <= 0 {
		maxInflight = defaultMaxInflight
	}
	c.sem = make(chan struct{}, maxInflight)
	c.valid.Store(true)
	return c, nil
}

// getOrLoad returns the cached value for key, or invokes load on miss. load
// returns (value, store, err): when store is false the value is returned to
// the caller but not cached (used for negative results); when err is non-nil
// nothing is cached and the error is returned.
// If ctx is canceled before the shared load finishes, getOrLoad returns ctx.Err() immediately
// while the load continues detached in the background, bounded by fetchTimeout.
// remove drops key if present; absent keys are a no-op. Forget detaches any
// in-flight singleflight load for key so a caller arriving after the eviction
// starts a FRESH source read instead of joining the doomed in-flight one (which
// read the pre-eviction boundary). The gen bump makes that doomed load skip its
// store and, in getOrLoad, re-read rather than return its now-stale value.
func (c *ttlCache[V]) remove(key string) {
	c.mu.Lock()
	c.gen++
	c.lru.Remove(key)
	c.sf.Forget(key)
	// Cancel the superseded in-flight load: Forget only detaches it, leaving the
	// goroutine + fetch context pinned until fetchTimeout. Under churn + slow Mongo
	// those pile up, so cancel it now — it fails closed rather than storing a value
	// read before this eviction.
	if h := c.inflight[key]; h != nil {
		h.cancel()
		delete(c.inflight, key)
	}
	c.mu.Unlock()
}

// purge drops every entry and bumps gen so any in-flight loads fail the
// stability check in getOrLoad and re-read (or fail closed) rather than storing
// a value read before the purge, and cancels every in-flight load so none pins a
// goroutine past the purge. Used to cold-start the cache on NATS reconnect, when
// this instance may have missed evictions while disconnected.
func (c *ttlCache[V]) purge() {
	c.mu.Lock()
	c.gen++
	c.lru.Purge()
	for key, h := range c.inflight {
		h.cancel()
		delete(c.inflight, key)
	}
	c.mu.Unlock()
}

// suspend stops the cache serving or storing entries and purges what it holds.
// Called on NATS disconnect: while offline this instance hears no evictions, so
// every cached grant may already be stale — reads must fall through to the source
// until resume re-arms the cache.
func (c *ttlCache[V]) suspend() {
	c.valid.Store(false)
	c.purge()
}

// resume purges any entries re-cached during the outage, then re-arms the cache.
// Ordering matters: valid flips true only after the purge, so no read is served
// from the pre-reconnect cache once the connection (and eviction delivery) is back.
func (c *ttlCache[V]) resume() {
	c.purge()
	c.valid.Store(true)
}

func (c *ttlCache[V]) getOrLoad(ctx context.Context, key string, load func(context.Context) (V, bool, error)) (V, error) {
	// Skip the cache entirely while suspended: a possibly-stale entry must not be
	// served after a disconnect until resume re-arms the cache.
	if c.valid.Load() {
		if v, ok := c.lru.Get(key); ok {
			c.metrics.Hit(ctx)
			return v, nil
		}
	}

	resCh := c.sf.DoChan(key, func() (any, error) {
		// Re-check under singleflight in case a sibling populated the entry.
		if c.valid.Load() {
			if cached, ok := c.lru.Get(key); ok {
				return cached, nil
			}
		}
		// Bound concurrent source loads so a burst of distinct-key misses can't
		// spawn unbounded goroutines. Detached from the caller ctx (like the fetch
		// below) so a leader's cancel never poisons its coalesced waiters; bounded
		// by fetchTimeout so a saturated cache fails closed rather than hanging.
		if c.sem != nil {
			select {
			case c.sem <- struct{}{}: // fast path: a slot is free, no timer allocated
			default:
				// Contended: wait for a slot, bounded so a saturated cache fails
				// closed rather than hanging.
				waitCtx, waitCancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
				select {
				case c.sem <- struct{}{}:
					waitCancel()
				case <-waitCtx.Done():
					waitCancel()
					var zero V
					return zero, fmt.Errorf("readcache: load slots exhausted: %w", waitCtx.Err())
				}
			}
			defer func() { <-c.sem }()
		}
		// Load, then confirm no eviction (a gen bump) raced this load. If one did,
		// the value we just read may predate the membership change that triggered
		// the eviction, so it must NOT be returned to ANY singleflight waiter — a
		// stale positive grant here lets a revoked member read history. Re-read
		// against the new generation and re-check, looping until the generation is
		// stable across a full load. Bounded: if evictions keep racing every
		// attempt, fail closed with an error (never a fabricated zero value that a
		// generic-V caller would read as real data).
		const maxLoadAttempts = 5
		for attempt := 0; attempt < maxLoadAttempts; attempt++ {
			fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
			// Register the cancel so a concurrent remove/purge for this key cancels
			// this now-superseded load rather than letting it run to fetchTimeout.
			h := &loadHandle{cancel: cancel}
			// One critical section: snapshot gen before loading (so a remove during
			// the load is detected) and publish the cancel handle together.
			c.mu.Lock()
			gen := c.gen
			c.inflight[key] = h
			c.mu.Unlock()

			val, store, err := load(fetchCtx)

			c.mu.Lock()
			if c.inflight[key] == h {
				delete(c.inflight, key)
			}
			c.mu.Unlock()
			cancel()
			if err != nil {
				return val, fmt.Errorf("load cache entry: %w", err)
			}

			// Check-and-store under the same lock as remove so an eviction can't
			// slip between the generation check and the Add. Never store while
			// suspended — the entry would outlive the barrier it is meant to bypass.
			c.mu.Lock()
			stable := c.gen == gen
			if stable && store && c.valid.Load() {
				c.lru.Add(key, val)
			}
			c.mu.Unlock()
			if stable {
				return val, nil
			}
			// An eviction landed during this load: loop and re-read the source.
		}
		var zero V
		return zero, errors.New("readcache: evictions raced every load attempt, failing closed")
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

// CacheOption tunes a cache constructor.
type CacheOption func(*cacheOptions)

type cacheOptions struct{ maxInflight int }

// WithMaxInflight caps concurrent source loads; <=0 takes defaultMaxInflight.
func WithMaxInflight(n int) CacheOption {
	return func(o *cacheOptions) { o.maxInflight = n }
}

// NewSubscriptionCache wraps inner with an LRU+TTL cache of size entries and
// the given TTL. size and ttl must be positive.
func NewSubscriptionCache(inner SubscriptionSource, size int, ttl time.Duration, opts ...CacheOption) (*SubscriptionCache, error) {
	var o cacheOptions
	for _, opt := range opts {
		opt(&o)
	}
	cache, err := newTTLCache[subEntry](size, ttl, o.maxInflight, cachemetrics.For("history_sub", "l1"))
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

// Evict drops the cached access-check entry for (account, roomID) so the next
// GetHistorySharedSince reloads the current subscription boundary from the
// source. A membership change (remove, restricted re-add) alters that boundary
// while the LRU keeps serving the stale full-access entry until its TTL — the
// #414 leak. The key must match GetHistorySharedSince's exactly. Local only,
// like PreviewCache.Invalidate: a sibling replica's copy lives out its own TTL.
func (c *SubscriptionCache) Evict(account, roomID string) {
	c.cache.remove(account + "\x00" + roomID)
}

// Purge drops every cached access-check entry. Core NATS delivers no replay, so
// an instance that was disconnected while a membership change fanned out never
// received the eviction and would keep serving the stale full-access entry until
// TTL (#414). Call this from the NATS reconnect handler to cold-start the cache:
// the next read of each key reloads the current boundary from the source.
func (c *SubscriptionCache) Purge() {
	c.cache.purge()
}

// Suspend stops the cache serving or storing grants and purges it. Call it from
// the NATS disconnect handler: while offline this instance receives no evictions,
// so a cached grant may already be stale — reads must fall through to the source
// (fail-closed against the primary) until Resume re-arms the cache.
func (c *SubscriptionCache) Suspend() {
	c.cache.suspend()
}

// Resume purges any grant re-cached during the outage and re-arms the cache.
// Call it from the reconnect handler: the purge runs before the cache is re-armed
// so no read races in on the pre-reconnect cache once eviction delivery is back.
func (c *SubscriptionCache) Resume() {
	c.cache.resume()
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
	times, err := newTTLCache[roomTimes](size, ttl, defaultMaxInflight, cachemetrics.For("history_room_times", "l1"))
	if err != nil {
		return nil, err
	}
	minSeen, err := newTTLCache[*time.Time](size, ttl, defaultMaxInflight, cachemetrics.For("history_room_min_seen", "l1"))
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
	c, err := newTTLCache[previewEntry](size, ttl, defaultMaxInflight, cachemetrics.For("history_room_preview", "l1"))
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
