package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// failoverReader answers "where is this site served from right now?" from SP4's
// FailoverStore, behind a short TTL cache so routing does not hit Mongo on every
// login. Fail-safe: a store error resolves to home and is not cached.
type failoverReader struct {
	store FailoverStore
	ttl   time.Duration
	now   func() time.Time

	mu    sync.Mutex
	cache map[string]cachedTarget
}

type cachedTarget struct {
	target  ServingTarget
	expires time.Time
}

func newFailoverReader(store FailoverStore, ttl time.Duration) *failoverReader {
	return &failoverReader{
		store: store,
		ttl:   ttl,
		now:   time.Now,
		cache: make(map[string]cachedTarget),
	}
}

// ServingTarget returns home or backup for siteID. On a cache hit within the TTL
// it returns the cached value; otherwise it reads the store, derives the target,
// and caches it. A store error resolves to home (fail-safe) and is not cached.
func (r *failoverReader) ServingTarget(ctx context.Context, siteID string) ServingTarget {
	r.mu.Lock()
	if c, ok := r.cache[siteID]; ok && r.now().Before(c.expires) {
		r.mu.Unlock()
		return c.target
	}
	r.mu.Unlock()

	// Read outside the lock so a slow Mongo call does not serialize all readers.
	st, err := r.store.Get(ctx, siteID)
	if err != nil {
		slog.WarnContext(ctx, "failover reader: store read failed, defaulting to home",
			"siteId", siteID, "error", err)
		return ServingHome
	}
	target := st.ServingTarget()

	r.mu.Lock()
	r.cache[siteID] = cachedTarget{target: target, expires: r.now().Add(r.ttl)}
	r.mu.Unlock()
	return target
}
