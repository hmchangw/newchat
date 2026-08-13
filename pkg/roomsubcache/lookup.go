package roomsubcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// memberFetchTimeout bounds the detached shared load so a hung backend cannot
// leak the singleflight goroutine or pin the in-flight key. See the design spec.
const memberFetchTimeout = 10 * time.Second

// Loader reads the canonical member list for a room; a function type so tests
// can inject a fake. Production callers use NewMongoLoader, which writes the
// complete Member projection — see the Lookup doc for why that matters.
type Loader func(ctx context.Context, roomID string) ([]Member, error)

// Lookup resolves members via Valkey → Loader. Single-flight collapses
// concurrent in-pod misses on the same room to one Valkey GET (and one loader
// call on a cold miss). No in-process tier — keeps per-pod memory bounded
// against rooms with thousands of members.
//
// The Valkey key is shared by every service caching the same room, so a Lookup
// must only ever be built with a loader that fills every Member field. A
// partial writer (say, one that only needs Account) would silently unmute muted
// users and widen history access windows for the services that do read those
// fields. NewMongoLoader is the one sanctioned production loader.
type Lookup struct {
	cache Cache // nil disables the L2 tier — every read goes to the loader
	load  Loader
	ttl   time.Duration
	now   func() time.Time
	sf    singleflight.Group
}

// NewLookup returns a Lookup over cache, falling back to load on a miss and
// populating cache with the given TTL. A nil cache is allowed and turns the
// Lookup into a pass-through to load, for deployments with no Valkey.
func NewLookup(cache Cache, load Loader, ttl time.Duration) *Lookup {
	return newLookupWithClock(cache, load, ttl, time.Now)
}

func newLookupWithClock(cache Cache, load Loader, ttl time.Duration, now func() time.Time) *Lookup {
	return &Lookup{cache: cache, load: load, ttl: ttl, now: now}
}

// refreshAfterFor derives the re-validation window from the entry TTL. Same
// derivation as the other read-through tiers (pkg/subauthcache, pkg/atrest,
// pkg/userstore).
func refreshAfterFor(ttl time.Duration) time.Duration { return ttl / 4 * 3 }

// GetMembers returns the member list, populating Valkey on a loader round-trip.
// Callers must not mutate the slice.
func (c *Lookup) GetMembers(ctx context.Context, roomID string) ([]Member, error) {
	if c.cache == nil {
		members, err := c.load(ctx, roomID)
		if err != nil {
			return nil, fmt.Errorf("load members for room %s: %w", roomID, err)
		}
		return members, nil
	}

	// Fast path: cache hits skip singleflight to avoid serializing concurrent
	// readers behind one in-flight caller.
	if entry, err := c.cache.Get(ctx, roomID); err == nil {
		return c.serveHit(ctx, roomID, entry)
	} else if !errors.Is(err, valkeyutil.ErrCacheMiss) {
		slog.WarnContext(ctx, "roomsubcache get failed, falling back to loader", "error", err, "roomId", roomID)
	}

	// Miss path: singleflight collapses concurrent loads on the same room.
	resCh := c.sf.DoChan(roomID, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memberFetchTimeout)
		defer cancel()
		// Re-check inside the flight in case a sibling caller already populated.
		if entry, err := c.cache.Get(fetchCtx, roomID); err == nil {
			return entry.Members, nil
		}
		loaded, lerr := c.load(fetchCtx, roomID)
		if lerr != nil {
			return nil, fmt.Errorf("load members for room %s: %w", roomID, lerr)
		}
		c.write(fetchCtx, roomID, loaded)
		return loaded, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			return nil, fmt.Errorf("get members for room %s: %w", roomID, res.Err)
		}
		return res.Val.([]Member), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// serveHit decides what a cache hit means.
//
// Confirmed within the refresh window it is served as a pure read. Older than
// that it is re-validated, and the failure branch is what keeps delivery alive:
// a room the loader confirmed once must not become unresolvable because Mongo
// is down, so the deadline is re-armed and the cached list is served. Without
// it the entry simply expires mid-outage and fan-out starts failing.
func (c *Lookup) serveHit(ctx context.Context, roomID string, entry Entry) ([]Member, error) {
	if c.now().Sub(time.UnixMilli(entry.CachedAt)) < refreshAfterFor(c.ttl) {
		return entry.Members, nil
	}
	// Collapse concurrent revalidations of the same room. Unlike the other
	// read-through tiers this one has no process-local cache in front of it and
	// is called per message, so an uncollapsed refresh means one loader call and
	// one write per in-flight message the moment the window opens. Its own key
	// space, so a refresh and a cold miss can never share a result.
	res, _, _ := c.sf.Do("refresh:"+roomID, func() (any, error) {
		// Detached like the miss path: the shared load outlives whichever caller
		// happened to lead it, so one cancellation cannot poison the waiters.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memberFetchTimeout)
		defer cancel()

		loaded, err := c.load(fetchCtx, roomID)
		if err != nil {
			if slideErr := c.cache.Slide(fetchCtx, roomID, c.ttl); slideErr != nil {
				slog.WarnContext(fetchCtx, "roomsubcache TTL slide failed (entry keeps its current deadline)",
					"error", slideErr, "roomId", roomID)
			}
			return entry.Members, nil // fail-open by design; see above
		}
		c.write(fetchCtx, roomID, loaded)
		return loaded, nil
	})
	return res.([]Member), nil
}

func (c *Lookup) write(ctx context.Context, roomID string, members []Member) {
	if err := c.cache.Set(ctx, roomID, members, c.ttl); err != nil {
		slog.WarnContext(ctx, "roomsubcache set failed", "error", err, "roomId", roomID)
	}
}

// Invalidate drops the room from Valkey on membership change. A no-op when the
// Lookup has no cache.
func (c *Lookup) Invalidate(ctx context.Context, roomID string) {
	if c.cache == nil {
		return
	}
	if err := c.cache.Invalidate(ctx, roomID); err != nil {
		slog.WarnContext(ctx, "roomsubcache invalidate failed", "error", err, "roomId", roomID)
	}
}

// GuardLoader fences load behind breaker so a stalled backend costs one
// server-selection timeout instead of one per lookup. Guard the loader, not
// the Lookup: an open breaker must still serve L2 hits, which are the only
// thing that can answer during the outage that opened it. A nil breaker fences
// nothing.
func GuardLoader(load Loader, breaker *circuitbreaker.Breaker) Loader {
	return func(ctx context.Context, roomID string) ([]Member, error) {
		return circuitbreaker.Do1(breaker, func() ([]Member, error) {
			return load(ctx, roomID)
		})
	}
}
