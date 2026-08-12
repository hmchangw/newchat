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
	sf    singleflight.Group
}

// NewLookup returns a Lookup over cache, falling back to load on a miss and
// populating cache with the given TTL. A nil cache is allowed and turns the
// Lookup into a pass-through to load, for deployments with no Valkey.
func NewLookup(cache Cache, load Loader, ttl time.Duration) *Lookup {
	return &Lookup{cache: cache, load: load, ttl: ttl}
}

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
	if got, err := c.cache.Get(ctx, roomID); err == nil {
		return got, nil
	} else if !errors.Is(err, valkeyutil.ErrCacheMiss) {
		slog.Warn("roomsubcache get failed, falling back to loader", "error", err, "roomId", roomID)
	}

	// Miss path: singleflight collapses concurrent loads on the same room.
	resCh := c.sf.DoChan(roomID, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memberFetchTimeout)
		defer cancel()
		// Re-check inside the flight in case a sibling caller already populated.
		if got, err := c.cache.Get(fetchCtx, roomID); err == nil {
			return got, nil
		}
		loaded, lerr := c.load(fetchCtx, roomID)
		if lerr != nil {
			return nil, fmt.Errorf("load members for room %s: %w", roomID, lerr)
		}
		if setErr := c.cache.Set(fetchCtx, roomID, loaded, c.ttl); setErr != nil {
			slog.Warn("roomsubcache set failed", "error", setErr, "roomId", roomID)
		}
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

// Invalidate drops the room from Valkey on membership change. A no-op when the
// Lookup has no cache.
func (c *Lookup) Invalidate(ctx context.Context, roomID string) {
	if c.cache == nil {
		return
	}
	if err := c.cache.Invalidate(ctx, roomID); err != nil {
		slog.Warn("roomsubcache invalidate failed", "error", err, "roomId", roomID)
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
