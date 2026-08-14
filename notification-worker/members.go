package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// memberFetchTimeout bounds the detached shared load so a hung backend cannot
// leak the singleflight goroutine or pin the in-flight key. See the design spec.
const memberFetchTimeout = 10 * time.Second

// memberLoader reads the canonical member list for a room; a function type so tests can inject a fake.
type memberLoader func(ctx context.Context, roomID string) ([]roomsubcache.Member, error)

// cachedMemberLookup resolves members via Valkey → Mongo. Single-flight collapses
// concurrent in-pod misses on the same room to one Valkey GET (and one Mongo
// query on a cold miss). No in-process tier — keeps per-pod memory bounded
// against rooms with thousands of members.
type cachedMemberLookup struct {
	cache roomsubcache.Cache
	load  memberLoader
	ttl   time.Duration
	sf    singleflight.Group
}

func newCachedMemberLookup(cache roomsubcache.Cache, load memberLoader, ttl time.Duration) *cachedMemberLookup {
	return &cachedMemberLookup{cache: cache, load: load, ttl: ttl}
}

// GetMembers returns the member list, populating Valkey on a Mongo round-trip.
// Callers must not mutate the slice.
func (c *cachedMemberLookup) GetMembers(ctx context.Context, roomID string) ([]roomsubcache.Member, error) {
	// Fast path: cache hits skip singleflight to avoid serializing concurrent
	// readers behind one in-flight caller.
	if got, err := c.cache.Get(ctx, roomID); err == nil {
		return got, nil
	} else if !errors.Is(err, valkeyutil.ErrCacheMiss) {
		valkeyutil.LogDegraded(ctx, "roomsubcache get failed, falling back to mongo", err, "roomId", roomID)
	}

	// Miss path: singleflight collapses concurrent Mongo loads on the same room.
	resCh := c.sf.DoChan(roomID, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memberFetchTimeout)
		defer cancel()
		// Re-check inside the flight in case a sibling caller already populated.
		got, getErr := c.cache.Get(fetchCtx, roomID)
		if getErr == nil {
			return got, nil
		}
		loaded, lerr := c.load(fetchCtx, roomID)
		if lerr != nil {
			return nil, fmt.Errorf("load members for room %s: %w", roomID, lerr)
		}
		// Don't marshal a member list Valkey is known to be refusing. With the
		// breaker open the Set is rejected in microseconds, so the JSON encode of a
		// large room would be pure waste — once per message, for the whole outage.
		if valkeyutil.IsUnavailable(getErr) {
			return loaded, nil
		}
		if setErr := c.cache.Set(fetchCtx, roomID, loaded, c.ttl); setErr != nil {
			valkeyutil.LogDegraded(fetchCtx, "roomsubcache set failed", setErr, "roomId", roomID)
		}
		return loaded, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			return nil, fmt.Errorf("get members for room %s: %w", roomID, res.Err)
		}
		return res.Val.([]roomsubcache.Member), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Invalidate drops the room from Valkey on membership change.
func (c *cachedMemberLookup) Invalidate(ctx context.Context, roomID string) {
	if err := c.cache.Invalidate(ctx, roomID); err != nil {
		valkeyutil.LogDegraded(ctx, "roomsubcache invalidate failed", err, "roomId", roomID)
	}
}
