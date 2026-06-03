package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

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
		slog.Warn("roomsubcache get failed, falling back to mongo", "error", err, "roomId", roomID)
	}

	// Miss path: singleflight collapses concurrent Mongo loads on the same room.
	members, err, _ := c.sf.Do(roomID, func() (any, error) {
		// Re-check inside the flight in case a sibling caller already populated.
		if got, err := c.cache.Get(ctx, roomID); err == nil {
			return got, nil
		}
		loaded, lerr := c.load(ctx, roomID)
		if lerr != nil {
			return nil, fmt.Errorf("load members for room %s: %w", roomID, lerr)
		}
		if setErr := c.cache.Set(ctx, roomID, loaded, c.ttl); setErr != nil {
			slog.Warn("roomsubcache set failed", "error", setErr, "roomId", roomID)
		}
		return loaded, nil
	})
	if err != nil {
		return nil, fmt.Errorf("get members for room %s: %w", roomID, err)
	}
	return members.([]roomsubcache.Member), nil
}

// Invalidate drops the room from Valkey on membership change.
func (c *cachedMemberLookup) Invalidate(ctx context.Context, roomID string) {
	if err := c.cache.Invalidate(ctx, roomID); err != nil {
		slog.Warn("roomsubcache invalidate failed", "error", err, "roomId", roomID)
	}
}
