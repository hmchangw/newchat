package main

import (
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/hmchangw/chat/pkg/cachemetrics"
)

// roomSubCache answers "does this site hold a subscription for this room" from
// memory. The activity refresh is broadcast to every peer, so without it each
// arriving message costs a Mongo read just to discard most of them — and the
// answer changes only on member_added/member_removed, which are rare against
// message traffic, so the hit rate approaches 1 after warmup.
//
// Both polarities are cached. Negative entries are the point: rooms this site
// has no member in are the common case and the ones being thrown away. A stale
// negative is safe because member_added seeds the ordering row directly on the
// join path, so a newly joined room is already correct in remote_rooms before
// any refresh arrives — and the handlers below keep the entry in step anyway.
type roomSubCache struct {
	// mu serializes the read-through fill against the membership handlers. The
	// refresh lane runs on its own subscription goroutine, so a fill that read
	// "not a member" can otherwise land after a concurrent member_added wrote
	// true — pinning the room's position at its join-time seed for the whole
	// TTL, exactly the staleness the handler hooks exist to prevent.
	mu      sync.Mutex
	entries *lru.LRU[string, bool]
	metrics cachemetrics.Recorder
}

func newRoomSubCache(size int, ttl time.Duration) *roomSubCache {
	if size <= 0 || ttl <= 0 {
		return nil // disabled: every refresh reads through
	}
	return &roomSubCache{
		entries: lru.NewLRU[string, bool](size, nil, ttl),
		metrics: cachemetrics.For("room_sub", "l1"),
	}
}

func (c *roomSubCache) get(ctx context.Context, roomID string) (bool, bool) {
	if c == nil {
		return false, false
	}
	c.mu.Lock()
	subscribed, ok := c.entries.Get(roomID)
	c.mu.Unlock()
	if ok {
		c.metrics.Hit(ctx)
	} else {
		c.metrics.Miss(ctx)
	}
	return subscribed, ok
}

// set records an authoritative answer — from a membership handler, which knows
// the outcome rather than inferring it from a read.
func (c *roomSubCache) set(roomID string, subscribed bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries.Add(roomID, subscribed)
}

// fill records a read-through result without overwriting an entry a membership
// handler wrote while the read was in flight. Those are authoritative; a
// concurrent read only ever observed a snapshot that may already be stale.
func (c *roomSubCache) fill(roomID string, subscribed bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries.Get(roomID); exists {
		return
	}
	c.entries.Add(roomID, subscribed)
}

// invalidate drops the entry so the next refresh re-resolves it. Used on
// member_removed: losing one account does not prove the site has no members
// left, and re-reading once is cheaper than reasoning about which guess is safe.
func (c *roomSubCache) invalidate(roomID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries.Remove(roomID)
}

// hasRoomSubscription answers from cache, falling back to the store and
// recording the result. A store error is returned uncached.
func (h *Handler) hasRoomSubscription(ctx context.Context, roomID string) (bool, error) {
	if subscribed, ok := h.roomSubs.get(ctx, roomID); ok {
		return subscribed, nil
	}
	subscribed, err := h.store.HasRoomSubscription(ctx, roomID)
	if err != nil {
		return false, err
	}
	h.roomSubs.fill(roomID, subscribed)
	return subscribed, nil
}
