package mongorepo

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/hmchangw/chat/pkg/cachemetrics"
)

// roomSortKey is the per-room projection that drives subscription-list ordering
// and pre-page filtering: Name feeds the ^Del- deleted-filter, LastMsgAt the
// activity sort and the withinDays window, CreatedAt the no-message fallback.
// Missing marks a room with no local doc (cross-site) — a negative entry, so
// repeat lists don't re-query Mongo for rooms that will never resolve locally.
type roomSortKey struct {
	Name      string
	LastMsgAt *time.Time
	CreatedAt *time.Time
	Missing   bool
}

// sortKeyCache is a process-local LRU+TTL cache of roomSortKey, modeled on
// pkg/roommetacache's L1 (including its cachemetrics series). lastMsgAt churns
// with every message, so entries are TTL-bounded rather than invalidated;
// staleness only affects list ordering and the pre-page deleted-filter — the
// page itself is enriched from a fresh room read, and window membership
// re-reads failing hits (see resolveSortKeys). A nil *sortKeyCache is the
// disabled cache: get always misses, add no-ops.
type sortKeyCache struct {
	lru     *lru.LRU[string, roomSortKey]
	metrics cachemetrics.Recorder
}

// newSortKeyCache returns a cache with the given capacity and TTL, or nil
// (disabled) when either is non-positive — the ops escape hatch to force every
// list back to fresh Mongo reads.
func newSortKeyCache(size int, ttl time.Duration) *sortKeyCache {
	if size <= 0 || ttl <= 0 {
		return nil
	}
	return &sortKeyCache{
		lru:     lru.NewLRU[string, roomSortKey](size, nil, ttl),
		metrics: cachemetrics.For("sub_sortkey", "l1"),
	}
}

func (c *sortKeyCache) get(ctx context.Context, roomID string) (roomSortKey, bool) {
	if c == nil {
		return roomSortKey{}, false
	}
	k, ok := c.lru.Get(roomID)
	if ok {
		c.metrics.Hit(ctx)
	} else {
		c.metrics.Miss(ctx)
	}
	return k, ok
}

func (c *sortKeyCache) add(roomID string, key roomSortKey) {
	if c == nil {
		return
	}
	c.lru.Add(roomID, key)
}
