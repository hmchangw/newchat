package mongorepo

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/hmchangw/chat/pkg/cachemetrics"
)

// roomSortKey holds the room fields that decide where a subscription lands in
// the list: Name for the "Del-" check, LastMsgAt for the activity sort and the
// withinDays window, CreatedAt as the fallback for a room with no messages.
// Missing means no room document on this site — normal for a room owned by
// another site, and caching that keeps repeat lists from looking it up again.
type roomSortKey struct {
	Name      string
	LastMsgAt *time.Time
	CreatedAt *time.Time
	Missing   bool
}

// sortKeyCache is an in-process LRU of roomSortKey with a TTL, built like
// pkg/roommetacache's L1 and reporting the same cachemetrics series. lastMsgAt
// changes with every message, so entries expire rather than being invalidated.
// A stale entry can only affect ordering and the deleted-room check: the page is
// enriched from a fresh room read, and resolveSortKeys re-reads keys that fall
// outside the window. A nil *sortKeyCache means caching is off.
type sortKeyCache struct {
	lru     *lru.LRU[string, roomSortKey]
	metrics cachemetrics.Recorder
}

// newSortKeyCache returns a cache with the given capacity and TTL, or nil when
// either is zero or less — the switch for sending every list back to Mongo.
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
