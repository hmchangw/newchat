package userstore

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/model"
)

// Cache is an in-process LRU+TTL cache fronting a UserStore. Shared by
// message-gatekeeper (sender display-name resolution), broadcast-worker (mention
// enrichment + sender lookup), and message-worker (mention resolution + sender
// lookup) so all three pay the same Mongo cost once per warm entry.
//
// Both lookups are cached. Every populate writes the user under both the by-ID
// and by-account prefixes so a hit on either path satisfies the other; the
// two LRUs share value pointers so a single User lives once in memory.
// Singleflight collapses concurrent FindUserByID misses for the same id.
//
// Pod-local in-memory is fine here: entries are tiny (~500 B/user), per-pod
// working set caps at a few MB for 10K warm users, writes are rare (display-name
// changes are admin events). Valkey overhead (network hop, serialization,
// error handling) buys nothing at this size.
type Cache struct {
	byID      *lru.LRU[string, *model.User]
	byAccount *lru.LRU[string, *model.User]
	store     UserStore
	sf        singleflight.Group

	hits     atomic.Uint64
	misses   atomic.Uint64
	loadErrs atomic.Uint64
}

// Stats is a snapshot of the cache's counters.
type Stats struct {
	Hits, Misses, LoadErrors uint64
	SizeByID, SizeByAccount  int
}

// NewCache returns a Cache fronting the given UserStore. size applies to each
// of the by-ID and by-account LRUs independently; ttl applies to both.
func NewCache(store UserStore, size int, ttl time.Duration) (*Cache, error) {
	if store == nil {
		return nil, fmt.Errorf("userstore: store must not be nil")
	}
	if size <= 0 {
		return nil, fmt.Errorf("userstore: cache size must be positive, got %d", size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("userstore: cache ttl must be positive, got %v", ttl)
	}
	return &Cache{
		byID:      lru.NewLRU[string, *model.User](size, nil, ttl),
		byAccount: lru.NewLRU[string, *model.User](size, nil, ttl),
		store:     store,
	}, nil
}

// FindUserByID serves from the by-ID LRU when hot, falls through to the store
// on miss. ErrUserNotFound propagates unwrapped; missing entries are NOT
// negatively cached.
func (c *Cache) FindUserByID(ctx context.Context, id string) (*model.User, error) {
	if v, ok := c.byID.Get(id); ok {
		c.hits.Add(1)
		return v, nil
	}
	c.misses.Add(1)
	v, err, _ := c.sf.Do(id, func() (interface{}, error) {
		if cached, ok := c.byID.Get(id); ok {
			return cached, nil
		}
		u, err := c.store.FindUserByID(ctx, id)
		if err != nil {
			return nil, err
		}
		c.populate(u)
		return u, nil
	})
	if err != nil {
		c.loadErrs.Add(1)
		if errors.Is(err, ErrUserNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("find cached user %q: %w", id, err)
	}
	return v.(*model.User), nil
}

// FindUsersByAccounts serves cache hits from the by-account LRU and forwards
// the missing set to the store in one batched call. Input duplicates are
// deduped; result order is not guaranteed to match input. A store error
// returns partial hits + a wrapped error.
func (c *Cache) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(accounts))
	hits := make([]model.User, 0, len(accounts))
	missing := make([]string, 0, len(accounts))
	for _, a := range accounts {
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		if u, ok := c.byAccount.Get(a); ok {
			c.hits.Add(1)
			hits = append(hits, *u)
			continue
		}
		c.misses.Add(1)
		missing = append(missing, a)
	}
	if len(missing) == 0 {
		return hits, nil
	}
	fresh, err := c.store.FindUsersByAccounts(ctx, missing)
	if err != nil {
		return hits, fmt.Errorf("cached find users by accounts: %w", err)
	}
	for i := range fresh {
		c.populate(&fresh[i])
	}
	return append(hits, fresh...), nil
}

// populate writes the user under both prefixes so a hit on either path
// satisfies the other. The same pointer is stored in both LRUs.
func (c *Cache) populate(u *model.User) {
	if u == nil {
		return
	}
	c.byID.Add(u.ID, u)
	if u.Account != "" {
		c.byAccount.Add(u.Account, u)
	}
}

// Stats returns a snapshot of cache counters.
func (c *Cache) Stats() Stats {
	return Stats{
		Hits:          c.hits.Load(),
		Misses:        c.misses.Load(),
		LoadErrors:    c.loadErrs.Load(),
		SizeByID:      c.byID.Len(),
		SizeByAccount: c.byAccount.Len(),
	}
}

// Invalidate drops any cached entries for the given user. Empty userID or
// account skips that prefix.
func (c *Cache) Invalidate(userID, account string) {
	if userID != "" {
		c.byID.Remove(userID)
	}
	if account != "" {
		c.byAccount.Remove(account)
	}
}
