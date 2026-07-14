package main

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"
)

// eidLoader is the single store method eidCache needs to populate itself.
type eidLoader interface {
	EmployeeID(ctx context.Context, account string) (eid string, found bool, err error)
}

// eidCache fronts the account→employeeId lookup with an LRU+TTL store and
// singleflight. The LRU handles steady-state hits (and evicts the oldest entry
// past capacity, no drop-all cliff); singleflight collapses concurrent misses
// for the same account into a single load. Only positive results are cached — a
// not-found is cheap to recompute and self-heals — but singleflight still
// dedups concurrent not-found lookups within the in-flight window.
type eidCache struct {
	lru   *lru.LRU[string, string]
	store eidLoader
	sf    singleflight.Group
}

func newEIDCache(store eidLoader, size int, ttl time.Duration) *eidCache {
	return &eidCache{
		lru:   lru.NewLRU[string, string](size, nil, ttl),
		store: store,
	}
}

// get returns the account's employeeId, loading and caching it on a miss.
// found=false means the account has no employeeId (and is not cached).
//
// The shared load detaches from the caller's cancellation (context.WithoutCancel)
// so a single caller's cancel/timeout cannot abort the in-flight load for the
// other callers coalesced onto it; each caller still abandons independently via
// its own ctx.Done() in the select below.
func (c *eidCache) get(ctx context.Context, account string) (string, bool, error) {
	if eid, ok := c.lru.Get(account); ok {
		return eid, true, nil
	}
	resCh := c.sf.DoChan(account, func() (any, error) {
		if eid, ok := c.lru.Get(account); ok { // populated while we waited
			return eid, nil
		}
		fetchCtx := context.WithoutCancel(ctx)
		eid, found, err := c.store.EmployeeID(fetchCtx, account)
		if err != nil {
			return "", err
		}
		if !found {
			return "", nil // empty sentinel: not cached
		}
		c.lru.Add(account, eid)
		return eid, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			return "", false, res.Err
		}
		eid := res.Val.(string)
		return eid, eid != "", nil
	case <-ctx.Done():
		return "", false, ctx.Err()
	}
}
