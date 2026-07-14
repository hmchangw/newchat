package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/model"
)

// subFetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const subFetchTimeout = 10 * time.Second

// cacheRecorder records the outcome of a cache lookup. cachemetrics.Recorder
// satisfies it; tests substitute a spy.
type cacheRecorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

// cachedSubscription is the projection of model.Subscription that
// gatekeeper actually reads on the hot path. Caching only these fields
// (vs the full doc) keeps the cache tight and makes the contract
// explicit at the cache boundary. ID is the user's entity ID (used by
// the handler to populate msg.UserID on the canonical message event).
type cachedSubscription struct {
	ID      string
	Account string
	Roles   []model.Role
}

type subKey struct {
	roomID  string
	account string
}

// cachedSubStore wraps a Store with an LRU+TTL cache of subscription
// lookups. Negative results (errNotSubscribed) and transient errors
// are NOT cached — see the spec for rationale. GetRoomMeta passes
// through unchanged.
type cachedSubStore struct {
	Store
	lru *lru.LRU[subKey, cachedSubscription]
	sf  singleflight.Group

	metrics cacheRecorder
}

func newCachedSubStore(inner Store, size int, ttl time.Duration) (*cachedSubStore, error) {
	if size <= 0 {
		return nil, fmt.Errorf("subcache: size must be positive, got %d", size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("subcache: ttl must be positive, got %v", ttl)
	}
	return &cachedSubStore{
		Store:   inner,
		lru:     lru.NewLRU[subKey, cachedSubscription](size, nil, ttl),
		metrics: cachemetrics.For("gatekeeper_sub", "l1"),
	}, nil
}

func (c *cachedSubStore) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	key := subKey{roomID: roomID, account: account}
	if v, ok := c.lru.Get(key); ok {
		c.metrics.Hit(ctx)
		return fromCached(v), nil
	}

	// roomIDs and accounts are validated as UUID/base62/email-style strings that
	// cannot contain NUL bytes; the \x00 separator therefore makes the key
	// collision-free across any (roomID, account) split.
	sfKey := roomID + "\x00" + account
	resCh := c.sf.DoChan(sfKey, func() (interface{}, error) {
		if cached, ok := c.lru.Get(key); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), subFetchTimeout)
		defer cancel()
		sub, err := c.Store.GetSubscription(fetchCtx, account, roomID)
		if err != nil {
			// Do not cache errNotSubscribed or transient errors — see spec.
			return nil, err
		}
		projected := cachedSubscription{
			ID:      sub.User.ID,
			Account: sub.User.Account,
			Roles:   append([]model.Role(nil), sub.Roles...),
		}
		c.lru.Add(key, projected)
		return projected, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			if errors.Is(res.Err, errNotSubscribed) {
				c.metrics.Miss(ctx)
			} else {
				c.metrics.Error(ctx)
			}
			return nil, fmt.Errorf("get cached subscription: %w", res.Err)
		}
		c.metrics.Miss(ctx)
		return fromCached(res.Val.(cachedSubscription)), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return nil, ctx.Err()
	}
}

// fromCached builds a partial *model.Subscription from the cached
// projection. Only the fields gatekeeper reads (User.ID, User.Account,
// Roles) are populated; other fields are zero. This is intentional —
// the cache contract is the projection, not the full doc.
//
// The Roles slice is not defensively copied: the slice is already owned
// by the cache (copied at cache-write in GetSubscription), and the only
// consumer (canBypassLargeRoomCap) range-reads without mutation.
func fromCached(c cachedSubscription) *model.Subscription {
	return &model.Subscription{
		User:  model.SubscriptionUser{ID: c.ID, Account: c.Account},
		Roles: c.Roles,
	}
}
