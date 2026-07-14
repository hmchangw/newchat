package userstore

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

// fetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const fetchTimeout = 10 * time.Second

// Recorder records the outcome of a cache lookup. cachemetrics.Recorder
// satisfies it; tests substitute a spy.
type Recorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

// Cache fronts a UserStore with two LRU+TTL stores (byID, byAccount) sharing
// value pointers; populate writes to both so a hit on either satisfies the
// other. Singleflight collapses concurrent misses. Pod-local: entries ~500B,
// few-MB working set, writes rare — Valkey buys nothing at this size.
type Cache struct {
	byID      *lru.LRU[string, *model.User]
	byAccount *lru.LRU[string, *model.User]
	store     UserStore
	sf        singleflight.Group

	metrics Recorder
}

// Option configures a Cache at construction.
type Option func(*Cache)

// WithMetrics overrides the hit/miss/error recorder. Defaults to the
// package-default cachemetrics recorder tagged cache="user",tier="l1".
func WithMetrics(r Recorder) Option {
	return func(c *Cache) { c.metrics = r }
}

// NewCache returns a Cache. size applies to each LRU independently; ttl applies to both.
func NewCache(store UserStore, size int, ttl time.Duration, opts ...Option) (*Cache, error) {
	if store == nil {
		return nil, fmt.Errorf("userstore: store must not be nil")
	}
	if size <= 0 {
		return nil, fmt.Errorf("userstore: cache size must be positive, got %d", size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("userstore: cache ttl must be positive, got %v", ttl)
	}
	c := &Cache{
		byID:      lru.NewLRU[string, *model.User](size, nil, ttl),
		byAccount: lru.NewLRU[string, *model.User](size, nil, ttl),
		store:     store,
		metrics:   cachemetrics.For("user", "l1"),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// FindUserByID serves from byID; misses fall through. ErrUserNotFound is not negatively cached.
func (c *Cache) FindUserByID(ctx context.Context, id string) (*model.User, error) {
	if v, ok := c.byID.Get(id); ok {
		c.metrics.Hit(ctx)
		return v, nil
	}
	resCh := c.sf.DoChan(id, func() (interface{}, error) {
		if cached, ok := c.byID.Get(id); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		u, err := c.store.FindUserByID(fetchCtx, id)
		if err != nil {
			return nil, err
		}
		c.populate(u)
		return u, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			if errors.Is(res.Err, ErrUserNotFound) {
				return nil, res.Err
			}
			return nil, fmt.Errorf("find cached user %q: %w", id, res.Err)
		}
		c.metrics.Miss(ctx)
		return res.Val.(*model.User), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return nil, ctx.Err()
	}
}

// FindUserByAccount serves from byAccount; cross-populates byID; SF key "account:"+account avoids ID collision.
func (c *Cache) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	if v, ok := c.byAccount.Get(account); ok {
		c.metrics.Hit(ctx)
		return v, nil
	}
	resCh := c.sf.DoChan("account:"+account, func() (interface{}, error) {
		if cached, ok := c.byAccount.Get(account); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		u, err := c.store.FindUserByAccount(fetchCtx, account)
		if err != nil {
			return nil, err
		}
		c.populate(u)
		return u, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			if errors.Is(res.Err, ErrUserNotFound) {
				return nil, res.Err
			}
			return nil, fmt.Errorf("find cached user by account %q: %w", account, res.Err)
		}
		c.metrics.Miss(ctx)
		return res.Val.(*model.User), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return nil, ctx.Err()
	}
}

// FindUsersByAccounts hits byAccount; misses forwarded in one call; input deduped; partial hits on store errors.
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
			c.metrics.Hit(ctx)
			hits = append(hits, *u)
			continue
		}
		missing = append(missing, a)
	}
	if len(missing) == 0 {
		return hits, nil
	}
	fresh, err := c.store.FindUsersByAccounts(ctx, missing)
	if err != nil {
		for range missing {
			c.metrics.Error(ctx)
		}
		return hits, fmt.Errorf("cached find users by accounts: %w", err)
	}
	for range missing {
		c.metrics.Miss(ctx)
	}
	for i := range fresh {
		c.populate(&fresh[i])
	}
	return append(hits, fresh...), nil
}

// populate writes the user (same pointer) under both byID and byAccount LRUs.
func (c *Cache) populate(u *model.User) {
	if u == nil {
		return
	}
	c.byID.Add(u.ID, u)
	if u.Account != "" {
		c.byAccount.Add(u.Account, u)
	}
}

// Invalidate drops cached entries; empty userID or account skips that LRU.
func (c *Cache) Invalidate(userID, account string) {
	if userID != "" {
		c.byID.Remove(userID)
	}
	if account != "" {
		c.byAccount.Remove(account)
	}
}
