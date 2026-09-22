package valkeyutil

import (
	"context"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// BreakerConfig bounds how many calls pay a full CallBudget before a tier stops
// trying. Build one breaker PER TIER from it; see New.
type BreakerConfig struct {
	// 0 disables fencing entirely: calls always pass through.
	Fails int `env:"VALKEY_BREAKER_FAILS" envDefault:"5"`
	// Keep generous: each half-open probe against a still-dead Valkey costs a
	// full CallBudget, so a short cooldown reinstates the tax fencing removes.
	Cooldown time.Duration `env:"VALKEY_BREAKER_COOLDOWN" envDefault:"10s"`
}

// Validate rejects negative values; zero is legal and means no fencing.
// envPrefix names the variable the operator actually set.
func (b BreakerConfig) Validate(envPrefix string) error {
	if b.Fails < 0 {
		return fmt.Errorf("%sVALKEY_BREAKER_FAILS must be >= 0, got %d", envPrefix, b.Fails)
	}
	if b.Cooldown < 0 {
		return fmt.Errorf("%sVALKEY_BREAKER_COOLDOWN must be >= 0, got %s", envPrefix, b.Cooldown)
	}
	return nil
}

// New builds a breaker reporting under name. Give each tier its own: a tier with
// oversized values can time out on a healthy Valkey and must not fence the rest.
func (b BreakerConfig) New(ctx context.Context, name string, opts ...circuitbreaker.Option) *circuitbreaker.Breaker {
	// Validate is opt-in and only 3 of 14 consumers call it, so clamp here: a
	// negative budget degrades to no fencing rather than to something undefined.
	fails, cooldown := b.Fails, b.Cooldown
	if fails < 0 || cooldown < 0 {
		fails = 0
	}
	base := []circuitbreaker.Option{
		circuitbreaker.Tracked(ctx, name),
		circuitbreaker.WithFailurePredicate(BreakerFailure()),
	}
	return circuitbreaker.New(fails, cooldown, append(base, opts...)...)
}

// BreakerFailure counts every error but ErrCacheMiss (the cache working) and
// Canceled; DeadlineExceeded must count, being how an unreachable Valkey presents.
func BreakerFailure(extra ...error) func(error) bool {
	return circuitbreaker.FailureExcept(append([]error{ErrCacheMiss}, extra...)...)
}

// Breakered fences every data method of a Client; Close is not fenced, or an
// open breaker would leak the pool on shutdown. A nil client stays nil.
func Breakered(c Client, b *circuitbreaker.Breaker) Client {
	if c == nil {
		return nil
	}
	return &breakeredClient{inner: c, breaker: b}
}

type breakeredClient struct {
	inner   Client
	breaker *circuitbreaker.Breaker
}

func (c *breakeredClient) Get(ctx context.Context, key string) (string, error) {
	return circuitbreaker.Do1(c.breaker, func() (string, error) { return c.inner.Get(ctx, key) })
}

func (c *breakeredClient) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	return circuitbreaker.Do1(c.breaker, func() (map[string]string, error) { return c.inner.MGet(ctx, keys) })
}

func (c *breakeredClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.breaker.Do(func() error { return c.inner.Set(ctx, key, value, ttl) })
}

func (c *breakeredClient) MSet(ctx context.Context, entries []KV, ttl time.Duration) error {
	return c.breaker.Do(func() error { return c.inner.MSet(ctx, entries, ttl) })
}

func (c *breakeredClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return circuitbreaker.Do1(c.breaker, func() (bool, error) { return c.inner.SetNX(ctx, key, value, ttl) })
}

func (c *breakeredClient) IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	return circuitbreaker.Do1(c.breaker, func() (int64, error) { return c.inner.IncrEx(ctx, key, ttl) })
}

func (c *breakeredClient) Del(ctx context.Context, keys ...string) error {
	return c.breaker.Do(func() error { return c.inner.Del(ctx, keys...) })
}

func (c *breakeredClient) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return circuitbreaker.Do1(c.breaker, func() (bool, error) { return c.inner.Expire(ctx, key, ttl) })
}

func (c *breakeredClient) Close() error { return c.inner.Close() }
