package valkeyutil

import (
	"context"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// BreakerConfig is an env-tagged Valkey circuit-breaker configuration, the
// companion to Profile: the profile bounds how long one call waits, this bounds
// how many calls pay that wait before the tier stops trying.
//
// Profile.CallBudget makes each payment survivable; this makes the payments
// rare. Neither replaces the other — the budget still governs the calls before
// the breaker opens, and every half-open probe after it does.
//
// Add it as a named field, call Validate() during config load, and build one
// breaker PER TIER with New. A service that prefixes its knobs puts an
// envPrefix on the field, so HISTORY_ reads HISTORY_VALKEY_BREAKER_FAILS.
type BreakerConfig struct {
	// Fails is the consecutive-failure budget before the breaker opens. 0
	// disables fencing entirely: calls always pass through.
	Fails int `env:"VALKEY_BREAKER_FAILS" envDefault:"5"`
	// Cooldown is how long an open breaker fences calls before admitting one
	// half-open probe. Keep it generous: a probe against a still-dead Valkey
	// costs a full CallBudget, so a short cooldown reinstates the tax the
	// breaker exists to remove.
	Cooldown time.Duration `env:"VALKEY_BREAKER_COOLDOWN" envDefault:"10s"`
}

// Validate rejects negative values. Zero is legal for both and means "no
// fencing"; negative means nothing.
//
// envPrefix is the field's own envPrefix ("" when unprefixed), so the message
// names the variable the operator actually set.
func (b BreakerConfig) Validate(envPrefix string) error {
	if b.Fails < 0 {
		return fmt.Errorf("%sVALKEY_BREAKER_FAILS must be >= 0, got %d", envPrefix, b.Fails)
	}
	if b.Cooldown < 0 {
		return fmt.Errorf("%sVALKEY_BREAKER_COOLDOWN must be >= 0, got %s", envPrefix, b.Cooldown)
	}
	return nil
}

// New builds a breaker from this config, reporting under name on the shared
// state gauge. Give each tier its own name: one breaker shared across tiers
// means a single tier with oversized values — roomsubcache accepts blobs up to
// DefaultMaxValueBytes — can time out against a healthy Valkey and switch the
// cache off for every other consumer, dumping their read load onto MongoDB.
//
// BreakerFailure is applied unless the caller supplies its own predicate, since
// unlike a Mongo store there is no per-call-site question here: a cache miss is
// never evidence of an unwell Valkey.
func (b BreakerConfig) New(ctx context.Context, name string, opts ...circuitbreaker.Option) *circuitbreaker.Breaker {
	base := []circuitbreaker.Option{
		circuitbreaker.Tracked(ctx, name),
		circuitbreaker.WithFailurePredicate(BreakerFailure()),
	}
	return circuitbreaker.New(b.Fails, b.Cooldown, append(base, opts...)...)
}

// BreakerFailure is the failure predicate for a Valkey tier: every error counts
// except ErrCacheMiss and the caller's own healthy-absence sentinels.
//
// A miss is the cache working. Counting it would open the breaker against a cold
// keyspace and disable a tier that is behaving perfectly.
//
// The asymmetry it inherits from FailureExcept is the load-bearing part and is
// easy to get backwards: context.Canceled is exempt, because a caller
// abandoning its request says nothing about Valkey — while DeadlineExceeded is
// NOT, because a deadline is precisely how an unreachable Valkey presents.
func BreakerFailure(extra ...error) func(error) bool {
	return circuitbreaker.FailureExcept(append([]error{ErrCacheMiss}, extra...)...)
}

// Breakered fences a Client behind a breaker, so a tier whose Valkey is down
// stops paying for it on every call.
//
// Every data method is fenced; Close is not. Close is lifecycle rather than a
// data call, and fencing it would leak the pool on shutdown exactly when the
// breaker is most likely to be open.
//
// A nil client stays nil — every tier already reads that as "no L2" — and a nil
// breaker passes through, matching Breaker.Do's own nil handling, so a
// deployment with fencing disabled runs the same code path.
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
