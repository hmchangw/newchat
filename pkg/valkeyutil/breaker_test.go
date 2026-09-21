package valkeyutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// countingClient records how many data calls reached the wire, which is the
// only thing that distinguishes a fenced call from a fast failure.
type countingClient struct {
	calls int
	err   error
}

func (c *countingClient) hit() error { c.calls++; return c.err }

func (c *countingClient) Get(context.Context, string) (string, error) { return "", c.hit() }
func (c *countingClient) MGet(context.Context, []string) (map[string]string, error) {
	return nil, c.hit()
}
func (c *countingClient) Set(context.Context, string, string, time.Duration) error { return c.hit() }
func (c *countingClient) MSet(context.Context, []KV, time.Duration) error          { return c.hit() }
func (c *countingClient) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return false, c.hit()
}
func (c *countingClient) IncrEx(context.Context, string, time.Duration) (int64, error) {
	return 0, c.hit()
}
func (c *countingClient) Del(context.Context, ...string) error { return c.hit() }
func (c *countingClient) Expire(context.Context, string, time.Duration) (bool, error) {
	return false, c.hit()
}
func (c *countingClient) Close() error { return c.hit() }

// every data method on the Client surface, so a decorator that forgets one is
// caught here rather than by an unbounded call in production. Close is
// deliberately absent — see TestBreakered_CloseBypassesTheBreaker.
var breakeredOps = map[string]func(context.Context, Client) error{
	"Get":  func(ctx context.Context, c Client) error { _, err := c.Get(ctx, "k"); return err },
	"MGet": func(ctx context.Context, c Client) error { _, err := c.MGet(ctx, []string{"k"}); return err },
	"Set":  func(ctx context.Context, c Client) error { return c.Set(ctx, "k", "v", time.Minute) },
	"MSet": func(ctx context.Context, c Client) error {
		return c.MSet(ctx, []KV{{Key: "k", Value: "v"}}, time.Minute)
	},
	"SetNX":  func(ctx context.Context, c Client) error { _, err := c.SetNX(ctx, "k", "v", time.Minute); return err },
	"IncrEx": func(ctx context.Context, c Client) error { _, err := c.IncrEx(ctx, "k", time.Minute); return err },
	"Del":    func(ctx context.Context, c Client) error { return c.Del(ctx, "k") },
	"Expire": func(ctx context.Context, c Client) error { _, err := c.Expire(ctx, "k", time.Minute); return err },
}

// The point of the breaker: once open, a call costs nothing at all rather than
// a CallBudget. Under a sustained outage every request would otherwise hold its
// handler slot for the budget on every tier it touches, and a service's
// concurrency ceiling — not its error handling — is what then sheds traffic it
// could have served from the source of truth.
func TestBreakered_OpenBreakerFencesEveryOperation(t *testing.T) {
	for name, op := range breakeredOps {
		t.Run(name, func(t *testing.T) {
			inner := &countingClient{err: errors.New("valkey down")}
			b := circuitbreaker.New(1, time.Minute, circuitbreaker.WithFailurePredicate(BreakerFailure()))
			c := Breakered(inner, b)
			ctx := context.Background()

			require.Error(t, op(ctx, c), "first call fails and trips the breaker")
			require.Equal(t, 1, inner.calls)
			require.Equal(t, circuitbreaker.StateOpen, b.State())

			err := op(ctx, c)
			require.ErrorIs(t, err, circuitbreaker.ErrOpen)
			assert.Equal(t, 1, inner.calls, "a fenced call must not reach Valkey")
		})
	}
}

// A miss is the cache working, not the cache failing. Counting it would open the
// breaker on a cold keyspace and disable a perfectly healthy tier.
func TestBreakered_CacheMissNeitherTripsNorIsRewritten(t *testing.T) {
	inner := &countingClient{err: ErrCacheMiss}
	b := circuitbreaker.New(2, time.Minute, circuitbreaker.WithFailurePredicate(BreakerFailure()))
	c := Breakered(inner, b)

	for range 10 {
		_, err := c.Get(context.Background(), "absent")
		require.ErrorIs(t, err, ErrCacheMiss, "the miss sentinel must reach the caller unchanged")
	}
	assert.Equal(t, circuitbreaker.StateClosed, b.State(), "misses must not trip the breaker")
	assert.Equal(t, 10, inner.calls)
}

// A caller abandoning its own request says nothing about Valkey's health. This
// asymmetry is load-bearing and easy to get backwards: Canceled is exempt,
// DeadlineExceeded is NOT — a deadline is how an unreachable Valkey presents.
func TestBreakered_CallerCancellationDoesNotTrip(t *testing.T) {
	inner := &countingClient{err: context.Canceled}
	b := circuitbreaker.New(1, time.Minute, circuitbreaker.WithFailurePredicate(BreakerFailure()))
	c := Breakered(inner, b)

	for range 5 {
		_, _ = c.Get(context.Background(), "k")
	}
	assert.Equal(t, circuitbreaker.StateClosed, b.State(), "a cancelled caller is not an unhealthy Valkey")

	inner.err = context.DeadlineExceeded
	_, _ = c.Get(context.Background(), "k")
	assert.Equal(t, circuitbreaker.StateOpen, b.State(), "a timeout is how an unreachable Valkey presents")
}

// Close is lifecycle, not a data call: fencing it during an outage would leak
// the pool on shutdown, exactly when the breaker is most likely to be open.
func TestBreakered_CloseBypassesTheBreaker(t *testing.T) {
	inner := &countingClient{err: errors.New("valkey down")}
	b := circuitbreaker.New(1, time.Minute, circuitbreaker.WithFailurePredicate(BreakerFailure()))
	c := Breakered(inner, b)

	_, _ = c.Get(context.Background(), "k")
	require.Equal(t, circuitbreaker.StateOpen, b.State())

	inner.err = nil
	require.NoError(t, c.Close())
	assert.Equal(t, 2, inner.calls, "Close must reach the client even with the breaker open")
}

// A nil breaker is "fencing off", matching Breaker.Do's own nil handling, so a
// deployment that disables it wires the same code path.
func TestBreakered_NilBreakerIsPassThrough(t *testing.T) {
	inner := &countingClient{}
	c := Breakered(inner, nil)
	require.NoError(t, c.Set(context.Background(), "k", "v", time.Minute))
	assert.Equal(t, 1, inner.calls)

	assert.Nil(t, Breakered(nil, nil), "a nil client stays nil — every tier treats that as disabled")
}

func TestValkeyBreakerConfig_Validate(t *testing.T) {
	assert.NoError(t, BreakerConfig{Fails: 5, Cooldown: 10 * time.Second}.Validate(""))
	assert.NoError(t, BreakerConfig{}.Validate(""), "zero means no fencing, which is a legal choice")

	err := BreakerConfig{Fails: -1}.Validate("HISTORY_")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_VALKEY_BREAKER_FAILS",
		"the message must name the variable the operator actually set")

	err = BreakerConfig{Cooldown: -time.Second}.Validate("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VALKEY_BREAKER_COOLDOWN")
}
