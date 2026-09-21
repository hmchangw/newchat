package valkeyutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// The knobs ride inside Config rather than as a second field each service
// mounts. Every Valkey consumer already mounts Config, so nesting is what makes
// fencing arrive everywhere without fourteen copies of the same two env tags —
// which is the duplication CLAUDE.md's shared-knob rule exists to prevent.
func TestConfig_CarriesBreakerKnobs(t *testing.T) {
	t.Setenv("VALKEY_ADDRS", "valkey:6379")

	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, 5, cfg.Breaker.Fails, "default budget arrives without a service declaring it")
	assert.Equal(t, 10*time.Second, cfg.Breaker.Cooldown)

	t.Setenv("VALKEY_BREAKER_FAILS", "3")
	t.Setenv("VALKEY_BREAKER_COOLDOWN", "45s")
	cfg, err = env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, 3, cfg.Breaker.Fails)
	assert.Equal(t, 45*time.Second, cfg.Breaker.Cooldown)
}

// Validate is opt-in — only three of the fourteen consumers call it — so it
// covers the breaker where it runs, and New below covers the rest.
func TestConfig_ValidateCoversTheBreaker(t *testing.T) {
	good := Config{Addrs: []string{"valkey:6379"}, Breaker: BreakerConfig{Fails: 5, Cooldown: time.Second}}
	assert.NoError(t, good.Validate())

	bad := Config{Addrs: []string{"valkey:6379"}, Breaker: BreakerConfig{Fails: -1}}
	err := bad.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VALKEY_BREAKER_FAILS")
}

// A negative budget must degrade to today's behaviour — no fencing — rather
// than to something undefined. Eleven of the fourteen consumers never call
// Validate, so New is the only place that sees every bad value.
func TestBreakerConfig_NegativeValuesDisableFencingRatherThanMisbehave(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("valkey down")

	for _, tc := range []struct {
		name string
		cfg  BreakerConfig
	}{
		{"negative fails", BreakerConfig{Fails: -1, Cooldown: time.Second}},
		{"negative cooldown", BreakerConfig{Fails: 1, Cooldown: -time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := &countingClient{err: boom}
			c := Breakered(inner, tc.cfg.New(ctx, "t:"+tc.name))

			for range 5 {
				_, err := c.Get(ctx, "k")
				require.ErrorIs(t, err, boom, "calls must pass through, never be fenced by a bad config")
			}
			assert.Equal(t, 5, inner.calls, "every call still reached Valkey")
		})
	}
}

// The happy path still fences, so the defensive clamp above cannot be a blanket
// "never fence" that silently disables the feature everywhere.
func TestBreakerConfig_ValidConfigStillFences(t *testing.T) {
	inner := &countingClient{err: errors.New("valkey down")}
	c := Breakered(inner, BreakerConfig{Fails: 1, Cooldown: time.Minute}.New(context.Background(), "t:valid"))

	_, err := c.Get(context.Background(), "k")
	require.Error(t, err)
	_, err = c.Get(context.Background(), "k")
	require.ErrorIs(t, err, circuitbreaker.ErrOpen)
	assert.Equal(t, 1, inner.calls)
}
