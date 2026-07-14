package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCapacityConfig_Defaults(t *testing.T) {
	cfg, err := parseCapacityConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, []int{10000, 20000, 50000, 100000, 200000}, cfg.Steps)
	assert.Equal(t, 30*time.Second, cfg.Warmup)
	assert.Equal(t, 120*time.Second, cfg.Hold)
	assert.Equal(t, 30*time.Second, cfg.Heartbeat)
	assert.InDelta(t, 0.001, cfg.FalseOfflineRate, 1e-9)
	assert.InDelta(t, 0.10, cfg.PingTolerance, 1e-9)
	assert.True(t, cfg.StopOnTrip)
}

func TestParseCapacityConfig_StepsShorthandAndOverrides(t *testing.T) {
	cfg, err := parseCapacityConfig([]string{"--steps=1k,2k", "--hold=10s", "--false-offline-rate=0.05"})
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 2000}, cfg.Steps)
	assert.Equal(t, 10*time.Second, cfg.Hold)
	assert.InDelta(t, 0.05, cfg.FalseOfflineRate, 1e-9)
}

func TestParseCapacityConfig_BadSteps(t *testing.T) {
	_, err := parseCapacityConfig([]string{"--steps=abc"})
	require.Error(t, err)
}

func TestRunStepCapacity_PassPath(t *testing.T) {
	c := newPresenceCollector()
	users := make([]*presenceUser, 100)
	for i := range users {
		users[i] = newPresenceUser(i, "site-test")
	}
	env := &capacityEnv{
		collector: c, users: users,
		thresholds: defaultCapacityThresholds(),
		warmup:     0, hold: 0, cooldown: 0, heartbeat: 30 * time.Second,
	}
	// Activation seam: simulate each hello resolving to online quickly.
	env.onActivated = func(e *capacityEnv, idx int) {
		sentAt := time.Now()
		e.collector.Expect(users[idx].account, "online", sentAt)
		e.collector.Observe(users[idx].account, "online", sentAt.Add(10*time.Millisecond))
	}
	env.afterReset = func(e *capacityEnv) {}

	r := runStepCapacity(context.Background(), env, 100, 0)
	assert.Equal(t, verdictPass, r.Kind)
	assert.Equal(t, 100, r.EffectiveN)
	assert.InDelta(t, 10, r.ConnectP50Ms, 5)
}

func TestRunStepCapacity_FalseOfflineTrips(t *testing.T) {
	c := newPresenceCollector()
	users := make([]*presenceUser, 100)
	for i := range users {
		users[i] = newPresenceUser(i, "site-test")
	}
	env := &capacityEnv{
		collector: c, users: users,
		thresholds: defaultCapacityThresholds(),
		warmup:     0, hold: 0, cooldown: 0, heartbeat: 30 * time.Second,
	}
	env.onActivated = func(e *capacityEnv, idx int) {
		sentAt := time.Now()
		e.collector.Expect(users[idx].account, "online", sentAt)
		e.collector.Observe(users[idx].account, "online", sentAt.Add(5*time.Millisecond))
	}
	// During the hold, the service falsely sweeps 10 users offline.
	env.afterReset = func(e *capacityEnv) {
		for i := 0; i < 10; i++ {
			e.collector.Observe(users[i].account, "offline", time.Now())
		}
	}
	r := runStepCapacity(context.Background(), env, 100, 0)
	assert.Equal(t, verdictTrip, r.Kind)
	assert.Equal(t, 10, r.FalseOfflines)
}
