package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestParsePresenceConfig_Defaults(t *testing.T) {
	cfg, err := parsePresenceConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 2000, 5000, 10000, 20000, 50000, 100000}, cfg.Steps)
	assert.Equal(t, 30*time.Second, cfg.Heartbeat)
	assert.True(t, cfg.StopOnTrip)
}

func TestParsePresenceConfig_Steps(t *testing.T) {
	cfg, err := parsePresenceConfig([]string{"--steps=1k,2k,5k", "--hold=10s"})
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 2000, 5000}, cfg.Steps)
	assert.Equal(t, 10*time.Second, cfg.Hold)
}

// stubPresenceFactory drives the ramp loop without a broker: onActivated is a
// no-op (so no real emitter/pool is needed) and afterReset seeds healthy
// latency samples *after* runStepPresence calls collector.Reset(), so they
// survive into the verdict.
type stubPresenceFactory struct{ built *presenceEnv }

//nolint:gocritic // cfg passed by value to satisfy presenceFactory interface
func (f *stubPresenceFactory) Build(cfg presenceConfig) *presenceEnv {
	c := newPresenceCollector()
	env := &presenceEnv{
		collector:   c,
		thresholds:  defaultPresenceThresholds(),
		siteID:      "site-local",
		users:       make([]*presenceUser, slicesMaxInt(cfg.Steps)),
		onActivated: func(env *presenceEnv, idx int) {},
		afterReset: func(env *presenceEnv) {
			t0 := time.Unix(0, 0)
			for i := 0; i < 50; i++ {
				acc := presenceAccount(i)
				env.collector.Expect(acc, model.StatusOnline, t0)
				env.collector.Observe(acc, model.StatusOnline, t0.Add(10*time.Millisecond))
			}
		},
	}
	for i := range env.users {
		env.users[i] = newPresenceUser(i, env.siteID)
	}
	f.built = env
	return env
}

func TestRunPresenceSustained_StubRamp(t *testing.T) {
	cfg := presenceConfig{Steps: []int{10, 20}, StopOnTrip: false}
	results, err := runPresenceSustainedForTest(context.Background(), cfg, &stubPresenceFactory{})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, 10, results[0].N)
	assert.Equal(t, verdictPass, results[0].Kind)
}
