package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStormConfig_Defaults(t *testing.T) {
	cfg, err := parseStormConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, 10000, cfg.Users)
	assert.Equal(t, []float64{0.10, 0.25, 0.50, 1.0}, cfg.StormSteps)
	assert.Equal(t, "graceful", cfg.Mode)
}

func TestParseStormConfig_Parsed(t *testing.T) {
	cfg, err := parseStormConfig([]string{"--users=500", "--storm-steps=0.5,1.0", "--storm-mode=silent"})
	require.NoError(t, err)
	assert.Equal(t, 500, cfg.Users)
	assert.Equal(t, []float64{0.5, 1.0}, cfg.StormSteps)
	assert.Equal(t, "silent", cfg.Mode)
}

func TestParseStormConfig_RejectsBadFraction(t *testing.T) {
	_, err := parseStormConfig([]string{"--storm-steps=1.5"})
	assert.Error(t, err)
}

func TestParseStormConfig_RejectsBadMode(t *testing.T) {
	_, err := parseStormConfig([]string{"--storm-mode=nope"})
	assert.Error(t, err)
}

func TestStormUserCount(t *testing.T) {
	assert.Equal(t, 250, stormUserCount(0.25, 1000))
	assert.Equal(t, 1000, stormUserCount(1.0, 1000))
	assert.Equal(t, 1, stormUserCount(0.0001, 1000)) // at least 1 when fraction>0
}

func TestParseFractionList(t *testing.T) {
	got, err := parseFractionList("0.1,0.25,1.0")
	require.NoError(t, err)
	assert.Equal(t, []float64{0.1, 0.25, 1.0}, got)
}

type stubStormFactory struct{}

//nolint:gocritic // hugeParam: cfg by value to satisfy stormFactory interface
func (stubStormFactory) Build(cfg stormConfig) *stormEnv {
	return &stormEnv{
		collector:  newPresenceCollector(),
		thresholds: defaultStormThresholds(),
		cfg:        cfg,
		runStorm: func(ctx context.Context, env *stormEnv, frac float64) stormStepInputs {
			// Healthy below 1.0, slow recovery at 1.0.
			rec := 2 * time.Second
			if frac >= 1.0 {
				rec = 30 * time.Second
			}
			return stormStepInputs{
				Fraction: frac, StormUsers: int(frac * 1000),
				RecoveryComplete: true, RecoveryElapsed: rec,
				SpikeLatencyMs: []float64{100}, Attempted: 1000, Failed: 0,
			}
		},
	}
}

func TestRunPresenceStorm_StubRamp(t *testing.T) {
	cfg := stormConfig{StormSteps: []float64{0.5, 1.0}, StopOnTrip: true, Settle: 0}
	results, err := runPresenceStormForTest(context.Background(), cfg, stubStormFactory{})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, verdictPass, results[0].Kind)
	assert.Equal(t, verdictTrip, results[1].Kind)
}
