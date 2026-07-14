package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func presenceTestThresholds() presenceThresholds {
	return presenceThresholds{P95Ms: 200, P99Ms: 500, ErrorRate: 0.01, GCPauseInconclusive: 50}
}

func TestEvaluatePresenceStep_Pass(t *testing.T) {
	in := presenceStepInputs{
		N: 1000, EffectiveN: 1000,
		LatencySamples: []float64{10, 20, 30, 40, 50},
		Attempted:      500, Failed: 0,
	}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictPass, r.Kind)
}

func TestEvaluatePresenceStep_TripLatency(t *testing.T) {
	samples := make([]float64, 100)
	for i := range samples {
		samples[i] = 600 // all over p99 cap
	}
	in := presenceStepInputs{N: 1000, EffectiveN: 1000, LatencySamples: samples, Attempted: 100}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
	assert.NotEmpty(t, r.Reasons)
}

func TestEvaluatePresenceStep_TripErrorRate(t *testing.T) {
	in := presenceStepInputs{
		N: 1000, EffectiveN: 1000,
		LatencySamples: []float64{10}, Attempted: 100, Failed: 5, // 5% > 1%
	}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
}

func TestEvaluatePresenceStep_InconclusiveGC(t *testing.T) {
	in := presenceStepInputs{
		N: 1000, EffectiveN: 1000, LatencySamples: []float64{10}, Attempted: 100,
		Self: SelfMetrics{GCPauseP99Ms: 80},
	}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
}

func TestEvaluatePresenceStep_InconclusiveZeroAttempts(t *testing.T) {
	in := presenceStepInputs{N: 1000, EffectiveN: 1000, Attempted: 0}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
}

func TestEvaluatePresenceStep_InconclusiveActivationShortfall(t *testing.T) {
	in := presenceStepInputs{
		N: 1000, EffectiveN: 800, // 80% < 95%
		LatencySamples: []float64{10}, Attempted: 100,
	}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
}

func stormTestThresholds() stormThresholds {
	return stormThresholds{RecoverySLO: 10 * time.Second, P99Ms: 1000, ErrorRate: 0.05, GCPauseInconclusive: 50}
}

func TestEvaluateStormStep_Pass(t *testing.T) {
	in := stormStepInputs{
		Fraction: 0.5, StormUsers: 500,
		RecoveryComplete: true, RecoveryElapsed: 3 * time.Second,
		SpikeLatencyMs: []float64{100, 200, 300}, Attempted: 500, Failed: 0,
	}
	r := evaluateStormStep(in, stormTestThresholds())
	assert.Equal(t, verdictPass, r.Kind)
}

func TestEvaluateStormStep_TripSlowRecovery(t *testing.T) {
	in := stormStepInputs{
		Fraction: 1.0, StormUsers: 1000,
		RecoveryComplete: true, RecoveryElapsed: 20 * time.Second,
		SpikeLatencyMs: []float64{100}, Attempted: 1000,
	}
	r := evaluateStormStep(in, stormTestThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
}

func TestEvaluateStormStep_TripIncompleteRecovery(t *testing.T) {
	in := stormStepInputs{
		Fraction: 1.0, StormUsers: 1000, RecoveryComplete: false,
		RecoveryRemaining: 12, SpikeLatencyMs: []float64{100}, Attempted: 1000,
	}
	r := evaluateStormStep(in, stormTestThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
}

func TestEvaluateStormStep_InconclusiveGC(t *testing.T) {
	in := stormStepInputs{
		Fraction: 0.5, StormUsers: 500, RecoveryComplete: true, RecoveryElapsed: time.Second,
		SpikeLatencyMs: []float64{10}, Attempted: 500, Self: SelfMetrics{GCPauseP99Ms: 80},
	}
	r := evaluateStormStep(in, stormTestThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
}
