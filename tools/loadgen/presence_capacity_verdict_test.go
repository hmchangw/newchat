package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func capThresholds() capacityThresholds {
	return capacityThresholds{
		ConnectP95Ms: 500, ConnectP99Ms: 1000,
		FalseOfflineRate: 0.001, ErrorRate: 0.01,
		PingTolerance: 0.10, GCPauseInconclusive: 50,
	}
}

func baseCapInputs() capacityStepInputs {
	return capacityStepInputs{
		N: 1000, EffectiveN: 1000,
		StartedAt: time.Now(), HoldDuration: time.Minute,
		ConnectLatencyMs: []float64{10, 20, 30, 40, 50},
		ConnectAttempted: 1000, ConnectFailed: 0,
		FalseOfflines: 0,
		PingsSent:     2000, PingsRequired: 2000,
		Self: SelfMetrics{GCPauseP99Ms: 5},
	}
}

func TestEvaluateCapacityStep_Pass(t *testing.T) {
	r := evaluateCapacityStep(baseCapInputs(), capThresholds())
	assert.Equal(t, verdictPass, r.Kind)
	assert.Empty(t, r.Reasons)
}

func TestEvaluateCapacityStep_GCInconclusive(t *testing.T) {
	in := baseCapInputs()
	in.Self.GCPauseP99Ms = 75
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "gc pause")
}

func TestEvaluateCapacityStep_ZeroAttemptsInconclusive(t *testing.T) {
	in := baseCapInputs()
	in.ConnectAttempted = 0
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "zero hellos")
}

func TestEvaluateCapacityStep_ActivationShortfallInconclusive(t *testing.T) {
	in := baseCapInputs()
	in.EffectiveN = 800 // 80% < 95%
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "activated")
}

func TestEvaluateCapacityStep_PingShortfallInconclusive(t *testing.T) {
	in := baseCapInputs()
	in.PingsSent = 1500 // 75% < 90%
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "ping sustainability")
}

func TestEvaluateCapacityStep_PingShortfallBeatsFalseOffline(t *testing.T) {
	// A load-box-induced ping shortfall must read INCONCLUSIVE, never TRIP,
	// even when false offlines also occurred.
	in := baseCapInputs()
	in.PingsSent = 1500    // shortfall
	in.FalseOfflines = 500 // would otherwise TRIP
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "ping sustainability")
}

func TestEvaluateCapacityStep_FalseOfflineTrip(t *testing.T) {
	in := baseCapInputs()
	in.FalseOfflines = 5 // rate 0.005 exceeds threshold 0.001
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
	assert.Contains(t, r.Reasons[0], "false_offline_rate")
}

func TestEvaluateCapacityStep_ConnectLatencyTrip(t *testing.T) {
	in := baseCapInputs()
	in.ConnectLatencyMs = []float64{600, 700, 800, 900, 1200}
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
	// Both p95 and p99 caps exceeded.
	assert.Len(t, r.Reasons, 2)
}

func TestEvaluateCapacityStep_ConnectErrorTrip(t *testing.T) {
	in := baseCapInputs()
	in.ConnectFailed = 50 // rate 0.05 exceeds threshold 0.01
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
	assert.Contains(t, r.Reasons[0], "connect error_rate")
}

func TestEvaluateCapacityStep_PropagatesSelf(t *testing.T) {
	in := baseCapInputs()
	in.Self = SelfMetrics{GCPauseP99Ms: 7, Goroutines: 123}
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, in.Self, r.Self)
}
