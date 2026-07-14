package main

import (
	"fmt"
	"time"
)

// presenceThresholds are the sustained-mode SLO cutoffs.
type presenceThresholds struct {
	P95Ms               float64
	P99Ms               float64
	ErrorRate           float64 // fraction (0.01 = 1%)
	GCPauseInconclusive float64 // loadgen self-saturation guard (ms)
}

func defaultPresenceThresholds() presenceThresholds {
	return presenceThresholds{P95Ms: 200, P99Ms: 500, ErrorRate: 0.01, GCPauseInconclusive: 50}
}

// presenceStepInputs is everything evaluatePresenceStep needs for one N step.
type presenceStepInputs struct {
	N              int
	EffectiveN     int
	StartedAt      time.Time
	HoldDuration   time.Duration
	LatencySamples []float64 // ms, transition -> observed publish
	Attempted      int64
	Failed         int64
	Self           SelfMetrics
}

// presenceStepResult is the sustained verdict for one N step.
type presenceStepResult struct {
	N            int
	EffectiveN   int
	StartedAt    time.Time
	HoldDuration time.Duration
	P50Ms        float64
	P95Ms        float64
	P99Ms        float64
	ErrorRate    float64
	Attempted    int64
	Failed       int64
	Self         SelfMetrics
	Kind         verdictKind
	Reasons      []string
}

//nolint:gocritic // hugeParam: pure-function copy cost is negligible per step.
func evaluatePresenceStep(in presenceStepInputs, th presenceThresholds) presenceStepResult {
	r := presenceStepResult{
		N: in.N, EffectiveN: in.EffectiveN,
		StartedAt: in.StartedAt, HoldDuration: in.HoldDuration,
		Attempted: in.Attempted, Failed: in.Failed, Self: in.Self,
		P50Ms: percentile(in.LatencySamples, 0.50),
		P95Ms: percentile(in.LatencySamples, 0.95),
		P99Ms: percentile(in.LatencySamples, 0.99),
	}
	if in.Attempted > 0 {
		r.ErrorRate = float64(in.Failed) / float64(in.Attempted)
	}

	// INCONCLUSIVE overrides PASS/TRIP.
	if in.Self.GCPauseP99Ms > th.GCPauseInconclusive {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: loadgen gc pause p99=%.1fms > %.0f", in.Self.GCPauseP99Ms, th.GCPauseInconclusive)}
		return r
	}
	if in.Attempted == 0 {
		r.Kind = verdictInconclusive
		r.Reasons = []string{"inconclusive: zero transitions attempted (publisher down or emitters not wired)"}
		return r
	}
	if in.N > 0 && in.EffectiveN > 0 && float64(in.EffectiveN)/float64(in.N) < 0.95 {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: only %d/%d users activated", in.EffectiveN, in.N)}
		return r
	}

	var reasons []string
	if r.P95Ms > th.P95Ms {
		reasons = append(reasons, fmt.Sprintf("p95=%.0fms > %.0f", r.P95Ms, th.P95Ms))
	}
	if r.P99Ms > th.P99Ms {
		reasons = append(reasons, fmt.Sprintf("p99=%.0fms > %.0f", r.P99Ms, th.P99Ms))
	}
	if r.ErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("error_rate=%.4f > %.4f", r.ErrorRate, th.ErrorRate))
	}
	if len(reasons) > 0 {
		r.Kind = verdictTrip
		r.Reasons = reasons
		return r
	}
	r.Kind = verdictPass
	return r
}

// stormThresholds are the per-storm-step SLO cutoffs.
type stormThresholds struct {
	RecoverySLO         time.Duration
	P99Ms               float64
	ErrorRate           float64
	GCPauseInconclusive float64
}

func defaultStormThresholds() stormThresholds {
	return stormThresholds{RecoverySLO: 10 * time.Second, P99Ms: 1000, ErrorRate: 0.05, GCPauseInconclusive: 50}
}

type stormStepInputs struct {
	Fraction          float64
	StormUsers        int
	RecoveryComplete  bool
	RecoveryElapsed   time.Duration
	RecoveryRemaining int
	SpikeLatencyMs    []float64
	Attempted         int64
	Failed            int64
	Self              SelfMetrics
}

type stormStepResult struct {
	Fraction         float64
	StormUsers       int
	RecoveryComplete bool
	RecoveryMs       float64
	P99Ms            float64
	ErrorRate        float64
	Kind             verdictKind
	Reasons          []string
}

//nolint:gocritic // hugeParam: pure-function copy cost negligible per step.
func evaluateStormStep(in stormStepInputs, th stormThresholds) stormStepResult {
	r := stormStepResult{
		Fraction: in.Fraction, StormUsers: in.StormUsers,
		RecoveryComplete: in.RecoveryComplete,
		RecoveryMs:       float64(in.RecoveryElapsed.Milliseconds()),
		P99Ms:            percentile(in.SpikeLatencyMs, 0.99),
	}
	if in.Attempted > 0 {
		r.ErrorRate = float64(in.Failed) / float64(in.Attempted)
	}
	if in.Self.GCPauseP99Ms > th.GCPauseInconclusive {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: loadgen gc pause p99=%.1fms > %.0f", in.Self.GCPauseP99Ms, th.GCPauseInconclusive)}
		return r
	}
	var reasons []string
	if !in.RecoveryComplete {
		reasons = append(reasons, fmt.Sprintf("recovery incomplete: %d users never observed back online within SLO", in.RecoveryRemaining))
	} else if in.RecoveryElapsed > th.RecoverySLO {
		reasons = append(reasons, fmt.Sprintf("recovery=%s > %s", in.RecoveryElapsed.Round(time.Millisecond), th.RecoverySLO))
	}
	if r.P99Ms > th.P99Ms {
		reasons = append(reasons, fmt.Sprintf("spike p99=%.0fms > %.0f", r.P99Ms, th.P99Ms))
	}
	if r.ErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("error_rate=%.4f > %.4f", r.ErrorRate, th.ErrorRate))
	}
	if len(reasons) > 0 {
		r.Kind = verdictTrip
		r.Reasons = reasons
		return r
	}
	r.Kind = verdictPass
	return r
}
