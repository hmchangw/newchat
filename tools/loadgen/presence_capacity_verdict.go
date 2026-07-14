package main

import (
	"fmt"
	"time"
)

// capacityThresholds are the presence-capacity SLO cutoffs.
type capacityThresholds struct {
	ConnectP95Ms        float64
	ConnectP99Ms        float64
	FalseOfflineRate    float64 // fraction of N swept offline (TRIP)
	ErrorRate           float64 // connect (hello) error-rate cap
	PingTolerance       float64 // ping-sustainability shortfall band (INCONCLUSIVE)
	GCPauseInconclusive float64 // loadgen self-saturation guard (ms)
}

func defaultCapacityThresholds() capacityThresholds {
	return capacityThresholds{
		ConnectP95Ms: 500, ConnectP99Ms: 1000,
		FalseOfflineRate: 0.001, ErrorRate: 0.01,
		PingTolerance: 0.10, GCPauseInconclusive: 50,
	}
}

// capacityStepInputs is everything evaluateCapacityStep needs for one N step.
type capacityStepInputs struct {
	N                int
	EffectiveN       int
	StartedAt        time.Time
	HoldDuration     time.Duration
	ConnectLatencyMs []float64 // hello -> online round-trips, captured at activation
	ConnectAttempted int64
	ConnectFailed    int64
	FalseOfflines    int
	PingsSent        int64
	PingsRequired    int64
	Self             SelfMetrics
}

// capacityStepResult is the graded verdict for one N step.
type capacityStepResult struct {
	N                int
	EffectiveN       int
	StartedAt        time.Time
	HoldDuration     time.Duration
	ConnectP50Ms     float64
	ConnectP95Ms     float64
	ConnectP99Ms     float64
	ConnectErrorRate float64
	FalseOfflines    int
	FalseOfflineRate float64
	PingSustain      float64 // PingsSent / PingsRequired
	Self             SelfMetrics
	Kind             verdictKind
	Reasons          []string
}

//nolint:gocritic // hugeParam: pure-function copy cost is negligible per step.
func evaluateCapacityStep(in capacityStepInputs, th capacityThresholds) capacityStepResult {
	r := capacityStepResult{
		N: in.N, EffectiveN: in.EffectiveN,
		StartedAt: in.StartedAt, HoldDuration: in.HoldDuration,
		FalseOfflines: in.FalseOfflines,
		Self:          in.Self,
		ConnectP50Ms:  percentile(in.ConnectLatencyMs, 0.50),
		ConnectP95Ms:  percentile(in.ConnectLatencyMs, 0.95),
		ConnectP99Ms:  percentile(in.ConnectLatencyMs, 0.99),
	}
	if in.ConnectAttempted > 0 {
		r.ConnectErrorRate = float64(in.ConnectFailed) / float64(in.ConnectAttempted)
	}
	if in.N > 0 {
		r.FalseOfflineRate = float64(in.FalseOfflines) / float64(in.N)
	}
	if in.PingsRequired > 0 {
		r.PingSustain = float64(in.PingsSent) / float64(in.PingsRequired)
	}

	// INCONCLUSIVE overrides PASS/TRIP. Order matters: a loadgen-induced ping
	// shortfall must mask any false-offline TRIP (the sweep was our fault).
	if in.Self.GCPauseP99Ms > th.GCPauseInconclusive {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: loadgen gc pause p99=%.1fms > %.0f", in.Self.GCPauseP99Ms, th.GCPauseInconclusive)}
		return r
	}
	if in.ConnectAttempted == 0 {
		r.Kind = verdictInconclusive
		r.Reasons = []string{"inconclusive: zero hellos attempted (publisher down or emitters not wired)"}
		return r
	}
	if in.N > 0 && in.EffectiveN > 0 && float64(in.EffectiveN)/float64(in.N) < 0.95 {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: only %d/%d users activated", in.EffectiveN, in.N)}
		return r
	}
	if in.PingsRequired > 0 && r.PingSustain < 1.0-th.PingTolerance {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: ping sustainability %.1f%% < %.0f%% (loadgen could not sustain heartbeats)",
			r.PingSustain*100, (1.0-th.PingTolerance)*100)}
		return r
	}

	var reasons []string
	if r.FalseOfflineRate > th.FalseOfflineRate {
		reasons = append(reasons, fmt.Sprintf("false_offline_rate=%.4f > %.4f (%d users swept offline)",
			r.FalseOfflineRate, th.FalseOfflineRate, in.FalseOfflines))
	}
	if r.ConnectP95Ms > th.ConnectP95Ms {
		reasons = append(reasons, fmt.Sprintf("connect p95=%.0fms > %.0f", r.ConnectP95Ms, th.ConnectP95Ms))
	}
	if r.ConnectP99Ms > th.ConnectP99Ms {
		reasons = append(reasons, fmt.Sprintf("connect p99=%.0fms > %.0f", r.ConnectP99Ms, th.ConnectP99Ms))
	}
	if r.ConnectErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("connect error_rate=%.4f > %.4f", r.ConnectErrorRate, th.ErrorRate))
	}
	if len(reasons) > 0 {
		r.Kind = verdictTrip
		r.Reasons = reasons
		return r
	}
	r.Kind = verdictPass
	return r
}
