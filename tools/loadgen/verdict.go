package main

import (
	"fmt"
	"time"
)

// rpsThresholds holds the SLO limits a step is judged against. Every gated
// latency series shares the same P95/P99 limits.
type rpsThresholds struct {
	P95, P99      time.Duration
	ErrorRate     float64
	PendingGrowth uint64 // messages only; per-durable end-start NumPending delta
	RateTolerance float64
}

// seriesSamples is one named latency tape (e.g. "E1","E2" or "history","thread").
type seriesSamples struct {
	Name    string
	Samples []time.Duration
}

// consumerPendingDelta is one durable's NumPending at the hold boundaries.
type consumerPendingDelta struct {
	Durable    string
	Start, End uint64
}

// Delta returns End-Start as a signed value (it can be negative if the backlog drained).
func (d consumerPendingDelta) Delta() int64 { return int64(d.End) - int64(d.Start) }

// rpsStepInputs is the normalized, workload-agnostic measurement of one step.
type rpsStepInputs struct {
	TargetRPS    int
	Hold         time.Duration
	AttemptedOps int
	FailedOps    int
	Saturation   int // open-loop self-saturation tally (corroborates shortfall)
	Latencies    []seriesSamples
	Pending      []consumerPendingDelta // empty for history
	// Inconclusive is set by the adapter when measurement itself failed (e.g. a
	// pending snapshot errored), independent of the system under test.
	Inconclusive       bool
	InconclusiveReason string
}

type verdictKind int

const (
	verdictPass verdictKind = iota
	verdictTrip
	verdictInconclusive
)

func (k verdictKind) String() string {
	switch k {
	case verdictTrip:
		return "TRIP"
	case verdictInconclusive:
		return "INCONCLUSIVE"
	default:
		return "PASS"
	}
}

// seriesPercentile is a named series' computed percentiles, for reporting.
type seriesPercentile struct {
	Name string
	Pct  Percentiles
}

// rpsStepResult is the verdict for one step.
type rpsStepResult struct {
	TargetRPS    int
	AchievedRPS  float64
	AttemptedOps int
	FailedOps    int
	Saturation   int
	ErrorRate    float64
	Latencies    []seriesPercentile
	WorstDurable string
	WorstDelta   int64
	Kind         verdictKind
	Reasons      []string
	// Pending carries every durable's backlog delta for the step (not just the
	// worst), so the bottleneck engine can map a delta to a pipeline stage.
	Pending []consumerPendingDelta
	// HoldStart/HoldEnd bound the step's measurement window in wall-clock time,
	// set by runRamp. Used to query container metrics over the same interval.
	HoldStart, HoldEnd time.Time
}

// evaluateRPSStep classifies a step PASS / TRIP / INCONCLUSIVE.
//
// Precedence (deliberately differs from PR #234):
//  1. explicit measurement-failure inconclusive (harness/measurement issue),
//  2. TRIP if any SLO signal is over threshold — server-induced backpressure
//     must NOT be misread as a harness limit,
//  3. INCONCLUSIVE if the harness could not push the target rate while the
//     system looked healthy (the load box is the limit, not the service),
//  4. PASS.
func evaluateRPSStep(in *rpsStepInputs, th rpsThresholds) rpsStepResult {
	res := rpsStepResult{
		TargetRPS:    in.TargetRPS,
		AttemptedOps: in.AttemptedOps,
		FailedOps:    in.FailedOps,
		Saturation:   in.Saturation,
	}
	res.Pending = in.Pending
	if in.Hold > 0 {
		res.AchievedRPS = float64(in.AttemptedOps) / in.Hold.Seconds()
	}
	if in.AttemptedOps > 0 {
		res.ErrorRate = float64(in.FailedOps) / float64(in.AttemptedOps)
	}

	// Compute percentiles for every series (always, so the report has data).
	for _, s := range in.Latencies {
		res.Latencies = append(res.Latencies, seriesPercentile{Name: s.Name, Pct: ComputePercentiles(s.Samples)})
	}
	// Worst pending delta (always, for the report column).
	for _, p := range in.Pending {
		if d := p.Delta(); d > res.WorstDelta || res.WorstDurable == "" {
			res.WorstDelta = d
			res.WorstDurable = p.Durable
		}
	}

	// (1) Measurement failure short-circuits.
	if in.Inconclusive {
		res.Kind = verdictInconclusive
		res.Reasons = []string{in.InconclusiveReason}
		return res
	}

	// (2) TRIP conditions (accumulate human-readable reasons).
	var reasons []string
	for _, sp := range res.Latencies {
		if sp.Pct.P95 > th.P95 {
			reasons = append(reasons, fmt.Sprintf("%s p95=%s > %s", sp.Name, sp.Pct.P95, th.P95))
		}
		if sp.Pct.P99 > th.P99 {
			reasons = append(reasons, fmt.Sprintf("%s p99=%s > %s", sp.Name, sp.Pct.P99, th.P99))
		}
	}
	if res.ErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("error rate %.3f%% > %.3f%%", res.ErrorRate*100, th.ErrorRate*100))
	}
	for _, p := range in.Pending {
		if d := p.Delta(); d > int64(th.PendingGrowth) {
			reasons = append(reasons, fmt.Sprintf("%s pending +%d > +%d", p.Durable, d, th.PendingGrowth))
		}
	}
	if len(reasons) > 0 {
		res.Kind = verdictTrip
		res.Reasons = reasons
		return res
	}

	// (3) Healthy-but-cannot-push -> INCONCLUSIVE.
	if th.RateTolerance > 0 && res.AchievedRPS < float64(in.TargetRPS)*(1-th.RateTolerance) {
		res.Kind = verdictInconclusive
		res.Reasons = []string{fmt.Sprintf(
			"achieved %.0f rps < %.0f%% of target %d rps (saturation=%d) — load box limited",
			res.AchievedRPS, (1-th.RateTolerance)*100, in.TargetRPS, in.Saturation)}
		return res
	}

	// (4) PASS.
	res.Kind = verdictPass
	return res
}
