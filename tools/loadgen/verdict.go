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
	Name           string
	Samples        []time.Duration
	DiagnosticOnly bool
}

type latencyBound int

const (
	latencyBoundP95 latencyBound = iota + 1
	latencyBoundP99
)

// eventRatioInput describes an event-based latency SLI. SuccessfulLatencies
// contains only successful events; Valid is the SLI denominator, which may
// also include failed eligible events (SLO-3) or only successes (SLO-8).
type eventRatioInput struct {
	Name                string
	Valid               int
	SuccessfulLatencies []time.Duration
	Target              float64
	Bound               latencyBound
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
	Saturation   int // in-flight pool full when an event was due (raise MaxInFlight)
	EmitUnderrun int // events the pacer could not release on schedule (load box CPU/scheduler limited)
	// MissingReplies/MissingBroadcasts count publishes that were never answered
	// by the drain deadline. Kept apart from FailedOps (and from each other)
	// because one send has two independent deliverables — o11y-slo.md §2 scores
	// persistence and publication as separate ratios for the same reason, and
	// summing them into one counter would count a fully dropped message twice.
	MissingReplies    int
	MissingBroadcasts int
	Latencies         []seriesSamples
	EventRatios       []eventRatioInput
	// ErrorRateDiagnosticOnly keeps the failed/attempted rate visible without
	// independently gating a workload whose SLO already includes failures in a
	// joint good/valid predicate.
	ErrorRateDiagnosticOnly bool
	Pending                 []consumerPendingDelta // empty for history
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
	Name           string
	Pct            Percentiles
	DiagnosticOnly bool
}

// eventRatioResult is the evaluated good/valid ratio used for an event-based
// SLO verdict and exposed in the console/CSV reports.
type eventRatioResult struct {
	Name        string
	Good, Valid int
	Ratio       float64
	Target      float64
	Bound       time.Duration
}

// rpsStepResult is the verdict for one step.
type rpsStepResult struct {
	TargetRPS    int
	AchievedRPS  float64
	AttemptedOps int
	FailedOps    int
	Saturation   int
	EmitUnderrun int
	ErrorRate    float64
	// Missing counts and their rates over AttemptedOps. A run that drops
	// deliveries shows up here; it used to show up nowhere, and the surviving
	// (faster) samples made the percentiles look better as more was dropped.
	MissingReplies       int
	MissingBroadcasts    int
	MissingReplyRate     float64
	MissingBroadcastRate float64
	Latencies            []seriesPercentile
	EventRatios          []eventRatioResult
	WorstDurable         string
	WorstDelta           int64
	Kind                 verdictKind
	Reasons              []string
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
		TargetRPS:         in.TargetRPS,
		AttemptedOps:      in.AttemptedOps,
		FailedOps:         in.FailedOps,
		Saturation:        in.Saturation,
		EmitUnderrun:      in.EmitUnderrun,
		MissingReplies:    in.MissingReplies,
		MissingBroadcasts: in.MissingBroadcasts,
	}
	res.Pending = in.Pending
	if in.Hold > 0 {
		res.AchievedRPS = float64(in.AttemptedOps) / in.Hold.Seconds()
	}
	if in.AttemptedOps > 0 {
		res.ErrorRate = float64(in.FailedOps) / float64(in.AttemptedOps)
		res.MissingReplyRate = float64(in.MissingReplies) / float64(in.AttemptedOps)
		res.MissingBroadcastRate = float64(in.MissingBroadcasts) / float64(in.AttemptedOps)
	}

	// Compute percentiles for every series (always, so the report has data).
	for _, s := range in.Latencies {
		res.Latencies = append(res.Latencies, seriesPercentile{
			Name: s.Name, Pct: ComputePercentiles(s.Samples), DiagnosticOnly: s.DiagnosticOnly,
		})
	}
	measurementIssue := ""
	for _, ratio := range in.EventRatios {
		bound, ok := ratio.bound(th)
		switch {
		case !ok:
			measurementIssue = fmt.Sprintf("%s has an invalid latency bound selector", ratio.Name)
		case ratio.Target <= 0 || ratio.Target > 1:
			measurementIssue = fmt.Sprintf("%s has invalid target %.6f", ratio.Name, ratio.Target)
		case ratio.Valid <= 0:
			measurementIssue = fmt.Sprintf("%s has no valid events", ratio.Name)
		case len(ratio.SuccessfulLatencies) > ratio.Valid:
			measurementIssue = fmt.Sprintf("%s has %d successful samples but only %d valid events",
				ratio.Name, len(ratio.SuccessfulLatencies), ratio.Valid)
		default:
			good := countWithin(ratio.SuccessfulLatencies, bound)
			res.EventRatios = append(res.EventRatios, eventRatioResult{
				Name: ratio.Name, Good: good, Valid: ratio.Valid,
				Ratio: float64(good) / float64(ratio.Valid), Target: ratio.Target, Bound: bound,
			})
		}
		if measurementIssue != "" {
			break
		}
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
	if measurementIssue != "" {
		res.Kind = verdictInconclusive
		res.Reasons = []string{measurementIssue}
		return res
	}

	// (2) TRIP conditions (accumulate human-readable reasons).
	var reasons []string
	for _, sp := range res.Latencies {
		if sp.DiagnosticOnly {
			continue
		}
		if sp.Pct.P95 > th.P95 {
			reasons = append(reasons, fmt.Sprintf("%s p95=%s > %s", sp.Name, sp.Pct.P95, th.P95))
		}
		if sp.Pct.P99 > th.P99 {
			reasons = append(reasons, fmt.Sprintf("%s p99=%s > %s", sp.Name, sp.Pct.P99, th.P99))
		}
	}
	if !in.ErrorRateDiagnosticOnly && res.ErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("error rate %.3f%% > %.3f%%", res.ErrorRate*100, th.ErrorRate*100))
	}
	for _, ratio := range res.EventRatios {
		if ratio.Ratio < ratio.Target {
			reasons = append(reasons, fmt.Sprintf(
				"%s good ratio %.3f%% < %.3f%% (%d/%d within %s)",
				ratio.Name, ratio.Ratio*100, ratio.Target*100,
				ratio.Good, ratio.Valid, ratio.Bound,
			))
		}
	}
	// Undelivered sends are gated at the same rate as hard errors: from the
	// sender's side a reply that never lands is indistinguishable from an error
	// reply. The drain window before Finalize is what keeps ordinary stragglers
	// out of these counts.
	if res.MissingReplyRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("missing reply rate %.3f%% > %.3f%%",
			res.MissingReplyRate*100, th.ErrorRate*100))
	}
	if res.MissingBroadcastRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("missing broadcast rate %.3f%% > %.3f%%",
			res.MissingBroadcastRate*100, th.ErrorRate*100))
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
		res.Reasons = []string{shortfallReason(&res, th)}
		return res
	}

	// (4) PASS.
	res.Kind = verdictPass
	return res
}

func (r eventRatioInput) bound(th rpsThresholds) (time.Duration, bool) {
	switch r.Bound {
	case latencyBoundP95:
		return th.P95, true
	case latencyBoundP99:
		return th.P99, true
	default:
		return 0, false
	}
}

func countWithin(samples []time.Duration, bound time.Duration) int {
	good := 0
	for _, d := range samples {
		if d <= bound {
			good++
		}
	}
	return good
}

// shortfallReason explains a healthy-but-short step and names the dominant
// load-box limit so the operator knows which knob to turn. Emit underrun
// (the pacer couldn't release on schedule) and pool saturation (in-flight cap
// too small) point at different fixes, so the message distinguishes them.
func shortfallReason(res *rpsStepResult, th rpsThresholds) string {
	base := fmt.Sprintf("achieved %.0f rps < %.0f%% of target %d rps",
		res.AchievedRPS, (1-th.RateTolerance)*100, res.TargetRPS)
	switch {
	case res.EmitUnderrun > res.Saturation:
		return fmt.Sprintf("%s — load box could not emit on schedule (emit underrun=%d, saturation=%d); "+
			"reduce per-box rate, add load shards, or give the load box more CPU",
			base, res.EmitUnderrun, res.Saturation)
	case res.Saturation > 0:
		return fmt.Sprintf("%s — in-flight pool saturated (saturation=%d, emit underrun=%d); "+
			"raise MaxInFlight (and/or reduce backend latency)",
			base, res.Saturation, res.EmitUnderrun)
	default:
		return fmt.Sprintf("%s (saturation=%d, emit underrun=%d) — load box limited",
			base, res.Saturation, res.EmitUnderrun)
	}
}
