package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProm returns canned counter samples per service name. The key is matched
// as a substring of the query (the query embeds the service selector).
type fakeProm struct {
	// fn maps a query + window to samples; nil samples => empty result.
	fn  func(query string, start, end time.Time) []promSample
	err error
}

func (f fakeProm) RangeQuery(_ context.Context, query string, start, end time.Time, _ time.Duration) ([]promSeries, error) {
	if f.err != nil {
		return nil, f.err
	}
	samples := f.fn(query, start, end)
	if samples == nil {
		return nil, nil
	}
	return []promSeries{{Labels: map[string]string{}, Samples: samples}}, nil
}

func counterSamples(start time.Time, startVal, cores float64, windowSec int) []promSample {
	// Linear counter: startVal at t0, rising `cores` per second for windowSec.
	return []promSample{
		{T: start, V: startVal},
		{T: start.Add(time.Duration(windowSec) * time.Second), V: startVal + cores*float64(windowSec)},
	}
}

func TestEngine_cpuCores(t *testing.T) {
	start := time.Unix(1000, 0)
	q := fakeProm{fn: func(_ string, s, _ time.Time) []promSample {
		return counterSamples(s, 100, 2.5, 30) // 2.5 cores over 30s
	}}
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	cores, reset, ok := eng.cpuCores(context.Background(), "message-worker", start, start.Add(30*time.Second))
	require.True(t, ok)
	assert.False(t, reset)
	assert.InDelta(t, 2.5, cores, 0.001)
}

func TestEngine_cpuCores_CounterReset(t *testing.T) {
	start := time.Unix(1000, 0)
	q := fakeProm{fn: func(_ string, s, _ time.Time) []promSample {
		return []promSample{{T: s, V: 500}, {T: s.Add(30 * time.Second), V: 3}} // dropped -> restart
	}}
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	_, reset, ok := eng.cpuCores(context.Background(), "x", start, start.Add(30*time.Second))
	require.True(t, ok)
	assert.True(t, reset)
}

func TestEngine_cpuCores_QueryError(t *testing.T) {
	q := fakeProm{err: assertErr{}}
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	_, _, ok := eng.cpuCores(context.Background(), "x", time.Unix(0, 0), time.Unix(30, 0))
	assert.False(t, ok)
}

func TestEngine_cpuCores_EmptyResult(t *testing.T) {
	q := fakeProm{fn: func(_ string, _, _ time.Time) []promSample { return nil }}
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	_, _, ok := eng.cpuCores(context.Background(), "x", time.Unix(0, 0), time.Unix(30, 0))
	assert.False(t, ok)
}

type assertErr struct{}

func (assertErr) Error() string { return "prom down" }

func tripResult(window time.Time) *rpsStepResult {
	return &rpsStepResult{
		TargetRPS: 2000,
		HoldStart: window, HoldEnd: window.Add(30 * time.Second),
		Latencies: []seriesPercentile{
			{Name: "E1", Pct: Percentiles{P95: 20 * time.Millisecond}},
			{Name: "E2", Pct: Percentiles{P95: 200 * time.Millisecond}},
		},
		Pending: []consumerPendingDelta{
			{Durable: "message-worker", Start: 0, End: 12000},
			{Durable: "broadcast-worker", Start: 0, End: 0},
		},
	}
}

func passResult(window time.Time) *rpsStepResult {
	return &rpsStepResult{
		TargetRPS: 1000,
		HoldStart: window, HoldEnd: window.Add(30 * time.Second),
	}
}

var slo = buildThresholds(100*time.Millisecond, 250*time.Millisecond, 0.001, 1000, 0.05)

// stageProm returns per-service cores keyed by service, with a plateau for
// services in `plateau` (same cores in both windows) and growth otherwise.
// Caller contract: plateau keys must be disjoint — no key may be a substring
// of another — because the helper matches the first key contained in the query
// and map iteration order is unspecified.
func stageProm(passT, tripT time.Time, plateau map[string]float64) fakeProm {
	return fakeProm{fn: func(query string, s, _ time.Time) []promSample {
		for svc, cores := range plateau {
			if strings.Contains(query, svc) {
				return counterSamples(s, 0, cores, 30) // same cores both windows -> plateau
			}
		}
		// non-plateau services: grow a lot from pass to trip window
		base := 0.2
		if s.Equal(tripT) {
			base = 2.0
		}
		return counterSamples(s, 0, base, 30)
	}}
}

func TestEngine_DependencyBound(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	// message-worker backs up; cassandra CPU plateaus -> Cassandra-bound, high.
	q := stageProm(passT, tripT, map[string]float64{"cassandra": 3.8, "message-worker": 0.4})
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), tripResult(tripT), passResult(passT), messagesStageGraph(), slo)
	require.True(t, v.Determined)
	assert.Equal(t, "message-worker", v.Component)
	assert.Equal(t, "Cassandra", v.Resource)
	assert.Equal(t, "high", v.Confidence)
}

func TestEngine_StageCPUBound(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	// E1 (gatekeeper) breaches and gatekeeper CPU plateaus -> CPU-bound, high.
	trip := tripResult(tripT)
	trip.Latencies[0].Pct.P95 = 150 * time.Millisecond // E1 over SLO
	trip.Pending[0].End = 0                            // no worker backlog
	q := stageProm(passT, tripT, map[string]float64{"message-gatekeeper": 4.0})
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), trip, passResult(passT), messagesStageGraph(), slo)
	require.True(t, v.Determined)
	assert.Equal(t, "message-gatekeeper", v.Component)
	assert.Equal(t, "CPU", v.Resource)
	assert.Equal(t, "high", v.Confidence)
}

func TestEngine_BacksUpNoKnee_Medium(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	// worker backs up, but nothing plateaus (all CPU still rising) -> medium/unknown.
	q := stageProm(passT, tripT, nil)
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), tripResult(tripT), passResult(passT), messagesStageGraph(), slo)
	require.True(t, v.Determined)
	assert.Equal(t, "message-worker", v.Component)
	assert.Equal(t, "unknown", v.Resource)
	assert.Equal(t, "medium", v.Confidence)
}

func TestEngine_NoBackup_FallbackRanking_Low(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	trip := tripResult(tripT)
	trip.Pending[0].End = 0                           // nothing backs up, no latency breach
	trip.Latencies[1].Pct.P95 = 50 * time.Millisecond // E2 under SLO: broadcast-worker not backing up
	// cassandra has the clearest plateau at the highest cores -> low-confidence pick.
	q := stageProm(passT, tripT, map[string]float64{"cassandra": 3.8})
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), trip, passResult(passT), messagesStageGraph(), slo)
	require.True(t, v.Determined)
	assert.Equal(t, "Cassandra", v.Component)
	assert.Equal(t, "low", v.Confidence)
}

func TestEngine_NoPassStep_Undetermined(t *testing.T) {
	eng := newBottleneckEngine(stageProm(time.Unix(1, 0), time.Unix(2, 0), nil), identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), tripResult(time.Unix(2000, 0)), nil, messagesStageGraph(), slo)
	assert.False(t, v.Determined)
	assert.Contains(t, v.Reasons[0], "no passing step")
}

func TestEngine_PromError_Undetermined(t *testing.T) {
	eng := newBottleneckEngine(fakeProm{err: assertErr{}}, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), tripResult(time.Unix(2000, 0)), passResult(time.Unix(1000, 0)), messagesStageGraph(), slo)
	assert.False(t, v.Determined)
}

func TestEngine_AllClear_Undetermined(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	trip := &rpsStepResult{
		TargetRPS: 2000,
		HoldStart: tripT, HoldEnd: tripT.Add(30 * time.Second),
		// no latency breaches, no backlog growth
		Latencies: []seriesPercentile{
			{Name: "E1", Pct: Percentiles{P95: 10 * time.Millisecond}},
			{Name: "E2", Pct: Percentiles{P95: 20 * time.Millisecond}},
		},
		Pending: []consumerPendingDelta{
			{Durable: "message-worker", Start: 0, End: 0},
			{Durable: "broadcast-worker", Start: 0, End: 0},
		},
	}
	// nil plateau -> every container's CPU grows (rise >> knee) -> nothing saturated.
	q := stageProm(passT, tripT, nil)
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), trip, passResult(passT), messagesStageGraph(), slo)
	assert.False(t, v.Determined)
	assert.Contains(t, v.Reasons[0], "no stage backed up")
}
