package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// nLatencies returns a slice of n identical-latency samples.
func nLatencies(n int, d time.Duration) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = d
	}
	return out
}

func defaultRPSThresholds() rpsThresholds {
	return rpsThresholds{
		P95:           ms(100),
		P99:           ms(250),
		ErrorRate:     0.001,
		PendingGrowth: 1000,
		RateTolerance: 0.05,
	}
}

func TestEvaluateRPSStep(t *testing.T) {
	th := defaultRPSThresholds()
	tests := []struct {
		name     string
		in       rpsStepInputs
		wantKind verdictKind
	}{
		{
			name: "all healthy passes",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000, FailedOps: 0,
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(20))}},
				Pending:   []consumerPendingDelta{{Durable: "message-worker", Start: 0, End: 10}},
			},
			wantKind: verdictPass,
		},
		{
			name: "p95 over threshold trips",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(150))}},
			},
			wantKind: verdictTrip,
		},
		{
			name: "p99 over threshold trips",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				// 98 samples at 20ms, 2 at 300ms -> p95=20ms (ok), p99=300ms (>250ms).
				// pick(0.99) = int(99*0.99) = index 98 = first 300ms value.
				Latencies: []seriesSamples{{Name: "E1", Samples: append(nLatencies(98, ms(20)), ms(300), ms(300))}},
			},
			wantKind: verdictTrip,
		},
		{
			name: "error rate over threshold trips",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000, FailedOps: 5, // 0.5% > 0.1%
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(20))}},
			},
			wantKind: verdictTrip,
		},
		{
			name: "pending growth over threshold trips",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(20))}},
				Pending:   []consumerPendingDelta{{Durable: "broadcast-worker", Start: 0, End: 1500}},
			},
			wantKind: verdictTrip,
		},
		{
			name: "per-endpoint: slow thread trips even if history fast",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{
					{Name: "history", Samples: nLatencies(100, ms(20))},
					{Name: "thread", Samples: nLatencies(100, ms(180))},
				},
			},
			wantKind: verdictTrip,
		},
		{
			name: "healthy but rate shortfall is inconclusive",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 800, // 80% < 95%
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(80, ms(20))}},
			},
			wantKind: verdictInconclusive,
		},
		{
			name: "trip beats shortfall: high latency AND low achieved is a TRIP",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 800, // shortfall...
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(80, ms(400))}}, // ...but slow
			},
			wantKind: verdictTrip,
		},
		{
			name: "explicit inconclusive flag short-circuits",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Inconclusive: true, InconclusiveReason: "pending snapshot failed",
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(20))}},
			},
			wantKind: verdictInconclusive,
		},
		{
			name: "p95 exactly at threshold passes (boundary)",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(100))}},
			},
			wantKind: verdictPass,
		},
		{
			name: "empty samples does not panic and passes on other signals",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{{Name: "E1", Samples: nil}},
			},
			wantKind: verdictPass,
		},
		{
			name: "explicit inconclusive beats TRIP signals",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Inconclusive: true, InconclusiveReason: "snapshot failed",
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(400))}},
			},
			wantKind: verdictInconclusive,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evaluateRPSStep(&tt.in, th)
			assert.Equal(t, tt.wantKind, got.Kind, "reasons=%v", got.Reasons)
			if tt.wantKind == verdictTrip {
				assert.NotEmpty(t, got.Reasons, "TRIP verdict must have at least one reason")
			}
		})
	}
}

func TestEvaluateRPSStep_AchievedAndErrorRate(t *testing.T) {
	th := defaultRPSThresholds()
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: 2 * time.Second, AttemptedOps: 1000, FailedOps: 100,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(10, ms(20))}},
	}
	got := evaluateRPSStep(&in, th)
	assert.InDelta(t, 500.0, got.AchievedRPS, 0.01) // 1000 ops / 2s
	assert.InDelta(t, 0.1, got.ErrorRate, 0.0001)   // 100/1000
}

func TestEvaluateRPSStep_JointRatioTripsWhenIndependentGatesWouldPass(t *testing.T) {
	th := defaultRPSThresholds()
	th.ErrorRate = 0.01

	// 8 failures and 8 slow successes each fit inside an independent 1%
	// failure/latency budget. The actual SLO-3 predicate has only 984 good
	// events out of 1000 eligible attempts, so it must trip its 99% objective.
	successes := append(nLatencies(984, ms(20)), nLatencies(8, ms(300))...)
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000, FailedOps: 8,
		ErrorRateDiagnosticOnly: true,
		Latencies:               []seriesSamples{{Name: "login", Samples: successes, DiagnosticOnly: true}},
		EventRatios: []eventRatioInput{{
			Name: "SLO-3", Valid: 1000, SuccessfulLatencies: successes,
			Target: 0.99, Bound: latencyBoundP99,
		}},
	}

	got := evaluateRPSStep(&in, th)

	require.Equal(t, verdictTrip, got.Kind)
	require.Len(t, got.EventRatios, 1)
	assert.Equal(t, 984, got.EventRatios[0].Good)
	assert.Equal(t, 1000, got.EventRatios[0].Valid)
	assert.InDelta(t, 0.984, got.EventRatios[0].Ratio, 0.000001)
	assert.Contains(t, got.Reasons[0], "SLO-3 good ratio")
}

func TestEvaluateRPSStep_JointRatioPassesAtBoundary(t *testing.T) {
	th := defaultRPSThresholds()
	successes := nLatencies(990, ms(20))
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000, FailedOps: 10,
		ErrorRateDiagnosticOnly: true,
		Latencies:               []seriesSamples{{Name: "login", Samples: successes, DiagnosticOnly: true}},
		EventRatios: []eventRatioInput{{
			Name: "SLO-3", Valid: 1000, SuccessfulLatencies: successes,
			Target: 0.99, Bound: latencyBoundP99,
		}},
	}

	got := evaluateRPSStep(&in, th)

	assert.Equal(t, verdictPass, got.Kind, "reasons=%v", got.Reasons)
	require.Len(t, got.EventRatios, 1)
	assert.InDelta(t, 0.99, got.EventRatios[0].Ratio, 0.000001)
}

func TestEvaluateRPSStep_WorstPendingReported(t *testing.T) {
	th := defaultRPSThresholds()
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(10, ms(20))}},
		Pending: []consumerPendingDelta{
			{Durable: "message-worker", Start: 0, End: 50},
			{Durable: "broadcast-worker", Start: 100, End: 700}, // delta 600, the worst
		},
	}
	got := evaluateRPSStep(&in, th)
	assert.Equal(t, "broadcast-worker", got.WorstDurable)
	assert.Equal(t, int64(600), got.WorstDelta)
	assert.Equal(t, verdictPass, got.Kind) // 600 < 1000

	// Over-threshold p95 trip: verify the reason string contains "p95=" and "> ".
	tripIn := rpsStepInputs{
		TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(400))}},
	}
	tripGot := evaluateRPSStep(&tripIn, th)
	assert.Equal(t, verdictTrip, tripGot.Kind)
	assert.NotEmpty(t, tripGot.Reasons)
	assert.Contains(t, tripGot.Reasons[0], "p95=")
	assert.Contains(t, tripGot.Reasons[0], "> ")
}

func TestEvaluateRPSStep_ShortfallBlamesUnderrun(t *testing.T) {
	th := defaultRPSThresholds()
	// Healthy latencies, but the achieved rate fell short while emit underrun
	// dominates: the load box couldn't even produce the load. The reason must
	// point at the emit-underrun knobs (shards / CPU), not MaxInFlight.
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: time.Second, AttemptedOps: 700,
		EmitUnderrun: 250, Saturation: 5,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(70, ms(20))}},
	}
	got := evaluateRPSStep(&in, th)
	assert.Equal(t, verdictInconclusive, got.Kind)
	require.NotEmpty(t, got.Reasons)
	assert.Contains(t, got.Reasons[0], "underrun")
	assert.Contains(t, got.Reasons[0], "shard")
	assert.Equal(t, 250, got.EmitUnderrun)
}

func TestEvaluateRPSStep_ShortfallBlamesSaturation(t *testing.T) {
	th := defaultRPSThresholds()
	// Shortfall with no emit underrun but heavy pool saturation: the load box
	// kept schedule but the in-flight pool was too small. Point at MaxInFlight.
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: time.Second, AttemptedOps: 700,
		EmitUnderrun: 0, Saturation: 300,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(70, ms(20))}},
	}
	got := evaluateRPSStep(&in, th)
	assert.Equal(t, verdictInconclusive, got.Kind)
	require.NotEmpty(t, got.Reasons)
	assert.Contains(t, got.Reasons[0], "MaxInFlight")
	assert.Equal(t, 300, got.Saturation)
}

func TestVerdictKind_String(t *testing.T) {
	assert.Equal(t, "PASS", verdictPass.String())
	assert.Equal(t, "TRIP", verdictTrip.String())
	assert.Equal(t, "INCONCLUSIVE", verdictInconclusive.String())
}

func TestEvaluateRPSStep_CopiesPending(t *testing.T) {
	in := &rpsStepInputs{
		TargetRPS:    1000,
		Hold:         30 * time.Second,
		AttemptedOps: 30000,
		Pending: []consumerPendingDelta{
			{Durable: "message-worker", Start: 0, End: 5000},
			{Durable: "broadcast-worker", Start: 0, End: 10},
		},
	}
	res := evaluateRPSStep(in, buildThresholds(100*time.Millisecond, 250*time.Millisecond, 0.001, 1000, 0.05))
	require.Len(t, res.Pending, 2)
	assert.Equal(t, "message-worker", res.Pending[0].Durable)
	assert.Equal(t, int64(5000), res.Pending[0].Delta())
}

// A malformed ratio means the harness cannot compute the SLI, which is a
// measurement failure — not a service failure. Each branch must reach
// INCONCLUSIVE and must not publish a ratio result, or the report would show a
// number derived from inputs the evaluator already rejected.
func TestEvaluateRPSStep_InvalidEventRatioIsInconclusive(t *testing.T) {
	th := rpsThresholds{P95: time.Second, P99: 2 * time.Second, ErrorRate: 0.5}
	fast := []time.Duration{10 * time.Millisecond}

	tests := []struct {
		name        string
		ratio       eventRatioInput
		wantInclude string
	}{
		{
			name: "unset bound selector",
			ratio: eventRatioInput{
				Name: "SLO-X", Valid: 10, SuccessfulLatencies: fast, Target: 0.99,
			},
			wantInclude: "invalid latency bound",
		},
		{
			name: "target above one",
			ratio: eventRatioInput{
				Name: "SLO-X", Valid: 10, SuccessfulLatencies: fast,
				Target: 1.5, Bound: latencyBoundP99,
			},
			wantInclude: "invalid target",
		},
		{
			name: "target of zero",
			ratio: eventRatioInput{
				Name: "SLO-X", Valid: 10, SuccessfulLatencies: fast,
				Target: 0, Bound: latencyBoundP99,
			},
			wantInclude: "invalid target",
		},
		{
			name: "no valid events",
			ratio: eventRatioInput{
				Name: "SLO-X", Valid: 0, Target: 0.99, Bound: latencyBoundP99,
			},
			wantInclude: "no valid events",
		},
		{
			// good/valid could exceed 1.0 — arithmetically impossible for a
			// ratio, so the inputs are wrong rather than the service.
			name: "more successes than valid events",
			ratio: eventRatioInput{
				Name:                "SLO-X",
				Valid:               1,
				SuccessfulLatencies: []time.Duration{time.Millisecond, time.Millisecond},
				Target:              0.99, Bound: latencyBoundP99,
			},
			wantInclude: "but only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := rpsStepInputs{
				TargetRPS: 100, Hold: time.Second,
				AttemptedOps: 100, FailedOps: 0,
				EventRatios: []eventRatioInput{tt.ratio},
			}
			res := evaluateRPSStep(&in, th)

			assert.Equal(t, verdictInconclusive, res.Kind)
			require.Len(t, res.Reasons, 1)
			assert.Contains(t, res.Reasons[0], tt.wantInclude)
			assert.Empty(t, res.EventRatios,
				"a rejected ratio must not surface a computed number")
		})
	}
}

// A measurement failure outranks a TRIP: an unusable SLI cannot prove the
// service breached anything.
func TestEvaluateRPSStep_InvalidRatioOutranksLatencyTrip(t *testing.T) {
	slow := make([]time.Duration, 100)
	for i := range slow {
		slow[i] = 10 * time.Second
	}
	in := rpsStepInputs{
		TargetRPS: 100, Hold: time.Second,
		AttemptedOps: 100, FailedOps: 100,
		Latencies:   []seriesSamples{{Name: "E2", Samples: slow}},
		EventRatios: []eventRatioInput{{Name: "SLO-X", Valid: 0, Target: 0.99, Bound: latencyBoundP99}},
	}
	res := evaluateRPSStep(&in, rpsThresholds{P95: time.Millisecond, P99: time.Millisecond, ErrorRate: 0.001})

	assert.Equal(t, verdictInconclusive, res.Kind)
}
