package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestVerdictKind_String(t *testing.T) {
	assert.Equal(t, "PASS", verdictPass.String())
	assert.Equal(t, "TRIP", verdictTrip.String())
	assert.Equal(t, "INCONCLUSIVE", verdictInconclusive.String())
}
