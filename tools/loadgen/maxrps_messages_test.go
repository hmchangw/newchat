package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time check: messagesWorkload satisfies rpsWorkload.
var _ rpsWorkload = (*messagesWorkload)(nil)

func TestDiffCounters(t *testing.T) {
	start := msgCounters{published: 100, err: map[string]float64{"publish": 1, "saturated": 5}}
	end := msgCounters{published: 1100, err: map[string]float64{"publish": 3, "saturated": 9}}
	d := diffCounters(start, end)
	assert.Equal(t, float64(1000), d.published)
	assert.Equal(t, float64(2), d.err["publish"])
	assert.Equal(t, float64(4), d.err["saturated"])
	assert.Equal(t, float64(0), d.err["marshal"])
	assert.Equal(t, float64(0), d.err["gatekeeper"])
	assert.Equal(t, float64(0), d.err["bad_reply"])
}

func TestBuildMessagesInputs(t *testing.T) {
	delta := msgCounters{
		published: 980,
		err:       map[string]float64{"publish": 10, "marshal": 0, "gatekeeper": 5, "bad_reply": 0, "saturated": 7},
	}
	e1 := nLatencies(50, ms(15))
	e2 := nLatencies(50, ms(30))
	pending := map[string]uint64{"message-worker": 12, "broadcast-worker": 40}
	startPending := map[string]uint64{"message-worker": 2, "broadcast-worker": 5}
	durables := []string{"message-worker", "broadcast-worker"}

	in := buildMessagesInputs(1000, 10*time.Second, delta, e1, e2, startPending, pending, durables, true, missCounts{})

	// AttemptedOps = published 980 + publish_err 10 + marshal_err 0 = 990
	assert.Equal(t, 990, in.AttemptedOps)
	// FailedOps = publish_err 10 + marshal_err 0 + gatekeeper 5 + bad_reply 0 = 15
	assert.Equal(t, 15, in.FailedOps)
	assert.Equal(t, 7, in.Saturation)
	assert.Len(t, in.Latencies, 2)
	assert.Equal(t, "E1", in.Latencies[0].Name)
	assert.Equal(t, "E2", in.Latencies[1].Name)
	assert.Len(t, in.Pending, 2)
	assert.Equal(t, uint64(2), in.Pending[0].Start)
	assert.Equal(t, uint64(12), in.Pending[0].End)
	assert.False(t, in.Inconclusive)
}

func TestBuildMessagesInputs_PopulatesEmitUnderrun(t *testing.T) {
	delta := msgCounters{
		published: 900,
		err: map[string]float64{
			"publish": 0, "marshal": 0, "gatekeeper": 0, "bad_reply": 0,
			"saturated": 3, "underrun": 50,
		},
	}
	in := buildMessagesInputs(1000, 10*time.Second, delta, nil, nil,
		map[string]uint64{}, map[string]uint64{}, nil, true, missCounts{})

	assert.Equal(t, 50, in.EmitUnderrun)
	assert.Equal(t, 3, in.Saturation)
	// Emit underrun is a load-box signal, not a request failure: it must not
	// inflate FailedOps (which would manufacture spurious TRIPs).
	assert.Equal(t, 0, in.FailedOps)
}

func TestBuildMessagesInputs_PendingUnavailableIsInconclusive(t *testing.T) {
	delta := msgCounters{published: 1000, err: map[string]float64{}}
	in := buildMessagesInputs(1000, time.Second, delta, nil, nil, nil, nil, []string{"message-worker"}, false, missCounts{})
	assert.True(t, in.Inconclusive)
	assert.Contains(t, in.InconclusiveReason, "pending")
	assert.Empty(t, in.Pending)
}

// A publish whose reply or broadcast never arrives is a delivery failure, not
// an absent sample. Excluding it lets the run look healthier the more messages
// the system drops, because the surviving samples are the fast ones.
// A fixed drain would flag in-flight messages as dropped whenever the operator
// raises the latency bound above it — manufacturing failures on exactly the
// exploratory runs that widen the bound on purpose.
func TestResolveDrainWindow(t *testing.T) {
	tests := []struct {
		name string
		p99  time.Duration
		want time.Duration
	}{
		{"default bound keeps the floor", 250 * time.Millisecond, 2 * time.Second},
		{"tight bound keeps the floor", 10 * time.Millisecond, 2 * time.Second},
		{"loose bound scales past the floor", 3 * time.Second, 6 * time.Second},
		{"exactly at the floor boundary", time.Second, 2 * time.Second},
		{"unset bound falls back to the floor", 0, 2 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, resolveDrainWindow(tc.p99))
		})
	}
}

func TestBuildMessagesInputs_CarriesMissingCounts(t *testing.T) {
	delta := msgCounters{
		published: 1000,
		err:       map[string]float64{"publish": 0, "marshal": 0, "gatekeeper": 0, "bad_reply": 0},
	}
	miss := missCounts{Replies: 30, Broadcasts: 45, BroadcastEligible: 900}

	in := buildMessagesInputs(1000, 10*time.Second, delta, nil, nil,
		map[string]uint64{}, map[string]uint64{}, nil, true, miss)

	assert.Equal(t, 1000, in.AttemptedOps)
	assert.Equal(t, 30, in.MissingReplies)
	assert.Equal(t, 45, in.MissingBroadcasts)
	assert.Equal(t, 900, in.BroadcastEligible)
	// Missing deliveries are tracked as their own signals, matching the way
	// sli-slo.md §2 keeps SLO-1b (publication) separate from SLO-1a: folding
	// both into FailedOps would count one dropped message twice.
	assert.Equal(t, 0, in.FailedOps)
}

func TestEvaluateRPSStep_TripsOnMissingReplies(t *testing.T) {
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: 10 * time.Second,
		AttemptedOps: 10000, FailedOps: 0, MissingReplies: 500,
	}
	res := evaluateRPSStep(&in, buildThresholds(ms(100), ms(250), 0.01, 1000, 0.1))

	assert.Equal(t, verdictTrip, res.Kind)
	assert.Equal(t, 0.05, res.MissingReplyRate)
	require.Len(t, res.Reasons, 1)
	assert.Contains(t, res.Reasons[0], "missing reply")
}

func TestEvaluateRPSStep_TripsOnMissingBroadcasts(t *testing.T) {
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: 10 * time.Second,
		AttemptedOps: 10000, FailedOps: 0,
		BroadcastEligible: 5000, MissingBroadcasts: 80,
	}
	res := evaluateRPSStep(&in, buildThresholds(ms(100), ms(250), 0.01, 1000, 0.1))

	assert.Equal(t, verdictTrip, res.Kind)
	assert.Equal(t, 5000, res.BroadcastEligible)
	assert.Equal(t, 0.016, res.MissingBroadcastRate)
	require.Len(t, res.Reasons, 1)
	assert.Contains(t, res.Reasons[0], "missing broadcast")
}

func TestEvaluateRPSStep_NoBroadcastEligibleKeepsMissingRateZero(t *testing.T) {
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: time.Second,
		AttemptedOps: 1000, BroadcastEligible: 0, MissingBroadcasts: 1,
	}

	res := evaluateRPSStep(&in, buildThresholds(ms(100), ms(250), 0.01, 1000, 0.1))

	assert.Zero(t, res.MissingBroadcastRate)
	assert.Equal(t, verdictPass, res.Kind)
}

// Stragglers within the threshold must not manufacture a trip — that was the
// original reason the counts were dropped entirely.
func TestEvaluateRPSStep_ToleratesMissingUnderThreshold(t *testing.T) {
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: 10 * time.Second,
		AttemptedOps: 10000, FailedOps: 0, MissingReplies: 50,
		BroadcastEligible: 10000, MissingBroadcasts: 50,
	}
	res := evaluateRPSStep(&in, buildThresholds(ms(100), ms(250), 0.01, 1000, 0.1))

	assert.Equal(t, verdictPass, res.Kind)
	assert.Empty(t, res.Reasons)
}
