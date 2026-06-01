package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	in := buildMessagesInputs(1000, 10*time.Second, delta, e1, e2, startPending, pending, durables, true)

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

func TestBuildMessagesInputs_PendingUnavailableIsInconclusive(t *testing.T) {
	delta := msgCounters{published: 1000, err: map[string]float64{}}
	in := buildMessagesInputs(1000, time.Second, delta, nil, nil, nil, nil, []string{"message-worker"}, false)
	assert.True(t, in.Inconclusive)
	assert.Contains(t, in.InconclusiveReason, "pending")
	assert.Empty(t, in.Pending)
}
