package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Compile-time check: historyWorkload satisfies rpsWorkload.
var _ rpsWorkload = (*historyWorkload)(nil)

func TestBuildHistoryInputs(t *testing.T) {
	c := NewHistoryCollector()
	now := time.Now()
	for i := 0; i < 40; i++ {
		c.RecordSample(HistorySample{Endpoint: HistoryEndpointHistory, Latency: ms(15), At: now})
	}
	for i := 0; i < 10; i++ {
		c.RecordSample(HistorySample{Endpoint: HistoryEndpointThread, Latency: ms(25), At: now})
	}
	c.RecordError(HistoryEndpointHistory, errClassTimeout, 0)
	c.RecordError(HistoryEndpointThread, errClassReply, 0)
	c.RecordSaturation()
	c.RecordSaturation()

	in := buildHistoryInputs(2000, 30*time.Second, c)

	// attempted = 40 + 10 history/thread samples + 2 errors (timeout+reply)
	assert.Equal(t, 52, in.AttemptedOps)
	assert.Equal(t, 2, in.FailedOps)
	assert.Equal(t, 2, in.Saturation)
	assert.Len(t, in.Latencies, 2)
	assert.Equal(t, "history", in.Latencies[0].Name)
	assert.Equal(t, "thread", in.Latencies[1].Name)
	assert.Len(t, in.Latencies[0].Samples, 40)
	assert.Len(t, in.Latencies[1].Samples, 10)
	assert.Empty(t, in.Pending) // history has no consumer queue
}
