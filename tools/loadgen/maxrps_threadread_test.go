package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time check: threadReadWorkload satisfies rpsWorkload.
var _ rpsWorkload = (*threadReadWorkload)(nil)

func TestBuildThreadReadInputs_MapsCollector(t *testing.T) {
	c := newThreadReadCollector()
	c.RecordSample(threadReadSample{Latency: 4 * time.Millisecond, At: time.Now()})
	c.RecordSample(threadReadSample{Latency: 6 * time.Millisecond, At: time.Now()})
	c.RecordError(errClassTimeout, time.Millisecond)
	c.RecordError(errClassReply, time.Millisecond)
	c.RecordBadReply(time.Millisecond)
	c.RecordSaturation()

	in := buildThreadReadInputs(1000, 30*time.Second, c)

	assert.Equal(t, 1000, in.TargetRPS)
	assert.Equal(t, 30*time.Second, in.Hold)
	assert.Equal(t, 3, in.FailedOps)    // 1 timeout + 1 reply + 1 bad reply
	assert.Equal(t, 5, in.AttemptedOps) // 2 samples + 3 failed
	assert.Equal(t, 1, in.Saturation)
	assert.Empty(t, in.Pending, "synchronous RPC has no pending durables")
	require.Len(t, in.Latencies, 1)
	assert.Equal(t, "thread-read", in.Latencies[0].Name)
	assert.Len(t, in.Latencies[0].Samples, 2)
}

func TestBuildThreadReadInputs_PopulatesEmitUnderrun(t *testing.T) {
	c := newThreadReadCollector()
	c.RecordUnderrun(7)
	c.RecordUnderrun(3)
	in := buildThreadReadInputs(2000, 30*time.Second, c)
	assert.Equal(t, 10, in.EmitUnderrun)
}

func TestThreadReadWorkload_Label(t *testing.T) {
	w := &threadReadWorkload{}
	assert.Equal(t, "thread-read", w.Label())
}

func TestDefaultSteps_ThreadRead(t *testing.T) {
	assert.Equal(t, "200,500,1000,2000,5000", defaultSteps("thread-read"))
}
