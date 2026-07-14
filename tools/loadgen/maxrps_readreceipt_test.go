package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Compile-time check: readReceiptWorkload satisfies rpsWorkload.
var _ rpsWorkload = (*readReceiptWorkload)(nil)

func TestBuildReadReceiptInputs(t *testing.T) {
	c := NewReadReceiptCollector()
	c.RecordSample(10 * time.Millisecond)
	c.RecordSample(12 * time.Millisecond)
	c.RecordSample(14 * time.Millisecond)
	c.RecordError(errClassTimeout)
	c.RecordError(errClassReply)
	c.RecordSaturation()

	in := buildReadReceiptInputs(1000, 10*time.Second, c)

	assert.Equal(t, 1000, in.TargetRPS)
	assert.Equal(t, 10*time.Second, in.Hold)
	// AttemptedOps = samples 3 + failed 2 = 5
	assert.Equal(t, 5, in.AttemptedOps)
	assert.Equal(t, 2, in.FailedOps)
	assert.Equal(t, 1, in.Saturation)
	assert.Len(t, in.Latencies, 1)
	assert.Equal(t, "read-receipt", in.Latencies[0].Name)
	assert.Len(t, in.Latencies[0].Samples, 3)
	assert.Empty(t, in.Pending)
	assert.False(t, in.Inconclusive)
}

func TestBuildReadReceiptInputs_PopulatesEmitUnderrun(t *testing.T) {
	c := NewReadReceiptCollector()
	c.RecordUnderrun(5)
	c.RecordUnderrun(4)
	in := buildReadReceiptInputs(2000, 30*time.Second, c)
	assert.Equal(t, 9, in.EmitUnderrun)
}
