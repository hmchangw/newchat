package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadReceiptCollector_Underrun(t *testing.T) {
	c := NewReadReceiptCollector()
	assert.Equal(t, 0, c.UnderrunCount())
	c.RecordUnderrun(5)
	c.RecordUnderrun(0) // zero is a no-op tick, must not change the tally
	c.RecordUnderrun(4)
	assert.Equal(t, 9, c.UnderrunCount())
}

func TestReadReceiptCollector_SamplesAndErrors(t *testing.T) {
	c := NewReadReceiptCollector()
	c.RecordSample(15 * time.Millisecond)
	c.RecordSample(20 * time.Millisecond)
	c.RecordError(errClassTimeout)
	c.RecordError(errClassReply)
	c.RecordError(errClassBadReply)
	c.RecordBadRequest()
	c.RecordSaturation()
	c.RecordSaturation()

	assert.Equal(t, []time.Duration{15 * time.Millisecond, 20 * time.Millisecond}, c.Samples())
	// Failed = timeout + reply + bad_reply + bad_request = 4
	assert.Equal(t, 4, c.Failed())
	assert.Equal(t, 2, c.Saturation())
}

func TestReadReceiptCollector_EmptyIsZero(t *testing.T) {
	c := NewReadReceiptCollector()
	assert.Empty(t, c.Samples())
	assert.Equal(t, 0, c.Failed())
	assert.Equal(t, 0, c.Saturation())
}
