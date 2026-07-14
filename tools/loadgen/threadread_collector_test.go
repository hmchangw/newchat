package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThreadReadCollector_RecordsSamplesAndErrors(t *testing.T) {
	c := newThreadReadCollector()
	c.RecordSample(threadReadSample{Latency: 3 * time.Millisecond, At: time.Now()})
	c.RecordSample(threadReadSample{Latency: 7 * time.Millisecond, At: time.Now()})
	c.RecordError(errClassTimeout, time.Millisecond)
	c.RecordError(errClassReply, time.Millisecond)
	c.RecordBadReply(time.Millisecond)
	c.RecordSaturation()
	c.RecordUnderrun(4)
	c.RecordUnderrun(0) // no-op
	c.RecordNoParents()

	assert.Len(t, c.Samples(), 2)
	assert.Equal(t, 1, c.TimeoutErrors())
	assert.Equal(t, 1, c.ReplyErrors())
	assert.Equal(t, 1, c.BadReplyCount())
	assert.Equal(t, 1, c.SaturationCount())
	assert.Equal(t, 4, c.UnderrunCount())
	assert.Equal(t, 1, c.NoParentsCount())
}

func TestThreadReadCollector_SamplesReturnsCopy(t *testing.T) {
	c := newThreadReadCollector()
	c.RecordSample(threadReadSample{Latency: time.Millisecond, At: time.Now()})
	got := c.Samples()
	got[0].Latency = 999 * time.Second // mutate the copy
	assert.Equal(t, time.Millisecond, c.Samples()[0].Latency, "Samples must return a defensive copy")
}
