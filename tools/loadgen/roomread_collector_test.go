package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRoomReadCollector_Aggregates(t *testing.T) {
	c := NewRoomReadCollector()

	c.RecordSample(RoomReadSample{Latency: 5 * time.Millisecond, At: time.Now()})
	c.RecordSample(RoomReadSample{Latency: 7 * time.Millisecond, At: time.Now()})
	c.RecordError(errClassTimeout, 9*time.Millisecond)
	c.RecordError(errClassReply, 2*time.Millisecond)
	c.RecordBadReply(3 * time.Millisecond)
	c.RecordSaturation()
	c.RecordSaturation()

	assert.Len(t, c.Samples(), 2)
	assert.Equal(t, 1, c.TimeoutErrors())
	assert.Equal(t, 1, c.ReplyErrors())
	assert.Equal(t, 1, c.BadReplyCount())
	assert.Equal(t, 2, c.SaturationCount())
}

func TestRoomReadCollector_Underrun(t *testing.T) {
	c := NewRoomReadCollector()
	assert.Equal(t, 0, c.UnderrunCount())
	c.RecordUnderrun(5)
	c.RecordUnderrun(0) // zero is a no-op tick, must not change the tally
	c.RecordUnderrun(4)
	assert.Equal(t, 9, c.UnderrunCount())
}

func TestRoomReadCollector_SamplesIsCopy(t *testing.T) {
	c := NewRoomReadCollector()
	c.RecordSample(RoomReadSample{Latency: time.Millisecond, At: time.Now()})
	got := c.Samples()
	got[0].Latency = 999 * time.Second
	assert.Equal(t, time.Millisecond, c.Samples()[0].Latency, "Samples must return a defensive copy")
}
