package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionListCollector_RecordsSamplesAndRowStats(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordSample(SubscriptionListSample{Latency: 10 * time.Millisecond, Rows: 40, HasMore: true})
	c.RecordSample(SubscriptionListSample{Latency: 30 * time.Millisecond, Rows: 20, HasMore: false})

	samples := c.Samples()
	require.Len(t, samples, 2)
	assert.Equal(t, 60, c.TotalRows())
	assert.InDelta(t, 30.0, c.MeanRows(), 0.001)
	assert.Equal(t, 1, c.HasMoreCount())
}

func TestSubscriptionListCollector_MeanRowsOnEmptyCollectorIsZero(t *testing.T) {
	c := NewSubscriptionListCollector()
	assert.Equal(t, 0, c.TotalRows())
	assert.InDelta(t, 0.0, c.MeanRows(), 0.001)
}

func TestSubscriptionListCollector_ErrorClassesAreCountedSeparately(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordError(errClassTimeout, time.Millisecond)
	c.RecordError(errClassTimeout, time.Millisecond)
	c.RecordError(errClassReply, time.Millisecond)
	c.RecordBadReply(time.Millisecond)
	c.RecordEmptyPage(time.Millisecond)

	assert.Equal(t, 2, c.TimeoutErrors())
	assert.Equal(t, 1, c.ReplyErrors())
	assert.Equal(t, 1, c.BadReplyCount())
	assert.Equal(t, 1, c.EmptyPageCount())
	// An empty page never lands in the latency tape: it is not a measurement of
	// the work the endpoint is supposed to be doing.
	assert.Empty(t, c.Samples())
}

func TestSubscriptionListCollector_SaturationAndUnderrun(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordSaturation()
	c.RecordSaturation()
	c.RecordUnderrun(5)
	c.RecordUnderrun(0)
	c.RecordUnderrun(-3)

	assert.Equal(t, 2, c.SaturationCount())
	assert.Equal(t, 5, c.UnderrunCount(), "non-positive underrun ticks are no-ops")
}

func TestSubscriptionListCollector_SamplesReturnsDefensiveCopy(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordSample(SubscriptionListSample{Latency: time.Millisecond, Rows: 1})

	got := c.Samples()
	got[0].Rows = 999
	assert.Equal(t, 1, c.Samples()[0].Rows)
}

func TestSubscriptionListCollector_ConcurrentUseIsSafe(t *testing.T) {
	c := NewSubscriptionListCollector()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.RecordSample(SubscriptionListSample{Latency: time.Millisecond, Rows: 2})
			c.RecordError(errClassTimeout, time.Millisecond)
			c.RecordEmptyPage(time.Millisecond)
			c.RecordSaturation()
			c.RecordUnderrun(1)
		}()
	}
	wg.Wait()

	assert.Len(t, c.Samples(), 50)
	assert.Equal(t, 100, c.TotalRows())
	assert.Equal(t, 50, c.TimeoutErrors())
	assert.Equal(t, 50, c.EmptyPageCount())
	assert.Equal(t, 50, c.SaturationCount())
	assert.Equal(t, 50, c.UnderrunCount())
}
