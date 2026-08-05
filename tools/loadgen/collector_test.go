package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollector_E1ReplyMatches(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	c.RecordPublish("req-1", "msg-1", now)
	c.RecordReply("req-1", now.Add(5*time.Millisecond))
	assert.Equal(t, 1, c.E1Count())
	assert.Equal(t, []time.Duration{5 * time.Millisecond}, c.E1Samples())
}

func TestCollector_E1UnknownIgnored(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	c.RecordReply("unknown", time.Unix(0, 0))
	assert.Equal(t, 0, c.E1Count())
}

func TestCollector_E2BroadcastMatches(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	c.RecordPublish("req-1", "msg-1", now)
	c.RecordBroadcast("msg-1", now.Add(8*time.Millisecond))
	assert.Equal(t, 1, c.E2Count())
	assert.Equal(t, []time.Duration{8 * time.Millisecond}, c.E2Samples())
}

func TestCollector_E1AndE2Independent(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	c.RecordPublish("req-1", "msg-1", now)
	c.RecordReply("req-1", now.Add(5*time.Millisecond))
	c.RecordBroadcast("msg-1", now.Add(8*time.Millisecond))
	assert.Equal(t, 1, c.E1Count())
	assert.Equal(t, 1, c.E2Count())
}

func TestCollector_MissingCountsAtFinalize(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	c.RecordPublish("req-1", "msg-1", now)
	c.RecordPublish("req-2", "msg-2", now)
	c.RecordReply("req-1", now.Add(5*time.Millisecond))
	// req-2 reply never arrives; msg-1 and msg-2 broadcasts never arrive
	missingReplies, missingBroadcasts := c.Finalize()
	assert.Equal(t, 1, missingReplies)
	assert.Equal(t, 2, missingBroadcasts)
}

func TestCollector_WarmupDiscards(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	start := time.Unix(0, 0)
	warmupEnd := start.Add(1 * time.Second)
	// In warmup window:
	c.RecordPublish("req-warm", "msg-warm", start)
	c.RecordReply("req-warm", start.Add(10*time.Millisecond))
	// Past warmup:
	c.RecordPublish("req-real", "msg-real", warmupEnd.Add(100*time.Millisecond))
	c.RecordReply("req-real", warmupEnd.Add(105*time.Millisecond))

	c.DiscardBefore(warmupEnd)
	require.Equal(t, 1, c.E1Count())
	assert.Equal(t, []time.Duration{5 * time.Millisecond}, c.E1Samples())
}

func TestCollector_E2UnknownIgnored(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	c.RecordBroadcast("unknown", time.Unix(0, 0))
	assert.Equal(t, 0, c.E2Count())
}

func TestCollector_SamplesReturnedSorted(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	// Publish three messages, record replies in a non-sorted order.
	c.RecordPublish("r-1", "m-1", now)
	c.RecordPublish("r-2", "m-2", now)
	c.RecordPublish("r-3", "m-3", now)
	c.RecordReply("r-1", now.Add(10*time.Millisecond))
	c.RecordReply("r-2", now.Add(2*time.Millisecond))
	c.RecordReply("r-3", now.Add(7*time.Millisecond))
	assert.Equal(t, []time.Duration{
		2 * time.Millisecond, 7 * time.Millisecond, 10 * time.Millisecond,
	}, c.E1Samples())
}

func TestCollector_ConcurrentRecordAndSnapshot(t *testing.T) {
	// Race-detector-friendly stress: one goroutine records publishes and
	// replies; another polls E1Samples. Verifies that no data race occurs
	// when snapshots are taken concurrently with mutations.
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			rid := "r-" + strconv.Itoa(i)
			mid := "m-" + strconv.Itoa(i)
			c.RecordPublish(rid, mid, now)
			c.RecordReply(rid, now.Add(time.Duration(i)*time.Microsecond))
		}
	}()
	for i := 0; i < 500; i++ {
		_ = c.E1Samples()
	}
	<-done
	require.GreaterOrEqual(t, c.E1Count(), 1)
}

func TestCollector_RecordPublishFailedRemovesOrphans(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	c.RecordPublish("r-1", "m-1", now)
	c.RecordPublish("r-2", "m-2", now)
	// r-1 / m-1 get replied + broadcast; r-2 / m-2 "failed to publish" and get cleaned up.
	c.RecordReply("r-1", now.Add(5*time.Millisecond))
	c.RecordBroadcast("m-1", now.Add(8*time.Millisecond))
	c.RecordPublishFailed("r-2", "m-2")

	missingReplies, missingBroadcasts := c.Finalize()
	assert.Equal(t, 0, missingReplies)
	assert.Equal(t, 0, missingBroadcasts)
}

func TestCollector_RecordPublishBroadcastOnly_IgnoredByE1(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	c.RecordPublishBroadcastOnly("m-1", now)
	// A reply correlated by requestID should NOT find this message
	// because we didn't populate byReqID.
	c.RecordReply("some-req-id", now.Add(5*time.Millisecond))
	assert.Equal(t, 0, c.E1Count())

	// A broadcast matching the msg-id should be recorded.
	c.RecordBroadcast("m-1", now.Add(8*time.Millisecond))
	assert.Equal(t, 1, c.E2Count())
}

func TestCollector_RecordPublishBroadcastOnly_FinalizeNoMissingReplies(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	c.RecordPublishBroadcastOnly("m-1", now)
	c.RecordPublishBroadcastOnly("m-2", now)
	c.RecordBroadcast("m-1", now.Add(5*time.Millisecond))
	// m-2 never gets a broadcast — that's the only missing event class.
	missingReplies, missingBroadcasts := c.Finalize()
	assert.Equal(t, 0, missingReplies, "canonical mode should never produce missing replies")
	assert.Equal(t, 1, missingBroadcasts)
}

// A malformed reply is already counted under the bad_reply error reason. If it
// also left its correlation entry behind, the same message would be counted a
// second time as a missing reply once Finalize feeds the verdict.
func TestCollector_DiscardReply_ConsumesWithoutSample(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	now := time.Unix(0, 0)
	c.RecordPublish("req-1", "msg-1", now)

	c.DiscardReply("req-1")

	assert.Equal(t, 0, c.E1Count(), "a discarded reply is not a latency sample")
	missingReplies, _ := c.Finalize()
	assert.Equal(t, 0, missingReplies, "a discarded reply must not also count as missing")
}

func TestCollector_DiscardReply_UnknownRequestIDIsNoop(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	c.DiscardReply("never-published")
	missingReplies, missingBroadcasts := c.Finalize()
	assert.Equal(t, 0, missingReplies)
	assert.Equal(t, 0, missingBroadcasts)
}

// Continuous-emitter scenarios (daily) can't stop publishing and drain, so
// "unmatched" alone would count everything still legitimately in flight.
// Age is what separates a dropped broadcast from an in-flight one.
func TestCollector_MissingBroadcastsOlderThan(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	now := time.Unix(1000, 0)
	cutoff := now.Add(-2 * time.Second)

	c.RecordPublishBroadcastOnly("old-dropped", now.Add(-5*time.Second))
	c.RecordPublishBroadcastOnly("old-delivered", now.Add(-4*time.Second))
	c.RecordPublishBroadcastOnly("recent-inflight", now.Add(-500*time.Millisecond))
	c.RecordBroadcast("old-delivered", now.Add(-3*time.Second))

	assert.Equal(t, 1, c.MissingBroadcastsOlderThan(cutoff),
		"only the aged-out unmatched publish counts")
}

func TestCollector_MissingBroadcastsOlderThan_BoundaryIsInclusive(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	cutoff := time.Unix(1000, 0)
	c.RecordPublishBroadcastOnly("exactly-at-cutoff", cutoff)

	assert.Equal(t, 1, c.MissingBroadcastsOlderThan(cutoff))
}

func TestCollector_MissingBroadcastsOlderThan_IgnoresReplyCorrelation(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	now := time.Unix(1000, 0)
	// RecordPublish populates byReqID too. A caller that never correlates
	// replies must not see those entries leak into the broadcast count.
	c.RecordPublish("req-1", "msg-1", now.Add(-10*time.Second))
	c.RecordBroadcast("msg-1", now.Add(-9*time.Second))

	assert.Equal(t, 0, c.MissingBroadcastsOlderThan(now))
}

func TestCollector_Reset(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	now := time.Now()
	c.RecordPublish("req-1", "msg-1", now)
	c.RecordReply("req-1", now.Add(10*time.Millisecond))
	c.RecordBroadcast("msg-1", now.Add(20*time.Millisecond))
	require.Equal(t, 1, c.E1Count())
	require.Equal(t, 1, c.E2Count())

	c.Reset()

	assert.Equal(t, 0, c.E1Count())
	assert.Equal(t, 0, c.E2Count())
	mr, mb := c.Finalize()
	assert.Equal(t, 0, mr)
	assert.Equal(t, 0, mb)
	// After reset, a fresh publish+reply correlates normally.
	c.RecordPublish("req-2", "msg-2", now)
	c.RecordReply("req-2", now.Add(5*time.Millisecond))
	assert.Equal(t, 1, c.E1Count())
}
