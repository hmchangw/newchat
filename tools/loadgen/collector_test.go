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

// Unmatched alone does not mean dropped. Two boundaries are needed: the step's
// start, because Reset races with a still-running generator and a warm-up
// publish can land in a freshly cleared shard; and the step's end, because a
// continuous emitter (daily) is still publishing while the grace elapses.
func TestCollector_MissingInWindow(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	start := time.Unix(1000, 0)
	end := start.Add(30 * time.Second)

	c.RecordPublishBroadcastOnly("before-window", start.Add(-time.Second))
	c.RecordPublishBroadcastOnly("in-window-dropped", start.Add(5*time.Second))
	c.RecordPublishBroadcastOnly("in-window-delivered", start.Add(6*time.Second))
	c.RecordPublishBroadcastOnly("after-window", end.Add(time.Second))
	c.RecordBroadcast("in-window-delivered", start.Add(7*time.Second))

	_, broadcasts := c.MissingInWindow(start, end)
	assert.Equal(t, 1, broadcasts, "only the in-window unmatched publish counts")
}

// A warm-up publish whose RecordPublish lands just after Reset keeps its
// pre-holdStart timestamp. DiscardBefore only filters the completed-sample
// slices, so without a start boundary that entry would trip the measured step.
func TestCollector_MissingInWindow_ExcludesWarmupLeak(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	holdStart := time.Unix(1000, 0)

	c.RecordPublish("warmup-req", "warmup-msg", holdStart.Add(-50*time.Millisecond))
	c.RecordPublish("hold-req", "hold-msg", holdStart.Add(time.Second))

	replies, broadcasts := c.MissingInWindow(holdStart, holdStart.Add(30*time.Second))
	assert.Equal(t, 1, replies, "the warm-up publish is outside the measured step")
	assert.Equal(t, 1, broadcasts)
}

func TestCollector_MissingInWindow_BoundariesAreInclusive(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	start := time.Unix(1000, 0)
	end := start.Add(10 * time.Second)
	c.RecordPublishBroadcastOnly("at-start", start)
	c.RecordPublishBroadcastOnly("at-end", end)

	_, broadcasts := c.MissingInWindow(start, end)
	assert.Equal(t, 2, broadcasts)
}

func TestCollector_BroadcastStatsInWindowUsesAcceptedPublishBoundary(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	start := time.Unix(1000, 0)
	end := start.Add(10 * time.Second)

	c.recordAcceptedBroadcastPublish("before", start.Add(-time.Second))
	c.recordAcceptedBroadcastPublish("delivered", start.Add(time.Second))
	c.recordAcceptedBroadcastPublish("missing", start.Add(2*time.Second))
	c.recordAcceptedBroadcastPublish("after", end.Add(time.Second))
	c.RecordBroadcast("delivered", start.Add(3*time.Second))

	eligible, missing := c.BroadcastStatsInWindow(start, end)
	assert.Equal(t, 2, eligible)
	assert.Equal(t, 1, missing)
}

// Broadcast-eligible attempts are the denominator for the missing-broadcast
// rate. Counting every action would understate the rate by the share of
// actions that cannot produce a broadcast at all.
func TestCollector_BroadcastEligibleOps(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	assert.Equal(t, int64(0), c.BroadcastEligibleOps())

	c.RecordActionAttempt()
	c.RecordActionAttempt()
	c.RecordBroadcastEligible()

	assert.Equal(t, int64(2), c.AttemptedOps())
	assert.Equal(t, int64(1), c.BroadcastEligibleOps(), "only sends are broadcast-eligible")
}

func TestCollector_Reset_ClearsBroadcastEligible(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	c.RecordBroadcastEligible()
	c.Reset()
	assert.Equal(t, int64(0), c.BroadcastEligibleOps())
}

// Broadcasts that land during the grace belong to the hold, but publishes made
// during the grace do not. Filtering by publish time keeps the slow tail
// (dropping it would make percentiles optimistic) without importing samples
// from outside the measured window.
func TestCollector_LatencySamplesInWindow(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	holdStart := time.Unix(999, 0)
	holdEnd := time.Unix(1000, 0)

	c.RecordPublishBroadcastOnly("before-hold", holdStart.Add(-time.Millisecond))
	c.RecordBroadcast("before-hold", holdStart.Add(time.Millisecond))
	c.RecordPublishBroadcastOnly("in-hold", holdEnd.Add(-time.Second))
	c.RecordBroadcast("in-hold", holdEnd.Add(300*time.Millisecond)) // arrived during grace
	c.RecordPublishBroadcastOnly("during-grace", holdEnd.Add(500*time.Millisecond))
	c.RecordBroadcast("during-grace", holdEnd.Add(600*time.Millisecond))

	samples := c.LatencySamplesInWindow(holdStart, holdEnd)
	assert.Len(t, samples, 1, "only publishes from inside the hold are timed")
	assert.InDelta(t, 1300.0, samples[0], 1.0, "the late-arriving slow sample is kept")
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

// recordAcceptedBroadcastPublish is the accepted-publish pair as the production
// callers apply it: register the correlation, then count the denominator. Tests
// that only care about the settled state use this instead of restating both
// calls; the ordering itself is covered in daily_actions_test.go.
func (c *Collector) recordAcceptedBroadcastPublish(messageID string, at time.Time) {
	c.RecordPublishBroadcastOnly(messageID, at)
	c.RecordBroadcastEligible()
}

// A broadcast that arrives before its correlation is registered used to be
// dropped as unmatched, and the late registration then read as a loss. With
// pre-publish registration the delivery matches.
func TestCollector_BroadcastArrivingBeforeEligibleCount(t *testing.T) {
	c := NewCollector(nil, "test")
	start := time.Now()

	c.RecordPublishBroadcastOnly("m-1", start)
	c.RecordBroadcast("m-1", start.Add(5*time.Millisecond)) // beats the accept
	c.RecordBroadcastEligible()

	eligible, missing := c.BroadcastStatsInWindow(start.Add(-time.Second), start.Add(time.Second))
	assert.Equal(t, 1, eligible)
	assert.Zero(t, missing, "a delivered broadcast must not be counted as dropped")
}

// A publish that fails after provisional registration leaves nothing behind.
func TestCollector_DiscardBroadcastPublish(t *testing.T) {
	c := NewCollector(nil, "test")
	start := time.Now()

	c.RecordPublishBroadcastOnly("m-1", start)
	c.DiscardBroadcastPublish("m-1")

	eligible, missing := c.BroadcastStatsInWindow(start.Add(-time.Second), start.Add(time.Second))
	assert.Zero(t, eligible)
	assert.Zero(t, missing, "a failed publish enters neither side of the ratio")
}

// The reply denominator is derived from the same accepted set as the numerator:
// unmatched correlations plus recorded replies, both filtered to the window.
func TestCollector_ReplyStatsInWindow(t *testing.T) {
	c := NewCollector(nil, "test")
	start := time.Now()
	end := start.Add(10 * time.Second)

	c.RecordPublish("before", "m-before", start.Add(-time.Second))
	c.RecordPublish("answered", "m-1", start.Add(time.Second))
	c.RecordPublish("dropped", "m-2", start.Add(2*time.Second))
	c.RecordPublish("after", "m-after", end.Add(time.Second))
	c.RecordReply("answered", start.Add(2*time.Second))

	eligible, missing := c.ReplyStatsInWindow(start, end)

	assert.Equal(t, 2, eligible, "one answered plus one dropped, both in window")
	assert.Equal(t, 1, missing)
}

// A publish that failed was removed from the correlation map, so it enters
// neither side — the property that stops it diluting the rate.
func TestCollector_ReplyStatsExcludesFailedPublishes(t *testing.T) {
	c := NewCollector(nil, "test")
	start := time.Now()

	c.RecordPublish("failed", "m-1", start)
	c.RecordPublishFailed("failed", "m-1")

	eligible, missing := c.ReplyStatsInWindow(start.Add(-time.Second), start.Add(time.Second))
	assert.Zero(t, eligible)
	assert.Zero(t, missing)
}
