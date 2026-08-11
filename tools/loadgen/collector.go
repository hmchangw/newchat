package main

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type publishEntry struct {
	publishedAt time.Time
}

// sample pairs a latency with its publish timestamp so warmup can discard by time.
type sample struct {
	publishedAt time.Time
	latency     time.Duration
}

// collectorShardCount controls how the byReqID/byMsgID maps and e1/e2 slices
// are split across per-shard mutexes. Must be a power of two so the modulo
// reduces to a bit-and. 64 is enough headroom for the ~520k locks/sec a
// busy daily-IM run produces at N=100k — that's ~8k/sec/shard, well under
// what a single mutex can absorb without measurable contention.
const collectorShardCount = 64

// reqShard holds the requestID-keyed correlation map and its replied-latency
// slice. RecordPublish and RecordPublishFailed write here; RecordReply reads
// and consumes here.
type reqShard struct {
	mu      sync.Mutex
	byReqID map[string]publishEntry
	e1      []sample
}

// msgShard holds the messageID-keyed correlation map and its broadcast-
// latency slice. RecordPublish/RecordPublishBroadcastOnly write here;
// RecordBroadcast reads and consumes here.
type msgShard struct {
	mu      sync.Mutex
	byMsgID map[string]publishEntry
	e2      []sample
}

// Collector correlates publishes with replies (E1) and broadcasts (E2).
// The correlation maps and latency slices are sharded by FNV-1a hash of the
// key (requestID or messageID) to eliminate the single-mutex bottleneck
// that capped throughput at ~150k locks/sec on busy daily-IM runs.
type Collector struct {
	m      *Metrics
	preset string

	reqShards [collectorShardCount]*reqShard
	msgShards [collectorShardCount]*msgShard

	multiplexDrops atomic.Int64
	attempted      atomic.Int64
	failed         atomic.Int64

	// broadcastEligible counts only the actions that actually register a
	// broadcast correlation. It is the denominator for the missing-broadcast
	// rate: `attempted` covers every action kind (mark-read, scroll-history,
	// room-create, …), none of which can produce a missing broadcast, so
	// dividing by it dilutes the rate by whatever the send share happens to
	// be — ~64% under the default weights, which turns a real 0.15% send-loss
	// into a reported 0.0948% and slips it past a 0.1% gate.
	broadcastEligible atomic.Int64

	// actMu guards actSamples. Per-action latency samples are kept here
	// (one slice per action kind) so the daily-IM report can surface
	// p50/p95/p99 broken down by sendMessage / scrollHistory / memberAdd /
	// etc. — separate from the broadcast-correlation samples in msgShards
	// (which only capture publish→broadcast for sendMessage/threadReply).
	actMu      sync.Mutex
	actSamples map[int][]time.Duration
}

func shardIdx(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32() & (collectorShardCount - 1)
}

// RecordMultiplexDrop increments the count of broadcasts dropped because the
// destination per-user inbox channel was full.
func (c *Collector) RecordMultiplexDrop() { c.multiplexDrops.Add(1) }

// MultiplexDrops returns the total number of dropped broadcasts.
func (c *Collector) MultiplexDrops() int64 { return c.multiplexDrops.Load() }

// NewCollector returns a ready-to-use Collector.
func NewCollector(m *Metrics, preset string) *Collector {
	c := &Collector{m: m, preset: preset, actSamples: make(map[int][]time.Duration)}
	for i := range c.reqShards {
		c.reqShards[i] = &reqShard{byReqID: make(map[string]publishEntry)}
	}
	for i := range c.msgShards {
		c.msgShards[i] = &msgShard{byMsgID: make(map[string]publishEntry)}
	}
	return c
}

// RecordPublish stores the publish time under both correlation keys.
// The two writes land on independent shards (no nesting), so concurrent
// callers contend per shard, not on a global mutex.
func (c *Collector) RecordPublish(requestID, messageID string, t time.Time) {
	pe := publishEntry{publishedAt: t}
	rs := c.reqShards[shardIdx(requestID)]
	rs.mu.Lock()
	rs.byReqID[requestID] = pe
	rs.mu.Unlock()
	ms := c.msgShards[shardIdx(messageID)]
	ms.mu.Lock()
	ms.byMsgID[messageID] = pe
	ms.mu.Unlock()
}

// RecordReply consumes one pending publish keyed by requestID.
func (c *Collector) RecordReply(requestID string, at time.Time) {
	rs := c.reqShards[shardIdx(requestID)]
	rs.mu.Lock()
	e, ok := rs.byReqID[requestID]
	if !ok {
		rs.mu.Unlock()
		return
	}
	delete(rs.byReqID, requestID)
	d := at.Sub(e.publishedAt)
	rs.e1 = append(rs.e1, sample{publishedAt: e.publishedAt, latency: d})
	rs.mu.Unlock()
	if c.m != nil {
		c.m.E1Latency.WithLabelValues(c.preset).Observe(d.Seconds())
	}
}

// RecordPublishBroadcastOnly stores only the message-ID correlation, for
// injection modes that bypass the gatekeeper (no reply is expected).
func (c *Collector) RecordPublishBroadcastOnly(messageID string, t time.Time) {
	ms := c.msgShards[shardIdx(messageID)]
	ms.mu.Lock()
	ms.byMsgID[messageID] = publishEntry{publishedAt: t}
	ms.mu.Unlock()
}

// DiscardBroadcastPublish removes a correlation registered before a publish
// that then failed, so the orphan is not reported as a dropped broadcast.
// Counterpart to registering with RecordPublishBroadcastOnly ahead of the
// publish; the denominator is only incremented (RecordBroadcastEligible) once
// the publish succeeds, so numerator and denominator still come from the same
// accepted set.
func (c *Collector) DiscardBroadcastPublish(messageID string) {
	ms := c.msgShards[shardIdx(messageID)]
	ms.mu.Lock()
	delete(ms.byMsgID, messageID)
	ms.mu.Unlock()
}

// RecordPublishFailed removes entries previously stored by RecordPublish.
// Use when the publish itself failed (message never reached NATS) so the
// orphans do not inflate Finalize's missing-reply / missing-broadcast counts.
func (c *Collector) RecordPublishFailed(requestID, messageID string) {
	rs := c.reqShards[shardIdx(requestID)]
	rs.mu.Lock()
	delete(rs.byReqID, requestID)
	rs.mu.Unlock()
	ms := c.msgShards[shardIdx(messageID)]
	ms.mu.Lock()
	delete(ms.byMsgID, messageID)
	ms.mu.Unlock()
}

// DiscardReply consumes one pending publish keyed by requestID without
// recording a latency sample. Use when a reply arrived but was unusable (e.g.
// a malformed body already counted under the bad_reply error reason), so the
// same message is not counted a second time as a missing reply by Finalize.
func (c *Collector) DiscardReply(requestID string) {
	rs := c.reqShards[shardIdx(requestID)]
	rs.mu.Lock()
	delete(rs.byReqID, requestID)
	rs.mu.Unlock()
}

// RecordBroadcast consumes one pending publish keyed by messageID.
func (c *Collector) RecordBroadcast(messageID string, at time.Time) {
	ms := c.msgShards[shardIdx(messageID)]
	ms.mu.Lock()
	e, ok := ms.byMsgID[messageID]
	if !ok {
		ms.mu.Unlock()
		return
	}
	delete(ms.byMsgID, messageID)
	d := at.Sub(e.publishedAt)
	ms.e2 = append(ms.e2, sample{publishedAt: e.publishedAt, latency: d})
	ms.mu.Unlock()
	if c.m != nil {
		c.m.E2Latency.WithLabelValues(c.preset).Observe(d.Seconds())
	}
}

// DiscardBefore drops any samples whose publish time is before cutoff (warmup).
func (c *Collector) DiscardBefore(cutoff time.Time) {
	for _, rs := range &c.reqShards {
		rs.mu.Lock()
		rs.e1 = filterAtOrAfter(rs.e1, cutoff)
		rs.mu.Unlock()
	}
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		ms.e2 = filterAtOrAfter(ms.e2, cutoff)
		ms.mu.Unlock()
	}
}

func filterAtOrAfter(in []sample, cutoff time.Time) []sample {
	out := in[:0]
	for i := range in {
		if !in[i].publishedAt.Before(cutoff) {
			out = append(out, in[i])
		}
	}
	return out
}

// Finalize returns the count of unmatched publishes as missing replies and broadcasts.
func (c *Collector) Finalize() (missingReplies int, missingBroadcasts int) {
	for _, rs := range &c.reqShards {
		rs.mu.Lock()
		missingReplies += len(rs.byReqID)
		rs.mu.Unlock()
	}
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		missingBroadcasts += len(ms.byMsgID)
		ms.mu.Unlock()
	}
	return
}

// MissingInWindow counts publishes that are still unmatched and were published
// within [start, end] inclusive. Both boundaries matter, for different reasons:
//
//   - end: a scenario whose emitters run continuously (daily) has no quiet
//     point at which every unmatched publish is a drop — at any instant some
//     are legitimately in flight. Callers wait out a delivery grace and pass
//     the hold boundary as end, so in-flight publishes from the grace itself
//     are not scored.
//   - start: Reset races with a still-running generator. A publish whose
//     RecordPublish lands just after the shard was cleared keeps its
//     pre-holdStart timestamp, and DiscardBefore only filters the completed
//     sample slices — not the correlation maps. Without a start boundary that
//     warm-up entry would trip the measured step.
//
// Callers that never call RecordReply should ignore the reply count: every
// byReqID entry stays unmatched forever, so it would read as 100% failure.
func (c *Collector) MissingInWindow(start, end time.Time) (replies, broadcasts int) {
	inWindow := func(t time.Time) bool { return !t.Before(start) && !t.After(end) }
	for _, rs := range &c.reqShards {
		rs.mu.Lock()
		for _, e := range rs.byReqID {
			if inWindow(e.publishedAt) {
				replies++
			}
		}
		rs.mu.Unlock()
	}
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		for _, e := range ms.byMsgID {
			if inWindow(e.publishedAt) {
				broadcasts++
			}
		}
		ms.mu.Unlock()
	}
	return replies, broadcasts
}

// ReplyStatsInWindow returns the reply-eligible publishes and the
// still-unmatched subset within [start, end], the reply-side counterpart to
// BroadcastStatsInWindow.
//
// Missing replies used to be divided by AttemptedOps, which counts marshal and
// publish failures too. Those never register a correlation, so they could only
// dilute the rate: 9 publish errors and 10 missing replies in 10,000 attempts
// reports exactly 0.1% and slips past a strict `>` gate, although 10 of the
// 9,991 requests that were actually accepted is over it. Deriving both sides
// from the same accepted set removes that.
func (c *Collector) ReplyStatsInWindow(start, end time.Time) (eligible, missing int) {
	inWindow := func(t time.Time) bool { return !t.Before(start) && !t.After(end) }
	for _, rs := range &c.reqShards {
		rs.mu.Lock()
		for _, e := range rs.byReqID {
			if inWindow(e.publishedAt) {
				eligible++
				missing++
			}
		}
		for _, s := range rs.e1 {
			if inWindow(s.publishedAt) {
				eligible++
			}
		}
		rs.mu.Unlock()
	}
	return eligible, missing
}

// BroadcastStatsInWindow returns the accepted broadcast-eligible publishes and
// the still-unmatched subset within [start, end]. It derives both values from
// the same correlation entries/samples so a hold-boundary race cannot put a
// publish in the numerator but outside the denominator.
func (c *Collector) BroadcastStatsInWindow(start, end time.Time) (eligible, missing int) {
	inWindow := func(t time.Time) bool { return !t.Before(start) && !t.After(end) }
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		for _, e := range ms.byMsgID {
			if inWindow(e.publishedAt) {
				eligible++
				missing++
			}
		}
		for _, s := range ms.e2 {
			if inWindow(s.publishedAt) {
				eligible++
			}
		}
		ms.mu.Unlock()
	}
	return eligible, missing
}

// LatencySamplesUpTo returns broadcast latencies in milliseconds for publishes
// made at or before cutoff.
//
// Reading samples only after a delivery grace would import publishes made
// during the grace; reading them before it would drop the broadcasts that
// arrived late — and those are the slowest, so excluding them makes the
// percentiles optimistic in exactly the direction that hides a struggling
// pipeline. Filtering by publish time keeps the slow tail without widening the
// window.
func (c *Collector) LatencySamplesUpTo(cutoff time.Time) []float64 {
	var out []float64
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		for i := range ms.e2 {
			if !ms.e2[i].publishedAt.After(cutoff) {
				out = append(out, float64(ms.e2[i].latency.Microseconds())/1000.0)
			}
		}
		ms.mu.Unlock()
	}
	return out
}

// E1Count returns the number of matched E1 samples.
func (c *Collector) E1Count() int {
	total := 0
	for _, rs := range &c.reqShards {
		rs.mu.Lock()
		total += len(rs.e1)
		rs.mu.Unlock()
	}
	return total
}

// E2Count returns the number of matched E2 samples.
func (c *Collector) E2Count() int {
	total := 0
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		total += len(ms.e2)
		ms.mu.Unlock()
	}
	return total
}

// E1Samples returns a sorted copy of E1 latencies for tests/reporting.
func (c *Collector) E1Samples() []time.Duration {
	var all []sample
	for _, rs := range &c.reqShards {
		rs.mu.Lock()
		all = append(all, rs.e1...)
		rs.mu.Unlock()
	}
	return snapshotLatencies(all)
}

// E2Samples returns a sorted copy of E2 latencies for tests/reporting.
func (c *Collector) E2Samples() []time.Duration {
	var all []sample
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		all = append(all, ms.e2...)
		ms.mu.Unlock()
	}
	return snapshotLatencies(all)
}

// RecordActionAttempt is called by the daily action emitter for every action
// dispatched, regardless of outcome.
func (c *Collector) RecordActionAttempt() { c.attempted.Add(1) }

// RecordActionFailure is called when an action returns an error.
func (c *Collector) RecordActionFailure() { c.failed.Add(1) }

// RecordBroadcastEligible is called for every action that registers a
// broadcast correlation (i.e. one whose loss would show up as a missing
// broadcast). It is deliberately separate from RecordActionAttempt so the
// missing-broadcast rate can be scored against its own denominator.
func (c *Collector) RecordBroadcastEligible() { c.broadcastEligible.Add(1) }

// BroadcastEligibleOps returns the count of broadcast-eligible actions since
// last Reset.
func (c *Collector) BroadcastEligibleOps() int64 { return c.broadcastEligible.Load() }

// AttemptedOps returns the total count of action attempts since last Reset.
func (c *Collector) AttemptedOps() int64 { return c.attempted.Load() }

// FailedOps returns the total count of failed actions since last Reset.
func (c *Collector) FailedOps() int64 { return c.failed.Load() }

// RecordActionLatency stores one wall-clock latency sample for the given
// action kind. Called by the daily emitter after each handler returns so
// the per-action breakdown in the report covers every action — not just
// the publish→broadcast round-trip the Collector's e2 slice captures.
func (c *Collector) RecordActionLatency(kind int, d time.Duration) {
	c.actMu.Lock()
	c.actSamples[kind] = append(c.actSamples[kind], d)
	c.actMu.Unlock()
}

// ActionLatencies returns a copy of the per-action latency samples in
// milliseconds, keyed by action-kind int. The caller computes whatever
// percentiles it needs.
func (c *Collector) ActionLatencies() map[int][]float64 {
	c.actMu.Lock()
	defer c.actMu.Unlock()
	out := make(map[int][]float64, len(c.actSamples))
	for k, v := range c.actSamples {
		ms := make([]float64, len(v))
		for i, d := range v {
			ms[i] = float64(d.Microseconds()) / 1000.0
		}
		out[k] = ms
	}
	return out
}

// Reset clears all per-step counters and sample slices.
// Called at the end of warmup so the hold window starts fresh.
func (c *Collector) Reset() {
	for _, rs := range &c.reqShards {
		rs.mu.Lock()
		rs.e1 = rs.e1[:0]
		clear(rs.byReqID)
		rs.mu.Unlock()
	}
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		ms.e2 = ms.e2[:0]
		clear(ms.byMsgID)
		ms.mu.Unlock()
	}
	c.actMu.Lock()
	clear(c.actSamples)
	c.actMu.Unlock()
	c.attempted.Store(0)
	c.failed.Store(0)
	c.broadcastEligible.Store(0)
}

// LatencySamples returns the current broadcast-latency samples in milliseconds.
// Used by the daily-IM verdict evaluator. Walks every shard once; per-shard
// lock is held only for the slice copy.
func (c *Collector) LatencySamples() []float64 {
	// Two-pass to pre-size: count first, then copy.
	total := 0
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		total += len(ms.e2)
		ms.mu.Unlock()
	}
	out := make([]float64, 0, total)
	for _, ms := range &c.msgShards {
		ms.mu.Lock()
		for i := range ms.e2 {
			out = append(out, float64(ms.e2[i].latency.Microseconds())/1000.0)
		}
		ms.mu.Unlock()
	}
	return out
}
