package main

import (
	"sync"
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

// Collector correlates publishes with replies (E1) and broadcasts (E2).
type Collector struct {
	m       *Metrics
	preset  string
	mu      sync.Mutex
	byReqID map[string]publishEntry
	byMsgID map[string]publishEntry
	e1      []sample
	e2      []sample
}

// NewCollector returns a ready-to-use Collector.
func NewCollector(m *Metrics, preset string) *Collector {
	return &Collector{
		m: m, preset: preset,
		byReqID: make(map[string]publishEntry),
		byMsgID: make(map[string]publishEntry),
	}
}

// Reset clears all correlation state and accumulated samples. Used by the
// max-rps ramp to start each step's hold window from a clean slate while the
// E1/E2 subscriptions (which hold this *Collector pointer) stay alive.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byReqID = make(map[string]publishEntry)
	c.byMsgID = make(map[string]publishEntry)
	c.e1 = nil
	c.e2 = nil
}

// RecordPublish stores the publish time under both correlation keys.
func (c *Collector) RecordPublish(requestID, messageID string, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byReqID[requestID] = publishEntry{publishedAt: t}
	c.byMsgID[messageID] = publishEntry{publishedAt: t}
}

// RecordReply consumes one pending publish keyed by requestID.
func (c *Collector) RecordReply(requestID string, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.byReqID[requestID]
	if !ok {
		return
	}
	delete(c.byReqID, requestID)
	d := at.Sub(e.publishedAt)
	c.e1 = append(c.e1, sample{publishedAt: e.publishedAt, latency: d})
	c.m.E1Latency.WithLabelValues(c.preset).Observe(d.Seconds())
}

// RecordPublishBroadcastOnly stores only the message-ID correlation, for
// injection modes that bypass the gatekeeper (no reply is expected).
func (c *Collector) RecordPublishBroadcastOnly(messageID string, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byMsgID[messageID] = publishEntry{publishedAt: t}
}

// RecordPublishFailed removes entries previously stored by RecordPublish.
// Use when the publish itself failed (message never reached NATS) so the
// orphans do not inflate Finalize's missing-reply / missing-broadcast counts.
func (c *Collector) RecordPublishFailed(requestID, messageID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.byReqID, requestID)
	delete(c.byMsgID, messageID)
}

// RecordBroadcast consumes one pending publish keyed by messageID.
func (c *Collector) RecordBroadcast(messageID string, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.byMsgID[messageID]
	if !ok {
		return
	}
	delete(c.byMsgID, messageID)
	d := at.Sub(e.publishedAt)
	c.e2 = append(c.e2, sample{publishedAt: e.publishedAt, latency: d})
	c.m.E2Latency.WithLabelValues(c.preset).Observe(d.Seconds())
}

// DiscardBefore drops any samples whose publish time is before cutoff (warmup).
func (c *Collector) DiscardBefore(cutoff time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.e1 = filterAtOrAfter(c.e1, cutoff)
	c.e2 = filterAtOrAfter(c.e2, cutoff)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.byReqID), len(c.byMsgID)
}

// E1Count returns the number of matched E1 samples.
func (c *Collector) E1Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.e1)
}

// E2Count returns the number of matched E2 samples.
func (c *Collector) E2Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.e2)
}

// E1Samples returns a sorted copy of E1 latencies for tests/reporting.
func (c *Collector) E1Samples() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return snapshotLatencies(c.e1)
}

// E2Samples returns a sorted copy of E2 latencies for tests/reporting.
func (c *Collector) E2Samples() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return snapshotLatencies(c.e2)
}
