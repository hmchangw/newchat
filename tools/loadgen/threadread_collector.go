package main

import (
	"sync"
	"time"
)

// threadReadSample captures one completed GetThreadMessages round-trip.
type threadReadSample struct {
	Latency time.Duration
	At      time.Time
}

// threadReadCollector aggregates samples and errors across a workload run.
// All methods are safe for concurrent use. Reuses the package-shared errClass
// consts (errClassTimeout / errClassReply / errClassBadReply).
type threadReadCollector struct {
	mu         sync.Mutex
	samples    []threadReadSample
	errors     map[errClass]int
	saturation int
	underrun   int
	noParents  int
}

// newThreadReadCollector returns an empty collector.
func newThreadReadCollector() *threadReadCollector {
	return &threadReadCollector{errors: map[errClass]int{}}
}

// RecordSample stores one completed-call sample.
func (c *threadReadCollector) RecordSample(s threadReadSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, s)
}

// RecordError tallies a per-class transport/reply error.
func (c *threadReadCollector) RecordError(class errClass, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[class]++
}

// RecordBadReply tallies a reply that was undecodable or missing parentMessage.
func (c *threadReadCollector) RecordBadReply(_ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[errClassBadReply]++
}

// RecordSaturation tallies a tick that fired while the in-flight pool was full.
func (c *threadReadCollector) RecordSaturation() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saturation++
}

// RecordUnderrun adds n events the pacer could not release on schedule. n<=0 is a no-op.
func (c *threadReadCollector) RecordUnderrun(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.underrun += n
}

// RecordNoParents tallies a request that landed on a room with no seeded thread
// parents and was skipped. Informational — not counted as a failure.
func (c *threadReadCollector) RecordNoParents() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.noParents++
}

// Samples returns a defensive copy of the sample tape.
func (c *threadReadCollector) Samples() []threadReadSample {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]threadReadSample, len(c.samples))
	copy(out, c.samples)
	return out
}

// TimeoutErrors returns the timeout-class error count.
func (c *threadReadCollector) TimeoutErrors() int { return c.errCount(errClassTimeout) }

// ReplyErrors returns the reply-class error count.
func (c *threadReadCollector) ReplyErrors() int { return c.errCount(errClassReply) }

// BadReplyCount returns the count of undecodable / missing-parent replies.
func (c *threadReadCollector) BadReplyCount() int { return c.errCount(errClassBadReply) }

func (c *threadReadCollector) errCount(class errClass) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errors[class]
}

// SaturationCount returns the count of saturation events.
func (c *threadReadCollector) SaturationCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saturation
}

// UnderrunCount returns the total emit-underrun events.
func (c *threadReadCollector) UnderrunCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.underrun
}

// NoParentsCount returns the count of no-parent skips.
func (c *threadReadCollector) NoParentsCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.noParents
}
