package main

import (
	"sync"
	"time"
)

// ReadReceiptCollector aggregates latency samples and error/saturation tallies
// for one read-receipt workload step. All methods are safe for concurrent use.
// The errClass constants are shared with the history collector (same package).
type ReadReceiptCollector struct {
	mu         sync.Mutex
	samples    []time.Duration
	errors     map[errClass]int
	saturation int
	underrun   int
}

// NewReadReceiptCollector returns an empty collector.
func NewReadReceiptCollector() *ReadReceiptCollector {
	return &ReadReceiptCollector{errors: map[errClass]int{}}
}

// RecordSample stores one completed request's latency.
func (c *ReadReceiptCollector) RecordSample(latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, latency)
}

// RecordError tallies a per-class request failure.
func (c *ReadReceiptCollector) RecordError(class errClass) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[class]++
}

// RecordBadRequest tallies a request that failed before issue (e.g. marshal).
func (c *ReadReceiptCollector) RecordBadRequest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[errClassBadRequest]++
}

// RecordSaturation tallies a tick that fired while the in-flight pool was full.
func (c *ReadReceiptCollector) RecordSaturation() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saturation++
}

// RecordUnderrun adds n events that the pacer could not release on schedule
// (the load box fell behind the target cadence). n<=0 ticks are no-ops.
func (c *ReadReceiptCollector) RecordUnderrun(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.underrun += n
}

// Samples returns a defensive copy of the latency tape.
func (c *ReadReceiptCollector) Samples() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.samples))
	copy(out, c.samples)
	return out
}

// Failed returns the total error count across all classes.
func (c *ReadReceiptCollector) Failed() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, n := range c.errors {
		total += n
	}
	return total
}

// Saturation returns the saturation-event count.
func (c *ReadReceiptCollector) Saturation() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saturation
}

// UnderrunCount returns the total emit-underrun events.
func (c *ReadReceiptCollector) UnderrunCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.underrun
}
