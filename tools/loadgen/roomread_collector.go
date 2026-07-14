package main

import (
	"sync"
	"time"
)

// RoomReadSample captures one completed message.read round-trip.
type RoomReadSample struct {
	Latency time.Duration
	At      time.Time
}

// RoomReadCollector aggregates samples and errors across a workload run.
// All methods are safe for concurrent use. Reuses the package-shared errClass
// consts (errClassTimeout / errClassReply / errClassBadReply).
type RoomReadCollector struct {
	mu         sync.Mutex
	samples    []RoomReadSample
	errors     map[errClass]int
	saturation int
	underrun   int
}

// NewRoomReadCollector returns an empty collector.
func NewRoomReadCollector() *RoomReadCollector {
	return &RoomReadCollector{errors: map[errClass]int{}}
}

// RecordSample stores one completed-call sample.
func (c *RoomReadCollector) RecordSample(s RoomReadSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, s)
}

// RecordError tallies a per-class transport/reply error.
func (c *RoomReadCollector) RecordError(class errClass, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[class]++
}

// RecordBadReply tallies a reply that was not the expected {"status":"accepted"}.
func (c *RoomReadCollector) RecordBadReply(_ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[errClassBadReply]++
}

// RecordSaturation tallies a tick that fired while the in-flight semaphore was full.
func (c *RoomReadCollector) RecordSaturation() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saturation++
}

// RecordUnderrun adds n events that the pacer could not release on schedule
// (the load box fell behind the target cadence). n<=0 ticks are no-ops.
func (c *RoomReadCollector) RecordUnderrun(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.underrun += n
}

// Samples returns a defensive copy of the sample tape.
func (c *RoomReadCollector) Samples() []RoomReadSample {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RoomReadSample, len(c.samples))
	copy(out, c.samples)
	return out
}

// TimeoutErrors returns the timeout-class error count.
func (c *RoomReadCollector) TimeoutErrors() int { return c.errCount(errClassTimeout) }

// ReplyErrors returns the reply-class error count.
func (c *RoomReadCollector) ReplyErrors() int { return c.errCount(errClassReply) }

// BadReplyCount returns the count of non-accepted / undecodable replies.
func (c *RoomReadCollector) BadReplyCount() int { return c.errCount(errClassBadReply) }

func (c *RoomReadCollector) errCount(class errClass) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errors[class]
}

// SaturationCount returns the count of saturation events.
func (c *RoomReadCollector) SaturationCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saturation
}

// UnderrunCount returns the total emit-underrun events.
func (c *RoomReadCollector) UnderrunCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.underrun
}
