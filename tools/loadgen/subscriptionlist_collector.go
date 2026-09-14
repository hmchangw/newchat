package main

import (
	"sync"
	"time"
)

// errClassEmptyPage marks a well-formed reply carrying no rows. Every seeded
// account owns subscriptions, so an empty page means the fixtures or the list
// type are wrong, not that the service is fast — counting it as a success would
// report a healthy ramp measuring nothing.
const errClassEmptyPage errClass = "empty_page"

// SubscriptionListSample captures one completed subscription.list round-trip.
// Rows and HasMore are kept so the report can show the page size actually
// measured: a ramp over 3-row pages is not a ramp over sidebar-sized ones.
type SubscriptionListSample struct {
	Latency time.Duration
	Rows    int
	HasMore bool
}

// SubscriptionListCollector aggregates samples and errors across a workload
// run. All methods are safe for concurrent use. Reuses the package-shared
// errClass consts (errClassTimeout / errClassReply / errClassBadReply).
type SubscriptionListCollector struct {
	mu         sync.Mutex
	samples    []SubscriptionListSample
	errors     map[errClass]int
	totalRows  int
	hasMore    int
	saturation int
	underrun   int
}

// NewSubscriptionListCollector returns an empty collector.
func NewSubscriptionListCollector() *SubscriptionListCollector {
	return &SubscriptionListCollector{errors: map[errClass]int{}}
}

// RecordSample stores one completed-call sample and folds in its row stats.
func (c *SubscriptionListCollector) RecordSample(s SubscriptionListSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, s)
	c.totalRows += s.Rows
	if s.HasMore {
		c.hasMore++
	}
}

// RecordError tallies a per-class transport/reply error.
func (c *SubscriptionListCollector) RecordError(class errClass, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[class]++
}

// RecordBadReply tallies a reply that could not be decoded.
func (c *SubscriptionListCollector) RecordBadReply(_ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[errClassBadReply]++
}

// RecordEmptyPage tallies a decodable reply with zero rows.
func (c *SubscriptionListCollector) RecordEmptyPage(_ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[errClassEmptyPage]++
}

// RecordSaturation tallies a tick that fired while the in-flight semaphore was full.
func (c *SubscriptionListCollector) RecordSaturation() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saturation++
}

// RecordUnderrun adds n events the pacer could not release on schedule. n<=0
// ticks are no-ops.
func (c *SubscriptionListCollector) RecordUnderrun(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.underrun += n
}

// Samples returns a defensive copy of the sample tape.
func (c *SubscriptionListCollector) Samples() []SubscriptionListSample {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]SubscriptionListSample, len(c.samples))
	copy(out, c.samples)
	return out
}

// TotalRows returns the summed row count across every successful page.
func (c *SubscriptionListCollector) TotalRows() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.totalRows
}

// MeanRows returns the mean rows per successful page, or 0 when none completed.
func (c *SubscriptionListCollector) MeanRows() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.samples) == 0 {
		return 0
	}
	return float64(c.totalRows) / float64(len(c.samples))
}

// HasMoreCount returns how many pages reported a further page behind them.
func (c *SubscriptionListCollector) HasMoreCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hasMore
}

// TimeoutErrors returns the timeout-class error count.
func (c *SubscriptionListCollector) TimeoutErrors() int { return c.errCount(errClassTimeout) }

// ReplyErrors returns the reply-class error count.
func (c *SubscriptionListCollector) ReplyErrors() int { return c.errCount(errClassReply) }

// BadReplyCount returns the count of undecodable replies.
func (c *SubscriptionListCollector) BadReplyCount() int { return c.errCount(errClassBadReply) }

// EmptyPageCount returns the count of zero-row replies.
func (c *SubscriptionListCollector) EmptyPageCount() int { return c.errCount(errClassEmptyPage) }

func (c *SubscriptionListCollector) errCount(class errClass) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errors[class]
}

// SaturationCount returns the count of saturation events.
func (c *SubscriptionListCollector) SaturationCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saturation
}

// UnderrunCount returns the total emit-underrun events.
func (c *SubscriptionListCollector) UnderrunCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.underrun
}
