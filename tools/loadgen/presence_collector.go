package main

import (
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// presenceExpectation is one in-flight state-changing transition awaiting its
// resulting PresenceState publish.
type presenceExpectation struct {
	status model.PresenceStatus
	sentAt time.Time
}

// presenceCollector correlates emitted transitions with observed state
// publishes. It is the single observer-callback target and is safe for
// concurrent use (many emitters + observer goroutines).
type presenceCollector struct {
	mu        sync.Mutex
	pending   map[string]presenceExpectation // account -> open expectation
	latencies []float64                      // ms, current hold window
	attempted int64
	failed    int64

	// recovery tracker (storm mode)
	recovering   bool
	recStart     time.Time
	recRemaining map[string]struct{}
	recElapsed   time.Duration
	recDone      bool

	// false-offline watcher (capacity mode). When watching, an observed
	// offline for an account in watchCohort is recorded once in falseOfflines.
	watching      bool
	watchCohort   map[string]struct{}
	falseOfflines map[string]struct{}
}

func newPresenceCollector() *presenceCollector {
	return &presenceCollector{pending: make(map[string]presenceExpectation)}
}

// RecordEmit counts one attempted state-changing transition.
func (c *presenceCollector) RecordEmit() {
	c.mu.Lock()
	c.attempted++
	c.mu.Unlock()
}

// RecordEmitFailure counts a publish that errored at send time.
func (c *presenceCollector) RecordEmitFailure() {
	c.mu.Lock()
	c.failed++
	c.mu.Unlock()
}

// Expect registers (and counts) one awaited transition. It both increments
// attempted and opens an expectation, so emitters call Expect instead of
// RecordEmit for transitions that should publish.
func (c *presenceCollector) Expect(account string, status model.PresenceStatus, sentAt time.Time) {
	c.mu.Lock()
	c.attempted++
	c.pending[account] = presenceExpectation{status: status, sentAt: sentAt}
	c.mu.Unlock()
}

// Observe is called for every PresenceState publish seen on the wildcard.
func (c *presenceCollector) Observe(account string, status model.PresenceStatus, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.pending[account]; ok && e.status == status {
		c.latencies = append(c.latencies, float64(at.Sub(e.sentAt).Microseconds())/1000.0)
		delete(c.pending, account)
	}
	if c.recovering && !c.recDone && status == model.StatusOnline {
		if _, want := c.recRemaining[account]; want {
			delete(c.recRemaining, account)
			if len(c.recRemaining) == 0 {
				c.recDone = true
				c.recElapsed = at.Sub(c.recStart)
			}
		}
	}
	if c.watching && status == model.StatusOffline {
		if _, want := c.watchCohort[account]; want {
			// falseOfflines is always non-nil while watching: WatchOnline,
			// the only setter of c.watching, allocates it.
			c.falseOfflines[account] = struct{}{}
		}
	}
}

// ReapMissing counts every still-open expectation as a missing observation
// (a transition that never produced its publish), then clears them.
func (c *presenceCollector) ReapMissing() {
	c.mu.Lock()
	c.failed += int64(len(c.pending))
	c.pending = make(map[string]presenceExpectation)
	c.mu.Unlock()
}

// Reset clears samples, counters, and stale expectations at hold start.
func (c *presenceCollector) Reset() {
	c.mu.Lock()
	c.pending = make(map[string]presenceExpectation)
	c.latencies = nil
	c.attempted = 0
	c.failed = 0
	c.watching = false
	c.watchCohort = nil
	c.falseOfflines = nil
	c.mu.Unlock()
}

func (c *presenceCollector) Attempted() int64 { c.mu.Lock(); defer c.mu.Unlock(); return c.attempted }
func (c *presenceCollector) Failed() int64    { c.mu.Lock(); defer c.mu.Unlock(); return c.failed }

func (c *presenceCollector) LatenciesMs() []float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]float64, len(c.latencies))
	copy(out, c.latencies)
	return out
}

// BeginRecovery arms the recovery tracker for a set of accounts expected to
// come back online, anchored at start.
func (c *presenceCollector) BeginRecovery(accounts []string, start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recovering = true
	c.recDone = false
	c.recStart = start
	c.recElapsed = 0
	c.recRemaining = make(map[string]struct{}, len(accounts))
	for _, a := range accounts {
		c.recRemaining[a] = struct{}{}
	}
}

func (c *presenceCollector) RecoveryComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recDone
}

func (c *presenceCollector) RecoveryElapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recElapsed
}

// RecoveryRemaining is the count of accounts not yet observed back online.
func (c *presenceCollector) RecoveryRemaining() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.recRemaining)
}

// WatchOnline arms a cohort of accounts expected to stay online. While armed,
// each distinct watched account observed going offline is one false offline.
func (c *presenceCollector) WatchOnline(accounts []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watching = true
	c.watchCohort = make(map[string]struct{}, len(accounts))
	for _, a := range accounts {
		c.watchCohort[a] = struct{}{}
	}
	c.falseOfflines = make(map[string]struct{})
}

// StopWatchOnline disarms the watcher but preserves the count for reading.
func (c *presenceCollector) StopWatchOnline() {
	c.mu.Lock()
	c.watching = false
	c.mu.Unlock()
}

// FalseOfflines is the number of distinct watched accounts seen going offline.
func (c *presenceCollector) FalseOfflines() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.falseOfflines)
}
