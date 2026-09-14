package reconcile

import (
	"sync"
	"time"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/search"
)

const (
	ClaimAdvanced    = "advanced"
	ClaimRetried     = "retried"
	ClaimIdle        = "idle"
	ClaimFailed      = "failed"
	ClaimUnavailable = "unavailable"
	ClaimDeferred    = "deferred"
)

func ClaimOutcomes() []string {
	return []string{ClaimAdvanced, ClaimRetried, ClaimIdle, ClaimFailed, ClaimUnavailable, ClaimDeferred}
}

func ProbeOutcome(probeErr error) string {
	if probeErr != nil {
		return ClaimUnavailable
	}
	return ClaimRetried
}

func SearchProbeOutcome(result search.IndexResult, probed bool, probeErr error) string {
	if probeErr != nil {
		return ClaimUnavailable
	}
	if result == search.IndexTooEarly {
		return ClaimDeferred
	}
	if !probed {
		return ClaimUnavailable
	}
	return ClaimRetried
}

func NextProbe(now, verifyAfter, deadline time.Time, retryInterval time.Duration) time.Time {
	wait := max(now.Sub(verifyAfter), retryInterval)
	next := now.Add(wait)
	if next.Before(deadline) {
		return next
	}
	final := deadline.Add(-retryInterval)
	if final.After(now) {
		return final
	}
	return deadline
}

type ShareGate struct {
	mu     sync.Mutex
	share  float64
	credit float64
}

func NewShareGate(share float64) *ShareGate {
	return &ShareGate{share: min(max(share, 0), 1)}
}

func (g *ShareGate) Refund() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.credit = min(g.credit+1, 1)
}

func (g *ShareGate) Allow() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.credit += g.share
	if g.credit < 1 {
		return false
	}
	g.credit--
	return true
}
