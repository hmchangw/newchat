package main

import (
	"sync"
	"testing"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

// The connection-state helpers must be idempotent: nats.go fires
// DisconnectedErrCB on an explicit Close() too (nats.go@v1.50.0 close()),
// so a naive Dec in the handler plus a Dec in close() double-counts.
func TestConnState_IdempotentTransitions(t *testing.T) {
	m := newMetrics()
	s := &simClient{m: m}

	s.markConnUp()
	s.markConnUp() // duplicate: must not double-count
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.ConnsActive), 0.001)

	s.markConnDown()
	s.markConnDown() // the close()-after-disconnect case
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.ConnsActive), 0.001)
}

// An outage must move conns_active, not just the disconnect counter —
// otherwise a failure test reads "all connections healthy" for minutes.
func TestConnState_OutageDropsActiveAndReady(t *testing.T) {
	m := newMetrics()
	s := &simClient{m: m}

	s.markConnUp()
	s.markReady()
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.ConnsActive), 0.001)
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.ConnsReady), 0.001)

	s.markConnDown() // broker went away
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.ConnsActive), 0.001)
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.ConnsReady), 0.001,
		"a disconnected client is not ready either")

	s.markConnUp() // reconnected, but the resync walk has not finished yet
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.ConnsActive), 0.001)
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.ConnsReady), 0.001)

	s.markReady()
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.ConnsReady), 0.001)
}

// markReady on a client that is not connected must not raise the gauge.
func TestConnState_ReadyRequiresUp(t *testing.T) {
	m := newMetrics()
	s := &simClient{m: m}
	s.markReady()
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.ConnsReady), 0.001)
}

// The peak is what the exit gate judges, so it must survive a drain to zero.
func TestMetrics_ReadyPeakSurvivesDrain(t *testing.T) {
	m := newMetrics()
	a, b := &simClient{m: m}, &simClient{m: m}
	a.markConnUp()
	a.markReady()
	b.markConnUp()
	b.markReady()
	assert.InDelta(t, 2, promtestutil.ToFloat64(m.ConnsReadyPeak), 0.001)

	a.markConnDown()
	b.markConnDown()
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.ConnsReady), 0.001)
	assert.InDelta(t, 2, promtestutil.ToFloat64(m.ConnsReadyPeak), 0.001,
		"peak must not follow the drain, or SIGTERM would void every run")
}

// The gate judges the peak, so the peak must be exact under concurrency —
// the exported gauge is written outside the compare-and-swap and can lag.
func TestMetrics_ReadyPeakExactUnderConcurrency(t *testing.T) {
	m := newMetrics()
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m.readyInc() }()
	}
	wg.Wait()
	assert.Equal(t, int64(n), m.readyPeak.Load(), "every increment must be reflected in the peak")
	m.captureReadyAtDrain()
	assert.NoError(t, readyGate(m, n, 1.0), "a fully-ready fleet must pass a 100%% gate")
}
