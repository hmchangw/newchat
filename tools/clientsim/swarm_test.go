package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeClient struct {
	started atomic.Int64
	closed  atomic.Int64
	block   bool // when true, run ignores ctx and never exits
}

func (f *fakeClient) run(ctx context.Context) error {
	f.started.Add(1)
	if f.block {
		select {} // deliberately undrainable
	}
	<-ctx.Done()
	return nil
}

func (f *fakeClient) close() { f.closed.Add(1) }

func TestRunSwarm_StartsEveryAccountAndStopsOnCancel(t *testing.T) {
	var mu sync.Mutex
	clients := map[string][]*fakeClient{}
	factory := func(account string) (runnable, error) {
		mu.Lock()
		defer mu.Unlock()
		fc := &fakeClient{}
		clients[account] = append(clients[account], fc)
		return fc, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runSwarm(ctx, []string{"a", "b", "c"}, 1000, 0, factory) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(clients) == 3
	}, 5*time.Second, 10*time.Millisecond, "ramp should start all clients")

	cancel()
	require.NoError(t, <-done)
	mu.Lock()
	defer mu.Unlock()
	for account, instances := range clients {
		for _, fc := range instances {
			assert.Equal(t, int64(1), fc.closed.Load(), "client %s closed on shutdown", account)
			assert.Equal(t, int64(1), fc.started.Load(), "client %s started once", account)
		}
	}
}

func TestRunSwarm_RampPacesStarts(t *testing.T) {
	var count atomic.Int64
	factory := func(string) (runnable, error) { count.Add(1); return &fakeClient{}, nil }

	// 20/s for ~250ms: the invariant asserted is pacing (not everything at
	// once), with only a generous upper bound to dodge CI clock noise.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_ = runSwarm(ctx, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, 20, 0, factory)
	assert.Less(t, count.Load(), int64(10), "ramp must pace, not thundering-herd")
}

func TestRunSwarm_HighRateBatchesInsteadOfClamping(t *testing.T) {
	// 5000/s with 50 accounts: under the old 1000/s silent clamp this
	// needs 50ms; batching should start everything well inside 2s.
	accounts := make([]string, 50)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("u%d", i)
	}
	var count atomic.Int64
	started := make(chan struct{})
	var once sync.Once
	factory := func(string) (runnable, error) {
		if count.Add(1) == 50 {
			once.Do(func() { close(started) })
		}
		return &fakeClient{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runSwarm(ctx, accounts, 5000, 0, factory) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("only %d/50 clients started at 5000/s", count.Load())
	}
	cancel()
	<-done
}

func TestRunSwarm_FactoryErrorDoesNotAbortOthers(t *testing.T) {
	var ok atomic.Int64
	okDone := make(chan struct{})
	var once sync.Once
	factory := func(account string) (runnable, error) {
		if account == "bad" {
			return nil, assert.AnError
		}
		if ok.Add(1) == 2 {
			once.Do(func() { close(okDone) })
		}
		return &fakeClient{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runSwarm(ctx, []string{"bad", "good1", "good2"}, 1000, 0, factory) }()
	select {
	case <-okDone:
	case <-time.After(2 * time.Second):
		t.Fatal("good clients did not start after a factory error")
	}
	cancel()
	<-done
	assert.Equal(t, int64(2), ok.Load())
}

func TestRunSwarm_ChurnRestartsClients(t *testing.T) {
	var starts atomic.Int64
	restarted := make(chan struct{})
	var once sync.Once
	factory := func(string) (runnable, error) {
		if starts.Add(1) > 2 { // beyond the initial ramp of both accounts
			once.Do(func() { close(restarted) })
		}
		return &fakeClient{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runSwarm(ctx, []string{"a", "b"}, 1000, 50, factory) }()
	select {
	case <-restarted:
	case <-time.After(5 * time.Second):
		t.Fatal("churn produced no restart within 5s")
	}
	cancel()
	<-done
}

func TestRunSwarm_DrainTimeoutIsAnError(t *testing.T) {
	old := swarmShutdownTimeout
	swarmShutdownTimeout = 100 * time.Millisecond
	t.Cleanup(func() { swarmShutdownTimeout = old })

	var started atomic.Int64
	factory := func(string) (runnable, error) {
		fc := &fakeClient{block: true}
		started.Add(1)
		return fc, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runSwarm(ctx, []string{"a"}, 1000, 0, factory) }()
	require.Eventually(t, func() bool { return started.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, errDrainTimeout)
	case <-time.After(5 * time.Second):
		t.Fatal("runSwarm did not return after drain timeout")
	}
}

func TestSummarize_DegradedAndPercentiles(t *testing.T) {
	m := newMetrics()
	m.Delivered.WithLabelValues("channel").Add(3)
	m.Disconnects.WithLabelValues("auth_expired").Inc()
	m.DecodeFailures.Inc() // trips degraded
	for i := 0; i < 100; i++ {
		m.BroadcastLatency.Observe(0.020) // all mass in the 0.025 bucket
	}

	s, err := summarize(m, "run-7", "d1g3st", 10)
	require.NoError(t, err)
	assert.True(t, s.Degraded, "any loss-counter increment must mark the run degraded")

	attrs := map[string]any{}
	for i := 0; i+1 < len(s.Attrs); i += 2 {
		attrs[s.Attrs[i].(string)] = s.Attrs[i+1]
	}
	assert.Equal(t, "run-7", attrs["runId"])
	assert.Equal(t, "d1g3st", attrs["configDigest"])
	assert.InDelta(t, 3, attrs["delivered_channel"].(float64), 0.001)
	assert.InDelta(t, 1, attrs["disconnects_auth_expired"].(float64), 0.001)
	assert.Equal(t, true, attrs["degraded"])

	p95 := attrs["clientsim_broadcast_to_client_latency_seconds_p95"].(float64)
	assert.Greater(t, p95, 0.010, "p95 must sit inside the observed bucket")
	assert.LessOrEqual(t, p95, 0.025, "p95 must not exceed the bucket's upper bound")
}

func TestSummarize_CleanRunNotDegraded(t *testing.T) {
	m := newMetrics()
	m.Delivered.WithLabelValues("user").Inc()
	s, err := summarize(m, "run-8", "d", 10)
	require.NoError(t, err)
	assert.False(t, s.Degraded)
}

// A client whose run() fails at auth/connect/walk must not stay parked in
// the running set: with the default CHURN_RATE=0 nothing would ever retry
// it, so the swarm silently shrinks for the rest of the soak.
func TestRunSwarm_RetriesClientsThatExitEarly(t *testing.T) {
	var starts atomic.Int64
	retried := make(chan struct{})
	var once sync.Once
	factory := func(string) (runnable, error) {
		n := starts.Add(1)
		if n >= 2 {
			once.Do(func() { close(retried) })
			return &fakeClient{}, nil // the retry holds
		}
		return &failFastClient{}, nil // first attempt dies immediately
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runSwarm(ctx, []string{"a"}, 1000, 0, factory) }()
	select {
	case <-retried:
	case <-time.After(2 * time.Second):
		t.Fatal("a client that exited early was never retried")
	}
	cancel()
	<-done
}

// Retries are bounded — a permanently broken account must not spin forever.
func TestRunSwarm_BoundsStartRetries(t *testing.T) {
	var starts atomic.Int64
	factory := func(string) (runnable, error) { starts.Add(1); return &failFastClient{}, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.NoError(t, runSwarm(ctx, []string{"a"}, 1000, 0, factory))
	assert.LessOrEqual(t, starts.Load(), int64(maxStartAttempts),
		"a permanently failing account must stop being retried")
}

// failFastClient models auth/connect/walk failing: run returns immediately.
type failFastClient struct{ closed atomic.Int64 }

func (f *failFastClient) run(context.Context) error { return assert.AnError }
func (f *failFastClient) close()                    { f.closed.Add(1) }

func TestReadyGate(t *testing.T) {
	cases := []struct {
		name    string
		peak    int
		current int
		target  int
		ratio   float64
		wantErr bool
	}{
		{"full fleet", 100, 100, 100, 0.95, false},
		{"within tolerance", 100, 96, 100, 0.95, false},
		{"exactly at threshold", 100, 95, 100, 0.95, false},
		{"peak reached but fleet collapsed", 100, 20, 100, 0.95, true},
		{"never reached threshold", 94, 94, 100, 0.95, true},
		{"zero connections", 0, 0, 100, 0.95, true},
		{"gate disabled", 0, 0, 100, 0, false},
		{"empty shard", 0, 0, 0, 0.95, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			m := newMetrics()
			m.readyPeak.Store(int64(tt.peak))
			m.readyNow.Store(int64(tt.current))
			m.captureReadyAtDrain()
			err := readyGate(m, tt.target, tt.ratio)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, "ready")
				return
			}
			assert.NoError(t, err)
		})
	}
}

// churn must hand the account to a fresh client and leave it there. The
// retired instance reports its own exit afterwards; if the swarm cannot tell
// that report apart from the replacement's, it tears down the client it just
// built and the account stays dark until the next refill tick.
func TestRunSwarm_ChurnKeepsTheFleetUp(t *testing.T) {
	var (
		mu   sync.Mutex
		live int
	)
	factory := func(string) (runnable, error) {
		return &countingClient{
			onRun:  func() { mu.Lock(); live++; mu.Unlock() },
			onExit: func() { mu.Lock(); live--; mu.Unlock() },
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runSwarm(ctx, []string{"a"}, 1000, 20, factory) }()

	// Sample the fleet after the first churn cycles have had time to land.
	time.Sleep(150 * time.Millisecond)
	up, samples := 0, 40
	for i := 0; i < samples; i++ {
		mu.Lock()
		if live == 1 {
			up++
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	require.NoError(t, <-done)

	assert.Greater(t, up, samples*3/4,
		"the single account should be connected for nearly the whole window; was up in only %d of %d samples", up, samples)
}

// countingClient reports when its run starts and when it returns.
type countingClient struct {
	onRun  func()
	onExit func()
}

func (c *countingClient) run(ctx context.Context) error {
	c.onRun()
	defer c.onExit()
	<-ctx.Done()
	return nil
}
func (c *countingClient) close() {}

// Above 1000/s the ramp starts a batch per 1ms tick. Rounding the batch up
// overshoots by up to a full extra connection per tick — at 1001/s that is
// two per millisecond, i.e. 2000/s, double what the operator configured.
// A soak that ramps twice as fast as its own knob says is not the test the
// operator asked for.
func TestRampBudget_HonoursTheConfiguredRate(t *testing.T) {
	cases := []struct {
		name  string
		rate  float64
		ticks int
		want  int
	}{
		{"just above the batch threshold", 1001, 1000, 1001},
		{"well above", 5500, 1000, 5500},
		{"exactly on a multiple", 2000, 1000, 2000},
		{"at or below the threshold starts one per tick", 750, 100, 100},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, b := rampPacing(tt.rate)
			total := 0
			for i := 0; i < tt.ticks; i++ {
				total += b.take()
			}
			assert.Equal(t, tt.want, total)
		})
	}
}

func TestRampPacing_KeepsSubThousandRatesOnTheirOwnInterval(t *testing.T) {
	interval, b := rampPacing(50)
	assert.Equal(t, 20*time.Millisecond, interval)
	assert.Equal(t, 1, b.take(), "below the batch threshold every tick starts exactly one")
}

// Samples above the highest finite bucket are invisible to histQuantile: it
// returns the top bound, exactly as Prometheus's histogram_quantile does. That
// is the right value but a misleading one to print alone — a p99 pinned to the
// bucket ceiling reads like a measured latency. The summary says how many
// samples overflowed so a clamped quantile cannot be mistaken for a real one.
func TestSummarize_ReportsSamplesAboveTheTopBucket(t *testing.T) {
	m := newMetrics()
	for i := 0; i < 5; i++ {
		m.BroadcastLatency.Observe(1e6) // far above any bucket
	}
	m.BroadcastLatency.Observe(0.001)
	s, err := summarize(m, "run-1", "digest", 1)
	require.NoError(t, err)

	attrs := map[string]any{}
	for i := 0; i+1 < len(s.Attrs); i += 2 {
		if k, ok := s.Attrs[i].(string); ok {
			attrs[k] = s.Attrs[i+1]
		}
	}
	assert.Equal(t, uint64(5), attrs["clientsim_broadcast_to_client_latency_seconds_over_top_bucket"],
		"a quantile clamped to the bucket ceiling must not be reported without its overflow count")
}

// A histogram entirely inside its buckets reports no overflow attribute at all.
func TestSummarize_OmitsTheOverflowAttrWhenNothingOverflowed(t *testing.T) {
	m := newMetrics()
	m.BroadcastLatency.Observe(0.001)
	s, err := summarize(m, "run-1", "digest", 1)
	require.NoError(t, err)
	for i := 0; i+1 < len(s.Attrs); i += 2 {
		assert.NotEqual(t, "clientsim_broadcast_to_client_latency_seconds_over_top_bucket", s.Attrs[i])
	}
}

// Churn had the same rounding bug the ramp did: above 1000 cycles/s it was
// clamped to one per 1ms tick, so a configured 5000/s silently ran at 1000/s.
// Both paths now share rampPacing.
func TestChurnPacing_HonoursRatesAboveTheBatchThreshold(t *testing.T) {
	interval, b := rampPacing(5000)
	assert.Equal(t, time.Millisecond, interval)
	total := 0
	for i := 0; i < 1000; i++ {
		total += b.take()
	}
	assert.Equal(t, 5000, total, "a churn rate above 1000/s must not be clamped to 1000/s")
}

// Removing a client scanned the whole order slice, so a full-fleet shutdown
// was quadratic: at 30k clients that is ~4.5e8 string comparisons against a
// 20s drain budget.
func TestOrderIndex_RemovalIsConstantTime(t *testing.T) {
	idx := newOrderIndex()
	for i := 0; i < 5; i++ {
		idx.add(fmt.Sprintf("u%d", i))
	}
	idx.remove("u2")
	assert.Equal(t, 4, idx.len())
	assert.NotContains(t, idx.accounts(), "u2")
	// Swap-with-last must keep the index of the moved element correct, or a
	// later removal silently drops the wrong account.
	idx.remove("u4")
	assert.Equal(t, 3, idx.len())
	assert.ElementsMatch(t, []string{"u0", "u1", "u3"}, idx.accounts())
	idx.remove("absent") // no-op, never a panic
	assert.Equal(t, 3, idx.len())
	assert.NotEmpty(t, idx.pick(secureIntN))
}

// The trough arms at the readiness floor so it describes the measurement
// window. That floor has to be the same bar readyGate holds the run to, and
// the gate compares in floats: at 7 accounts and a 0.9 ratio it demands 7
// ready clients, so truncating 6.3 to 6 would open the window while the fleet
// is still one client short of the floor — exactly the ramp the arming exists
// to exclude.
func TestReadyFloor_MatchesTheGatesBar(t *testing.T) {
	tests := []struct {
		name   string
		ratio  float64
		target int
		want   int
	}{
		{name: "exact multiple", ratio: 0.95, target: 100, want: 95},
		{name: "fractional rounds up", ratio: 0.9, target: 7, want: 7},
		{name: "just over an integer", ratio: 0.5, target: 5, want: 3},
		{name: "gate disabled", ratio: 0, target: 100, want: 0},
		{name: "whole fleet", ratio: 1, target: 3, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := readyFloor(tt.ratio, tt.target)
			assert.Equal(t, tt.want, got)
			if tt.ratio <= 0 || tt.target == 0 {
				return
			}
			// The floor is the SMALLEST ready count the gate accepts: one
			// below it must fail, so the two can never disagree by a client.
			pass := newMetrics()
			pass.readyPeak.Store(int64(got))
			pass.readyAtDrain.Store(int64(got))
			pass.readyCaptured.Store(true)
			assert.NoError(t, readyGate(pass, tt.target, tt.ratio))

			fail := newMetrics()
			fail.readyPeak.Store(int64(got - 1))
			fail.readyAtDrain.Store(int64(got - 1))
			fail.readyCaptured.Store(true)
			assert.Error(t, readyGate(fail, tt.target, tt.ratio))
		})
	}
}
