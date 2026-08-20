package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingPublisher struct {
	mu    sync.Mutex
	calls []publishCall
}

type publishCall struct {
	subject string
	data    []byte
}

func (r *recordingPublisher) Publish(_ context.Context, subject string, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, publishCall{subject: subject, data: append([]byte(nil), data...)})
	return nil
}

func (r *recordingPublisher) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingPublisher) snapshot() []publishCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]publishCall, len(r.calls))
	copy(out, r.calls)
	return out
}

type errorPublisher struct{}

func (e *errorPublisher) Publish(_ context.Context, _ string, _ []byte) error {
	return fmt.Errorf("publish error")
}

func TestGenerator_SendsExpectedCount(t *testing.T) {
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	rp := &recordingPublisher{}
	m := NewMetrics()
	c := NewCollector(m, p.Name)
	g := NewGenerator(&GeneratorConfig{
		Preset:    &p,
		Fixtures:  f,
		SiteID:    "site-local",
		Rate:      200,
		Inject:    InjectFrontdoor,
		Publisher: rp,
		Metrics:   m,
		Collector: c,
	}, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	require.NoError(t, g.Run(ctx))

	count := rp.count()
	// 200 msg/s for ~250ms: expect 30-70 publishes (wide tolerance for scheduler).
	assert.GreaterOrEqual(t, count, 30)
	assert.LessOrEqual(t, count, 70)
}

func TestGenerator_UsesFrontdoorSubject(t *testing.T) {
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	rp := &recordingPublisher{}
	m := NewMetrics()
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-local",
		Rate: 100, Inject: InjectFrontdoor,
		Publisher: rp, Metrics: m,
		Collector: NewCollector(m, p.Name),
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = g.Run(ctx)
	calls := rp.snapshot()
	require.NotEmpty(t, calls)
	for i := range calls {
		assert.Contains(t, calls[i].subject, ".msg.send")
		assert.Contains(t, calls[i].subject, "site-local")
	}
}

func TestGenerator_UsesCanonicalSubjectWhenInjectCanonical(t *testing.T) {
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	rp := &recordingPublisher{}
	m := NewMetrics()
	c := NewCollector(m, p.Name)
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-local",
		Rate: 100, Inject: InjectCanonical,
		Publisher: rp, Metrics: m,
		Collector: c,
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = g.Run(ctx)
	calls := rp.snapshot()
	require.NotEmpty(t, calls)
	for i := range calls {
		assert.Contains(t, calls[i].subject, "chat.msg.canonical.site-local.created")
	}

	// In canonical mode, the Generator should NOT populate byReqID because
	// canonical injection bypasses the gatekeeper (no reply is expected).
	// Consequently Finalize should report zero missing replies even though
	// no replies ever arrived.
	missingReplies, _ := c.Finalize()
	assert.Equal(t, 0, missingReplies)
}

func TestGenerator_IncrementsPublishedMetric(t *testing.T) {
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	rp := &recordingPublisher{}
	m := NewMetrics()
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-local",
		Rate: 100, Inject: InjectFrontdoor,
		Publisher: rp, Metrics: m,
		Collector: NewCollector(m, p.Name),
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = g.Run(ctx)

	var got int64
	metrics, err := m.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range metrics {
		if mf.GetName() == "loadgen_published_total" {
			for _, metric := range mf.GetMetric() {
				got += int64(metric.GetCounter().GetValue())
			}
		}
	}
	assert.Greater(t, got, int64(0))
}

func TestGenerator_Run_ReturnsErrorForZeroRate(t *testing.T) {
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	rp := &recordingPublisher{}
	m := NewMetrics()
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-local",
		Rate: 0, Inject: InjectFrontdoor,
		Publisher: rp, Metrics: m,
		Collector: NewCollector(m, p.Name),
	}, 1)
	err := g.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate must be > 0")
}

func TestGenerator_PublishError_IncrementsErrorMetric(t *testing.T) {
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	ep := &errorPublisher{}
	m := NewMetrics()
	c := NewCollector(m, p.Name)
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-local",
		Rate: 100, Inject: InjectFrontdoor,
		Publisher: ep, Metrics: m,
		Collector: c,
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = g.Run(ctx)

	var publishErrors int64
	metrics, err := m.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range metrics {
		if mf.GetName() == "loadgen_publish_errors_total" {
			for _, metric := range mf.GetMetric() {
				publishErrors += int64(metric.GetCounter().GetValue())
			}
		}
	}
	assert.Greater(t, publishErrors, int64(0))

	// Publish errors should have cleaned up the pending entries, so Finalize
	// reports no "missing replies" or "missing broadcasts" attributable to
	// publish-side failures.
	missingReplies, missingBroadcasts := c.Finalize()
	assert.Equal(t, 0, missingReplies)
	assert.Equal(t, 0, missingBroadcasts)
}

func TestGenerator_Content_WithMentionRate(t *testing.T) {
	p, _ := BuiltinPreset("realistic")
	f := BuildFixtures(&p, 42, "site-local")
	rp := &recordingPublisher{}
	m := NewMetrics()
	// Run long enough to statistically hit the 10% mention rate.
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-local",
		Rate: 500, Inject: InjectFrontdoor,
		Publisher: rp, Metrics: m,
		Collector: NewCollector(m, p.Name),
	}, 99)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = g.Run(ctx)
	calls := rp.snapshot()
	require.NotEmpty(t, calls)
	// With 10% mention rate and ~100 messages, at least one should contain "@user-".
	foundMention := false
	for i := range calls {
		if strings.Contains(string(calls[i].data), "@user-") {
			foundMention = true
			break
		}
	}
	assert.True(t, foundMention, "expected at least one message with a mention")
}

func TestGenerator_EmptySubscriptions_NoPublish(t *testing.T) {
	p, _ := BuiltinPreset("small")
	rp := &recordingPublisher{}
	m := NewMetrics()
	// Use empty fixtures — no subscriptions.
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: Fixtures{}, SiteID: "site-local",
		Rate: 200, Inject: InjectFrontdoor,
		Publisher: rp, Metrics: m,
		Collector: NewCollector(m, p.Name),
	}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = g.Run(ctx)
	assert.Equal(t, 0, rp.count())
}

func TestGenerator_MaxInFlightZeroRunsSerially(t *testing.T) {
	// MaxInFlight=0 preserves the legacy serial-on-ticker behavior.
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	rp := &recordingPublisher{}
	m := NewMetrics()
	c := NewCollector(m, p.Name)
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-local",
		Rate: 200, Inject: InjectFrontdoor,
		Publisher: rp, Metrics: m,
		Collector:   c,
		MaxInFlight: 0,
	}, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	require.NoError(t, g.Run(ctx))

	// Same tolerance as the default SendsExpectedCount test.
	count := rp.count()
	assert.GreaterOrEqual(t, count, 30)
	assert.LessOrEqual(t, count, 70)
}

// blockingPublisher blocks every Publish call until unblock is closed.
// Used to force worker-pool saturation.
type blockingPublisher struct {
	unblock chan struct{}
	mu      sync.Mutex
	count   int
}

func (b *blockingPublisher) Publish(ctx context.Context, _ string, _ []byte) error {
	select {
	case <-b.unblock:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.mu.Lock()
	b.count++
	b.mu.Unlock()
	return nil
}

func TestGenerator_PoolSaturationCountedAsError(t *testing.T) {
	// With MaxInFlight=1 and a publisher that never returns while the run is
	// active, every tick after the first must see the pool saturated and
	// increment loadgen_publish_errors_total{reason="saturated"}.
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	bp := &blockingPublisher{unblock: make(chan struct{})}
	m := NewMetrics()
	c := NewCollector(m, p.Name)
	g := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-local",
		Rate: 500, Inject: InjectFrontdoor,
		Publisher: bp, Metrics: m,
		Collector:   c,
		MaxInFlight: 1,
	}, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	_ = g.Run(ctx)
	close(bp.unblock)

	mfs, err := m.Registry.Gather()
	require.NoError(t, err)
	var saturated float64
	for _, mf := range mfs {
		if mf.GetName() != "loadgen_publish_errors_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == "saturated" {
					saturated += metric.GetCounter().GetValue()
				}
			}
		}
	}
	assert.Greater(t, saturated, float64(0), "expected saturated counter to increment under pool-full conditions")
}

// dispatchPeak returns the highest number of publishes the generator completes
// in window over trials short runs, at the given rate and MaxInFlight.
//
// Both callers measure a ceiling — the *most* a dispatch path can do — so the
// peak, not the mean, is the meaningful sample: a single run is one GC pause or
// scheduler stall away from under-counting.
func dispatchPeak(t *testing.T, maxInFlight, rate int, window time.Duration, trials int) int {
	t.Helper()
	best := 0
	for i := 0; i < trials; i++ {
		p, _ := BuiltinPreset("small")
		f := BuildFixtures(&p, 42, "site-local")
		rp := &recordingPublisher{}
		m := NewMetrics()
		g := NewGenerator(&GeneratorConfig{
			Preset: &p, Fixtures: f, SiteID: "site-local",
			Rate: rate, Inject: InjectFrontdoor,
			Publisher: rp, Metrics: m, Collector: NewCollector(m, p.Name),
			MaxInFlight: maxInFlight,
		}, 1)
		ctx, cancel := context.WithTimeout(context.Background(), window)
		err := g.Run(ctx)
		cancel()
		require.NoError(t, err)
		if c := rp.count(); c > best {
			best = c
		}
	}
	return best
}

const (
	pacedMeasureRate   = 100_000
	pacedMeasureWindow = 200 * time.Millisecond
	pacedMeasureTrials = 3
)

// The regression that produced the batched pacer: the serial path releases one
// event per delivered tick, and the runtime cannot deliver a tick per
// microsecond, so it plateaus far below any high target. This is that property
// with the timer taken out of it. pacedDispatchRateWithTicks accepts the tick
// channel, so one tick can be shown to release the whole batch the rate is owed
// — 200 events here — where a one-per-tick regression releases exactly one.
//
// This is the blocking assertion. The comparison against the serial path can
// only be measured on real timers, and no measurement of it is safe to assert
// on: the two paths degrade at completely different rates under load, so their
// ratio has no upper bound to write a threshold against.
func TestPacedDispatch_ReleasesTheWholeBatchOnOneTick(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	pacer := newPacer(pacedMeasureRate, start)
	require.Equal(t, minEmitInterval, pacer.interval,
		"a target this high must clamp to the tick floor for a batch to exist")
	require.Greater(t, pacer.perTick, 1.0)

	var dispatched atomic.Int64
	ticks := make(chan time.Time)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(done)
		pacedDispatchRateWithTicks(ctx, pacer, ticks, 4096,
			func(int) {}, func() {},
			func(context.Context) { dispatched.Add(1) })
	}()

	// Unbuffered: the send returns only once the dispatch loop has taken the
	// tick, so cancelling afterwards cannot race ahead of the batch it owes.
	ticks <- start.Add(pacer.interval)
	cancel()
	<-done

	assert.Equal(t, int64(pacer.perTick), dispatched.Load(),
		"one tick must release the events the rate owes for that interval")
}

// A measurement, not an assertion. Both counts are host-dependent and the
// review history of this file is three attempts to find a threshold that holds
// across machines: a fixed target is reachable on a fast host and unreachable
// on a slow one, and a target scaled from the serial plateau fails too, because
// under load the serial ticker collapses far harder than the pool does
// (observed: plateau 400, paced 8000 — a ratio of 20 where an idle host shows
// 2.2). The numbers are logged because they are useful when tuning the pacer;
// the only thing asserted is that both paths dispatched at all.
func TestGenerator_MeasuresBothDispatchPaths(t *testing.T) {
	serial := dispatchPeak(t, 0, pacedMeasureRate, pacedMeasureWindow, pacedMeasureTrials)
	paced := dispatchPeak(t, 5000, pacedMeasureRate, pacedMeasureWindow, pacedMeasureTrials)

	t.Logf("at %d rps over %s: serial=%d paced=%d", pacedMeasureRate, pacedMeasureWindow, serial, paced)
	require.Positive(t, serial, "the serial ticker dispatched nothing")
	require.Positive(t, paced, "the batched pacer dispatched nothing")
}

func TestParseInjectMode(t *testing.T) {
	cases := []struct {
		in   string
		want InjectMode
		err  bool
	}{
		{"frontdoor", InjectFrontdoor, false},
		{"canonical", InjectCanonical, false},
		{"", "", true},
		{"http", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseInjectMode(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
