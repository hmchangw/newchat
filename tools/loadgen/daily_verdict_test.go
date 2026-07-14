package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateStep_AllGreen(t *testing.T) {
	s := stepInputs{
		N: 1000, HoldDuration: 180 * time.Second,
		LatencySamples: []float64{10, 20, 50, 100, 200},
		AttemptedOps:   10000, FailedOps: 0,
		ConsumerPending: map[string]ConsumerPendingDelta{
			"message-worker":   {Start: 100, End: 110, Delta: 10},
			"broadcast-worker": {Start: 50, End: 55, Delta: 5},
		},
		ServiceErrors: map[string]int64{},
		Self:          SelfMetrics{GCPauseP99Ms: 5, CPUPercent: 40, Goroutines: 50000},
	}
	r := evaluateStep(s, defaultThresholds())
	require.False(t, r.Tripped)
	require.False(t, r.Inconclusive)
	require.Empty(t, r.TrippedReasons)
}

func TestEvaluateStep_TripsOnPendingGrowth(t *testing.T) {
	s := stepInputs{
		N: 5000, HoldDuration: 180 * time.Second,
		LatencySamples: []float64{10, 20},
		AttemptedOps:   1000,
		ConsumerPending: map[string]ConsumerPendingDelta{
			"broadcast-worker": {Start: 100, End: 2000, Delta: 1900},
		},
	}
	r := evaluateStep(s, defaultThresholds())
	require.True(t, r.Tripped)
	require.Contains(t, r.TrippedReasons[0], "broadcast-worker")
}

func TestEvaluateStep_TripsOnP95Latency(t *testing.T) {
	// Half the samples are elevated above the 500ms threshold so the p95
	// index (94 of 100 sorted ascending) lands in the elevated region.
	samples := make([]float64, 100)
	for i := 0; i < 50; i++ {
		samples[i] = 200
	}
	for i := 50; i < 100; i++ {
		samples[i] = 600
	}
	s := stepInputs{
		N: 5000, HoldDuration: 180 * time.Second,
		LatencySamples: samples, AttemptedOps: 1000,
	}
	r := evaluateStep(s, defaultThresholds())
	require.True(t, r.Tripped)
	require.Contains(t, r.TrippedReasons[0], "p95")
}

func TestEvaluateStep_DoesNotTripOnNotificationWorkerPending(t *testing.T) {
	// Push-notification delivery delay is tolerated by design, so a growing
	// notification-worker backlog must NOT fail the run — even far past the
	// pending-growth threshold that would trip any other durable.
	s := stepInputs{
		N: 5000, EffectiveN: 5000, HoldDuration: 180 * time.Second,
		LatencySamples: []float64{10, 20},
		AttemptedOps:   1000,
		ConsumerPending: map[string]ConsumerPendingDelta{
			"notification-worker": {Start: 100, End: 100000, Delta: 99900},
		},
	}
	r := evaluateStep(s, defaultThresholds())
	require.False(t, r.Tripped, "reasons: %v", r.TrippedReasons)
	require.Empty(t, r.TrippedReasons)
}

func TestEvaluateStep_NotificationWorkerExclusionScopedToItself(t *testing.T) {
	// Excluding notification-worker must not mask a real backlog on another
	// durable measured in the same step.
	s := stepInputs{
		N: 5000, EffectiveN: 5000, HoldDuration: 180 * time.Second,
		LatencySamples: []float64{10, 20},
		AttemptedOps:   1000,
		ConsumerPending: map[string]ConsumerPendingDelta{
			"notification-worker": {Start: 100, End: 100000, Delta: 99900},
			"broadcast-worker":    {Start: 100, End: 2000, Delta: 1900},
		},
	}
	r := evaluateStep(s, defaultThresholds())
	require.True(t, r.Tripped)
	joined := strings.Join(r.TrippedReasons, "|")
	require.Contains(t, joined, "broadcast-worker")
	require.NotContains(t, joined, "notification-worker")
}

func TestEvaluateStep_NotificationWorkerDisappearanceStillTrips(t *testing.T) {
	// The exclusion is performance-only: a notification-worker that vanished
	// mid-hold (consumer crashed or was deleted) is an availability failure,
	// not a tolerated delay, so it still trips.
	s := stepInputs{
		N: 5000, EffectiveN: 5000, HoldDuration: 180 * time.Second,
		LatencySamples: []float64{10, 20},
		AttemptedOps:   1000,
		ConsumerPending: map[string]ConsumerPendingDelta{
			"notification-worker": {Start: 500, End: 0, Delta: -500},
		},
	}
	r := evaluateStep(s, defaultThresholds())
	require.True(t, r.Tripped)
	require.Contains(t, strings.Join(r.TrippedReasons, "|"), "notification-worker disappeared")
}

func TestEvaluateStep_InconclusiveOnHighGC(t *testing.T) {
	s := stepInputs{
		N: 20000, HoldDuration: 180 * time.Second,
		LatencySamples: []float64{10},
		AttemptedOps:   1000,
		Self:           SelfMetrics{GCPauseP99Ms: 80, CPUPercent: 90, Goroutines: 100000},
	}
	r := evaluateStep(s, defaultThresholds())
	require.True(t, r.Inconclusive)
	require.False(t, r.Tripped) // inconclusive overrides trip
}

func TestEvaluateStep_TripsOnErrorRate(t *testing.T) {
	s := stepInputs{
		N: 5000, HoldDuration: 180 * time.Second,
		LatencySamples: []float64{10},
		AttemptedOps:   10000, FailedOps: 50, // 0.5% > 0.1%
	}
	r := evaluateStep(s, defaultThresholds())
	require.True(t, r.Tripped)
	require.Contains(t, r.TrippedReasons[0], "error_rate")
}

func TestSelfMetricsSnapshot_ReturnsSaneValues(t *testing.T) {
	s := snapshotSelfMetrics()
	require.Greater(t, s.Goroutines, 0)
	require.GreaterOrEqual(t, s.GCPauseP99Ms, 0.0)
	require.GreaterOrEqual(t, s.CPUPercent, 0.0)
}

func TestDiffPending_BuildsDelta(t *testing.T) {
	start := map[string]int64{"a": 100, "b": 50}
	end := map[string]int64{"a": 150, "b": 50, "c": 10}
	got := diffPending(start, end)
	require.Equal(t, int64(50), got["a"].Delta)
	require.Equal(t, int64(0), got["b"].Delta)
	require.Equal(t, int64(10), got["c"].Delta) // c was added mid-window
}

func TestPollPending_ParsesJsz(t *testing.T) {
	body := `{
      "account_details": [{
        "stream_detail": [{
          "consumer_detail": [
            {"name": "message-worker", "num_pending": 42},
            {"name": "broadcast-worker", "num_pending": 7}
          ]
        }]
      }]
    }`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/jsz", r.URL.Path)
		require.Equal(t, "consumers=true", r.URL.RawQuery)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	got, err := pollPending(context.Background(), srv.URL+"/jsz")
	require.NoError(t, err)
	require.Equal(t, int64(42), got["message-worker"])
	require.Equal(t, int64(7), got["broadcast-worker"])
}

func TestPollPending_ReturnsErrorOnBadURL(t *testing.T) {
	_, err := pollPending(context.Background(), "http://127.0.0.1:1/jsz")
	require.Error(t, err)
}

func TestScrapeErrorCounter_SumsFamily(t *testing.T) {
	body := `# HELP slog_errors_total Total errors logged
# TYPE slog_errors_total counter
slog_errors_total{level="error"} 5
slog_errors_total{level="warn"} 0
# unrelated counter
other_total 100
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	v, err := scrapeErrorCounter(context.Background(), srv.URL)
	require.NoError(t, err)
	require.Equal(t, 5.0, v)
}

func TestSumCounterFamily_HandlesCommentsAndBlankLines(t *testing.T) {
	body := `
# HELP foo
# TYPE foo counter
foo_total{a="x"} 3
foo_total{a="y"} 4
unrelated 99
`
	require.Equal(t, 7.0, sumCounterFamily(body, "foo_total"))
	require.Equal(t, 0.0, sumCounterFamily(body, "missing"))
}

func TestServiceScraper_DeltaAfterBaseline(t *testing.T) {
	var counter atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "slog_errors_total %d\n", counter.Load())
	}))
	t.Cleanup(srv.Close)

	s := newServiceScraper()
	urls := map[string]string{"svc": srv.URL}

	// First call records baseline; returns 0.
	out, err := s.Scrape(context.Background(), urls)
	require.NoError(t, err)
	require.Equal(t, int64(0), out["svc"])

	counter.Add(3)
	out, err = s.Scrape(context.Background(), urls)
	require.NoError(t, err)
	require.Equal(t, int64(3), out["svc"])
}

func TestEvaluateStep_TripsOnPerActionP95(t *testing.T) {
	in := stepInputs{
		N: 1000, EffectiveN: 1000, HoldDuration: 60 * time.Second,
		LatencySamples: []float64{10, 20, 30}, AttemptedOps: 100,
		ActionSamplesMs: map[string][]float64{
			"mark_read": repeatFloat(60, 100), // p95 ≈ 60ms, under 100ms cap
			"scroll_history": append( // p95 lands at 800ms, over 500ms cap
				repeatFloat(50, 90), repeatFloat(800, 10)...,
			),
		},
	}
	r := evaluateStep(in, defaultThresholds())
	require.True(t, r.Tripped)
	require.NotEmpty(t, r.TrippedReasons)
	// One reason should mention scroll_history p95
	joined := strings.Join(r.TrippedReasons, "|")
	require.Contains(t, joined, "scroll_history p95=")
	require.NotContains(t, joined, "read_receipt p95=")
}

func TestEvaluateStep_NoTripWhenActionLatenciesUnderCap(t *testing.T) {
	in := stepInputs{
		N: 1000, EffectiveN: 1000, HoldDuration: 60 * time.Second,
		LatencySamples: []float64{10, 20, 30}, AttemptedOps: 100,
		ActionSamplesMs: map[string][]float64{
			"mark_read":         repeatFloat(50, 100),
			"scroll_history":    repeatFloat(200, 100),
			"member_add":        repeatFloat(80, 100),
			"refresh_room_list": repeatFloat(40, 100),
		},
	}
	r := evaluateStep(in, defaultThresholds())
	require.False(t, r.Tripped, "reasons: %v", r.TrippedReasons)
	require.False(t, r.Inconclusive)
}

func repeatFloat(v float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func TestEvaluateStep_IgnoresPresence(t *testing.T) {
	// evaluateStep must never read Presence; a healthy step stays PASS
	// regardless of presence stats, which are populated outside the verdict.
	in := stepInputs{
		N: 100, EffectiveN: 100,
		HoldDuration:   time.Minute,
		LatencySamples: []float64{10, 20, 30},
		AttemptedOps:   1000, FailedOps: 0,
		Self: SelfMetrics{GCPauseP99Ms: 5},
	}
	r := evaluateStep(in, defaultThresholds())
	assert.False(t, r.Tripped)
	assert.False(t, r.Inconclusive)
	assert.Nil(t, r.Presence) // evaluateStep does not set it
}

func TestPresenceObsStats_Shape(t *testing.T) {
	s := PresenceObsStats{P50Ms: 5, P95Ms: 40, P99Ms: 90, Attempted: 100, Failed: 2}
	if s.Attempted > 0 {
		s.ErrorRate = float64(s.Failed) / float64(s.Attempted)
	}
	assert.InDelta(t, 0.02, s.ErrorRate, 1e-9)
}
