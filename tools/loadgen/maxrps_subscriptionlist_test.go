package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time check: subscriptionListWorkload satisfies rpsWorkload.
var _ rpsWorkload = (*subscriptionListWorkload)(nil)

func TestBuildSubscriptionListInputs_MapsCollector(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordSample(SubscriptionListSample{Latency: 4 * time.Millisecond, Rows: 40})
	c.RecordSample(SubscriptionListSample{Latency: 6 * time.Millisecond, Rows: 20})
	c.RecordError(errClassTimeout, time.Millisecond)
	c.RecordError(errClassReply, time.Millisecond)
	c.RecordBadReply(time.Millisecond)
	c.RecordSaturation()

	in := buildSubscriptionListInputs(1000, 30*time.Second, c)

	assert.Equal(t, 1000, in.TargetRPS)
	assert.Equal(t, 30*time.Second, in.Hold)
	assert.Equal(t, 3, in.FailedOps)    // 1 timeout + 1 reply + 1 bad reply
	assert.Equal(t, 5, in.AttemptedOps) // 2 samples + 3 failed
	assert.Equal(t, 1, in.Saturation)
	assert.Empty(t, in.Pending, "synchronous RPC has no pending durables")
	require.Len(t, in.Latencies, 1)
	assert.Equal(t, "subscription-list", in.Latencies[0].Name)
	assert.Len(t, in.Latencies[0].Samples, 2)
}

// An empty page is a broken run, not a fast one: it must gate the ramp rather
// than be averaged away as a very quick success.
func TestBuildSubscriptionListInputs_EmptyPagesCountAsFailures(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordSample(SubscriptionListSample{Latency: time.Millisecond, Rows: 5})
	c.RecordEmptyPage(time.Millisecond)
	c.RecordEmptyPage(time.Millisecond)

	in := buildSubscriptionListInputs(500, 10*time.Second, c)

	assert.Equal(t, 2, in.FailedOps)
	assert.Equal(t, 3, in.AttemptedOps)
	assert.Len(t, in.Latencies[0].Samples, 1, "an empty page contributes no latency")
}

func TestBuildSubscriptionListInputs_PopulatesEmitUnderrun(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordUnderrun(7)
	c.RecordUnderrun(3)
	in := buildSubscriptionListInputs(2000, 30*time.Second, c)
	assert.Equal(t, 10, in.EmitUnderrun)
}

func TestSubscriptionListWorkload_Label(t *testing.T) {
	w := &subscriptionListWorkload{}
	assert.Equal(t, "subscription-list", w.Label())
}

func TestDefaultSteps_SubscriptionList(t *testing.T) {
	assert.Equal(t, "200,500,1000,2000,5000", defaultSteps("subscription-list"))
}

// newTestSubListWorkload builds the workload without newSubscriptionListWorkload,
// which dials NATS. Everything below the constructor — generator wiring, the
// warmup/hold split and the drain — is exercised against a fake requester.
func newTestSubListWorkload(t *testing.T, req SubscriptionListRequester) *subscriptionListWorkload {
	t.Helper()
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	include := true
	return &subscriptionListWorkload{
		cfg:                &config{SiteID: "site-a", MaxInFlight: 4},
		fixtures:           BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC()),
		seed:               42,
		requestTimeout:     time.Second,
		requester:          req,
		listType:           "current",
		limit:              200,
		includeLastMessage: &include,
	}
}

func TestSubscriptionListWorkload_RunStepMeasuresHoldOnly(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 12, false)}
	w := newTestSubListWorkload(t, req)

	in, err := w.RunStep(context.Background(), 200, 40*time.Millisecond, 60*time.Millisecond)
	require.NoError(t, err)

	assert.Equal(t, 200, in.TargetRPS)
	assert.Equal(t, 60*time.Millisecond, in.Hold)
	require.Len(t, in.Latencies, 1)
	assert.Equal(t, "subscription-list", in.Latencies[0].Name)
	assert.NotEmpty(t, in.Latencies[0].Samples, "the hold window must produce samples")

	// The warmup ran against the same requester, so it dispatched more requests
	// than the hold window alone reports as samples.
	subjects, _ := req.seen()
	assert.Greater(t, len(subjects), len(in.Latencies[0].Samples),
		"warmup requests are dispatched but discarded from the measured inputs")
}

func TestSubscriptionListWorkload_RunStepWithoutWarmup(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 5, false)}
	w := newTestSubListWorkload(t, req)

	in, err := w.RunStep(context.Background(), 200, 0, 60*time.Millisecond)
	require.NoError(t, err)
	assert.NotEmpty(t, in.Latencies[0].Samples)
}

func TestSubscriptionListWorkload_RunStepPropagatesCancellation(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 5, false)}
	w := newTestSubListWorkload(t, req)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := w.RunStep(ctx, 200, 0, time.Minute)
	assert.Error(t, err, "a cancelled run must not report a completed step")
}

func TestSubscriptionListWorkload_NewGeneratorCarriesRequestShape(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 1, false)}
	w := newTestSubListWorkload(t, req)

	g := w.newGenerator(context.Background(), NewSubscriptionListCollector(), 750)

	assert.Equal(t, 750, g.cfg.Rate)
	assert.Equal(t, "current", g.cfg.ListType)
	assert.Equal(t, 200, g.cfg.Limit)
	assert.Equal(t, "site-a", g.cfg.SiteID)
	assert.Equal(t, 4, g.cfg.MaxInFlight)
	require.NotNil(t, g.cfg.IncludeLastMessage)
	assert.True(t, *g.cfg.IncludeLastMessage)
}

func TestLogStepPageShape_SkipsEmptyCollector(t *testing.T) {
	// No samples means nothing was measured; logging a zero-row page shape would
	// read as a degenerate ramp rather than an absent one.
	assert.NotPanics(t, func() { logStepPageShape(500, NewSubscriptionListCollector()) })
}

// Asserts the emitted record, not the collector: the point of this function is
// the log line, so reading the collector back would pass even if it emitted
// nothing or emitted the wrong values.
func TestLogStepPageShape_ReportsMeasuredPages(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordSample(SubscriptionListSample{Latency: time.Millisecond, Rows: 4, HasMore: true})
	c.RecordSample(SubscriptionListSample{Latency: time.Millisecond, Rows: 2})
	c.RecordEmptyPage(time.Millisecond)
	c.RecordError(errClassTimeout, time.Millisecond)
	c.RecordError(errClassReply, time.Millisecond)
	c.RecordBadReply(time.Millisecond)

	attrs := captureLogAttrs(t, func() { logStepPageShape(500, c) })

	assert.Equal(t, float64(500), attrs["rps"])
	assert.Equal(t, float64(2), attrs["pages"])
	assert.InDelta(t, 3.0, attrs["mean_rows"], 0.001)
	assert.Equal(t, float64(1), attrs["has_more_pages"])
	assert.Equal(t, float64(1), attrs["timeout_errors"])
	assert.Equal(t, float64(1), attrs["service_errors"])
	assert.Equal(t, float64(1), attrs["bad_replies"])
	assert.Equal(t, float64(1), attrs["empty_pages"])
}

// A step that produced nothing at all must stay silent: a zero-row line would
// read as a degenerate ramp rather than an absent one.
func TestLogStepPageShape_EmitsNothingWhenNothingHappened(t *testing.T) {
	attrs := captureLogAttrs(t, func() { logStepPageShape(500, NewSubscriptionListCollector()) })
	assert.Empty(t, attrs)
}

// A step with only failures still reports, or a fully failing step would be as
// quiet as one that never ran.
func TestLogStepPageShape_ReportsFailureOnlySteps(t *testing.T) {
	c := NewSubscriptionListCollector()
	c.RecordEmptyPage(time.Millisecond)

	attrs := captureLogAttrs(t, func() { logStepPageShape(500, c) })

	require.NotEmpty(t, attrs)
	assert.Equal(t, float64(0), attrs["pages"])
	assert.Equal(t, float64(1), attrs["empty_pages"])
}

// captureLogAttrs swaps in a JSON slog handler for the duration of fn and
// returns the attributes of the single record it emitted, or an empty map.
func captureLogAttrs(t *testing.T, fn func()) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	fn()

	line := strings.TrimSpace(buf.String())
	if line == "" {
		return map[string]any{}
	}
	var attrs map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &attrs))
	return attrs
}
