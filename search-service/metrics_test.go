package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/errcode"
)

// newTestMetrics wires appMetrics to a MeterProvider backed by an in-memory
// manual reader and returns that reader for collection. It restores the prior
// appMetrics on cleanup so the tests stay independent.
func newTestMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := newMetrics(mp.Meter("search-service"))
	require.NoError(t, err)
	old := appMetrics
	appMetrics = m
	t.Cleanup(func() { appMetrics = old })
	return reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// The OTel instrument names deliberately omit the Prometheus suffixes; otelprom
// appends `_total` to the counter and `_seconds` (from the `s` unit) to the
// histograms, reproducing the original series names on the SDK's :2112 endpoint.
func TestObserveRequest_RecordsCounterAndDuration(t *testing.T) {
	reader := newTestMetrics(t)

	var errNil error
	observeRequest(context.Background(), metricKindMessages, &errNil)()
	errBad := error(errcode.BadRequest("bad"))
	observeRequest(context.Background(), metricKindMessages, &errBad)()

	rm := collectMetrics(t, reader)

	reqs, ok := findMetric(rm, "search_service_requests")
	require.True(t, ok, "counter search_service_requests must be recorded")
	sum, ok := reqs.Data.(metricdata.Sum[int64])
	require.True(t, ok, "requests must be an int64 sum")

	got := map[string]int64{}
	for _, dp := range sum.DataPoints {
		kind, _ := dp.Attributes.Value(attribute.Key("kind"))
		status, _ := dp.Attributes.Value(attribute.Key("status"))
		got[kind.AsString()+"/"+status.AsString()] = dp.Value
	}
	require.Equal(t, int64(1), got["messages/ok"], "nil-err request → status=ok")
	require.Equal(t, int64(1), got["messages/bad_request"], "errcode request → status=bad_request")

	dur, ok := findMetric(rm, "search_service_request_duration")
	require.True(t, ok, "histogram search_service_request_duration must be recorded")
	hist, ok := dur.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.NotEmpty(t, hist.DataPoints)
	require.Equal(t, defBuckets, hist.DataPoints[0].Bounds, "duration buckets must match DefBuckets")
}

func TestObserveES_RecordsHistogram(t *testing.T) {
	reader := newTestMetrics(t)

	observeES(context.Background())()

	rm := collectMetrics(t, reader)
	es, ok := findMetric(rm, "search_service_es_duration")
	require.True(t, ok, "histogram search_service_es_duration must be recorded")
	hist, ok := es.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.NotEmpty(t, hist.DataPoints)
	require.Equal(t, defBuckets, hist.DataPoints[0].Bounds, "ES buckets must match DefBuckets")
}

// Before initMetrics runs (unit tests, and any handler exercised without a real
// meter), the observe helpers must be safe no-ops rather than nil-panic.
func TestObserve_NoopWhenUninitialized(t *testing.T) {
	old := appMetrics
	appMetrics = nil
	t.Cleanup(func() { appMetrics = old })

	var err error
	require.NotPanics(t, func() {
		observeRequest(context.Background(), metricKindMessages, &err)()
		observeES(context.Background())()
	})
}

func TestStatusLabel_OkOnNil(t *testing.T) {
	if got := statusLabel(nil); got != "ok" {
		t.Fatalf("nil err → status = %q, want %q", got, "ok")
	}
}

func TestStatusLabel_CanonicalErrcodePassesThrough(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{errcode.BadRequest("x"), "bad_request"},
		{errcode.Unauthenticated("x"), "unauthenticated"},
		{errcode.Forbidden("x"), "forbidden"},
		{errcode.NotFound("x"), "not_found"},
		{errcode.Conflict("x"), "conflict"},
		{errcode.TooManyRequests("x"), "too_many_requests"},
		{errcode.Unavailable("x"), "unavailable"},
		{errcode.Internal("x"), "internal"},
	}
	for _, tc := range cases {
		if got := statusLabel(tc.err); got != tc.want {
			t.Errorf("statusLabel(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// Wrapped *errcode.Error (the actual production shape from handler.go where
// callers fmt.Errorf("ctx: %w", errcodeErr) before returning) must traverse
// the chain via errors.As and still pin the right label.
func TestStatusLabel_WrappedErrcodePassesThrough(t *testing.T) {
	wrapped := fmt.Errorf("handler load: %w", errcode.BadRequest("missing field"))
	if got := statusLabel(wrapped); got != "bad_request" {
		t.Fatalf("wrapped errcode → %q, want bad_request", got)
	}
}

func TestStatusLabel_NonCanonicalCodeCollapsesToInternal(t *testing.T) {
	// Synthetic *errcode.Error with a non-canonical Code (e.g. a federation peer
	// shipped a foreign envelope). Must not mint a new Prometheus series — the
	// allowedStatusLabels guard collapses it to "internal".
	bad := &errcode.Error{Code: errcode.Code("made_up_category"), Message: "x"}
	if got := statusLabel(bad); got != "internal" {
		t.Fatalf("non-canonical Code → status = %q, want %q", got, "internal")
	}
}

func TestStatusLabel_RawErrorCollapsesToInternal(t *testing.T) {
	if got := statusLabel(errors.New("mongo down")); got != "internal" {
		t.Fatalf("raw err → status = %q, want %q", got, "internal")
	}
	wrapped := fmt.Errorf("ctx: %w", errors.New("mongo down"))
	if got := statusLabel(wrapped); got != "internal" {
		t.Fatalf("wrapped raw err → status = %q, want %q", got, "internal")
	}
}
