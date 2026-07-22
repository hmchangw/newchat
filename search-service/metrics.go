package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/errcode"
)

// defBuckets mirrors prometheus.DefBuckets so the histograms keep the same
// boundaries after the move from client_golang to the OTel meter — a Grafana
// `histogram_quantile` over the old series stays valid. Set as the instrument's
// advisory boundaries (WithExplicitBucketBoundaries) since OTel's default
// aggregation buckets differ.
var defBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// metrics holds the search-service app instruments. They are emitted through
// the o11y SDK's meter, so otelprom exposes them on the SDK's :2112 endpoint
// alongside the runtime/SDK metrics — no separate promhttp listener. otelprom
// reconstructs the original Prometheus names by appending `_total` to the
// counter and the `_seconds` unit suffix to the histograms.
type metrics struct {
	requests        metric.Int64Counter     // → search_service_requests_total
	requestDuration metric.Float64Histogram // → search_service_request_duration_seconds
	esDuration      metric.Float64Histogram // → search_service_es_duration_seconds
}

// appMetrics is set once by initMetrics after obs.Init has installed the global
// meter provider. It stays nil in unit tests (and any path that skips
// initMetrics), where the observe helpers degrade to no-ops.
var appMetrics *metrics

// newMetrics builds the app instruments from the given meter. It is separate
// from initMetrics so tests can attach a manual-reader meter.
func newMetrics(meter metric.Meter) (*metrics, error) {
	requests, err := meter.Int64Counter(
		"search_service_requests",
		metric.WithDescription("Total NATS request/reply invocations handled, partitioned by endpoint and terminal status."),
	)
	if err != nil {
		return nil, fmt.Errorf("create requests counter: %w", err)
	}
	requestDuration, err := meter.Float64Histogram(
		"search_service_request_duration",
		metric.WithUnit("s"),
		metric.WithDescription("End-to-end handler latency in seconds, from NATS request receipt to response emission."),
		metric.WithExplicitBucketBoundaries(defBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create request-duration histogram: %w", err)
	}
	esDuration, err := meter.Float64Histogram(
		"search_service_es_duration",
		metric.WithUnit("s"),
		metric.WithDescription("Elasticsearch _search call latency in seconds."),
		metric.WithExplicitBucketBoundaries(defBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create es-duration histogram: %w", err)
	}
	return &metrics{requests: requests, requestDuration: requestDuration, esDuration: esDuration}, nil
}

// initMetrics builds the app instruments from meter and installs them as the
// process-wide appMetrics. Call once, after obs.Init.
func initMetrics(meter metric.Meter) error {
	m, err := newMetrics(meter)
	if err != nil {
		return err
	}
	appMetrics = m
	return nil
}

// Per-kind request labels. The `status` label on the counter is resolved lazily
// from the handler's returned error (statusLabel); the pinned label set keeps
// cardinality bounded.
const (
	metricKindMessages = "messages"
	metricKindRooms    = "subscriptions"
	metricKindApps     = "apps"
	metricKindUsers    = "users"
	metricKindOrgs     = "orgs"
)

// observeRequest captures a handler's total latency and terminal status. The
// status is classified at fire-time from the named `err` return, so late-bound
// error classification (wrapping, defer-assigned) is counted correctly. Usage:
//
//	func (h *handler) search(c *natsrouter.Context, ...) (resp *R, err error) {
//	    defer observeRequest(c, metricKindMessages, &err)()
//	    ...
//	}
func observeRequest(ctx context.Context, kind string, errPtr *error) func() {
	if appMetrics == nil {
		return func() {}
	}
	start := time.Now()
	return func() {
		appMetrics.requestDuration.Record(ctx, time.Since(start).Seconds(),
			metric.WithAttributes(attribute.String("kind", kind)))
		appMetrics.requests.Add(ctx, 1, metric.WithAttributes(
			attribute.String("kind", kind),
			attribute.String("status", statusLabel(*errPtr)),
		))
	}
}

// observeES records the latency of a single Elasticsearch _search call.
func observeES(ctx context.Context) func() {
	if appMetrics == nil {
		return func() {}
	}
	start := time.Now()
	return func() {
		appMetrics.esDuration.Record(ctx, time.Since(start).Seconds())
	}
}

// statusLabel maps a handler's returned error onto the requests `status` label.
// nil → "ok"; a non-empty *errcode.Error in the chain → its Code (one of the 8
// canonical Codes below); everything else → "internal".
//
// The label set is pinned to keep cardinality bounded. A non-canonical Code
// (e.g. a future Code constant added without updating this allowlist, or a
// foreign envelope on a federation path) collapses to "internal" rather than
// minting a fresh time series.
func statusLabel(err error) string {
	if err == nil {
		return "ok"
	}
	var ee *errcode.Error
	if errors.As(err, &ee) && ee.Code != "" {
		if _, ok := allowedStatusLabels[string(ee.Code)]; ok {
			return string(ee.Code)
		}
	}
	return string(errcode.CodeInternal)
}

// allowedStatusLabels pins the cardinality of the requests `status` label to the
// 8 canonical errcode Codes + "ok". Any label outside this set collapses to
// "internal" via statusLabel.
var allowedStatusLabels = map[string]struct{}{
	"ok":                                {},
	string(errcode.CodeBadRequest):      {},
	string(errcode.CodeUnauthenticated): {},
	string(errcode.CodeForbidden):       {},
	string(errcode.CodeNotFound):        {},
	string(errcode.CodeConflict):        {},
	string(errcode.CodeTooManyRequests): {},
	string(errcode.CodeUnavailable):     {},
	string(errcode.CodeInternal):        {},
}
