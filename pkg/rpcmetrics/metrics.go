// Package rpcmetrics holds the shared Prometheus collectors and status
// taxonomy for synchronous RPC handler metrics, emitted identically by the
// NATS (pkg/natsrouter) and HTTP (pkg/ginutil) middlewares.
package rpcmetrics

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/hmchangw/chat/pkg/errcode"
)

// Collectors register with the default Prometheus registry via promauto, so a
// plain promhttp.Handler() (or otelutil.MetricsServer) exposes them on /metrics.
var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rpc_server_requests_total",
		Help: "Total RPC request/reply invocations handled, partitioned by service, route pattern, and terminal status.",
	}, []string{"service", "route", "status"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rpc_server_request_duration_seconds",
		Help:    "End-to-end RPC handler latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"service", "route"})
)

// Observe records one completed RPC: its latency and terminal status.
// route MUST be a pattern/template (e.g. "chat.user.{account}.request.room.get"
// or a Gin FullPath), never a live subject/URL, to keep cardinality bounded.
func Observe(service, route, status string, d time.Duration) {
	requestDuration.WithLabelValues(service, route).Observe(d.Seconds())
	requestsTotal.WithLabelValues(service, route, status).Inc()
}

// StatusLabel maps a handler's returned error onto the `status` label:
// nil -> "ok"; a non-empty *errcode.Error Code in the chain that is in the
// pinned allowlist -> that Code; everything else -> "internal". It is a pure,
// non-logging Code extractor (errors.As) — it never double-logs against
// errcode.Classify, so it is safe to call on the reply path.
func StatusLabel(err error) string {
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

// allowedStatusLabels pins the cardinality of the status label to the 8
// canonical errcode Codes + "ok". Any label outside this set collapses to
// "internal" via StatusLabel, so a future Code added without updating this
// allowlist cannot mint a fresh time series.
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

// CounterValue returns the current rpc_server_requests_total value for the
// given label tuple. It is a test seam for consumer packages (natsrouter,
// ginutil) that cannot reach the unexported collector; side-effect-free.
func CounterValue(service, route, status string) float64 {
	return testutil.ToFloat64(requestsTotal.WithLabelValues(service, route, status))
}
