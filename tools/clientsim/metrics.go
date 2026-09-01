package main

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// latencyBuckets are loadgen's shared histogram buckets verbatim (its
// metrics.go hand-picked 1-2-5 series), so histogram_quantile lines up
// across the two tools' Grafana panels.
// handshakeBuckets cover the auth client's 10s timeout. Deliberately not
// latencyBuckets: those mirror loadgen so histogram_quantile lines up across
// the two tools' panels, and nothing compares handshake timings to loadgen —
// whereas a 5s ceiling would report a degrading issuer as a flat p99=5.000.
var handshakeBuckets = []float64{
	0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
	1.000, 2.500, 5.000, 7.500, 10.000, 15.000,
}

var latencyBuckets = []float64{
	0.001, 0.002, 0.005, 0.010, 0.025, 0.050,
	0.100, 0.250, 0.500, 1.000, 2.500, 5.000,
}

type metrics struct {
	Registry *prometheus.Registry

	ConnsActive     prometheus.Gauge
	ConnsConnecting prometheus.Gauge
	// ConnsReady counts clients that are connected AND carrying their full
	// subscription plan. ConnsActive alone answers "did the socket open",
	// which is not the same question — a client can be connected while
	// missing rooms, and that gap is what the exit gate judges.
	ConnsReady prometheus.Gauge
	// ConnsReadyPeak is the high-water mark of ConnsReady. The gate reads
	// the peak, not the final value, because SIGTERM drains the fleet to
	// zero before the summary runs.
	ConnsReadyPeak   prometheus.Gauge
	AuthDuration     prometheus.Histogram
	ConnectDuration  prometheus.Histogram
	Disconnects      *prometheus.CounterVec
	Reconnects       prometheus.Counter
	JWTRefreshes     *prometheus.CounterVec
	Delivered        *prometheus.CounterVec
	BroadcastLatency prometheus.Histogram
	CanonicalLatency prometheus.Histogram
	DecodeFailures   prometheus.Counter
	InvalidTimestamp prometheus.Counter
	// SlowConsumer counts slow-consumer EPISODES (Active->SlowConsumer
	// transitions), never dropped-message totals — Subscription.Dropped()
	// is lifetime-cumulative and callback-adding it double-counts; see
	// pkg/natsutil/slowconsumer.go for the full trap description.
	SlowConsumer prometheus.Counter
	AuthFailures prometheus.Counter
	// Errors counts stage failures (stage: auth|connect|walk|resync|
	// room_subscribe) so
	// error RATE is queryable, not just grep-able from logs.
	Errors  *prometheus.CounterVec
	RunInfo *prometheus.GaugeVec

	// Pre-resolved children of Delivered — the delivery path is per-message,
	// so it must not pay the label-hash lookup on every copy.
	deliveredUser    prometheus.Counter
	deliveredChannel prometheus.Counter

	// readyNow backs ConnsReady so the peak can be maintained without
	// reading a gauge's value back out of the registry.
	readyNow  atomic.Int64
	readyPeak atomic.Int64
}

// readyInc/readyDec move the readiness gauge and keep the high-water mark.
func (m *metrics) readyInc() {
	n := m.readyNow.Add(1)
	m.ConnsReady.Set(float64(n))
	for {
		peak := m.readyPeak.Load()
		if n <= peak {
			return
		}
		if m.readyPeak.CompareAndSwap(peak, n) {
			m.ConnsReadyPeak.Set(float64(n))
			return
		}
	}
}

func (m *metrics) readyDec() {
	m.ConnsReady.Set(float64(m.readyNow.Add(-1)))
}

func newMetrics() *metrics {
	r := prometheus.NewRegistry()
	m := &metrics{
		Registry:        r,
		ConnsActive:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_active", Help: "Connections currently established."}),
		ConnsConnecting: prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_connecting", Help: "Connections currently dialing the WebSocket transport. The auth exchange precedes this — see clientsim_auth_duration_seconds and clientsim_errors_total{stage=\"auth\"}."}),
		ConnsReady:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_ready", Help: "Connections that completed the subscription walk with their full plan applied."}),
		ConnsReadyPeak:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_ready_peak", Help: "High-water mark of clientsim_conns_ready; what the fleet-readiness exit gate judges."}),
		AuthDuration:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "clientsim_auth_duration_seconds", Help: "POST /api/v1/auth exchange duration (successes only).", Buckets: handshakeBuckets}),
		ConnectDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "clientsim_connect_duration_seconds", Help: "NATS WebSocket connect duration.", Buckets: handshakeBuckets}),
		Disconnects:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_disconnects_total", Help: "Disconnections by reason."}, []string{"reason"}),
		Reconnects:      prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_reconnects_total", Help: "Successful reconnects."}),
		JWTRefreshes:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_jwt_refreshes_total", Help: "JWT re-mints by lifecycle mode."}, []string{"mode"}),
		Delivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clientsim_msgs_delivered_total",
			Help: "Fan-out copies received, by lane. Per-connection copies — NOT comparable to loadgen's logical send counters.",
		}, []string{"lane"}),
		BroadcastLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "clientsim_broadcast_to_client_latency_seconds", Buckets: latencyBuckets,
			Help: "receive - RoomEvent.Timestamp (broadcast publish -> client edge; carries inter-host clock skew).",
		}),
		CanonicalLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "clientsim_canonical_to_client_latency_seconds", Buckets: latencyBuckets,
			Help: "receive - RoomEvent.EventTimestamp (canonical publish -> client edge; carries inter-host clock skew).",
		}),
		DecodeFailures:   prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_decode_failures_total", Help: "Envelope decode failures; any increment marks the window degraded."}),
		InvalidTimestamp: prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_invalid_timestamp_total", Help: "Zero or negative observed event age (beyond the skew tolerance); any increment marks the window degraded."}),
		SlowConsumer:     prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_slow_consumer_events_total", Help: "Slow-consumer episodes (per transition, not per dropped message); any increment marks the window degraded."}),
		AuthFailures:     prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_auth_failures_total", Help: "Auth exchange failures (transport errors and non-2xx rejections)."}),
		Errors:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_errors_total", Help: "Stage failures."}, []string{"stage"}),
		RunInfo:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "clientsim_run_info", Help: "Static run metadata; value is always 1. Run ID stays in logs (unbounded label), matching loadgen."}, []string{"jwtMode", "shardIndex", "shardCount"}),
	}
	m.deliveredUser = m.Delivered.WithLabelValues("user")
	m.deliveredChannel = m.Delivered.WithLabelValues("channel")
	r.MustRegister(
		m.ConnsActive, m.ConnsConnecting, m.ConnsReady, m.ConnsReadyPeak,
		m.AuthDuration, m.ConnectDuration,
		m.Disconnects, m.Reconnects, m.JWTRefreshes, m.Delivered,
		m.BroadcastLatency, m.CanonicalLatency,
		m.DecodeFailures, m.InvalidTimestamp, m.SlowConsumer,
		m.AuthFailures, m.Errors, m.RunInfo,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// delivered returns the pre-resolved counter for a lane.
func (m *metrics) delivered(lane string) prometheus.Counter {
	if lane == "user" {
		return m.deliveredUser
	}
	return m.deliveredChannel
}

// Handler serves this registry (mounted at the metrics server root, like loadgen).
func (m *metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
