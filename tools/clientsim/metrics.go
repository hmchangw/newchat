package main

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// handshakeBuckets cover the auth client's 10s timeout. Deliberately not
// latencyBuckets: those mirror loadgen so histogram_quantile lines up across
// the two tools' panels, and nothing compares handshake timings to loadgen —
// whereas a 5s ceiling would report a degrading issuer as a flat p99=5.000.
var handshakeBuckets = []float64{
	0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
	1.000, 2.500, 5.000, 7.500, 10.000, 15.000,
}

// reconnectAttemptBuckets straddle the client's backoff bands (2s to 5,
// 5s to 10, exponential to 17, then flat), so a quantile says which band
// the fleet is sitting in.
var reconnectAttemptBuckets = []float64{1, 2, 3, 5, 8, 10, 14, 17, 25, 50, 100}

// roomQueueDepthBuckets span an empty queue up to the default
// CLIENTSIM_SUB_PENDING_MSGS (512), which is where the channel blocks and
// nats.go starts dropping.
var roomQueueDepthBuckets = []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512}

// latencyBuckets are loadgen's shared histogram buckets verbatim (its
// metrics.go hand-picked 1-2-5 series), so histogram_quantile lines up
// across the two tools' Grafana panels.
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
	// ConnsReadyPeak is the high-water mark of ConnsReady. The gate requires
	// both this initial reachability proof and a pre-drain terminal snapshot.
	ConnsReadyPeak prometheus.Gauge
	// ConnsReadyMin is the low-water mark once the fleet has first reached
	// its peak. Peak and the pre-drain snapshot are two instants; without a
	// trough a fleet that collapsed mid-window and recovered looks perfect.
	ConnsReadyMin   prometheus.Gauge
	AuthDuration    prometheus.Histogram
	ConnectDuration prometheus.Histogram
	Disconnects     *prometheus.CounterVec
	Reconnects      prometheus.Counter
	// ReconnectAttempt records how deep into the backoff curve each
	// successful reconnect went. The counter behind it only resets after the
	// stability window, so a flapping fleet shows up here as a rising tail.
	ReconnectAttempt prometheus.Histogram
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
	// RoomQueueDepth samples how many deliveries were waiting behind each one
	// the pump took. The room lane is bounded in MESSAGES, not bytes — nats.go
	// rejects SetPendingLimits on a channel subscription and a real byte limit
	// would cost a dispatcher goroutine per room per client — so this is the
	// signal that says whether the pump is keeping up and roughly how much is
	// sitting in memory. Depth x observed payload size is the byte picture.
	RoomQueueDepth prometheus.Histogram
	AuthFailures   prometheus.Counter
	// Errors counts stage failures (stage: auth|connect|walk|resync|
	// room_subscribe|async|conn_closed) so
	// error RATE is queryable, not just grep-able from logs.
	Errors  *prometheus.CounterVec
	RunInfo *prometheus.GaugeVec

	// Pre-resolved children of Delivered — the delivery path is per-message,
	// so it must not pay the label-hash lookup on every copy.
	deliveredUser    prometheus.Counter
	deliveredChannel prometheus.Counter
	deliveredMember  prometheus.Counter

	// readyNow backs ConnsReady so the water marks can be maintained without
	// reading a gauge's value back out of the registry. They stay atomic
	// because readyGate and summarize read them without the lock; readyMu
	// exists only to ORDER a transition's counter update against the gauge it
	// publishes. Two lock-free transitions could otherwise interleave their
	// read-modify-publish and leave a gauge holding the older value until the
	// next transition happened to correct it.
	readyMu sync.Mutex
	// troughFloor is the readiness the fleet must first REACH before the
	// low-water mark starts tracking. Seeding it on the first decrement made a
	// churn cycle during the ramp — when the fleet has not come up yet —
	// permanently pin the trough near zero however healthy the rest of the run
	// was, which is not what the series claims to describe.
	troughFloor   int64
	troughArmed   atomic.Bool
	readyNow      atomic.Int64
	readyPeak     atomic.Int64
	readyMin      atomic.Int64
	readyMinSet   atomic.Bool
	readyFrozen   atomic.Bool
	readyAtDrain  atomic.Int64
	readyCaptured atomic.Bool
}

// armTroughAt sets the readiness the fleet must reach before the trough starts
// tracking. A floor of 0 (the gate disabled) arms on the first ready client.
func (m *metrics) armTroughAt(floor int) {
	m.readyMu.Lock()
	defer m.readyMu.Unlock()
	m.troughFloor = int64(floor)
}

// readyInc/readyDec move the readiness gauge and keep the high-water mark.
func (m *metrics) readyInc() {
	m.readyMu.Lock()
	defer m.readyMu.Unlock()
	n := m.readyNow.Add(1)
	m.ConnsReady.Set(float64(n))
	if n >= m.troughFloor && m.troughArmed.CompareAndSwap(false, true) && m.troughFloor > 0 {
		// Seed the low-water mark as the window opens. Waiting for the first
		// decrement leaves the gauge at 0 for a fleet that has never dipped —
		// scraped mid-run that is indistinguishable from a collapse.
		//
		// Only with a real floor: at floor 0 the window opens on the first
		// ready client, so seeding there would make the ramp itself the
		// trough — the very thing the arming exists to keep out.
		m.recordTroughLocked(n)
	}
	if n > m.readyPeak.Load() {
		m.readyPeak.Store(n)
		m.ConnsReadyPeak.Set(float64(n))
	}
}

func (m *metrics) readyDec() {
	m.readyMu.Lock()
	defer m.readyMu.Unlock()
	n := m.readyNow.Add(-1)
	m.ConnsReady.Set(float64(n))
	m.recordTroughLocked(n)
}

// recordTroughLocked keeps the low-water mark. Seeded on the first decrement
// rather than at zero, so the ramp up from an empty fleet is not itself the
// trough. Caller holds readyMu.
func (m *metrics) recordTroughLocked(n int64) {
	// The drain that ends every run walks readiness down to zero. Without this
	// guard the trough of any completed run is 0, which describes the shutdown
	// rather than the measurement window.
	if m.readyFrozen.Load() {
		return
	}
	// Before the fleet first reached its floor, a dip is the ramp, not the
	// window's trough.
	if !m.troughArmed.Load() {
		return
	}
	if m.readyMinSet.CompareAndSwap(false, true) {
		m.readyMin.Store(n)
		m.ConnsReadyMin.Set(float64(n))
		return
	}
	if n < m.readyMin.Load() {
		m.readyMin.Store(n)
		m.ConnsReadyMin.Set(float64(n))
	}
}

// captureReadyAtDrain records the fleet before cancellation drains every
// connection and drives readyNow to zero. Store the value before publishing
// the captured flag so readyGate never observes an uninitialized snapshot.
func (m *metrics) captureReadyAtDrain() {
	// Under readyMu so the snapshot cannot land between a transition's counter
	// update and the gauge it publishes.
	m.readyMu.Lock()
	defer m.readyMu.Unlock()
	n := m.readyNow.Load()
	m.readyAtDrain.Store(n)
	// A run that never dipped has no recorded trough; its minimum is the fleet
	// it held, not the zero of a counter nothing ever touched.
	if m.readyMinSet.CompareAndSwap(false, true) {
		m.readyMin.Store(n)
		m.ConnsReadyMin.Set(float64(n))
	}
	// Freeze before the caller cancels the swarm, so the drain's own
	// decrements cannot rewrite the window's low-water mark.
	m.readyFrozen.Store(true)
	m.readyCaptured.Store(true)
}

func newMetrics() *metrics {
	r := prometheus.NewRegistry()
	m := &metrics{
		Registry:        r,
		ConnsActive:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_active", Help: "Connections currently established."}),
		ConnsConnecting: prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_connecting", Help: "Connections currently dialing the WebSocket transport. The auth exchange precedes this — see clientsim_auth_duration_seconds and clientsim_errors_total{stage=\"auth\"}."}),
		ConnsReady:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_ready", Help: "Connections that completed the subscription walk with their full plan applied."}),
		ConnsReadyPeak:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_ready_peak", Help: "High-water mark of clientsim_conns_ready; paired with a pre-drain snapshot by the fleet-readiness exit gate."}),
		ConnsReadyMin:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_ready_min", Help: "Low-water mark of clientsim_conns_ready after the fleet first came up; a mid-run collapse that recovered is invisible in the peak and the pre-drain snapshot alike."}),
		AuthDuration:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "clientsim_auth_duration_seconds", Help: "POST /api/v1/auth exchange duration (successes only).", Buckets: handshakeBuckets}),
		ConnectDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "clientsim_connect_duration_seconds", Help: "NATS WebSocket connect duration.", Buckets: handshakeBuckets}),
		Disconnects:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_disconnects_total", Help: "Disconnections by reason."}, []string{"reason"}),
		Reconnects:      prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_reconnects_total", Help: "Successful reconnects."}),
		ReconnectAttempt: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "clientsim_reconnect_attempt", Buckets: reconnectAttemptBuckets,
			Help: "Attempt number each successful reconnect landed on; the counter resets only after the stability window, so this is the depth into the backoff curve.",
		}),
		JWTRefreshes: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_jwt_refreshes_total", Help: "JWT re-mints by lifecycle mode."}, []string{"mode"}),
		Delivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clientsim_msgs_delivered_total",
			Help: "Fan-out copies received, by lane (user | channel | member). Per-connection copies — NOT comparable to loadgen's logical send counters.",
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
		RoomQueueDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "clientsim_room_queue_depth", Buckets: roomQueueDepthBuckets,
			Help: "Deliveries still queued behind each one the pump took; the room lane is bounded in messages, not bytes.",
		}),
		AuthFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_auth_failures_total", Help: "Auth exchange failures (transport errors and non-2xx rejections)."}),
		Errors:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_errors_total", Help: "Stage failures."}, []string{"stage"}),
		RunInfo:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "clientsim_run_info", Help: "Static run metadata; value is always 1. Run ID stays in logs (unbounded label), matching loadgen."}, []string{"jwtMode", "shardIndex", "shardCount"}),
	}
	m.deliveredUser = m.Delivered.WithLabelValues("user")
	m.deliveredChannel = m.Delivered.WithLabelValues("channel")
	m.deliveredMember = m.Delivered.WithLabelValues("member")
	r.MustRegister(
		m.ConnsActive, m.ConnsConnecting, m.ConnsReady, m.ConnsReadyPeak, m.ConnsReadyMin,
		m.AuthDuration, m.ConnectDuration,
		m.Disconnects, m.Reconnects, m.ReconnectAttempt, m.JWTRefreshes, m.Delivered,
		m.BroadcastLatency, m.CanonicalLatency,
		m.DecodeFailures, m.InvalidTimestamp, m.SlowConsumer, m.RoomQueueDepth,
		m.AuthFailures, m.Errors, m.RunInfo,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// delivered returns the pre-resolved counter for a lane.
func (m *metrics) delivered(lane string) prometheus.Counter {
	switch lane {
	case "user":
		return m.deliveredUser
	case "member":
		return m.deliveredMember
	default:
		return m.deliveredChannel
	}
}

// Handler serves this registry (mounted at the metrics server root, like loadgen).
func (m *metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
