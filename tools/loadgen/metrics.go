package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors used across loadgen components.
type Metrics struct {
	Registry            *prometheus.Registry
	Published           *prometheus.CounterVec
	PublishErrors       *prometheus.CounterVec
	E1Latency           *prometheus.HistogramVec
	E2Latency           *prometheus.HistogramVec
	ConsumerPending     *prometheus.GaugeVec
	ConsumerAckPending  *prometheus.GaugeVec
	ConsumerRedelivered *prometheus.GaugeVec

	MemberPublished     *prometheus.CounterVec
	MemberPublishErrors *prometheus.CounterVec
	MemberE1Latency     *prometheus.HistogramVec
	MemberE2Latency     *prometheus.HistogramVec
	MemberRoomSize      *prometheus.GaugeVec

	BotRoomPublished     *prometheus.CounterVec
	BotRoomPublishErrors *prometheus.CounterVec
	BotRoomE2ELatency    *prometheus.HistogramVec
	BotRoomReadLatency   *prometheus.HistogramVec

	SoakOperations            *prometheus.CounterVec
	SoakRetries               *prometheus.CounterVec
	SoakErrors                *prometheus.CounterVec
	SoakRPCLatency            *prometheus.HistogramVec
	SoakVerifications         *prometheus.CounterVec
	SoakMutationTargetMissing prometheus.Counter
	SoakSaturation            *prometheus.CounterVec
}

// NewMetrics constructs a dedicated Prometheus registry with all loadgen
// collectors registered. A dedicated registry avoids colliding with default
// Go/process collectors.
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	buckets := []float64{
		0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.000, 2.500, 5.000,
	}
	m := &Metrics{
		Registry: r,
		Published: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "loadgen_published_total", Help: "Messages published by preset and phase (warmup|measured)."},
			[]string{"preset", "phase"},
		),
		PublishErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "loadgen_publish_errors_total", Help: "Publish-side errors."},
			[]string{"preset", "reason"},
		),
		E1Latency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: "loadgen_e1_latency_seconds", Help: "Gatekeeper ack latency.", Buckets: buckets},
			[]string{"preset"},
		),
		E2Latency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: "loadgen_e2_latency_seconds", Help: "Broadcast-visible latency.", Buckets: buckets},
			[]string{"preset"},
		),
		ConsumerPending: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_pending", Help: "JetStream consumer num_pending."},
			[]string{"stream", "durable"},
		),
		ConsumerAckPending: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_ack_pending", Help: "JetStream consumer num_ack_pending."},
			[]string{"stream", "durable"},
		),
		ConsumerRedelivered: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_redelivered", Help: "JetStream consumer num_redelivered."},
			[]string{"stream", "durable"},
		),
	}
	m.MemberPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_member_published_total", Help: "Member-add requests published by preset/phase/inject/shape."},
		[]string{"preset", "phase", "inject", "shape"},
	)
	m.MemberPublishErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_member_publish_errors_total", Help: "Member-add publish-side errors by reason (publish|room_service|timeout|marshal|saturated|underrun)."},
		[]string{"reason"},
	)
	m.MemberE1Latency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "loadgen_member_e1_latency_seconds", Help: "Member-add room-service reply latency.", Buckets: buckets},
		[]string{"preset", "inject"},
	)
	m.MemberE2Latency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "loadgen_member_e2_latency_seconds", Help: "Member-add event-emission latency: publish until room-worker emits RoomMemberEvent (reflects room-worker ROOMS-consumer throughput, not a broadcast fan-out).", Buckets: buckets},
		[]string{"preset", "inject"},
	)
	m.MemberRoomSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loadgen_member_room_size", Help: "Current member count per room (capacity mode only)."},
		[]string{"room_id"},
	)
	m.BotRoomPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_botroom_published_total", Help: "Bot messages published by preset/phase/size."},
		[]string{"preset", "phase", "size"},
	)
	m.BotRoomPublishErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_botroom_publish_errors_total", Help: "Bot publish errors by reason (publish|marshal|gatekeeper|timeout|saturated|underrun)."},
		[]string{"reason"},
	)
	m.BotRoomE2ELatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "loadgen_botroom_e2e_latency_seconds", Help: "Publish→broadcast latency by room size.", Buckets: buckets},
		[]string{"size"},
	)
	m.BotRoomReadLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "loadgen_botroom_read_latency_seconds", Help: "room-service read latency by room size.", Buckets: buckets},
		[]string{"size"},
	)
	m.SoakOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_operations_total",
			Help: "Cassandra soak operations by bounded action, outcome, and phase.",
		},
		[]string{"action", "outcome", "phase"},
	)
	m.SoakRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_retries_total",
			Help: "Cassandra soak retries by bounded action.",
		},
		[]string{"action"},
	)
	m.SoakErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_errors_total",
			Help: "Cassandra soak failures by bounded action and error class.",
		},
		[]string{"action", "class"},
	)
	m.SoakRPCLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loadgen_soak_rpc_latency_seconds",
			Help:    "Cassandra soak per-RPC end-to-end latency by bounded action.",
			Buckets: buckets,
		},
		[]string{"action"},
	)
	m.SoakVerifications = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_verifications_total",
			Help: "Cassandra soak read-back results by bounded action and result class.",
		},
		[]string{"action", "class"},
	)
	m.SoakMutationTargetMissing = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "loadgen_soak_mutation_target_missing_total",
			Help: "Cassandra soak mutation targets still missing after the dedicated retry policy.",
		},
	)
	m.SoakSaturation = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_saturation_total",
			Help: "Cassandra soak operations skipped because the bounded in-flight budget was full.",
		},
		[]string{"lane"},
	)
	r.MustRegister(
		m.Published, m.PublishErrors,
		m.E1Latency, m.E2Latency,
		m.ConsumerPending, m.ConsumerAckPending, m.ConsumerRedelivered,
		m.MemberPublished, m.MemberPublishErrors,
		m.MemberE1Latency, m.MemberE2Latency, m.MemberRoomSize,
		m.BotRoomPublished, m.BotRoomPublishErrors,
		m.BotRoomE2ELatency, m.BotRoomReadLatency,
		m.SoakOperations, m.SoakRetries, m.SoakErrors,
		m.SoakRPCLatency, m.SoakVerifications,
		m.SoakMutationTargetMissing, m.SoakSaturation,
	)
	return m
}

// Handler returns an http.Handler serving this metrics registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
