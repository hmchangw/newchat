package main

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors used across loadgen components.
type Metrics struct {
	natsHealthMu   sync.Mutex
	natsPoolHealth map[string]*loadgenNATSPoolState
	// recipientExpectations is installed once the recipient evidence exists,
	// which is well after the registry is built, and is read on the scrape
	// goroutine.
	recipientExpectations atomic.Pointer[func() int]
	soakHeartbeatSource   atomic.Pointer[func() soakHeartbeatSnapshot]
	mongoProbeSource      atomic.Pointer[func() soakMongoProbeSnapshot]
	Registry              *prometheus.Registry
	Published             *prometheus.CounterVec
	PublishErrors         *prometheus.CounterVec
	E1Latency             *prometheus.HistogramVec
	E2Latency             *prometheus.HistogramVec
	ConsumerPending       *prometheus.GaugeVec
	ConsumerAckPending    *prometheus.GaugeVec
	ConsumerRedelivered   *prometheus.GaugeVec
	ConsumerUp            *prometheus.GaugeVec
	ConsumerDelivered     *prometheus.GaugeVec
	ConsumerAckFloor      *prometheus.GaugeVec
	ConsumerStreamFloor   *prometheus.GaugeVec
	ConsumerMaxDeliver    *prometheus.GaugeVec
	ConsumerAckWait       *prometheus.GaugeVec
	ConsumerLastActive    *prometheus.GaugeVec
	ConsumerAckFloorStall *prometheus.GaugeVec

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
	SoakErrorReasons          *prometheus.CounterVec
	SoakRPCLatency            *prometheus.HistogramVec
	SoakVerifications         *prometheus.CounterVec
	SoakMutationTargetMissing prometheus.Counter
	SoakConfiguredRate        *prometheus.GaugeVec
	SoakIntended              *prometheus.CounterVec
	SoakDispatched            *prometheus.CounterVec
	SoakSchedulerUnderrun     *prometheus.CounterVec
	SoakLaneSaturation        *prometheus.CounterVec
	SoakGlobalSaturation      *prometheus.CounterVec

	SoakRoomCandidates            *prometheus.GaugeVec
	SoakRoomQuarantineProbes      *prometheus.CounterVec
	SoakRoomPoolExhausted         *prometheus.CounterVec
	SoakRoomPoolDegraded          prometheus.Gauge
	SoakRoomCreateBudgetRemaining prometheus.Gauge
	SoakEncryptionPreflight       prometheus.Gauge
	SoakRoomStateSources          *prometheus.CounterVec
	SoakLaneAttempts              *prometheus.CounterVec
	SoakPresenceSignals           *prometheus.CounterVec
	SoakPresenceChecks            *prometheus.CounterVec
	SoakPresenceConnections       *prometheus.GaugeVec
	SoakHeartbeatDegraded         prometheus.GaugeFunc
	SoakHeartbeatSuccessTimestamp prometheus.GaugeFunc
	SoakHeartbeatAttempts         *prometheus.CounterVec
	MongoUp                       prometheus.GaugeFunc
	MongoProbeTimestamp           prometheus.GaugeFunc

	FailureOperations            *prometheus.CounterVec
	FailureObservations          *prometheus.CounterVec
	FailureObservationReasons    *prometheus.CounterVec
	FailureInflight              *prometheus.GaugeVec
	FailureRecipientExpectations prometheus.GaugeFunc
	FailureRecovered             prometheus.Counter
	FailureInvalidations         *prometheus.CounterVec
	FailureJournalBytes          prometheus.Gauge
	FailureUntracked             *prometheus.CounterVec
	FailureReconcileClaims       *prometheus.CounterVec
	FailureReconcileLag          prometheus.Histogram
	FailureDropped               prometheus.Counter
	FailureNotSent               *prometheus.CounterVec
	FailureWALAppendDuration     prometheus.Histogram
	FailureWALAppends            *prometheus.CounterVec
	FailureWALFlushDuration      *prometheus.HistogramVec
	FailureWALFlushBatchSize     *prometheus.HistogramVec
	FailureEvidenceFlushDuration *prometheus.HistogramVec
	FailureEvidenceRecords       *prometheus.CounterVec
	FailureAbandonedJournals     prometheus.Gauge

	NATSConnected             *prometheus.GaugeVec
	NATSConnectionEvents      *prometheus.CounterVec
	NATSOutageDuration        *prometheus.HistogramVec
	NATSCurrentOutage         *prometheus.GaugeVec
	FailureObserverUp         *prometheus.GaugeVec
	FailureObserverConfigured *prometheus.GaugeVec
	FailureObserverEligible   *prometheus.CounterVec
	FailureObserverEvents     *prometheus.CounterVec
	FailureObserverQueueDepth *prometheus.GaugeVec
	ConsumerSampleErrors      *prometheus.CounterVec
	RunInfo                   *prometheus.GaugeVec
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
		natsPoolHealth: make(map[string]*loadgenNATSPoolState),
		Registry:       r,
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
		ConsumerUp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_up", Help: "Whether the configured durable consumer was sampled successfully."},
			[]string{"stream", "durable"},
		),
		ConsumerDelivered: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_delivered_sequence", Help: "Latest delivered consumer sequence."},
			[]string{"stream", "durable"},
		),
		ConsumerAckFloor: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_ack_floor_sequence", Help: "Latest acknowledged consumer sequence."},
			[]string{"stream", "durable"},
		),
		ConsumerStreamFloor: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_ack_floor_stream_sequence", Help: "Latest acknowledged stream sequence."},
			[]string{"stream", "durable"},
		),
		ConsumerMaxDeliver: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_max_deliver", Help: "Configured maximum delivery attempts."},
			[]string{"stream", "durable"},
		),
		ConsumerAckWait: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_ack_wait_seconds", Help: "Configured acknowledgment wait duration."},
			[]string{"stream", "durable"},
		),
		ConsumerLastActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_last_active_timestamp_seconds", Help: "Timestamp of the latest consumer delivery sample."},
			[]string{"stream", "durable"},
		),
		ConsumerAckFloorStall: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "loadgen_consumer_ack_floor_stall_seconds", Help: "Seconds since the ack floor last advanced while work remains pending."},
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
		prometheus.GaugeOpts{Name: "loadgen_member_room_size", Help: "Rooms currently in each bounded member-count bucket (capacity mode only)."},
		[]string{"size_bucket"},
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
			Help: "Cassandra soak retries by bounded action and run phase.",
		},
		[]string{"action", "phase"},
	)
	m.SoakErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_errors_total",
			Help: "Cassandra soak failures by bounded action, error class, and run phase.",
		},
		[]string{"action", "class", "phase"},
	)
	m.SoakErrorReasons = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_error_reasons_total",
			Help: "Cassandra soak failures by bounded action, error class, service-supplied reason, and run phase.",
		},
		[]string{"action", "class", "reason", "phase"},
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
			Help: "Cassandra soak read-back results by bounded action, result class, and disagreeing field.",
		},
		[]string{"action", "class", "field"},
	)
	m.SoakMutationTargetMissing = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "loadgen_soak_mutation_target_missing_total",
			Help: "Cassandra soak mutation targets still missing after the dedicated retry policy.",
		},
	)
	m.SoakConfiguredRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loadgen_soak_configured_rate", Help: "Configured open-loop target rate by bounded soak lane."},
		[]string{"lane"},
	)
	m.SoakIntended = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_soak_intended_total", Help: "Pacing events due by bounded soak lane."},
		[]string{"lane"},
	)
	m.SoakDispatched = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_soak_dispatched_total", Help: "Pacing events dispatched by bounded soak lane."},
		[]string{"lane"},
	)
	m.SoakSchedulerUnderrun = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_soak_scheduler_underrun_total", Help: "Pacing events skipped after scheduler delay by bounded soak lane."},
		[]string{"lane"},
	)
	m.SoakLaneSaturation = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_soak_lane_saturation_total", Help: "Pacing events skipped because the lane in-flight budget was full."},
		[]string{"lane"},
	)
	m.SoakGlobalSaturation = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_soak_global_saturation_total", Help: "Pacing events skipped because the global in-flight budget was full."},
		[]string{"lane"},
	)
	m.SoakRoomCandidates = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_soak_room_candidates",
			Help: "Member candidates by bounded lifecycle state.",
		},
		[]string{"state"},
	)
	m.SoakRoomQuarantineProbes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_room_quarantine_probes_total",
			Help: "Quarantined member candidate re-probes by bounded result.",
		},
		[]string{"result"},
	)
	m.SoakRoomPoolExhausted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_room_pool_exhausted_total",
			Help: "Room and member mutations skipped for lack of a usable target, by bounded reason.",
		},
		[]string{"reason"},
	)
	m.SoakRoomPoolDegraded = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "loadgen_soak_room_pool_degraded",
			Help: "Whether the member candidate pool is currently degraded. Reversible: it clears when the pool recovers.",
		},
	)
	m.SoakRoomCreateBudgetRemaining = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "loadgen_soak_room_create_budget_remaining",
			Help: "Rooms the create lane may still add before it stops for this run.",
		},
	)
	m.SoakEncryptionPreflight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "loadgen_soak_encryption_preflight",
			Help: "1 once this run proved a message reached Cassandra encrypted and wrapped the room DEK. 0 means not proven — skipped by configuration, still in flight, failed, or a loadgen mode that never runs the check. Alert on a sustained 0 during soak, not on any 0.",
		},
	)
	m.SoakRoomStateSources = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_room_state_source_total",
			Help: "Room-state observer source outcomes by bounded source and result.",
		},
		[]string{"source", "result"},
	)
	m.SoakLaneAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_lane_attempts_total",
			Help: "Lane slots by outcome (sent, no_target, refused). loadgen_soak_dispatched_total counts scheduler slots, which a lane with no usable target still consumes; only this distinguishes offered load from a lane idling on an exhausted pool or one whose mutations the ledger is refusing.",
		},
		[]string{"lane", "outcome"},
	)
	m.SoakPresenceSignals = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_presence_signals_total",
			Help: "Presence signals published by bounded kind. A publish is unacknowledged and is never evidence the server saw it.",
		},
		[]string{"signal"},
	)
	m.SoakPresenceChecks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_presence_checks_total",
			Help: "Presence query comparisons by bounded result.",
		},
		[]string{"result"},
	)
	m.SoakPresenceConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_soak_presence_connections",
			Help: "Virtual presence connections by the status the lane last asked for.",
		},
		[]string{"status"},
	)
	m.FailureOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_failure_operations_total",
			Help: "Completed fault-observation operations by bounded scenario, lane, and result.",
		},
		[]string{"scenario", "lane", "result"},
	)
	m.FailureObservations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_failure_observations_total",
			Help: "Fault-observation results by bounded scenario, lane, observer, and result.",
		},
		[]string{"scenario", "lane", "observer", "result"},
	)
	m.SoakHeartbeatDegraded = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "loadgen_soak_heartbeat_degraded",
			Help: "Whether the latest soak heartbeat attempt failed while the run remains active (1 degraded, 0 otherwise).",
		},
		m.soakHeartbeatDegraded,
	)
	m.SoakHeartbeatSuccessTimestamp = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "loadgen_soak_heartbeat_success_timestamp_seconds",
			Help: "Unix timestamp of the latest successful soak manifest heartbeat; zero until the first success.",
		},
		m.soakHeartbeatSuccessTimestamp,
	)
	m.SoakHeartbeatAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_soak_heartbeat_attempts_total",
			Help: "Soak manifest heartbeat attempts by bounded outcome (success, error, not_active).",
		},
		[]string{"outcome"},
	)
	m.MongoUp = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "loadgen_mongo_up",
			Help: "Whether loadgen's latest bounded Ping to the Mongo primary succeeded (1 up, 0 down or not yet probed).",
		},
		m.mongoUp,
	)
	m.MongoProbeTimestamp = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "loadgen_mongo_probe_timestamp_seconds",
			Help: "Unix timestamp when loadgen's latest Mongo primary Ping completed, regardless of outcome; zero until the first probe.",
		},
		m.mongoProbeTimestamp,
	)
	m.FailureObservationReasons = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_failure_observation_reasons_total",
			Help: "Failure observations by bounded result reason.",
		},
		[]string{"scenario", "lane", "observer", "result", "reason"},
	)
	m.FailureInflight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_failure_inflight",
			Help: "Operations awaiting one or more fault-observation results.",
		},
		[]string{"scenario", "lane"},
	)
	m.FailureRecipientExpectations = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "loadgen_failure_recipient_expectations",
			Help: "Recipient expectations retained in memory; healthy runs track loadgen_failure_inflight.",
		},
		m.recipientExpectationCount,
	)
	m.FailureRecovered = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "loadgen_failure_recovered_operations_total",
			Help: "Unresolved operations recovered from the persistent failure ledger.",
		},
	)
	m.FailureInvalidations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_failure_invalidations_total",
			Help: "Conditions that invalidate fault-test evidence, by bounded reason.",
		},
		[]string{"reason"},
	)
	m.FailureJournalBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "loadgen_failure_journal_bytes",
			Help: "Current persistent failure-ledger WAL size in bytes.",
		},
	)
	m.FailureUntracked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_failure_untracked_total",
			Help: "Operations the ledger could not account for, by bounded reason.",
		},
		[]string{"reason"},
	)
	m.FailureReconcileClaims = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_failure_reconcile_claims_total",
			Help: "Reconcile claims by what the claim achieved; retried is the poll cost the capacity rule cannot model, idle is the lane's remaining slack.",
		},
		[]string{"outcome"},
	)
	// Published at zero before anything happens. The documented saturation rule
	// reads rate(...{outcome="idle"}[5m]) == 0, and a counter child exists only
	// once something increments it — so a lane saturated from the first second
	// would emit no idle series at all, and the rule that detects saturation
	// cannot be evaluated against a series that is absent. Without this the
	// board shows the same nothing for "saturated" and "not instrumented".
	for _, outcome := range soakReconcileClaimOutcomes() {
		m.FailureReconcileClaims.WithLabelValues(outcome)
	}
	m.FailureReconcileLag = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "loadgen_failure_reconcile_lag_seconds",
			Help:    "Seconds past its scheduled probe an operation was when the reconciler claimed it; the capacity floor cannot model fault-time retries, so lag is what shows the lane falling behind, and lag near SOAK_RECONCILE_DEADLINE means operations expire unverified for want of a probe.",
			Buckets: soakReconcileLagBuckets(),
		},
	)
	m.FailureDropped = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "loadgen_failure_dropped_total",
			Help: "Recovered operations discarded at startup because the journal exceeded capacity.",
		},
	)
	m.FailureNotSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_failure_not_sent_total", Help: "Proven local pre-publish failures by bounded reason."},
		[]string{"scenario", "lane", "reason"},
	)
	m.FailureWALAppendDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "loadgen_failure_wal_append_duration_seconds",
			Help:    "Caller-observed failure-ledger append duration; intent records include the grouped durability barrier.",
			Buckets: []float64{0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1},
		},
	)
	m.FailureWALAppends = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_failure_wal_appends_total",
			Help: "Failure-ledger append attempts by bounded result (success or error).",
		},
		[]string{"result"},
	)
	m.FailureWALFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loadgen_failure_wal_flush_duration_seconds",
			Help:    "Duration of grouped failure-ledger fsync barriers by bounded result.",
			Buckets: []float64{0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1},
		},
		[]string{"result"},
	)
	m.FailureWALFlushBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loadgen_failure_wal_flush_batch_size",
			Help:    "Number of WAL records committed by each grouped durability barrier.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
		},
		[]string{"result"},
	)
	m.FailureEvidenceFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loadgen_failure_evidence_flush_duration_seconds",
			Help:    "Duration of batched recipient sidecar durability barriers.",
			Buckets: []float64{0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1},
		},
		[]string{"claim", "result"},
	)
	m.FailureEvidenceRecords = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_failure_evidence_records_total",
			Help: "Recipient sidecar evidence records durably flushed by bounded kind.",
		},
		[]string{"kind"},
	)
	m.FailureAbandonedJournals = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "loadgen_failure_abandoned_journals",
			Help: "Retained failure journals for this run ID that belong to an earlier ledger epoch and are never replayed.",
		},
	)
	m.NATSConnected = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_nats_connected",
			Help: "Whether an instrumented loadgen NATS pool is currently connected.",
		},
		[]string{"pool"},
	)
	m.NATSConnectionEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_nats_connection_events_total",
			Help: "Loadgen NATS connection lifecycle events.",
		},
		[]string{"pool", "event"},
	)
	m.NATSOutageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loadgen_nats_outage_duration_seconds",
			Help:    "Observed duration of loadgen NATS disconnections.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
		[]string{"pool"},
	)
	m.NATSCurrentOutage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loadgen_nats_current_outage_seconds", Help: "Current duration of an in-progress loadgen NATS outage."},
		[]string{"pool"},
	)
	m.FailureObserverUp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loadgen_failure_observer_up", Help: "Whether a required failure observer is healthy."},
		[]string{"observer"},
	)
	m.FailureObserverConfigured = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loadgen_failure_observer_configured", Help: "Whether a bounded failure observer is configured for new operations."},
		[]string{"observer"},
	)
	m.FailureObserverEligible = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_failure_observer_eligible_total", Help: "Operations eligible for a configured observer result."},
		[]string{"scenario", "lane", "observer"},
	)
	m.FailureObserverEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_failure_observer_events_total", Help: "Normalized failure observer events."},
		[]string{"observer", "result"},
	)
	m.FailureObserverQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loadgen_failure_observer_queue_depth", Help: "Queued failure observer events."},
		[]string{"observer"},
	)
	m.ConsumerSampleErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "loadgen_consumer_sample_errors_total", Help: "Configured consumer sampling failures by bounded reason."},
		[]string{"stream", "durable", "reason"},
	)
	m.RunInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loadgen_run_info", Help: "Bounded loadgen runtime metadata without a run ID."},
		[]string{"environment", "scenario", "traffic_profile"},
	)
	r.MustRegister(
		m.Published, m.PublishErrors,
		m.E1Latency, m.E2Latency,
		m.ConsumerPending, m.ConsumerAckPending, m.ConsumerRedelivered,
		m.ConsumerUp, m.ConsumerDelivered, m.ConsumerAckFloor, m.ConsumerStreamFloor,
		m.ConsumerMaxDeliver, m.ConsumerAckWait, m.ConsumerLastActive, m.ConsumerAckFloorStall,
		m.MemberPublished, m.MemberPublishErrors,
		m.MemberE1Latency, m.MemberE2Latency, m.MemberRoomSize,
		m.BotRoomPublished, m.BotRoomPublishErrors,
		m.BotRoomE2ELatency, m.BotRoomReadLatency,
		m.SoakOperations, m.SoakRetries, m.SoakErrors, m.SoakErrorReasons,
		m.SoakRPCLatency, m.SoakVerifications,
		m.SoakMutationTargetMissing, m.SoakConfiguredRate, m.SoakIntended,
		m.SoakDispatched, m.SoakSchedulerUnderrun, m.SoakLaneSaturation, m.SoakGlobalSaturation,
		m.SoakRoomCandidates, m.SoakRoomQuarantineProbes, m.SoakRoomPoolExhausted,
		m.SoakRoomPoolDegraded, m.SoakRoomCreateBudgetRemaining, m.SoakRoomStateSources,
		m.SoakEncryptionPreflight,
		m.SoakLaneAttempts,
		m.SoakPresenceSignals, m.SoakPresenceChecks, m.SoakPresenceConnections,
		m.SoakHeartbeatDegraded, m.SoakHeartbeatSuccessTimestamp,
		m.SoakHeartbeatAttempts, m.MongoUp, m.MongoProbeTimestamp,
		m.FailureOperations, m.FailureObservations, m.FailureObservationReasons, m.FailureInflight,
		m.FailureRecipientExpectations,
		m.FailureRecovered, m.FailureInvalidations, m.FailureJournalBytes,
		m.FailureUntracked, m.FailureReconcileClaims, m.FailureReconcileLag,
		m.FailureDropped, m.FailureNotSent,
		m.FailureWALAppendDuration, m.FailureWALAppends,
		m.FailureWALFlushDuration, m.FailureWALFlushBatchSize,
		m.FailureEvidenceFlushDuration, m.FailureEvidenceRecords,
		m.FailureAbandonedJournals,
		m.NATSConnected, m.NATSConnectionEvents, m.NATSOutageDuration, m.NATSCurrentOutage,
		m.FailureObserverUp, m.FailureObserverConfigured, m.FailureObserverEligible,
		m.FailureObserverEvents, m.FailureObserverQueueDepth,
		m.ConsumerSampleErrors, m.RunInfo,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler returns an http.Handler serving this metrics registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// SetRecipientExpectationSource installs the count the expectation gauge reads.
//
// The gauge is collected at scrape time rather than written by the expiry
// sweep: a sweep that publishes it makes the value depend on the sweep still
// running, and a ledger stalled on its journal is precisely the condition the
// gauge exists to expose. Published from there it would freeze at the last
// healthy number and read as normal.
func (m *Metrics) SetRecipientExpectationSource(source func() int) {
	if m == nil || source == nil {
		return
	}
	m.recipientExpectations.Store(&source)
}

// recipientExpectationCount reports zero until a source is installed, which is
// the honest answer for a run with no recipient observer.
func (m *Metrics) recipientExpectationCount() float64 {
	if m == nil {
		return 0
	}
	if source := m.recipientExpectations.Load(); source != nil {
		return float64((*source)())
	}
	return 0
}

func (m *Metrics) SetSoakHeartbeatSource(source func() soakHeartbeatSnapshot) {
	if m == nil || source == nil {
		return
	}
	m.soakHeartbeatSource.Store(&source)
}

func (m *Metrics) soakHeartbeatSnapshot() soakHeartbeatSnapshot {
	if m != nil {
		if source := m.soakHeartbeatSource.Load(); source != nil {
			return (*source)()
		}
	}
	return soakHeartbeatSnapshot{}
}

func (m *Metrics) soakHeartbeatDegraded() float64 {
	if m.soakHeartbeatSnapshot().Degraded {
		return 1
	}
	return 0
}

func (m *Metrics) soakHeartbeatSuccessTimestamp() float64 {
	success := m.soakHeartbeatSnapshot().LastSuccess
	if success.IsZero() {
		return 0
	}
	return float64(success.Unix())
}

func (m *Metrics) SetMongoProbeSource(source func() soakMongoProbeSnapshot) {
	if m == nil || source == nil {
		return
	}
	m.mongoProbeSource.Store(&source)
}

func (m *Metrics) mongoProbeSnapshot() soakMongoProbeSnapshot {
	if m != nil {
		if source := m.mongoProbeSource.Load(); source != nil {
			return (*source)()
		}
	}
	return soakMongoProbeSnapshot{}
}

func (m *Metrics) mongoUp() float64 {
	if m.mongoProbeSnapshot().Up {
		return 1
	}
	return 0
}

func (m *Metrics) mongoProbeTimestamp() float64 {
	completedAt := m.mongoProbeSnapshot().CompletedAt
	if completedAt.IsZero() {
		return 0
	}
	return float64(completedAt.Unix())
}
