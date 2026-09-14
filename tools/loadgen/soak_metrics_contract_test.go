package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The soak dashboards and alerts consume more than metric names: changing a
// family type, label name, help string, or histogram boundary can silently
// change the meaning of an otherwise healthy-looking run. Keep the normalized
// contract readable so package moves can prove they preserved the wire surface.
func TestNewMetrics_SoakFamiliesPreservePrometheusContract(t *testing.T) {
	metrics := NewMetrics()
	touchSoakMetricFamilies(metrics)

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)

	var got []string
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "loadgen_soak_") {
			got = append(got, normalizedMetricFamily(family))
		}
	}
	sort.Strings(got)

	const want = `loadgen_soak_configured_rate|GAUGE|Configured open-loop target rate by bounded soak lane.|labels=lane|buckets=
loadgen_soak_dispatched_total|COUNTER|Pacing events dispatched by bounded soak lane.|labels=lane|buckets=
loadgen_soak_encryption_preflight|GAUGE|1 once this run proved a message reached Cassandra encrypted and wrapped the room DEK. 0 means not proven — skipped by configuration, still in flight, failed, or a loadgen mode that never runs the check. Alert on a sustained 0 during soak, not on any 0.|labels=|buckets=
loadgen_soak_error_reasons_total|COUNTER|Cassandra soak failures by bounded action, error class, service-supplied reason, and run phase.|labels=action,class,phase,reason|buckets=
loadgen_soak_errors_total|COUNTER|Cassandra soak failures by bounded action, error class, and run phase.|labels=action,class,phase|buckets=
loadgen_soak_global_saturation_total|COUNTER|Pacing events skipped because the global in-flight budget was full.|labels=lane|buckets=
loadgen_soak_heartbeat_attempts_total|COUNTER|Soak manifest heartbeat attempts by bounded outcome (success, error, not_active).|labels=outcome|buckets=
loadgen_soak_heartbeat_degraded|GAUGE|Whether the latest soak heartbeat attempt failed while the run remains active (1 degraded, 0 otherwise).|labels=|buckets=
loadgen_soak_heartbeat_success_timestamp_seconds|GAUGE|Unix timestamp of the latest successful soak manifest heartbeat; zero until the first success.|labels=|buckets=
loadgen_soak_intended_total|COUNTER|Pacing events due by bounded soak lane.|labels=lane|buckets=
loadgen_soak_lane_attempts_total|COUNTER|Lane slots by outcome (sent, no_target, refused). loadgen_soak_dispatched_total counts scheduler slots, which a lane with no usable target still consumes; only this distinguishes offered load from a lane idling on an exhausted pool or one whose mutations the ledger is refusing.|labels=lane,outcome|buckets=
loadgen_soak_lane_saturation_total|COUNTER|Pacing events skipped because the lane in-flight budget was full.|labels=lane|buckets=
loadgen_soak_mutation_target_missing_total|COUNTER|Cassandra soak mutation targets still missing after the dedicated retry policy.|labels=|buckets=
loadgen_soak_operations_total|COUNTER|Cassandra soak operations by bounded action, outcome, and phase.|labels=action,outcome,phase|buckets=
loadgen_soak_presence_checks_total|COUNTER|Presence query comparisons by bounded result.|labels=result|buckets=
loadgen_soak_presence_connections|GAUGE|Virtual presence connections by the status the lane last asked for.|labels=status|buckets=
loadgen_soak_presence_signals_total|COUNTER|Presence signals published by bounded kind. A publish is unacknowledged and is never evidence the server saw it.|labels=signal|buckets=
loadgen_soak_reply_bytes|HISTOGRAM|Cassandra soak reply size in bytes for successful paged reads, by bounded action.|labels=action|buckets=512,1024,2048,4096,8192,16384,32768,65536,98304,131072
loadgen_soak_retries_total|COUNTER|Cassandra soak retries by bounded action and run phase.|labels=action,phase|buckets=
loadgen_soak_room_candidates|GAUGE|Member candidates by bounded lifecycle state.|labels=state|buckets=
loadgen_soak_room_create_budget_remaining|GAUGE|Rooms the create lane may still add before it stops for this run.|labels=|buckets=
loadgen_soak_room_pool_degraded|GAUGE|Whether the member candidate pool is currently degraded. Reversible: it clears when the pool recovers.|labels=|buckets=
loadgen_soak_room_pool_exhausted_total|COUNTER|Room and member mutations skipped for lack of a usable target, by bounded reason.|labels=reason|buckets=
loadgen_soak_room_quarantine_probes_total|COUNTER|Quarantined member candidate re-probes by bounded result.|labels=result|buckets=
loadgen_soak_room_state_source_total|COUNTER|Room-state observer source outcomes by bounded source and result.|labels=result,source|buckets=
loadgen_soak_rows|HISTOGRAM|Rows returned per Cassandra soak paged read by bounded action.|labels=action|buckets=1,2,5,10,20,40,100,200,400
loadgen_soak_rpc_latency_seconds|HISTOGRAM|Cassandra soak per-RPC end-to-end latency by bounded action.|labels=action|buckets=0.001,0.002,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5,5
loadgen_soak_scheduler_underrun_total|COUNTER|Pacing events skipped after scheduler delay by bounded soak lane.|labels=lane|buckets=
loadgen_soak_verifications_total|COUNTER|Cassandra soak read-back results by bounded action, result class, and disagreeing field.|labels=action,class,field|buckets=`
	assert.Equal(t, strings.TrimSpace(want), strings.Join(got, "\n"))
}

func touchSoakMetricFamilies(metrics *Metrics) {
	metrics.SoakOperations.WithLabelValues("action", "outcome", "phase").Inc()
	metrics.SoakRetries.WithLabelValues("action", "phase").Inc()
	metrics.SoakErrors.WithLabelValues("action", "class", "phase").Inc()
	metrics.SoakErrorReasons.WithLabelValues("action", "class", "reason", "phase").Inc()
	metrics.SoakRPCLatency.WithLabelValues("action").Observe(0)
	metrics.SoakReplyBytes.WithLabelValues("action").Observe(0)
	metrics.SoakRows.WithLabelValues("action").Observe(0)
	metrics.SoakVerifications.WithLabelValues("action", "class", "field").Inc()
	metrics.SoakMutationTargetMissing.Inc()
	metrics.SoakConfiguredRate.WithLabelValues("lane").Set(0)
	metrics.SoakIntended.WithLabelValues("lane").Inc()
	metrics.SoakDispatched.WithLabelValues("lane").Inc()
	metrics.SoakSchedulerUnderrun.WithLabelValues("lane").Inc()
	metrics.SoakLaneSaturation.WithLabelValues("lane").Inc()
	metrics.SoakGlobalSaturation.WithLabelValues("lane").Inc()
	metrics.SoakRoomCandidates.WithLabelValues("state").Set(0)
	metrics.SoakRoomQuarantineProbes.WithLabelValues("result").Inc()
	metrics.SoakRoomPoolExhausted.WithLabelValues("reason").Inc()
	metrics.SoakRoomPoolDegraded.Set(0)
	metrics.SoakRoomCreateBudgetRemaining.Set(0)
	metrics.SoakEncryptionPreflight.Set(0)
	metrics.SoakRoomStateSources.WithLabelValues("source", "result").Inc()
	metrics.SoakLaneAttempts.WithLabelValues("lane", "outcome").Inc()
	metrics.SoakPresenceSignals.WithLabelValues("signal").Inc()
	metrics.SoakPresenceChecks.WithLabelValues("result").Inc()
	metrics.SoakPresenceConnections.WithLabelValues("status").Set(0)
	metrics.SoakHeartbeatAttempts.WithLabelValues("outcome").Inc()
}

func normalizedMetricFamily(family *dto.MetricFamily) string {
	labels := []string(nil)
	buckets := []string(nil)
	if len(family.GetMetric()) > 0 {
		for _, label := range family.GetMetric()[0].GetLabel() {
			labels = append(labels, label.GetName())
		}
		sort.Strings(labels)
		for _, bucket := range family.GetMetric()[0].GetHistogram().GetBucket() {
			buckets = append(buckets,
				strconv.FormatFloat(bucket.GetUpperBound(), 'g', -1, 64))
		}
	}
	return fmt.Sprintf("%s|%s|%s|labels=%s|buckets=%s",
		family.GetName(), family.GetType().String(), family.GetHelp(),
		strings.Join(labels, ","), strings.Join(buckets, ","))
}
