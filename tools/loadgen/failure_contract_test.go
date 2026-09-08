package main

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureOperationPersistenceContract_PreservesVersionTwoWireShape(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	operation := failureOperation{
		SchemaVersion: 2, ID: "operation-1", CorrelationID: "correlation-1", RunID: "run-1",
		Scenario: soakFailureScenario, Lane: soakFailureLaneMessageSend,
		OperationType: failureOperationMessageCreate, StartedAt: now,
		VerifyAfter: now.Add(time.Second), Deadline: now.Add(time.Minute),
		Targets:    map[string]string{"messageId": "message-1"},
		Effects:    messageCreateExpectedEffectsForObservers(true, false, 1, "sha256"),
		Expected:   []failureObserver{failureObserverAdmission, failureObserverHistory, failureObserverRecipient},
		Attributes: map[string]string{"phase": "measured"},
		Observations: map[failureObserver]failureObservation{
			failureObserverAdmission: failureObservationGood,
		},
		ObservationReasons: map[failureObserver]failureReason{
			failureObserverAdmission: failureReasonAdmissionRejected,
		},
		FinalResult: failureResultBad, FinalReason: failureReasonHistoryContentMismatch,
		EvidenceRefs: []string{"recipient:operation-1"}, LifecycleState: failureOperationActive,
	}

	encoded, err := json.Marshal(operation)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))

	assert.ElementsMatch(t, []string{
		"schemaVersion", "operationId", "correlationId", "runId", "scenario", "lane",
		"operationType", "startedAt", "verifyAfter", "deadline", "targets",
		"expectedEffects", "expected", "attributes", "observations",
		"observationReasons", "finalResult", "finalReason", "evidenceRefs", "lifecycleState",
	}, mapKeys(fields))
	assert.JSONEq(t, `"operation-1"`, string(fields["operationId"]))
	assert.NotContains(t, fields, "id")
	assert.NotContains(t, string(encoded), "nextVerifyAt")
	assert.NotContains(t, string(encoded), "claimed")
	assert.NotContains(t, string(encoded), "heapIndex")
}

func TestFailureOperationPersistenceContract_PreservesOmissionAndLegacyID(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	minimal := failureOperation{
		SchemaVersion: 2, ID: "operation-1", Scenario: soakFailureScenario,
		Lane: soakFailureLaneMessageSend, StartedAt: now, VerifyAfter: now, Deadline: now,
	}

	encoded, err := json.Marshal(minimal)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	assert.ElementsMatch(t, []string{
		"schemaVersion", "operationId", "scenario", "lane", "startedAt", "verifyAfter", "deadline",
	}, mapKeys(fields))

	legacy := failureOperation{
		ID: "legacy-1", Scenario: soakFailureScenario, Lane: soakFailureLaneMessageSend,
		StartedAt: now, VerifyAfter: now, Deadline: now,
	}
	encoded, err = json.Marshal(legacy)
	require.NoError(t, err)
	fields = nil
	require.NoError(t, json.Unmarshal(encoded, &fields))
	assert.ElementsMatch(t, []string{
		"id", "operationId", "scenario", "lane", "startedAt", "verifyAfter", "deadline",
	}, mapKeys(fields))
	assert.Equal(t, json.RawMessage(`"legacy-1"`), fields["id"])
	assert.Equal(t, json.RawMessage(`""`), fields["operationId"])
	assert.NotContains(t, fields, "schemaVersion")

	var decoded failureOperation
	require.NoError(t, json.Unmarshal([]byte(`{
		"id":"legacy-2","scenario":"message_soak","lane":"message_send",
		"startedAt":"2026-08-12T01:02:03Z","verifyAfter":"2026-08-12T01:02:03Z",
		"deadline":"2026-08-12T01:02:03Z"
	}`), &decoded))
	assert.Equal(t, "legacy-2", decoded.ID)
}

func TestFailurePersistenceContract_PreservesEventObserverAndRecipientShapes(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	event := failureLedgerEvent{
		SchemaVersion: failureWALSchemaVersion, Type: failureLedgerEventFinalized,
		OperationID: "operation-1", Observer: failureObserverHistory,
		Observation: failureObservationMissingAfterDeadline, Reason: failureReasonHistoryMissing,
		Result:  failureResultMissingAfterDeadline,
		Results: map[failureResult]uint64{failureResultGood: 1},
		ObservationCounts: map[failureObserver]map[failureObservation]uint64{
			failureObserverHistory: {failureObservationGood: 1},
		},
		NotSent: []string{string(failureReasonPublishLocalError)}, InvalidReason: invalidReasonWAL, At: now,
	}
	assertJSONFieldNames(t, event, []string{
		"schemaVersion", "type", "operationId", "observer", "observation", "reason", "result",
		"results", "observationCounts", "notSent", "invalidReason", "at",
	})

	contract := newFailureObserverContract(true, true)
	assertJSONFieldNames(t, contract, []string{
		"schemaVersion", "scenario", "observers", "lanes", "recipientObserverEnabled",
	})
	assert.Equal(t, failureObserverContractSchemaVersion, contract.SchemaVersion)
	assert.Equal(t, []failureObserver{
		failureObserverAdmission, failureObserverHistory, failureObserverRecipient, failureObserverRoomState,
		failureObserverSearchIndex,
	}, contract.Observers)

	record := failureRecipientEvidenceRecord{
		Kind: "missing", OperationID: "operation-1", Recipient: "account-1",
	}
	assertJSONFieldNames(t, record, []string{"operationId", "recipient"})
	record.Recipient = ""
	assertJSONFieldNames(t, record, []string{"operationId"})
}

func TestNewMetrics_FailureFamiliesPreservePrometheusContract(t *testing.T) {
	metrics := NewMetrics()
	touchFailureMetricFamilies(metrics)

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	var got []string
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "loadgen_failure_") {
			got = append(got, normalizedMetricFamily(family))
		}
	}
	sort.Strings(got)

	const want = `loadgen_failure_abandoned_journals|GAUGE|Retained failure journals for this run ID that belong to an earlier ledger epoch and are never replayed.|labels=|buckets=
loadgen_failure_dropped_total|COUNTER|Recovered operations discarded at startup because the journal exceeded capacity.|labels=|buckets=
loadgen_failure_evidence_flush_duration_seconds|HISTOGRAM|Duration of batched recipient sidecar durability barriers.|labels=claim,result|buckets=0.0001,0.0002,0.0005,0.001,0.002,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1
loadgen_failure_evidence_records_total|COUNTER|Recipient sidecar evidence records durably flushed by bounded kind.|labels=kind|buckets=
loadgen_failure_inflight|GAUGE|Operations awaiting one or more fault-observation results.|labels=lane,scenario|buckets=
loadgen_failure_invalidations_total|COUNTER|Conditions that invalidate fault-test evidence, by bounded reason.|labels=reason|buckets=
loadgen_failure_journal_bytes|GAUGE|Current persistent failure-ledger WAL size in bytes.|labels=|buckets=
loadgen_failure_not_sent_total|COUNTER|Proven local pre-publish failures by bounded reason.|labels=lane,reason,scenario|buckets=
loadgen_failure_observation_reasons_total|COUNTER|Failure observations by bounded result reason.|labels=lane,observer,reason,result,scenario|buckets=
loadgen_failure_observations_total|COUNTER|Fault-observation results by bounded scenario, lane, observer, and result.|labels=lane,observer,result,scenario|buckets=
loadgen_failure_observer_configured|GAUGE|Whether a bounded failure observer is configured for new operations.|labels=observer|buckets=
loadgen_failure_observer_eligible_total|COUNTER|Operations eligible for a configured observer result.|labels=lane,observer,scenario|buckets=
loadgen_failure_observer_events_total|COUNTER|Normalized failure observer events.|labels=observer,result|buckets=
loadgen_failure_observer_queue_depth|GAUGE|Queued failure observer events.|labels=observer|buckets=
loadgen_failure_observer_up|GAUGE|Whether a required failure observer is healthy.|labels=observer|buckets=
loadgen_failure_operations_total|COUNTER|Completed fault-observation operations by bounded scenario, lane, and result.|labels=lane,result,scenario|buckets=
loadgen_failure_recipient_expectations|GAUGE|Recipient expectations retained in memory; healthy runs track loadgen_failure_inflight.|labels=|buckets=
loadgen_failure_reconcile_claims_total|COUNTER|Reconcile claims by what the claim achieved; retried is the poll cost the capacity rule cannot model, idle is the lane's remaining slack.|labels=outcome|buckets=
loadgen_failure_reconcile_lag_seconds|HISTOGRAM|Seconds past its scheduled probe an operation was when the reconciler claimed it; the capacity floor cannot model fault-time retries, so lag is what shows the lane falling behind, and lag near SOAK_RECONCILE_DEADLINE means operations expire unverified for want of a probe.|labels=|buckets=0.01,0.1,0.5,1,2,5,10,30,60,120,300,600,1200,1800,3600,5400
loadgen_failure_recovered_operations_total|COUNTER|Unresolved operations recovered from the persistent failure ledger.|labels=|buckets=
loadgen_failure_untracked_total|COUNTER|Operations the ledger could not account for, by bounded reason.|labels=reason|buckets=
loadgen_failure_wal_append_duration_seconds|HISTOGRAM|Caller-observed failure-ledger append duration; intent records include the grouped durability barrier.|labels=|buckets=0.0001,0.0002,0.0005,0.001,0.002,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1
loadgen_failure_wal_appends_total|COUNTER|Failure-ledger append attempts by bounded result (success or error).|labels=result|buckets=
loadgen_failure_wal_flush_batch_size|HISTOGRAM|Number of WAL records committed by each grouped durability barrier.|labels=result|buckets=1,2,4,8,16,32,64,128,256
loadgen_failure_wal_flush_duration_seconds|HISTOGRAM|Duration of grouped failure-ledger fsync barriers by bounded result.|labels=result|buckets=0.0001,0.0002,0.0005,0.001,0.002,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1`
	assert.Equal(t, strings.TrimSpace(want), strings.Join(got, "\n"))
}

func touchFailureMetricFamilies(metrics *Metrics) {
	metrics.FailureOperations.WithLabelValues("scenario", "lane", "result").Inc()
	metrics.FailureObservations.WithLabelValues("scenario", "lane", "observer", "result").Inc()
	metrics.FailureObservationReasons.WithLabelValues("scenario", "lane", "observer", "result", "reason").Inc()
	metrics.FailureInflight.WithLabelValues("scenario", "lane").Set(0)
	metrics.FailureInvalidations.WithLabelValues("reason").Inc()
	metrics.FailureUntracked.WithLabelValues("reason").Inc()
	metrics.FailureNotSent.WithLabelValues("scenario", "lane", "reason").Inc()
	metrics.FailureWALAppendDuration.Observe(0)
	metrics.FailureWALAppends.WithLabelValues("result").Inc()
	metrics.FailureWALFlushDuration.WithLabelValues("result").Observe(0)
	metrics.FailureWALFlushBatchSize.WithLabelValues("result").Observe(0)
	metrics.FailureEvidenceFlushDuration.WithLabelValues("claim", "result").Observe(0)
	metrics.FailureEvidenceRecords.WithLabelValues("kind").Inc()
	metrics.FailureObserverUp.WithLabelValues("observer").Set(0)
	metrics.FailureObserverConfigured.WithLabelValues("observer").Set(0)
	metrics.FailureObserverEligible.WithLabelValues("scenario", "lane", "observer").Inc()
	metrics.FailureObserverEvents.WithLabelValues("observer", "result").Inc()
	metrics.FailureObserverQueueDepth.WithLabelValues("observer").Set(0)
}

func assertJSONFieldNames(t *testing.T, value any, want []string) {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	assert.ElementsMatch(t, want, mapKeys(fields))
}

func mapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
