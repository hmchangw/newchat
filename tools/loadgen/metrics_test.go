package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics_BotRoomFamiliesRegistered(t *testing.T) {
	m := NewMetrics()
	m.BotRoomPublished.WithLabelValues("botroom-small", "measured", "100").Inc()
	m.BotRoomPublishErrors.WithLabelValues("publish").Inc()
	m.BotRoomE2ELatency.WithLabelValues("100").Observe(0.01)
	m.BotRoomReadLatency.WithLabelValues("100").Observe(0.01)

	mfs, err := m.Registry.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	require.True(t, names["loadgen_botroom_published_total"])
	require.True(t, names["loadgen_botroom_publish_errors_total"])
	require.True(t, names["loadgen_botroom_e2e_latency_seconds"])
	require.True(t, names["loadgen_botroom_read_latency_seconds"])
}

func TestFailureLedgerPromRecorder_RecordsBoundedOutcomes(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	metrics := NewMetrics()
	recorder := newFailureLedgerPromRecorder(metrics)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Recorder: recorder, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))
	_, err = ledger.Observe(
		"message-1", failureObserverAdmission, failureObservationGood, now,
	)
	require.NoError(t, err)
	_, err = ledger.Observe(
		"message-1", failureObserverHistory, failureObservationGood, now,
	)
	require.NoError(t, err)

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureOperations.WithLabelValues(
			"message_soak", "message_send", "good",
		),
	))
	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.FailureInflight.WithLabelValues("message_soak", "message_send"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureObservations.WithLabelValues(
			"message_soak", "message_send", "cassandra_history", "good",
		),
	))
}

func TestFailureLedgerPromRecorder_RecordsLifecycleAndGuardsNil(t *testing.T) {
	metrics := NewMetrics()
	recorder := newFailureLedgerPromRecorder(metrics)
	operation := testFailureOperation("message-1", time.Now().UTC())

	recorder.Recovered(3)
	recorder.Invalidated("capacity")
	recorder.JournalSize(512)
	recorder.ObservationRecorded(
		operation, failureObserverAdmission, failureObservationBad,
	)
	recorder.OperationStarted(operation)
	recorder.OperationFinalized(operation, failureResultUnverified)

	assert.Equal(t, float64(3), testutil.ToFloat64(metrics.FailureRecovered))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues("capacity"),
	))
	assert.Equal(t, float64(512), testutil.ToFloat64(metrics.FailureJournalBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureObservations.WithLabelValues(
			"message_soak", "message_send", "admission", "bad",
		),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureOperations.WithLabelValues(
			"message_soak", "message_send", "unverified",
		),
	))

	recorder.OperationStarted(nil)
	recorder.ObservationRecorded(nil, failureObserverAdmission, failureObservationGood)
	recorder.OperationFinalized(nil, failureResultGood)
	recorder.Recovered(0)
	var nilRecorder *failureLedgerPromRecorder
	nilRecorder.OperationStarted(operation)
	nilRecorder.ObservationRecorded(
		operation, failureObserverAdmission, failureObservationGood,
	)
	nilRecorder.OperationFinalized(operation, failureResultGood)
	nilRecorder.Recovered(1)
	nilRecorder.Invalidated("wal")
	nilRecorder.JournalSize(1)

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureOperations.WithLabelValues(
			"message_soak", "message_send", "unverified",
		),
	))
}

func TestNewMetrics_RegistersFailureAndProcessFamilies(t *testing.T) {
	metrics := NewMetrics()
	metrics.FailureInvalidations.WithLabelValues("wal").Inc()
	metrics.FailureJournalBytes.Set(123)
	metrics.NATSConnected.WithLabelValues("soak").Set(1)
	metrics.NATSConnectionEvents.WithLabelValues("soak", "reconnected").Inc()
	metrics.NATSCurrentOutage.WithLabelValues("soak").Set(0)
	metrics.FailureObserverUp.WithLabelValues("recipient_broadcast").Set(1)
	metrics.FailureObserverEvents.WithLabelValues("recipient_broadcast", "good").Inc()
	metrics.FailureObserverQueueDepth.WithLabelValues("recipient_broadcast").Set(0)
	metrics.FailureWALAppendDuration.Observe(0.002)
	metrics.FailureWALAppends.WithLabelValues("success").Inc()
	metrics.FailureWALFlushDuration.WithLabelValues("success").Observe(0.004)
	metrics.FailureWALFlushBatchSize.WithLabelValues("success").Observe(6)
	metrics.FailureEvidenceFlushDuration.WithLabelValues("positive", "success").Observe(0.003)
	metrics.FailureEvidenceRecords.WithLabelValues("duplicate").Add(2)
	metrics.ConsumerSampleErrors.WithLabelValues("MESSAGES-CANONICAL-site-local", "broadcast-worker", "lookup").Inc()
	metrics.ConsumerAckFloorStall.WithLabelValues("MESSAGES-CANONICAL-site-local", "broadcast-worker").Set(3)
	metrics.RunInfo.WithLabelValues("staging", soakFailureScenario, "cassandra-soak-v1").Set(1)

	mfs, err := metrics.Registry.Gather()
	require.NoError(t, err)
	names := make(map[string]bool, len(mfs))
	for _, family := range mfs {
		names[family.GetName()] = true
	}
	for _, name := range []string{
		"loadgen_failure_invalidations_total",
		"loadgen_failure_journal_bytes",
		"loadgen_nats_connected",
		"loadgen_nats_connection_events_total",
		"loadgen_nats_current_outage_seconds",
		"loadgen_failure_observer_up",
		"loadgen_failure_observer_events_total",
		"loadgen_failure_observer_queue_depth",
		"loadgen_failure_wal_append_duration_seconds",
		"loadgen_failure_wal_appends_total",
		"loadgen_failure_wal_flush_duration_seconds",
		"loadgen_failure_wal_flush_batch_size",
		"loadgen_failure_evidence_flush_duration_seconds",
		"loadgen_failure_evidence_records_total",
		"loadgen_consumer_sample_errors_total",
		"loadgen_consumer_ack_floor_stall_seconds",
		"loadgen_run_info",
		"go_goroutines",
		"process_resident_memory_bytes",
	} {
		assert.True(t, names[name], name)
	}
}

func TestFailureMetrics_SetSoakRunInfo(t *testing.T) {
	metrics := NewMetrics()

	setSoakRunInfo(metrics, "staging")

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.RunInfo.WithLabelValues(
			"staging",
			soakFailureScenario,
			soakFailureTrafficProfile,
		),
	))
}

func TestMetrics_FailureObservationConfigurationAndPacingFamilies(t *testing.T) {
	metrics := NewMetrics()
	metrics.FailureObserverConfigured.WithLabelValues(string(failureObserverAdmission)).Set(1)
	metrics.FailureObserverConfigured.WithLabelValues(string(failureObserverRecipient)).Set(0)
	metrics.FailureObserverEligible.WithLabelValues(
		soakFailureScenario,
		soakFailureLaneMessageSend,
		string(failureObserverHistory),
	).Inc()
	metrics.SoakConfiguredRate.WithLabelValues("send").Set(100)
	metrics.SoakIntended.WithLabelValues("send").Add(4)
	metrics.SoakDispatched.WithLabelValues("send").Inc()
	metrics.SoakSchedulerUnderrun.WithLabelValues("send").Inc()
	metrics.SoakLaneSaturation.WithLabelValues("send").Inc()
	metrics.SoakGlobalSaturation.WithLabelValues("send").Inc()

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	for _, name := range []string{
		"loadgen_failure_observer_configured",
		"loadgen_failure_observer_eligible_total",
		"loadgen_soak_configured_rate",
		"loadgen_soak_intended_total",
		"loadgen_soak_dispatched_total",
		"loadgen_soak_scheduler_underrun_total",
		"loadgen_soak_lane_saturation_total",
		"loadgen_soak_global_saturation_total",
	} {
		assert.Contains(t, names, name)
	}
}

func TestSoakPacing_RecordsOneExclusiveOutcomePerIntendedEvent(t *testing.T) {
	metrics := NewMetrics()
	recorder := newSoakPacingMetrics(metrics)
	recorder.Configure("send", 100)
	recorder.Record("send", soakPacingDispatched, 2)
	recorder.Record("send", soakPacingSchedulerUnderrun, 3)
	recorder.Record("send", soakPacingLaneSaturation, 5)
	recorder.Record("send", soakPacingGlobalSaturation, 7)
	recorder.Record("send", soakPacingOutcome("unknown"), 11)

	intended := testutil.ToFloat64(metrics.SoakIntended.WithLabelValues("send"))
	dispatched := testutil.ToFloat64(metrics.SoakDispatched.WithLabelValues("send"))
	underrun := testutil.ToFloat64(metrics.SoakSchedulerUnderrun.WithLabelValues("send"))
	laneSaturation := testutil.ToFloat64(metrics.SoakLaneSaturation.WithLabelValues("send"))
	globalSaturation := testutil.ToFloat64(metrics.SoakGlobalSaturation.WithLabelValues("send"))
	assert.Equal(t, float64(17), intended)
	assert.Equal(t, intended, dispatched+underrun+laneSaturation+globalSaturation)
	assert.Equal(t, float64(100), testutil.ToFloat64(metrics.SoakConfiguredRate.WithLabelValues("send")))
}

func TestSoakPacing_BlockingActionDoesNotReduceOfferedRate(t *testing.T) {
	start := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	pacer := newRatePacer(100, start)
	metrics := NewMetrics()
	recorder := newSoakPacingMetrics(metrics)
	ticks := make(chan time.Time, 4)
	actionStarted := make(chan struct{})
	releaseAction := make(chan struct{})
	saturated := make(chan struct{}, 3)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var startedOnce sync.Once
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			cancel()
			close(releaseAction)
		})
	}
	t.Cleanup(stop)

	go func() {
		defer close(done)
		pacedDispatchRateWithTicks(
			ctx,
			pacer,
			ticks,
			1,
			func(count int) {
				recorder.Record("send", soakPacingSchedulerUnderrun, count)
			},
			func() {
				recorder.Record("send", soakPacingLaneSaturation, 1)
				saturated <- struct{}{}
			},
			func(context.Context) {
				recorder.Record("send", soakPacingDispatched, 1)
				startedOnce.Do(func() { close(actionStarted) })
				<-releaseAction
			},
		)
	}()

	ticks <- start.Add(pacer.interval)
	select {
	case <-actionStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "first paced action did not start")
	}
	for index := 2; index <= 4; index++ {
		ticks <- start.Add(time.Duration(index) * pacer.interval)
		select {
		case <-saturated:
		case <-time.After(time.Second):
			require.FailNow(t, "blocking action reduced offered pacing", "tick=%d", index)
		}
	}
	stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "paced dispatch did not terminate")
	}

	intended := testutil.ToFloat64(metrics.SoakIntended.WithLabelValues("send"))
	dispatched := testutil.ToFloat64(metrics.SoakDispatched.WithLabelValues("send"))
	underrun := testutil.ToFloat64(metrics.SoakSchedulerUnderrun.WithLabelValues("send"))
	laneSaturation := testutil.ToFloat64(metrics.SoakLaneSaturation.WithLabelValues("send"))
	assert.Equal(t, float64(4), intended)
	assert.Equal(t, float64(1), dispatched)
	assert.Equal(t, float64(3), laneSaturation)
	assert.Equal(t, intended, dispatched+underrun+laneSaturation)
}

func TestNewMetrics_RoomLaneFamiliesUseBoundedLabels(t *testing.T) {
	metrics := NewMetrics()
	metrics.SoakRoomCandidates.WithLabelValues("available").Set(1)
	metrics.SoakRoomQuarantineProbes.WithLabelValues("resolved").Inc()
	metrics.SoakRoomPoolExhausted.WithLabelValues("quarantine_full").Inc()
	metrics.SoakRoomStateSources.WithLabelValues("room_service", "matched").Inc()
	metrics.SoakRoomPoolDegraded.Set(0)
	metrics.SoakRoomCreateBudgetRemaining.Set(10)
	metrics.SoakEncryptionPreflight.Set(1)
	metrics.FailureAbandonedJournals.Set(0)

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)

	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				assert.NotContains(t,
					[]string{"room_id", "account", "message_id", "run_id", "operation_id"},
					label.GetName(),
					"family %s must not carry high-cardinality labels", family.GetName())
			}
		}
	}
	for _, name := range []string{
		"loadgen_soak_room_candidates", "loadgen_soak_room_quarantine_probes_total",
		"loadgen_soak_room_pool_exhausted_total", "loadgen_soak_room_pool_degraded",
		"loadgen_soak_room_create_budget_remaining", "loadgen_soak_room_state_source_total",
		"loadgen_soak_encryption_preflight",
		"loadgen_failure_abandoned_journals",
	} {
		assert.Contains(t, names, name)
	}
}
