package main

import (
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
			"cassandra_soak", "message_send", "good",
		),
	))
	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.FailureInflight.WithLabelValues("cassandra_soak", "message_send"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureObservations.WithLabelValues(
			"cassandra_soak", "message_send", "cassandra_history", "good",
		),
	))
}

func TestNewMetrics_RegistersFailureAndProcessFamilies(t *testing.T) {
	metrics := NewMetrics()
	metrics.FailureInvalidations.WithLabelValues("wal").Inc()
	metrics.FailureJournalBytes.Set(123)
	metrics.NATSConnected.WithLabelValues("soak").Set(1)
	metrics.NATSConnectionEvents.WithLabelValues("soak", "reconnected").Inc()

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
		"go_goroutines",
		"process_resident_memory_bytes",
	} {
		assert.True(t, names[name], name)
	}
}
