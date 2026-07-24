package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSoakCollector_CountsOutcomesRetriesAndDistinctRPCs(t *testing.T) {
	collector := NewSoakCollector(nil, time.Unix(100, 0), 10*time.Second, time.Minute)
	at := time.Unix(111, 0)
	for _, sample := range []soakOperationSample{
		{Action: soakRPCReact, Outcome: soakOutcomeSucceeded, At: at, Latency: 10 * time.Millisecond, Retries: 1},
		{Action: soakRPCEdit, Outcome: soakOutcomeFailed, At: at, Latency: 20 * time.Millisecond, ErrorClass: soakErrorInternal},
		{Action: soakRPCDelete, Outcome: soakOutcomeSkipped, At: at},
		{Action: soakRPCPin, Outcome: soakOutcomeSucceeded, At: at, Latency: 30 * time.Millisecond},
		{Action: soakRPCUnpin, Outcome: soakOutcomeSucceeded, At: at, Latency: 40 * time.Millisecond},
		{Action: soakRPCPinnedList, Outcome: soakOutcomeSucceeded, At: at, Latency: 50 * time.Millisecond},
	} {
		require.NoError(t, collector.Record(&sample))
	}

	snapshot := collector.Snapshot(at.Add(time.Second))
	assert.Equal(t, uint64(6), snapshot.Total.Attempted)
	assert.Equal(t, uint64(4), snapshot.Total.Succeeded)
	assert.Equal(t, uint64(1), snapshot.Total.Failed)
	assert.Equal(t, uint64(1), snapshot.Total.Skipped)
	assert.Equal(t, uint64(1), snapshot.Total.Retries)
	for _, action := range []soakRPCAction{
		soakRPCReact, soakRPCEdit, soakRPCDelete, soakRPCPin,
		soakRPCUnpin, soakRPCPinnedList,
	} {
		assert.Equal(t, uint64(1), snapshot.Actions[action].Attempted)
	}
}

func TestSoakCollector_AchievedRateExcludesWarmup(t *testing.T) {
	start := time.Unix(100, 0)
	collector := NewSoakCollector(nil, start, 10*time.Second, time.Minute)
	for index := range 10 {
		require.NoError(t, collector.Record(&soakOperationSample{
			Action: soakRPCLoadHistory, Outcome: soakOutcomeSucceeded,
			At: start.Add(time.Duration(index) * time.Second),
		}))
	}
	for index := range 100 {
		require.NoError(t, collector.Record(&soakOperationSample{
			Action: soakRPCLoadHistory, Outcome: soakOutcomeSucceeded,
			At: start.Add(10*time.Second + time.Duration(index)*100*time.Millisecond),
		}))
	}

	snapshot := collector.Snapshot(start.Add(20 * time.Second))
	assert.Equal(t, uint64(100), snapshot.Total.Attempted)
	assert.InDelta(t, 10, snapshot.Total.AchievedRate, 0.001)
	assert.Equal(t, uint64(10), snapshot.WarmupAttempted)
}

func TestSoakCollector_FixedHistogramPercentiles(t *testing.T) {
	start := time.Unix(100, 0)
	collector := NewSoakCollector(nil, start, 0, time.Minute)
	for _, latency := range []time.Duration{
		time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond,
		10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
		100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
		500 * time.Millisecond,
	} {
		require.NoError(t, collector.Record(&soakOperationSample{
			Action: soakRPCGetMessage, Outcome: soakOutcomeSucceeded,
			At: start.Add(time.Second), Latency: latency,
		}))
	}

	stats := collector.Snapshot(start.Add(10 * time.Second)).Actions[soakRPCGetMessage]
	assert.Equal(t, 25*time.Millisecond, stats.Latency.P50)
	assert.Equal(t, 500*time.Millisecond, stats.Latency.P95)
	assert.Equal(t, 500*time.Millisecond, stats.Latency.P99)
	assert.Equal(t, 500*time.Millisecond, stats.Latency.Max)
}

func TestSoakCollector_EarlyLateP99Drift(t *testing.T) {
	start := time.Unix(100, 0)
	collector := NewSoakCollector(nil, start, 0, 100*time.Second)
	for range 100 {
		require.NoError(t, collector.Record(&soakOperationSample{
			Action: soakRPCLoadHistory, Outcome: soakOutcomeSucceeded,
			At: start.Add(10 * time.Second), Latency: 10 * time.Millisecond,
		}))
		require.NoError(t, collector.Record(&soakOperationSample{
			Action: soakRPCLoadHistory, Outcome: soakOutcomeSucceeded,
			At: start.Add(90 * time.Second), Latency: 100 * time.Millisecond,
		}))
	}

	stats := collector.Snapshot(start.Add(100 * time.Second)).Actions[soakRPCLoadHistory]
	assert.Equal(t, 10*time.Millisecond, stats.EarlyP99)
	assert.Equal(t, 100*time.Millisecond, stats.LateP99)
	assert.Equal(t, 90*time.Millisecond, stats.P99Drift)
}

func TestSoakCollector_TracksErrorsVerificationAndMissingTargets(t *testing.T) {
	start := time.Unix(100, 0)
	collector := NewSoakCollector(nil, start, 0, time.Minute)
	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCEdit, Outcome: soakOutcomeSkipped, At: start,
		ErrorClass: soakErrorMutationTargetMissing, TargetMissing: true,
	}))
	require.NoError(t, collector.RecordVerification(&soakVerifyResult{
		Class: soakVerifyMismatch, Action: soakRPCGetMessage,
		RoomID: "must-not-be-a-label", MessageID: "must-not-be-a-label",
	}))

	snapshot := collector.Snapshot(start.Add(time.Second))
	assert.Equal(t, uint64(1), snapshot.MutationTargetMissing)
	assert.Equal(t, uint64(1), snapshot.Errors[soakRPCEdit][soakErrorMutationTargetMissing])
	assert.Equal(t, uint64(1), snapshot.Verifications[soakRPCGetMessage][soakVerifyMismatch])
}

func TestSoakCollector_RejectsUnboundedLabels(t *testing.T) {
	collector := NewSoakCollector(nil, time.Now(), 0, time.Minute)
	err := collector.Record(&soakOperationSample{
		Action:     soakRPCAction("room-123"),
		Outcome:    soakOutcomeFailed,
		ErrorClass: soakErrorClass("raw database error"),
		At:         time.Now(),
	})
	require.Error(t, err)
	assert.Empty(t, collector.Snapshot(time.Now()).Actions)
}

func TestSoakCollector_MemoryShapeDoesNotGrowWithEventCount(t *testing.T) {
	start := time.Unix(100, 0)
	collector := NewSoakCollector(nil, start, 0, time.Hour)
	before := collector.Shape()
	for index := range 1000000 {
		require.NoError(t, collector.Record(&soakOperationSample{
			Action: soakRPCReact, Outcome: soakOutcomeSucceeded,
			At:      start.Add(time.Duration(index) * time.Millisecond),
			Latency: time.Duration(index%1000) * time.Millisecond,
		}))
	}
	after := collector.Shape()

	assert.Equal(t, before.BucketsPerAction, after.BucketsPerAction)
	assert.Equal(t, 1, after.ActionCount)
	assert.LessOrEqual(t, after.ErrorCells, len(soakAllErrorClasses))
}

func TestSoakPrometheusMetrics_StableNamesAndBoundedLabels(t *testing.T) {
	metrics := NewMetrics()
	collector := NewSoakCollector(metrics, time.Unix(100, 0), 0, time.Minute)
	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCReact, Outcome: soakOutcomeFailed,
		ErrorClass: soakErrorTimeout, Retries: 2,
		At: time.Unix(101, 0), Latency: 50 * time.Millisecond,
	}))
	require.NoError(t, collector.RecordVerification(&soakVerifyResult{
		Class: soakVerifyMissing, Action: soakRPCGetMessage,
		RoomID: "room-secret", MessageID: "message-secret",
	}))

	gathered, err := metrics.Registry.Gather()
	require.NoError(t, err)
	text := metricFamiliesText(gathered)
	for _, name := range []string{
		"loadgen_soak_operations_total",
		"loadgen_soak_retries_total",
		"loadgen_soak_errors_total",
		"loadgen_soak_rpc_latency_seconds",
		"loadgen_soak_verifications_total",
	} {
		assert.Contains(t, text, name)
	}
	assert.NotContains(t, text, "room-secret")
	assert.NotContains(t, text, "message-secret")

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakOperations.WithLabelValues("reaction", "failed", "measured"),
	))
	assert.Equal(t, float64(2), testutil.ToFloat64(
		metrics.SoakRetries.WithLabelValues("reaction"),
	))
}

func metricFamiliesText(families any) string {
	return fmt.Sprintf("%v", families)
}

func TestSoakMetrics_DoNotExposeRunOrEntityLabelNames(t *testing.T) {
	metrics := NewMetrics()
	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	text := strings.ToLower(metricFamiliesText(families))
	for _, forbidden := range []string{
		"account", "user_id", "room_id", "message_id", "run_id", "error_text",
	} {
		assert.NotContains(t, text, forbidden)
	}
}
