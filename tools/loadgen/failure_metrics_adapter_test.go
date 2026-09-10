package main

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestFailureMetricsAdapter_RecordsRecipientRuntimeSignals(t *testing.T) {
	metrics := NewMetrics()
	adapter := newFailureMetricsAdapter(metrics)

	adapter.SetObserverUp(failureObserverRecipient, true)
	adapter.SetObserverQueueDepth(failureObserverRecipient, 3)
	adapter.RecordObserverEvent(failureObserverRecipient, failureObservationGood)
	adapter.RecordUntracked(failureUntrackedReasonObserve)
	adapter.ObserveEvidenceFlush("positive", "success", time.Second)
	adapter.RecordEvidence("duplicate")

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureObserverUp.WithLabelValues(string(failureObserverRecipient))))
	assert.Equal(t, float64(3), testutil.ToFloat64(
		metrics.FailureObserverQueueDepth.WithLabelValues(string(failureObserverRecipient))))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureObserverEvents.WithLabelValues(string(failureObserverRecipient), string(failureObservationGood))))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureUntracked.WithLabelValues(failureUntrackedReasonObserve)))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureEvidenceRecords.WithLabelValues("duplicate")))
	assert.Equal(t, 1, testutil.CollectAndCount(
		metrics.FailureEvidenceFlushDuration, "loadgen_failure_evidence_flush_duration_seconds"))
}
