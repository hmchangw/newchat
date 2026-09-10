package main

import "time"

type failureMetricsAdapter struct {
	metrics *Metrics
}

type failureLedgerPromRecorder = failureMetricsAdapter

func newFailureMetricsAdapter(metrics *Metrics) *failureMetricsAdapter {
	return &failureMetricsAdapter{metrics: metrics}
}

func newFailureLedgerPromRecorder(metrics *Metrics) *failureMetricsAdapter {
	return newFailureMetricsAdapter(metrics)
}

func (r *failureMetricsAdapter) OperationStarted(operation *failureOperation) {
	if r == nil || r.metrics == nil || operation == nil {
		return
	}
	r.metrics.FailureInflight.WithLabelValues(
		operation.Scenario,
		operation.Lane,
	).Inc()
	for _, observer := range operation.Expected {
		r.metrics.FailureObserverEligible.WithLabelValues(
			operation.Scenario,
			operation.Lane,
			string(observer),
		).Inc()
	}
}

func (r *failureMetricsAdapter) ObservationRecorded(
	operation *failureOperation,
	observer failureObserver,
	observation failureObservation,
) {
	if r == nil || r.metrics == nil || operation == nil {
		return
	}
	r.metrics.FailureObservations.WithLabelValues(
		operation.Scenario,
		operation.Lane,
		string(observer),
		string(observation),
	).Inc()
}

func (r *failureMetricsAdapter) ObservationReasonRecorded(
	operation *failureOperation,
	observer failureObserver,
	observation failureObservation,
	reason failureReason,
) {
	if r == nil || r.metrics == nil || operation == nil || reason == failureReasonNone {
		return
	}
	r.metrics.FailureObservationReasons.WithLabelValues(
		operation.Scenario,
		operation.Lane,
		string(observer),
		string(observation),
		string(reason),
	).Inc()
}

func (r *failureMetricsAdapter) OperationFinalized(
	operation *failureOperation,
	result failureResult,
) {
	if r == nil || r.metrics == nil || operation == nil {
		return
	}
	r.metrics.FailureInflight.WithLabelValues(
		operation.Scenario,
		operation.Lane,
	).Dec()
	r.metrics.FailureOperations.WithLabelValues(
		operation.Scenario,
		operation.Lane,
		string(result),
	).Inc()
}

func (r *failureMetricsAdapter) FinalizationReasonRecorded(
	operation *failureOperation,
	result failureResult,
	reason failureReason,
) {
	if r == nil || r.metrics == nil || operation == nil ||
		result != failureResultNotSent || reason == failureReasonNone {
		return
	}
	r.metrics.FailureNotSent.WithLabelValues(
		operation.Scenario,
		operation.Lane,
		string(reason),
	).Inc()
}

func (r *failureMetricsAdapter) Recovered(count int) {
	if r == nil || r.metrics == nil || count <= 0 {
		return
	}
	r.metrics.FailureRecovered.Add(float64(count))
}

func (r *failureMetricsAdapter) Invalidated(reason string) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureInvalidations.WithLabelValues(reason).Inc()
}

func (r *failureMetricsAdapter) JournalSize(bytes int64) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureJournalBytes.Set(float64(bytes))
}

func (r *failureMetricsAdapter) SetObserverUp(observer failureObserver, up bool) {
	if r == nil || r.metrics == nil {
		return
	}
	value := float64(0)
	if up {
		value = 1
	}
	r.metrics.FailureObserverUp.WithLabelValues(string(observer)).Set(value)
}

func (r *failureMetricsAdapter) SetObserverQueueDepth(observer failureObserver, depth int) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureObserverQueueDepth.WithLabelValues(string(observer)).Set(float64(depth))
}

func (r *failureMetricsAdapter) RecordObserverEvent(observer failureObserver, observation failureObservation) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureObserverEvents.WithLabelValues(string(observer), string(observation)).Inc()
}

func (r *failureMetricsAdapter) RecordUntracked(reason string) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureUntracked.WithLabelValues(reason).Inc()
}

func (r *failureMetricsAdapter) ObserveEvidenceFlush(claim, result string, elapsed time.Duration) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureEvidenceFlushDuration.WithLabelValues(claim, result).Observe(elapsed.Seconds())
}

func (r *failureMetricsAdapter) RecordEvidence(kind string) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureEvidenceRecords.WithLabelValues(kind).Inc()
}
