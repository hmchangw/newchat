package main

type failureLedgerPromRecorder struct {
	metrics *Metrics
}

func newFailureLedgerPromRecorder(metrics *Metrics) *failureLedgerPromRecorder {
	return &failureLedgerPromRecorder{metrics: metrics}
}

func (r *failureLedgerPromRecorder) OperationStarted(operation *failureOperation) {
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

func (r *failureLedgerPromRecorder) ObservationRecorded(
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

func (r *failureLedgerPromRecorder) ObservationReasonRecorded(
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

func (r *failureLedgerPromRecorder) OperationFinalized(
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

func (r *failureLedgerPromRecorder) FinalizationReasonRecorded(
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

func (r *failureLedgerPromRecorder) Recovered(count int) {
	if r == nil || r.metrics == nil || count <= 0 {
		return
	}
	r.metrics.FailureRecovered.Add(float64(count))
}

func (r *failureLedgerPromRecorder) Invalidated(reason string) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureInvalidations.WithLabelValues(reason).Inc()
}

func (r *failureLedgerPromRecorder) JournalSize(bytes int64) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureJournalBytes.Set(float64(bytes))
}
