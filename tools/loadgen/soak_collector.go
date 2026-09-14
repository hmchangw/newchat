package main

import (
	"time"

	soakcollector "github.com/hmchangw/chat/tools/loadgen/internal/soak/collector"
	soakrpc "github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
)

const (
	soakOutcomeSucceeded = soakcollector.OutcomeSucceeded
	soakOutcomeFailed    = soakcollector.OutcomeFailed
	soakOutcomeSkipped   = soakcollector.OutcomeSkipped
)

type soakOperationSample = soakcollector.Sample
type SoakCollector = soakcollector.Collector
type soakLatencySummary = soakcollector.LatencySummary
type soakActionStats = soakcollector.ActionStats
type soakCollectorSnapshot = soakcollector.Snapshot

var soakAllErrorClasses = [...]soakrpc.ErrorClass{
	soakrpc.ErrorTimeout, soakrpc.ErrorNoResponder, soakrpc.ErrorDisconnected,
	soakrpc.ErrorUnavailable, soakrpc.ErrorInternal, soakrpc.ErrorNotFound,
	soakrpc.ErrorForbidden, soakrpc.ErrorBadRequest, soakrpc.ErrorConflict,
	soakrpc.ErrorRequestEncode, soakrpc.ErrorResponseDecode,
	soakrpc.ErrorAssertion, soakrpc.ErrorAmbiguous,
	soakrpc.ErrorMutationTargetMissing, soakrpc.ErrorResponseTooLarge,
	soakrpc.ErrorCanceled,
}

func NewSoakCollector(
	metrics *Metrics,
	start time.Time,
	warmup time.Duration,
	duration time.Duration,
) *SoakCollector {
	var observer soakcollector.Observer
	if metrics != nil {
		observer = &soakCollectorMetrics{metrics: metrics}
	}
	return soakcollector.New(observer, start, warmup, duration)
}

type soakCollectorMetrics struct {
	metrics *Metrics
}

func (m *soakCollectorMetrics) ObserveOperation(sample *soakcollector.OperationObservation) {
	action := string(sample.Action)
	m.metrics.SoakOperations.WithLabelValues(action, string(sample.Outcome), sample.Phase).Inc()
	if sample.Retries > 0 {
		m.metrics.SoakRetries.WithLabelValues(action, sample.Phase).Add(float64(sample.Retries))
	}
	if sample.ErrorClass != "" {
		m.metrics.SoakErrors.WithLabelValues(
			action, string(sample.ErrorClass), sample.Phase,
		).Inc()
		if sample.ErrorReason != "" {
			m.metrics.SoakErrorReasons.WithLabelValues(
				action, string(sample.ErrorClass), string(sample.ErrorReason), sample.Phase,
			).Inc()
		}
	}
	if sample.Phase != "measured" {
		return
	}
	if sample.Latency > 0 {
		m.metrics.SoakRPCLatency.WithLabelValues(action).Observe(sample.Latency.Seconds())
	}
	if sample.Outcome == soakcollector.OutcomeSucceeded && sample.RowsCounted {
		if sample.ReplyBytes > 0 {
			m.metrics.SoakReplyBytes.WithLabelValues(action).Observe(float64(sample.ReplyBytes))
		}
		m.metrics.SoakRows.WithLabelValues(action).Observe(float64(sample.Rows))
	}
	if sample.TargetMissing {
		m.metrics.SoakMutationTargetMissing.Inc()
	}
}

func (m *soakCollectorMetrics) ObserveVerification(result soakcollector.VerificationObservation) {
	m.metrics.SoakVerifications.WithLabelValues(
		string(result.Action), string(result.Class), string(result.Field),
	).Inc()
}
