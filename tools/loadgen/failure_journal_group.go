package main

import (
	"time"

	failuremodel "github.com/hmchangw/chat/tools/loadgen/internal/failure"
)

type failureJournalGroupCommit = failuremodel.GroupCommit

type failureWALFlushMetrics struct {
	metrics *Metrics
}

func (r *failureWALFlushMetrics) RecordWALFlush(
	duration time.Duration,
	batchSize int,
	err error,
) {
	if r == nil || r.metrics == nil {
		return
	}
	result := "success"
	if err != nil {
		result = "error"
	}
	r.metrics.FailureWALFlushDuration.WithLabelValues(result).Observe(duration.Seconds())
	r.metrics.FailureWALFlushBatchSize.WithLabelValues(result).Observe(float64(batchSize))
}

func newFailureJournalGroupCommit(
	journal bufferedFailureJournal,
	maxDelay time.Duration,
	maxBatchSize int,
	metricSets ...*Metrics,
) *failureJournalGroupCommit {
	var recorder failuremodel.WALFlushRecorder
	if len(metricSets) > 0 && metricSets[0] != nil {
		recorder = &failureWALFlushMetrics{metrics: metricSets[0]}
	}
	return failuremodel.NewGroupCommit(journal, maxDelay, maxBatchSize, recorder)
}
