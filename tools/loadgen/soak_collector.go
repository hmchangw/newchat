package main

import (
	"fmt"
	"math"
	"sync"
	"time"
)

var soakLatencyBounds = [...]time.Duration{
	time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
}

var soakAllErrorClasses = [...]soakErrorClass{
	soakErrorTimeout,
	soakErrorNoResponder,
	soakErrorDisconnected,
	soakErrorUnavailable,
	soakErrorInternal,
	soakErrorNotFound,
	soakErrorForbidden,
	soakErrorBadRequest,
	soakErrorConflict,
	soakErrorRequestEncode,
	soakErrorResponseDecode,
	soakErrorAssertion,
	soakErrorAmbiguous,
	soakErrorMutationTargetMissing,
	soakErrorResponseTooLarge,
}

type soakOperationOutcome string

const (
	soakOutcomeSucceeded soakOperationOutcome = "succeeded"
	soakOutcomeFailed    soakOperationOutcome = "failed"
	soakOutcomeSkipped   soakOperationOutcome = "skipped"
)

func validSoakOutcome(outcome soakOperationOutcome) bool {
	switch outcome {
	case soakOutcomeSucceeded, soakOutcomeFailed, soakOutcomeSkipped:
		return true
	default:
		return false
	}
}

type soakOperationSample struct {
	Action        soakRPCAction
	Outcome       soakOperationOutcome
	At            time.Time
	Latency       time.Duration
	Retries       int
	ErrorClass    soakErrorClass
	ErrorReason   soakErrorReason
	TargetMissing bool
}

type soakFixedHistogram struct {
	buckets [len(soakLatencyBounds) + 1]uint64
	count   uint64
	max     time.Duration
}

func (h *soakFixedHistogram) Observe(value time.Duration) {
	index := len(soakLatencyBounds)
	for i, bound := range soakLatencyBounds {
		if value <= bound {
			index = i
			break
		}
	}
	h.buckets[index]++
	h.count++
	if value > h.max {
		h.max = value
	}
}

func (h *soakFixedHistogram) Quantile(quantile float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	target := uint64(math.Ceil(quantile * float64(h.count)))
	var cumulative uint64
	for i, count := range h.buckets {
		cumulative += count
		if cumulative < target {
			continue
		}
		if i < len(soakLatencyBounds) {
			return soakLatencyBounds[i]
		}
		return h.max
	}
	return h.max
}

type soakActionAccumulator struct {
	attempted uint64
	succeeded uint64
	failed    uint64
	skipped   uint64
	retries   uint64
	latency   soakFixedHistogram
	early     soakFixedHistogram
	late      soakFixedHistogram
}

type SoakCollector struct {
	mu sync.Mutex

	metrics        *Metrics
	start          time.Time
	warmupDeadline time.Time
	end            time.Time
	midpoint       time.Time
	bounded        bool

	warmupAttempted       uint64
	warmupActions         map[soakRPCAction]uint64
	actions               map[soakRPCAction]*soakActionAccumulator
	errors                map[soakRPCAction]map[soakErrorClass]uint64
	verifications         map[soakRPCAction]map[soakVerifyClass]uint64
	mutationTargetMissing uint64
}

func NewSoakCollector(
	metrics *Metrics,
	start time.Time,
	warmup time.Duration,
	duration time.Duration,
) *SoakCollector {
	warmupDeadline := start.Add(max(0, warmup))
	end := start.Add(max(0, duration))
	measuredDuration := max(time.Duration(0), end.Sub(warmupDeadline))
	return &SoakCollector{
		metrics: metrics, start: start, warmupDeadline: warmupDeadline, end: end,
		midpoint:      warmupDeadline.Add(measuredDuration / 2),
		bounded:       duration > 0,
		actions:       make(map[soakRPCAction]*soakActionAccumulator),
		warmupActions: make(map[soakRPCAction]uint64),
		errors:        make(map[soakRPCAction]map[soakErrorClass]uint64),
		verifications: make(map[soakRPCAction]map[soakVerifyClass]uint64),
	}
}

func (c *SoakCollector) Record(sample *soakOperationSample) error {
	if sample == nil {
		return fmt.Errorf("soak operation sample is required")
	}
	if !validSoakRPCAction(sample.Action) {
		return fmt.Errorf("invalid soak action label %q", sample.Action)
	}
	if !validSoakOutcome(sample.Outcome) {
		return fmt.Errorf("invalid soak outcome label %q", sample.Outcome)
	}
	if sample.ErrorClass != "" && !validSoakErrorClass(sample.ErrorClass) {
		return fmt.Errorf("invalid soak error label %q", sample.ErrorClass)
	}
	if !validSoakErrorReason(sample.ErrorReason) {
		return fmt.Errorf("invalid soak error reason label %q", sample.ErrorReason)
	}
	phase := "measured"
	if sample.At.Before(c.warmupDeadline) {
		phase = "warmup"
	}

	c.mu.Lock()
	if phase == "warmup" {
		c.warmupAttempted++
		c.warmupActions[sample.Action]++
	} else {
		accumulator := c.actions[sample.Action]
		if accumulator == nil {
			accumulator = &soakActionAccumulator{}
			c.actions[sample.Action] = accumulator
		}
		accumulator.attempted++
		switch sample.Outcome {
		case soakOutcomeSucceeded:
			accumulator.succeeded++
		case soakOutcomeFailed:
			accumulator.failed++
		case soakOutcomeSkipped:
			accumulator.skipped++
		}
		if sample.Retries > 0 {
			accumulator.retries += uint64(sample.Retries)
		}
		if sample.Latency > 0 {
			accumulator.latency.Observe(sample.Latency)
			if c.bounded {
				if sample.At.Before(c.midpoint) {
					accumulator.early.Observe(sample.Latency)
				} else {
					accumulator.late.Observe(sample.Latency)
				}
			}
		}
		if sample.ErrorClass != "" {
			if c.errors[sample.Action] == nil {
				c.errors[sample.Action] = make(map[soakErrorClass]uint64)
			}
			c.errors[sample.Action][sample.ErrorClass]++
		}
		if sample.TargetMissing {
			c.mutationTargetMissing++
		}
	}
	c.mu.Unlock()

	if c.metrics != nil {
		action := string(sample.Action)
		c.metrics.SoakOperations.WithLabelValues(
			action,
			string(sample.Outcome),
			phase,
		).Inc()
		// Failures and retries count in both phases: a process that keeps
		// restarting spends most of its life warming up, and gating them on
		// "measured" leaves the run that most needs diagnosing reporting almost
		// nothing. Latency stays measured-only — a warm-up sample is taken
		// against cold caches and would bias every percentile the run reports.
		if sample.Retries > 0 {
			c.metrics.SoakRetries.WithLabelValues(action, phase).Add(
				float64(sample.Retries),
			)
		}
		if sample.ErrorClass != "" {
			c.metrics.SoakErrors.WithLabelValues(
				action,
				string(sample.ErrorClass),
				phase,
			).Inc()
			if sample.ErrorReason != "" {
				c.metrics.SoakErrorReasons.WithLabelValues(
					action,
					string(sample.ErrorClass),
					sample.ErrorReason,
				).Inc()
			}
		}
		if phase == "measured" {
			if sample.Latency > 0 {
				c.metrics.SoakRPCLatency.WithLabelValues(action).Observe(
					sample.Latency.Seconds(),
				)
			}
			if sample.TargetMissing {
				c.metrics.SoakMutationTargetMissing.Inc()
			}
		}
	}
	return nil
}

func (c *SoakCollector) RecordVerification(
	result *soakVerifyResult,
) error {
	if result == nil {
		return fmt.Errorf("soak verification result is required")
	}
	if !validSoakRPCAction(result.Action) {
		return fmt.Errorf("invalid soak verification action %q", result.Action)
	}
	if !validSoakVerifyClass(result.Class) {
		return fmt.Errorf("invalid soak verification class %q", result.Class)
	}
	if !validSoakVerifyField(result.Field) {
		return fmt.Errorf("invalid soak verification field %q", result.Field)
	}
	c.mu.Lock()
	if c.verifications[result.Action] == nil {
		c.verifications[result.Action] = make(map[soakVerifyClass]uint64)
	}
	c.verifications[result.Action][result.Class]++
	c.mu.Unlock()
	if c.metrics != nil {
		c.metrics.SoakVerifications.WithLabelValues(
			string(result.Action),
			string(result.Class),
			result.Field,
		).Inc()
	}
	return nil
}

func validSoakVerifyClass(class soakVerifyClass) bool {
	switch class {
	case soakVerifyOK, soakVerifySkipped, soakVerifyMissing, soakVerifyMismatch,
		soakVerifyMalformed, soakVerifyRetryable, soakVerifyRPCError:
		return true
	default:
		return false
	}
}

type soakLatencySummary struct {
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Max time.Duration
}

type soakActionStats struct {
	Attempted    uint64
	Succeeded    uint64
	Failed       uint64
	Skipped      uint64
	Retries      uint64
	AchievedRate float64
	Latency      soakLatencySummary
	EarlyP99     time.Duration
	LateP99      time.Duration
	P99Drift     time.Duration
}

type soakCollectorSnapshot struct {
	Actions               map[soakRPCAction]soakActionStats
	Errors                map[soakRPCAction]map[soakErrorClass]uint64
	Verifications         map[soakRPCAction]map[soakVerifyClass]uint64
	Total                 soakActionStats
	WarmupAttempted       uint64
	WarmupActions         map[soakRPCAction]uint64
	MutationTargetMissing uint64
	MeasuredDuration      time.Duration
}

func (c *SoakCollector) Snapshot(now time.Time) soakCollectorSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	measuredEnd := now
	if c.bounded && measuredEnd.After(c.end) {
		measuredEnd = c.end
	}
	measuredDuration := max(time.Duration(0), measuredEnd.Sub(c.warmupDeadline))
	snapshot := soakCollectorSnapshot{
		Actions:               make(map[soakRPCAction]soakActionStats, len(c.actions)),
		Errors:                cloneSoakErrors(c.errors),
		Verifications:         cloneSoakVerifications(c.verifications),
		WarmupAttempted:       c.warmupAttempted,
		WarmupActions:         make(map[soakRPCAction]uint64, len(c.warmupActions)),
		MutationTargetMissing: c.mutationTargetMissing,
		MeasuredDuration:      measuredDuration,
	}
	for action, attempted := range c.warmupActions {
		snapshot.WarmupActions[action] = attempted
	}
	for action, accumulator := range c.actions {
		stats := snapshotSoakAction(accumulator, measuredDuration)
		snapshot.Actions[action] = stats
		addSoakStats(&snapshot.Total, &stats)
	}
	if measuredDuration > 0 {
		snapshot.Total.AchievedRate = float64(snapshot.Total.Attempted) /
			measuredDuration.Seconds()
	}
	return snapshot
}

func snapshotSoakAction(
	accumulator *soakActionAccumulator,
	measuredDuration time.Duration,
) soakActionStats {
	earlyP99 := accumulator.early.Quantile(0.99)
	lateP99 := accumulator.late.Quantile(0.99)
	stats := soakActionStats{
		Attempted: accumulator.attempted,
		Succeeded: accumulator.succeeded,
		Failed:    accumulator.failed,
		Skipped:   accumulator.skipped,
		Retries:   accumulator.retries,
		Latency: soakLatencySummary{
			P50: accumulator.latency.Quantile(0.50),
			P95: accumulator.latency.Quantile(0.95),
			P99: accumulator.latency.Quantile(0.99),
			Max: accumulator.latency.max,
		},
		EarlyP99: earlyP99,
		LateP99:  lateP99,
		P99Drift: lateP99 - earlyP99,
	}
	if measuredDuration > 0 {
		stats.AchievedRate = float64(stats.Attempted) /
			measuredDuration.Seconds()
	}
	return stats
}

func addSoakStats(total, action *soakActionStats) {
	total.Attempted += action.Attempted
	total.Succeeded += action.Succeeded
	total.Failed += action.Failed
	total.Skipped += action.Skipped
	total.Retries += action.Retries
}

func cloneSoakErrors(
	source map[soakRPCAction]map[soakErrorClass]uint64,
) map[soakRPCAction]map[soakErrorClass]uint64 {
	cloned := make(map[soakRPCAction]map[soakErrorClass]uint64, len(source))
	for action, classes := range source {
		cloned[action] = make(map[soakErrorClass]uint64, len(classes))
		for class, count := range classes {
			cloned[action][class] = count
		}
	}
	return cloned
}

func cloneSoakVerifications(
	source map[soakRPCAction]map[soakVerifyClass]uint64,
) map[soakRPCAction]map[soakVerifyClass]uint64 {
	cloned := make(map[soakRPCAction]map[soakVerifyClass]uint64, len(source))
	for action, classes := range source {
		cloned[action] = make(map[soakVerifyClass]uint64, len(classes))
		for class, count := range classes {
			cloned[action][class] = count
		}
	}
	return cloned
}

type soakCollectorShape struct {
	ActionCount      int
	BucketsPerAction int
	ErrorCells       int
}

func (c *SoakCollector) Shape() soakCollectorShape {
	c.mu.Lock()
	defer c.mu.Unlock()
	errorCells := 0
	for _, classes := range c.errors {
		errorCells += len(classes)
	}
	return soakCollectorShape{
		ActionCount:      len(c.actions),
		BucketsPerAction: 3 * (len(soakLatencyBounds) + 1),
		ErrorCells:       errorCells,
	}
}
