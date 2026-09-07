package collector

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/read"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
)

var latencyBounds = [...]time.Duration{
	time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond,
	10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
	100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
	time.Second, 2500 * time.Millisecond, 5 * time.Second,
}

type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
	OutcomeSkipped   Outcome = "skipped"
)

func validOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeSucceeded, OutcomeFailed, OutcomeSkipped:
		return true
	default:
		return false
	}
}

type Sample struct {
	Action        rpc.Action
	Outcome       Outcome
	At            time.Time
	Latency       time.Duration
	Retries       int
	ErrorClass    rpc.ErrorClass
	ErrorReason   rpc.ErrorReason
	TargetMissing bool
	// RowsCounted marks samples whose Rows is the number of rows in the reply.
	// An empty page is a real count, so this cannot be inferred from Rows > 0.
	RowsCounted bool
	ReplyBytes  int
	Rows        int
}

type OperationObservation struct {
	Action        rpc.Action
	Outcome       Outcome
	Phase         string
	Latency       time.Duration
	Retries       int
	ErrorClass    rpc.ErrorClass
	ErrorReason   rpc.ErrorReason
	TargetMissing bool
	RowsCounted   bool
	ReplyBytes    int
	Rows          int
}

type VerificationObservation struct {
	Action rpc.Action
	Class  read.VerifyClass
	Field  read.VerifyField
}

// Observer is the collector's metrics boundary. Both observations contain
// only validated closed-set labels; entity identities never cross it.
type Observer interface {
	ObserveOperation(*OperationObservation)
	ObserveVerification(VerificationObservation)
}

type fixedHistogram struct {
	buckets [len(latencyBounds) + 1]uint64
	count   uint64
	max     time.Duration
}

func (h *fixedHistogram) Observe(value time.Duration) {
	index := len(latencyBounds)
	for i, bound := range latencyBounds {
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

func (h *fixedHistogram) Quantile(quantile float64) time.Duration {
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
		if i < len(latencyBounds) {
			return latencyBounds[i]
		}
		return h.max
	}
	return h.max
}

type actionAccumulator struct {
	attempted uint64
	succeeded uint64
	failed    uint64
	skipped   uint64
	retries   uint64
	latency   fixedHistogram
	early     fixedHistogram
	late      fixedHistogram
}

type Collector struct {
	mu sync.Mutex

	observer       Observer
	warmupDeadline time.Time
	end            time.Time
	midpoint       time.Time
	bounded        bool

	warmupAttempted       uint64
	warmupActions         map[rpc.Action]uint64
	actions               map[rpc.Action]*actionAccumulator
	errors                map[rpc.Action]map[rpc.ErrorClass]uint64
	verifications         map[rpc.Action]map[read.VerifyClass]uint64
	mutationTargetMissing uint64
}

func New(observer Observer, start time.Time, warmup, duration time.Duration) *Collector {
	warmupDeadline := start.Add(max(0, warmup))
	end := start.Add(max(0, duration))
	measuredDuration := max(time.Duration(0), end.Sub(warmupDeadline))
	return &Collector{
		observer: observer, warmupDeadline: warmupDeadline, end: end,
		midpoint:      warmupDeadline.Add(measuredDuration / 2),
		bounded:       duration > 0,
		actions:       make(map[rpc.Action]*actionAccumulator),
		warmupActions: make(map[rpc.Action]uint64),
		errors:        make(map[rpc.Action]map[rpc.ErrorClass]uint64),
		verifications: make(map[rpc.Action]map[read.VerifyClass]uint64),
	}
}

func (c *Collector) Record(sample *Sample) error {
	if sample == nil {
		return fmt.Errorf("operation sample is required")
	}
	if !rpc.ValidAction(sample.Action) {
		return fmt.Errorf("invalid action label %q", sample.Action)
	}
	if !validOutcome(sample.Outcome) {
		return fmt.Errorf("invalid outcome label %q", sample.Outcome)
	}
	if sample.ErrorClass != "" && !rpc.ValidErrorClass(sample.ErrorClass) {
		return fmt.Errorf("invalid error label %q", sample.ErrorClass)
	}
	if !rpc.ValidErrorReason(sample.ErrorReason) {
		return fmt.Errorf("invalid error reason label %q", sample.ErrorReason)
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
			accumulator = &actionAccumulator{}
			c.actions[sample.Action] = accumulator
		}
		accumulator.attempted++
		switch sample.Outcome {
		case OutcomeSucceeded:
			accumulator.succeeded++
		case OutcomeFailed:
			accumulator.failed++
		case OutcomeSkipped:
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
				c.errors[sample.Action] = make(map[rpc.ErrorClass]uint64)
			}
			c.errors[sample.Action][sample.ErrorClass]++
		}
		if sample.TargetMissing {
			c.mutationTargetMissing++
		}
	}
	c.mu.Unlock()

	if c.observer != nil {
		c.observer.ObserveOperation(&OperationObservation{
			Action: sample.Action, Outcome: sample.Outcome, Phase: phase,
			Latency: sample.Latency, Retries: sample.Retries,
			ErrorClass: sample.ErrorClass, ErrorReason: sample.ErrorReason,
			TargetMissing: sample.TargetMissing, RowsCounted: sample.RowsCounted,
			ReplyBytes: sample.ReplyBytes, Rows: sample.Rows,
		})
	}
	return nil
}

func (c *Collector) RecordVerification(result *read.VerifyResult) error {
	if result == nil {
		return fmt.Errorf("verification result is required")
	}
	if !rpc.ValidAction(result.Action) {
		return fmt.Errorf("invalid verification action %q", result.Action)
	}
	if !validVerifyClass(result.Class) {
		return fmt.Errorf("invalid verification class %q", result.Class)
	}
	if !read.ValidVerifyField(result.Field) {
		return fmt.Errorf("invalid verification field %q", result.Field)
	}
	c.mu.Lock()
	if c.verifications[result.Action] == nil {
		c.verifications[result.Action] = make(map[read.VerifyClass]uint64)
	}
	c.verifications[result.Action][result.Class]++
	c.mu.Unlock()
	if c.observer != nil {
		c.observer.ObserveVerification(VerificationObservation{
			Action: result.Action, Class: result.Class, Field: result.Field,
		})
	}
	return nil
}

func validVerifyClass(class read.VerifyClass) bool {
	switch class {
	case read.VerifyOK, read.VerifySkipped, read.VerifyMissing,
		read.VerifyMismatch, read.VerifyMalformed, read.VerifyRetryable,
		read.VerifyRPCError:
		return true
	default:
		return false
	}
}

type LatencySummary struct {
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Max time.Duration
}

type ActionStats struct {
	Attempted    uint64
	Succeeded    uint64
	Failed       uint64
	Skipped      uint64
	Retries      uint64
	AchievedRate float64
	Latency      LatencySummary
	EarlyP99     time.Duration
	LateP99      time.Duration
	P99Drift     time.Duration
}

type Snapshot struct {
	Actions               map[rpc.Action]ActionStats
	Errors                map[rpc.Action]map[rpc.ErrorClass]uint64
	Verifications         map[rpc.Action]map[read.VerifyClass]uint64
	Total                 ActionStats
	WarmupAttempted       uint64
	WarmupActions         map[rpc.Action]uint64
	MutationTargetMissing uint64
	MeasuredDuration      time.Duration
}

func (c *Collector) Snapshot(now time.Time) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	measuredEnd := now
	if c.bounded && measuredEnd.After(c.end) {
		measuredEnd = c.end
	}
	measuredDuration := max(time.Duration(0), measuredEnd.Sub(c.warmupDeadline))
	snapshot := Snapshot{
		Actions: make(map[rpc.Action]ActionStats, len(c.actions)),
		Errors:  cloneErrors(c.errors), Verifications: cloneVerifications(c.verifications),
		WarmupAttempted:       c.warmupAttempted,
		WarmupActions:         make(map[rpc.Action]uint64, len(c.warmupActions)),
		MutationTargetMissing: c.mutationTargetMissing,
		MeasuredDuration:      measuredDuration,
	}
	for action, attempted := range c.warmupActions {
		snapshot.WarmupActions[action] = attempted
	}
	for action, accumulator := range c.actions {
		stats := snapshotAction(accumulator, measuredDuration)
		snapshot.Actions[action] = stats
		addStats(&snapshot.Total, &stats)
	}
	if measuredDuration > 0 {
		snapshot.Total.AchievedRate = float64(snapshot.Total.Attempted) / measuredDuration.Seconds()
	}
	return snapshot
}

func snapshotAction(accumulator *actionAccumulator, measuredDuration time.Duration) ActionStats {
	earlyP99 := accumulator.early.Quantile(0.99)
	lateP99 := accumulator.late.Quantile(0.99)
	stats := ActionStats{
		Attempted: accumulator.attempted, Succeeded: accumulator.succeeded,
		Failed: accumulator.failed, Skipped: accumulator.skipped, Retries: accumulator.retries,
		Latency: LatencySummary{
			P50: accumulator.latency.Quantile(0.50), P95: accumulator.latency.Quantile(0.95),
			P99: accumulator.latency.Quantile(0.99), Max: accumulator.latency.max,
		},
		EarlyP99: earlyP99, LateP99: lateP99, P99Drift: lateP99 - earlyP99,
	}
	if measuredDuration > 0 {
		stats.AchievedRate = float64(stats.Attempted) / measuredDuration.Seconds()
	}
	return stats
}

func addStats(total, action *ActionStats) {
	total.Attempted += action.Attempted
	total.Succeeded += action.Succeeded
	total.Failed += action.Failed
	total.Skipped += action.Skipped
	total.Retries += action.Retries
}

func cloneErrors(source map[rpc.Action]map[rpc.ErrorClass]uint64) map[rpc.Action]map[rpc.ErrorClass]uint64 {
	cloned := make(map[rpc.Action]map[rpc.ErrorClass]uint64, len(source))
	for action, classes := range source {
		cloned[action] = make(map[rpc.ErrorClass]uint64, len(classes))
		for class, count := range classes {
			cloned[action][class] = count
		}
	}
	return cloned
}

func cloneVerifications(source map[rpc.Action]map[read.VerifyClass]uint64) map[rpc.Action]map[read.VerifyClass]uint64 {
	cloned := make(map[rpc.Action]map[read.VerifyClass]uint64, len(source))
	for action, classes := range source {
		cloned[action] = make(map[read.VerifyClass]uint64, len(classes))
		for class, count := range classes {
			cloned[action][class] = count
		}
	}
	return cloned
}

type Shape struct {
	ActionCount      int
	BucketsPerAction int
	ErrorCells       int
}

func (c *Collector) Shape() Shape {
	c.mu.Lock()
	defer c.mu.Unlock()
	errorCells := 0
	for _, classes := range c.errors {
		errorCells += len(classes)
	}
	return Shape{
		ActionCount:      len(c.actions),
		BucketsPerAction: 3 * (len(latencyBounds) + 1), ErrorCells: errorCells,
	}
}
