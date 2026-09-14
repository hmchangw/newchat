package collector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/read"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
)

func TestSoakCollector_CountsWarmupMeasuredOutcomesAndObservations(t *testing.T) {
	start := time.Unix(100, 0)
	observer := &captureObserver{}
	collector := New(observer, start, 10*time.Second, time.Minute)
	samples := []Sample{
		{Action: rpc.ActionSend, Outcome: OutcomeSucceeded, At: start.Add(time.Second)},
		{Action: rpc.ActionReact, Outcome: OutcomeSucceeded, At: start.Add(11 * time.Second), Latency: 10 * time.Millisecond, Retries: 2, RowsCounted: true, Rows: 0},
		{Action: rpc.ActionEdit, Outcome: OutcomeFailed, At: start.Add(12 * time.Second), ErrorClass: rpc.ErrorForbidden, ErrorReason: "not_subscribed", TargetMissing: true},
		{Action: rpc.ActionDelete, Outcome: OutcomeSkipped, At: start.Add(13 * time.Second)},
	}
	for i := range samples {
		require.NoError(t, collector.Record(&samples[i]))
	}

	snapshot := collector.Snapshot(start.Add(20 * time.Second))
	assert.Equal(t, uint64(1), snapshot.WarmupAttempted)
	assert.Equal(t, uint64(1), snapshot.WarmupActions[rpc.ActionSend])
	assert.Equal(t, uint64(3), snapshot.Total.Attempted)
	assert.Equal(t, uint64(1), snapshot.Total.Succeeded)
	assert.Equal(t, uint64(1), snapshot.Total.Failed)
	assert.Equal(t, uint64(1), snapshot.Total.Skipped)
	assert.Equal(t, uint64(2), snapshot.Total.Retries)
	assert.Equal(t, uint64(1), snapshot.Errors[rpc.ActionEdit][rpc.ErrorForbidden])
	assert.Equal(t, uint64(1), snapshot.MutationTargetMissing)
	require.Len(t, observer.operations, 4)
	assert.Equal(t, "warmup", observer.operations[0].Phase)
	assert.Equal(t, "measured", observer.operations[1].Phase)
}

func TestSoakCollector_HistogramRateDriftAndBoundedSnapshot(t *testing.T) {
	start := time.Unix(100, 0)
	collector := New(nil, start, 0, 100*time.Second)
	for _, sample := range []Sample{
		{Action: rpc.ActionLoadHistory, Outcome: OutcomeSucceeded, At: start.Add(10 * time.Second), Latency: time.Millisecond},
		{Action: rpc.ActionLoadHistory, Outcome: OutcomeSucceeded, At: start.Add(10 * time.Second), Latency: 25 * time.Millisecond},
		{Action: rpc.ActionLoadHistory, Outcome: OutcomeSucceeded, At: start.Add(90 * time.Second), Latency: 7 * time.Second},
	} {
		sample := sample
		require.NoError(t, collector.Record(&sample))
	}
	stats := collector.Snapshot(start.Add(2 * time.Minute)).Actions[rpc.ActionLoadHistory]
	assert.Equal(t, 25*time.Millisecond, stats.Latency.P50)
	assert.Equal(t, 7*time.Second, stats.Latency.P95)
	assert.Equal(t, 7*time.Second, stats.Latency.Max)
	assert.Equal(t, 25*time.Millisecond, stats.EarlyP99)
	assert.Equal(t, 7*time.Second, stats.LateP99)
	assert.Equal(t, 7*time.Second-25*time.Millisecond, stats.P99Drift)
	assert.InDelta(t, 0.03, stats.AchievedRate, 0.0001)
}

func TestSoakCollector_UnboundedDurationUsesElapsedTime(t *testing.T) {
	start := time.Unix(100, 0)
	collector := New(nil, start, time.Minute, 0)
	require.NoError(t, collector.Record(&Sample{
		Action: rpc.ActionSend, Outcome: OutcomeSucceeded,
		At: start.Add(3 * time.Hour), Latency: time.Millisecond,
	}))
	snapshot := collector.Snapshot(start.Add(4 * time.Hour))
	assert.Equal(t, 3*time.Hour+59*time.Minute, snapshot.MeasuredDuration)
	assert.Positive(t, snapshot.Total.AchievedRate)
}

func TestSoakCollector_RecordsAndValidatesVerification(t *testing.T) {
	observer := &captureObserver{}
	collector := New(observer, time.Unix(100, 0), 0, time.Minute)
	result := read.VerifyResult{
		Action: rpc.ActionGetMessage, Class: read.VerifyMismatch,
		Field: read.VerifyFieldContent, RoomID: "not-a-label", MessageID: "not-a-label",
	}
	require.NoError(t, collector.RecordVerification(&result))
	assert.Equal(t, uint64(1), collector.Snapshot(time.Unix(101, 0)).Verifications[rpc.ActionGetMessage][read.VerifyMismatch])
	assert.Equal(t, []VerificationObservation{{
		Action: rpc.ActionGetMessage, Class: read.VerifyMismatch, Field: read.VerifyFieldContent,
	}}, observer.verifications)

	for name, invalid := range map[string]*read.VerifyResult{
		"nil":    nil,
		"action": {Action: "entity-id", Class: read.VerifyOK},
		"class":  {Action: rpc.ActionGetMessage, Class: "unbounded"},
		"field":  {Action: rpc.ActionGetMessage, Class: read.VerifyMismatch, Field: "message-123"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, collector.RecordVerification(invalid))
		})
	}
}

func TestSoakCollector_RejectsInvalidSamplesWithoutMutation(t *testing.T) {
	collector := New(nil, time.Unix(100, 0), 0, time.Minute)
	for name, sample := range map[string]*Sample{
		"nil":     nil,
		"action":  {Action: "room-123", Outcome: OutcomeFailed},
		"outcome": {Action: rpc.ActionSend, Outcome: "unknown"},
		"class":   {Action: rpc.ActionSend, Outcome: OutcomeFailed, ErrorClass: "raw error"},
		"reason":  {Action: rpc.ActionSend, Outcome: OutcomeFailed, ErrorReason: "account-123"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, collector.Record(sample))
		})
	}
	assert.Empty(t, collector.Snapshot(time.Unix(101, 0)).Actions)
}

func TestSoakCollector_SnapshotIsIndependentAndShapeIsBounded(t *testing.T) {
	start := time.Unix(100, 0)
	collector := New(nil, start, 0, time.Hour)
	for range 100 {
		require.NoError(t, collector.Record(&Sample{
			Action: rpc.ActionReact, Outcome: OutcomeFailed, At: start,
			ErrorClass: rpc.ErrorInternal,
		}))
	}
	first := collector.Snapshot(start.Add(time.Second))
	first.Errors[rpc.ActionReact][rpc.ErrorInternal] = 0
	first.Actions[rpc.ActionReact] = ActionStats{}
	second := collector.Snapshot(start.Add(time.Second))
	assert.Equal(t, uint64(100), second.Errors[rpc.ActionReact][rpc.ErrorInternal])
	assert.Equal(t, uint64(100), second.Actions[rpc.ActionReact].Attempted)
	shape := collector.Shape()
	assert.Equal(t, 1, shape.ActionCount)
	assert.Equal(t, 39, shape.BucketsPerAction)
	assert.Equal(t, 1, shape.ErrorCells)
}

type captureObserver struct {
	operations    []OperationObservation
	verifications []VerificationObservation
}

func (o *captureObserver) ObserveOperation(sample *OperationObservation) {
	o.operations = append(o.operations, *sample)
}

func (o *captureObserver) ObserveVerification(result VerificationObservation) {
	o.verifications = append(o.verifications, result)
}
