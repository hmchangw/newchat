package failure

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObserverHealth_BoundsIntervalsWithoutClaimingTruncatedHistory(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	var absent *ObserverHealth
	absent.Set(true, start, "ignored")
	assert.Empty(t, absent.Snapshot(start).Intervals)

	health := NewObserverHealth(ObserverRecipient, start)
	health.Set(true, start.Add(-time.Second), "out-of-order")
	health.Set(true, start, "connected")
	health.Set(true, start.Add(time.Millisecond), "probe")
	for index := 1; index <= ObserverHealthIntervalLimit+2; index++ {
		health.Set(index%2 == 0, start.Add(time.Duration(index)*time.Second), "transition")
	}

	end := start.Add(time.Duration(ObserverHealthIntervalLimit+3) * time.Second)
	snapshot := health.Snapshot(end)

	assert.True(t, snapshot.HistoryTruncated)
	assert.Len(t, snapshot.Intervals, ObserverHealthIntervalLimit)
	assert.False(t, health.HealthyThroughout(start, end))
	require.False(t, snapshot.HistoryAvailableFrom.IsZero())
	assert.False(t, HealthSnapshotCovers(&snapshot, start, end))
}

func TestObserverHealthSnapshotCovers_RequiresContinuousHealthyHistory(t *testing.T) {
	start := time.Date(2026, 8, 15, 1, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Minute)
	tests := []struct {
		name     string
		snapshot *ObserverHealthSnapshot
		want     bool
	}{
		{name: "nil snapshot"},
		{name: "empty history", snapshot: &ObserverHealthSnapshot{}},
		{
			name: "continuous healthy history",
			snapshot: &ObserverHealthSnapshot{Intervals: []HealthInterval{
				{Start: start.Add(-time.Minute), End: start.Add(time.Minute), Up: true},
				{Start: start.Add(time.Minute), End: end.Add(time.Minute), Up: true},
			}},
			want: true,
		},
		{
			name: "gap",
			snapshot: &ObserverHealthSnapshot{Intervals: []HealthInterval{
				{Start: start, End: start.Add(30 * time.Second), Up: true},
				{Start: start.Add(time.Minute), End: end, Up: true},
			}},
		},
		{
			name: "down interval",
			snapshot: &ObserverHealthSnapshot{Intervals: []HealthInterval{
				{Start: start, End: start.Add(time.Minute), Up: true},
				{Start: start.Add(time.Minute), End: end, Up: false},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, HealthSnapshotCovers(test.snapshot, start, end))
		})
	}
}

func TestObserverHealth_HealthyThroughoutRejectsPreProcessWindow(t *testing.T) {
	operationStarted := time.Date(2026, 8, 15, 1, 0, 0, 0, time.UTC)
	processStarted := operationStarted.Add(30 * time.Second)
	deadline := processStarted.Add(30 * time.Second)
	health := NewObserverHealth(ObserverRecipient, processStarted)
	health.Set(true, processStarted, "subscribed")

	assert.False(t, health.HealthyThroughout(operationStarted, deadline))
}
