package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureObserverHealth_BoundsIntervalsWithoutClaimingTruncatedHistory(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	health := newFailureObserverHealth(failureObserverRecipient, start)
	for index := 1; index <= failureObserverHealthIntervalLimit+2; index++ {
		health.Set(index%2 == 0, start.Add(time.Duration(index)*time.Second), "transition")
	}

	end := start.Add(time.Duration(failureObserverHealthIntervalLimit+3) * time.Second)
	snapshot := health.Snapshot(end)

	assert.True(t, snapshot.HistoryTruncated)
	assert.Len(t, snapshot.Intervals, failureObserverHealthIntervalLimit)
	assert.False(t, health.HealthyThroughout(start, end))
	require.False(t, snapshot.HistoryAvailableFrom.IsZero())
	assert.False(t, failureHealthSnapshotCovers(&snapshot, start, end))
}

func TestFailureObserverHealthSnapshotCovers_RequiresContinuousHealthyHistory(t *testing.T) {
	start := time.Date(2026, 8, 15, 1, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Minute)
	tests := []struct {
		name     string
		snapshot *failureObserverHealthSnapshot
		want     bool
	}{
		{name: "nil snapshot"},
		{name: "empty history", snapshot: &failureObserverHealthSnapshot{}},
		{
			name: "continuous healthy history",
			snapshot: &failureObserverHealthSnapshot{Intervals: []failureHealthInterval{
				{Start: start.Add(-time.Minute), End: start.Add(time.Minute), Up: true},
				{Start: start.Add(time.Minute), End: end.Add(time.Minute), Up: true},
			}},
			want: true,
		},
		{
			name: "gap",
			snapshot: &failureObserverHealthSnapshot{Intervals: []failureHealthInterval{
				{Start: start, End: start.Add(30 * time.Second), Up: true},
				{Start: start.Add(time.Minute), End: end, Up: true},
			}},
		},
		{
			name: "down interval",
			snapshot: &failureObserverHealthSnapshot{Intervals: []failureHealthInterval{
				{Start: start, End: start.Add(time.Minute), Up: true},
				{Start: start.Add(time.Minute), End: end, Up: false},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, failureHealthSnapshotCovers(test.snapshot, start, end))
		})
	}
}

func TestFailureObserverHealth_HealthyThroughoutRejectsPreProcessWindow(t *testing.T) {
	operationStarted := time.Date(2026, 8, 15, 1, 0, 0, 0, time.UTC)
	processStarted := operationStarted.Add(30 * time.Second)
	deadline := processStarted.Add(30 * time.Second)
	health := newFailureObserverHealth(failureObserverRecipient, processStarted)
	health.Set(true, processStarted, "subscribed")

	assert.False(t, health.HealthyThroughout(operationStarted, deadline))
}
