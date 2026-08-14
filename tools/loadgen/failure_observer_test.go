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
