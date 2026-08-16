package main

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	failureWALCharacterizationOfferedOperations = 100.0
	// At the default configuration the recipient observer is disabled, so a
	// completed message operation journals started, activated, admission,
	// history, and finalized records.
	failureWALCharacterizationEventsPerOperation = 5.0
)

type delayedFailureJournal struct {
	journal failureJournal
	delay   time.Duration
}

func (j *delayedFailureJournal) Replay() ([]failureLedgerEvent, error) {
	return j.journal.Replay()
}

func (j *delayedFailureJournal) Append(event *failureLedgerEvent) error {
	if j.delay > 0 {
		time.Sleep(j.delay)
	}
	return j.journal.Append(event)
}

func (j *delayedFailureJournal) Compact(events []failureLedgerEvent) error {
	return j.journal.Compact(events)
}

func (j *delayedFailureJournal) Size() int64 { return j.journal.Size() }

func (j *delayedFailureJournal) Close() error { return j.journal.Close() }

func (j *delayedFailureJournal) ConfigureObserverContract(
	contract failureObserverContract,
	active []failureOperation,
) error {
	configurer, ok := j.journal.(interface {
		ConfigureObserverContract(failureObserverContract, []failureOperation) error
	})
	if !ok {
		return nil
	}
	return configurer.ConfigureObserverContract(contract, active)
}

func (j *delayedFailureJournal) NeedsUpgrade() bool {
	upgrade, ok := j.journal.(interface{ NeedsUpgrade() bool })
	return ok && upgrade.NeedsUpgrade()
}

type failureWALCharacterizationResult struct {
	InjectedLatency              time.Duration
	MeanObservedLatency          time.Duration
	P95ObservedLatency           time.Duration
	SustainableOperations        float64
	GroupedSustainableOperations float64
	OfferedRateCoverage          float64
	PredictedSaturation          float64
}

func characterizeFailureWAL(
	t *testing.T,
	injectedLatency time.Duration,
	samples int,
) failureWALCharacterizationResult {
	t.Helper()
	wal, err := openFailureWAL(filepath.Join(t.TempDir(), fmt.Sprintf("wal-%d.jsonl", injectedLatency.Nanoseconds())))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	require.NoError(t, wal.ConfigureObserverContract(newFailureObserverContract(false), nil))
	journal := &delayedFailureJournal{journal: wal, delay: injectedLatency}
	durations := make([]time.Duration, 0, samples)
	for sequence := 1; sequence <= samples; sequence++ {
		startedAt := time.Now()
		require.NoError(t, journal.Append(&failureLedgerEvent{
			Type: failureLedgerEventCheckpoint, At: startedAt.UTC(),
		}))
		durations = append(durations, time.Since(startedAt))
	}
	slices.Sort(durations)
	var total time.Duration
	for _, duration := range durations {
		total += duration
	}
	mean := total / time.Duration(len(durations))
	p95 := durations[(len(durations)*95+99)/100-1]
	sustainable := 1 / mean.Seconds() / failureWALCharacterizationEventsPerOperation
	groupedSustainable := 1 / mean.Seconds()
	coverage := min(1, sustainable/failureWALCharacterizationOfferedOperations)
	return failureWALCharacterizationResult{
		InjectedLatency: injectedLatency, MeanObservedLatency: mean, P95ObservedLatency: p95,
		SustainableOperations: sustainable, OfferedRateCoverage: coverage,
		GroupedSustainableOperations: groupedSustainable,
		PredictedSaturation:          max(0, failureWALCharacterizationOfferedOperations-sustainable),
	}
}

func TestFailureWALCharacterization(t *testing.T) {
	for _, latency := range []time.Duration{
		200 * time.Microsecond,
		500 * time.Microsecond,
		time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
	} {
		result := characterizeFailureWAL(t, latency, 40)
		require.Positive(t, result.MeanObservedLatency)
		t.Logf(
			"injected=%s observed_mean=%s observed_p95=%s per_record_fsync_ops_per_second=%.1f grouped_non_intent_ops_per_second=%.1f offered_rate_coverage=%.1f%% predicted_lane_saturation_per_second=%.1f",
			result.InjectedLatency,
			result.MeanObservedLatency,
			result.P95ObservedLatency,
			result.SustainableOperations,
			result.GroupedSustainableOperations,
			result.OfferedRateCoverage*100,
			result.PredictedSaturation,
		)
	}
}
