package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureLedger_FinalizesOnlyAfterEveryObservation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, ledger.Start(&failureOperation{
		ID: "message-1", Scenario: "cassandra_soak", Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}))

	finalized, err := ledger.Observe(
		"message-1", failureObserverAdmission, failureObservationGood, now.Add(time.Second),
	)
	require.NoError(t, err)
	assert.False(t, finalized)
	assert.Equal(t, 1, ledger.Snapshot().Active)

	finalized, err = ledger.Observe(
		"message-1", failureObserverHistory, failureObservationGood, now.Add(11*time.Second),
	)
	require.NoError(t, err)
	assert.True(t, finalized)

	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[failureResultGood])
}

func TestFailureLedger_RejectedAdmissionDoesNotBecomeMissingSideEffect(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		ID: "ambiguous-message", Scenario: "cassandra_soak", Lane: "message_send",
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}))

	_, err = ledger.Observe(
		"ambiguous-message", failureObserverAdmission, failureObservationBad, now,
	)
	require.NoError(t, err)

	finalized, err := ledger.Expire(now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, finalized)
	assert.Equal(
		t, uint64(1),
		ledger.Snapshot().Results[failureResultBad],
	)
}

func TestFailureLedger_ClaimsDueOperationOnceUntilReleased(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		ID: "message-1", Scenario: "cassandra_soak", Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}))

	_, ok := ledger.ClaimDue(now.Add(9 * time.Second))
	assert.False(t, ok)

	operation, ok := ledger.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "message-1", operation.ID)
	_, ok = ledger.ClaimDue(now.Add(10 * time.Second))
	assert.False(t, ok)

	require.NoError(t, ledger.ReleaseClaim("message-1", now.Add(20*time.Second)))
	_, ok = ledger.ClaimDue(now.Add(19 * time.Second))
	assert.False(t, ok)
	_, ok = ledger.ClaimDue(now.Add(20 * time.Second))
	assert.True(t, ok)
}

func TestFailureLedger_CapacityFailureInvalidatesRun(t *testing.T) {
	now := time.Now().UTC()
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))

	err = ledger.Start(testFailureOperation("message-2", now))
	require.Error(t, err)
	assert.ErrorIs(t, err, errFailureLedgerCapacity)
	assert.Equal(t, "capacity", ledger.Snapshot().InvalidReason)
}

func TestFailureLedger_FileWALRecoversUnresolvedOperation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, Journal: wal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))
	_, err = ledger.Observe(
		"message-1", failureObserverAdmission, failureObservationGood, now.Add(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	reopenedWAL, err := openFailureWAL(path)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, Journal: reopenedWAL, Now: func() time.Time { return now.Add(2 * time.Second) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })

	snapshot := recovered.Snapshot()
	assert.Equal(t, 1, snapshot.Active)
	assert.Equal(t, 1, snapshot.Recovered)
	operation, ok := recovered.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "message-1", operation.ID)
}

func TestFailureLedger_CompactsFinalizedHistoryToActiveSet(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 3, CompactEvery: 1, Journal: wal,
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("completed", now)))
	require.NoError(t, ledger.Start(testFailureOperation("active", now)))
	_, err = ledger.Observe(
		"completed", failureObserverAdmission, failureObservationGood, now,
	)
	require.NoError(t, err)
	_, err = ledger.Observe(
		"completed", failureObserverHistory, failureObservationGood, now,
	)
	require.NoError(t, err)
	compactedSize := ledger.Snapshot().JournalBytes
	require.NoError(t, ledger.Close())

	reopenedWAL, err := openFailureWAL(path)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{
		Capacity: 3, CompactEvery: 1, Journal: reopenedWAL,
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	assert.Equal(t, 1, recovered.Snapshot().Active)
	assert.Less(t, compactedSize, int64(1000))
	operation, ok := recovered.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "active", operation.ID)
}

func TestFailureLedger_WALAppendFailureDoesNotPublishOperation(t *testing.T) {
	wantErr := errors.New("disk full")
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1,
		Journal:  &failingFailureJournal{err: wantErr},
	})
	require.NoError(t, err)

	err = ledger.Start(testFailureOperation("message-1", time.Now().UTC()))
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, "wal", snapshot.InvalidReason)
}

func TestFailureWAL_ReplayIgnoresTornFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.Append(&failureLedgerEvent{
		Type:      failureLedgerEventStarted,
		Operation: testFailureOperation("message-1", time.Now().UTC()),
		At:        time.Now().UTC(),
	}))
	require.NoError(t, wal.Close())
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.WriteString(`{"type":"observed","operationId":"message-1"`)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	events, err := reopened.Replay()
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, failureLedgerEventStarted, events[0].Type)
	require.NoError(t, reopened.Append(&failureLedgerEvent{
		Type: failureLedgerEventObserved, OperationID: "message-1",
		Observer: failureObserverAdmission, Observation: failureObservationGood,
		At: time.Now().UTC(),
	}))
	require.NoError(t, reopened.Close())
	reopenedAgain, err := openFailureWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopenedAgain.Close()) })
	events, err = reopenedAgain.Replay()
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func testFailureOperation(id string, now time.Time) *failureOperation {
	return &failureOperation{
		ID: id, Scenario: "cassandra_soak", Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}
}

type failingFailureJournal struct {
	err error
}

func (j *failingFailureJournal) Replay() ([]failureLedgerEvent, error) { return nil, nil }
func (j *failingFailureJournal) Append(*failureLedgerEvent) error      { return j.err }
func (j *failingFailureJournal) Compact([]failureLedgerEvent) error    { return j.err }
func (j *failingFailureJournal) Size() int64                           { return 0 }
func (j *failingFailureJournal) Close() error                          { return nil }
