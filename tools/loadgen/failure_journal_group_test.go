package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingBufferedFailureJournal struct {
	mu          sync.Mutex
	events      []failureLedgerEvent
	syncs       int
	written     chan struct{}
	closed      bool
	syncErr     error
	compactions int
}

func newRecordingBufferedFailureJournal() *recordingBufferedFailureJournal {
	return &recordingBufferedFailureJournal{written: make(chan struct{}, 16)}
}

func (j *recordingBufferedFailureJournal) Replay() ([]failureLedgerEvent, error) { return nil, nil }
func (j *recordingBufferedFailureJournal) Append(event *failureLedgerEvent) error {
	if err := j.AppendBuffered(event); err != nil {
		return err
	}
	return j.Sync()
}
func (j *recordingBufferedFailureJournal) AppendBuffered(event *failureLedgerEvent) error {
	j.mu.Lock()
	j.events = append(j.events, *event)
	j.mu.Unlock()
	j.written <- struct{}{}
	return nil
}
func (j *recordingBufferedFailureJournal) Sync() error {
	j.mu.Lock()
	j.syncs++
	j.mu.Unlock()
	return j.syncErr
}
func (j *recordingBufferedFailureJournal) Compact([]failureLedgerEvent) error {
	j.mu.Lock()
	j.compactions++
	j.mu.Unlock()
	return nil
}
func (j *recordingBufferedFailureJournal) Size() int64 { return 0 }
func (j *recordingBufferedFailureJournal) Close() error {
	j.mu.Lock()
	j.closed = true
	j.mu.Unlock()
	return nil
}

func (j *recordingBufferedFailureJournal) syncCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.syncs
}

func (j *recordingBufferedFailureJournal) compactCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.compactions
}

func TestFailureJournalGroupCommit_BatchesConcurrentDurableIntents(t *testing.T) {
	inner := newRecordingBufferedFailureJournal()
	journal := newFailureJournalGroupCommit(inner, 100*time.Millisecond, 256)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	errors := make(chan error, 2)
	for _, id := range []string{"message-1", "message-2"} {
		id := id
		go func() {
			errors <- journal.Append(&failureLedgerEvent{
				Type:      failureLedgerEventStarted,
				Operation: &failureOperation{ID: id},
			})
		}()
		select {
		case <-inner.written:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for buffered WAL write")
		}
	}

	require.NoError(t, <-errors)
	require.NoError(t, <-errors)
	assert.Equal(t, 1, inner.syncCount())
}

func TestFailureLedger_StartAllowsConcurrentIntentsToShareDurabilityBarrier(t *testing.T) {
	inner := newRecordingBufferedFailureJournal()
	journal := newFailureJournalGroupCommit(inner, 100*time.Millisecond, 256)
	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 2, Journal: journal})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ledger.Close()) })
	errors := make(chan error, 2)
	now := time.Date(2026, 8, 16, 1, 2, 3, 0, time.UTC)
	for _, id := range []string{"message-1", "message-2"} {
		operation := testFailureOperation(id, now)
		go func() { errors <- ledger.Start(operation) }()
		select {
		case <-inner.written:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for ledger intent write")
		}
	}

	require.NoError(t, <-errors)
	require.NoError(t, <-errors)
	assert.Equal(t, 1, inner.syncCount())
	assert.Equal(t, 2, ledger.Snapshot().Active)
}

func TestFailureJournalGroupCommit_NonIntentReturnsBeforeShutdownFlush(t *testing.T) {
	inner := newRecordingBufferedFailureJournal()
	journal := newFailureJournalGroupCommit(inner, time.Hour, 256)

	require.NoError(t, journal.Append(&failureLedgerEvent{Type: failureLedgerEventObserved}))
	assert.Zero(t, inner.syncCount())
	require.NoError(t, journal.Close())
	assert.Equal(t, 1, inner.syncCount())
}

func TestFailureJournalGroupCommit_SyncFailureRejectsDurableIntentAndSticks(t *testing.T) {
	inner := newRecordingBufferedFailureJournal()
	inner.syncErr = assert.AnError
	journal := newFailureJournalGroupCommit(inner, time.Hour, 1)
	t.Cleanup(func() { _ = journal.Close() })

	err := journal.Append(&failureLedgerEvent{Type: failureLedgerEventStarted})
	require.ErrorIs(t, err, assert.AnError)
	err = journal.Append(&failureLedgerEvent{Type: failureLedgerEventObserved})
	require.ErrorIs(t, err, assert.AnError)
}

func TestFailureLedger_DoesNotCompactAwayAConcurrentStartingIntent(t *testing.T) {
	inner := newRecordingBufferedFailureJournal()
	journal := newFailureJournalGroupCommit(inner, 100*time.Millisecond, 256)
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 2, CompactEvery: 1, Journal: journal,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ledger.Close()) })
	now := time.Date(2026, 8, 16, 1, 2, 3, 0, time.UTC)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))
	awaitBufferedWrite(t, inner.written, "first intent")

	startResult := make(chan error, 1)
	go func() { startResult <- ledger.Start(testFailureOperation("message-2", now)) }()
	awaitBufferedWrite(t, inner.written, "concurrent starting intent")
	require.NoError(t, ledger.Abandon("message-1", failureResultNotSent, now))
	assert.Zero(t, inner.compactCount())
	require.NoError(t, <-startResult)

	require.NoError(t, ledger.Abandon("message-2", failureResultNotSent, now))
	assert.Equal(t, 1, inner.compactCount())
}

// awaitBufferedWrite fails the test instead of hanging until the package
// timeout when a buffered write never reaches the journal.
func awaitBufferedWrite(t *testing.T, written <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}
