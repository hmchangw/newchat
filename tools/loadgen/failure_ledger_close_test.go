package main

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Start appends outside the mutex on purpose, so a starter that is already past
// the closed check can still be writing while Close runs. If that append fails
// it raises the wal cause, and if the invalidation append fails too the cause
// exists only in memory. Flushing before waiting for those starters lets Close
// compute a nil error over an invalidation that had not been raised yet, and
// report success over a run whose journal will replay as trustworthy.
func TestFailureLedger_CloseWaitsForInFlightStartersBeforeTheFinalFlush(t *testing.T) {
	now := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	journal := &blockingStartFailureJournal{
		blockID: "in-flight",
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 8, Journal: journal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	startErr := make(chan error, 1)
	go func() { startErr <- ledger.Start(testFailureOperation("in-flight", now)) }()
	<-journal.entered

	closeErr := make(chan error, 1)
	go func() { closeErr <- ledger.Close() }()
	awaitLedgerClosed(t, ledger, now)

	// Only now does the in-flight append fail, which is what raises the wal
	// cause the closing ledger has to account for.
	close(journal.release)
	require.Error(t, <-startErr)

	assert.ErrorContains(t, <-closeErr, invalidReasonWAL,
		"Close reported success over an invalidation it never tried to persist")
}

// awaitLedgerClosed blocks until Close has marked the ledger closed, which is
// the only externally visible point in Close's first critical section. A
// timer would only make the interleaving this test needs probabilistic.
func awaitLedgerClosed(t *testing.T, ledger *failureLedger, now time.Time) {
	t.Helper()
	for range 1_000_000 {
		err := ledger.Start(testFailureOperation("probe", now))
		if err != nil && errors.Is(err, errFailureLedgerClosed) {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("ledger never reported closed")
}

type blockingStartFailureJournal struct {
	memoryFailureJournal
	blockID string
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
}

// The blocked append fails once released, and the invalidation it triggers
// fails too: a journal that cannot hold the start record is exactly the journal
// that cannot hold the verdict about it.
func (j *blockingStartFailureJournal) Append(event *failureLedgerEvent) error {
	if event.Type == failureLedgerEventStarted && event.Operation != nil &&
		event.Operation.ID == j.blockID {
		close(j.entered)
		<-j.release
		return errors.New("no space left on device")
	}
	if event.Type == failureLedgerEventInvalidated {
		return errors.New("no space left on device")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.memoryFailureJournal.Append(event)
}
