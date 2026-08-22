package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A transient append failure used to leave the cause in memory until the next
// invalidation, a compaction, or Close. A soak run is killed rather than closed
// often enough — OOM, node drain, SIGKILL — that "the next graceful shutdown"
// is not a durability story. Every successful ledger write is a proof the
// journal is accepting records again, so it is also the cheapest moment to land
// a verdict the file is still missing.
func TestFailureLedger_ARecoveredJournalLandsTheVerdictOnTheNextWrite(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	journal := &flakyInvalidationJournal{failures: 1}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 4, Journal: journal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	operation := testFailureOperation("m1", now)
	operation.LifecycleState = failureOperationJournaled
	require.NoError(t, ledger.Start(operation))

	ledger.Invalidate(invalidReasonReconcileCapacity)
	require.ElementsMatch(t,
		[]string{invalidReasonReconcileCapacity, invalidReasonWAL},
		ledger.UnpersistedInvalidations(),
		"the refused append should leave both causes owed to the journal")

	// An ordinary ledger write, with no invalidation, no compaction and no
	// Close anywhere in sight.
	require.NoError(t, ledger.Activate("m1", now))

	assert.Empty(t, ledger.UnpersistedInvalidations())
	assert.ElementsMatch(t,
		[]string{invalidReasonReconcileCapacity, invalidReasonWAL},
		journaledInvalidationReasons(journal),
		"a run killed after this point must replay as disowned")
}

func journaledInvalidationReasons(journal *flakyInvalidationJournal) []string {
	var reasons []string
	for i := range journal.events {
		if journal.events[i].Type == failureLedgerEventInvalidated {
			reasons = append(reasons, journal.events[i].InvalidReason)
		}
	}
	return reasons
}

// flakyInvalidationJournal refuses the first failures invalidation appends and
// accepts everything afterwards, which is the transient case: the disk recovers
// but nothing goes back for the record it lost.
type flakyInvalidationJournal struct {
	memoryFailureJournal
	failures int
}

func (j *flakyInvalidationJournal) Append(event *failureLedgerEvent) error {
	if event.Type == failureLedgerEventInvalidated && j.failures > 0 {
		j.failures--
		return errors.New("no space left on device")
	}
	return j.memoryFailureJournal.Append(event)
}

// A restart that drops recovered operations over capacity disqualifies the run,
// and that verdict is derived during replay rather than read from the journal —
// so nothing has written it. Marking it persisted anyway breaks the invariant
// the retry depends on: the reason is never retried, Close never reports it,
// and only a later compaction happens to rewrite it.
func TestFailureLedger_ACapacityVerdictFromReplayIsOwedToTheJournal(t *testing.T) {
	now := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	journal := &memoryFailureJournal{}
	for _, id := range []string{"m1", "m2"} {
		operation := testFailureOperation(id, now)
		operation.LifecycleState = failureOperationJournaled
		journal.events = append(journal.events, failureLedgerEvent{
			Type: failureLedgerEventStarted, Operation: operation, At: now,
		})
	}

	// Capacity one against two recovered operations: the second is dropped.
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 1, Journal: journal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.Equal(t, 1, ledger.Snapshot().Dropped)
	require.Equal(t, invalidReasonCapacity, ledger.Snapshot().InvalidReason)

	assert.Equal(t, []string{invalidReasonCapacity}, ledger.UnpersistedInvalidations(),
		"replay writes nothing, so the verdict is still owed to the journal")

	// The first ordinary write pays that debt.
	require.NoError(t, ledger.Activate("m1", now))
	assert.Empty(t, ledger.UnpersistedInvalidations())
	assert.Equal(t, []string{invalidReasonCapacity},
		journaledMemoryInvalidationReasons(journal))
}

func journaledMemoryInvalidationReasons(journal *memoryFailureJournal) []string {
	var reasons []string
	for i := range journal.events {
		if journal.events[i].Type == failureLedgerEventInvalidated {
			reasons = append(reasons, journal.events[i].InvalidReason)
		}
	}
	return reasons
}

// Start appends off the mutex rather than through appendLocked, so it is the
// one write that would otherwise leave an owed verdict owed. A run whose next
// journal activity happens to be a start must settle the debt like any other.
func TestFailureLedger_AStartSettlesAnOwedVerdictToo(t *testing.T) {
	now := time.Date(2026, 8, 22, 14, 0, 0, 0, time.UTC)
	journal := &flakyInvalidationJournal{failures: 1}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 4, Journal: journal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	ledger.Invalidate(invalidReasonReconcileCapacity)
	require.NotEmpty(t, ledger.UnpersistedInvalidations())

	operation := testFailureOperation("m1", now)
	operation.LifecycleState = failureOperationJournaled
	require.NoError(t, ledger.Start(operation))

	assert.Empty(t, ledger.UnpersistedInvalidations())
	assert.ElementsMatch(t,
		[]string{invalidReasonReconcileCapacity, invalidReasonWAL},
		journaledInvalidationReasons(journal))
}

// A WAL error branch used to answer a failed append by immediately attempting
// another one — the invalidation recording the failure. On a journal that is
// not coming back, every workload event then cost two doomed writes and two
// error logs, adding pressure to the failing volume for as long as the run
// lasted. The only moment worth attempting the write is after one has
// succeeded, which is what the retry already does.
func TestFailureLedger_AFailedAppendDoesNotImmediatelyRetryTheJournal(t *testing.T) {
	now := time.Date(2026, 8, 22, 17, 0, 0, 0, time.UTC)
	journal := &countingFailureJournal{fail: true}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 4, Journal: journal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	require.Error(t, ledger.Start(testFailureOperation("m1", now)))

	assert.Equal(t, 1, journal.attempts,
		"the failed start append must not be answered with a second write")
	assert.Equal(t, []string{invalidReasonWAL}, ledger.UnpersistedInvalidations(),
		"the cause is owed until a write succeeds, not dropped")
}

type countingFailureJournal struct {
	memoryFailureJournal
	fail     bool
	attempts int
}

func (j *countingFailureJournal) Append(event *failureLedgerEvent) error {
	j.attempts++
	if j.fail {
		return errors.New("no space left on device")
	}
	return j.memoryFailureJournal.Append(event)
}

// InvalidReason is the first cause, the point the run's evidence stopped
// standing, and replay rebuilds it from the order the journal holds. A retry
// loop that steps over a reason it could not write lets a later cause be
// journaled first, so a restart reports the wrong one as the first.
func TestFailureLedger_ARetryNeverLetsALaterCauseOvertakeTheFirst(t *testing.T) {
	now := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	journal := &reasonFailingFailureJournal{refuse: invalidReasonReconcileCapacity}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 4, Journal: journal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	// The first cause is refused, which raises the wal cause behind it.
	ledger.Invalidate(invalidReasonReconcileCapacity)
	require.Equal(t,
		[]string{invalidReasonReconcileCapacity, invalidReasonWAL},
		ledger.UnpersistedInvalidations())

	// A successful append drives the retry, where the second cause would land.
	operation := testFailureOperation("m1", now)
	operation.LifecycleState = failureOperationJournaled
	require.NoError(t, ledger.Start(operation))

	assert.Empty(t, journaledMemoryInvalidationReasons(&journal.memoryFailureJournal),
		"no cause may be journaled ahead of the one that came first")
}

type reasonFailingFailureJournal struct {
	memoryFailureJournal
	refuse string
}

func (j *reasonFailingFailureJournal) Append(event *failureLedgerEvent) error {
	if event.Type == failureLedgerEventInvalidated && event.InvalidReason == j.refuse {
		return errors.New("no space left on device")
	}
	return j.memoryFailureJournal.Append(event)
}
