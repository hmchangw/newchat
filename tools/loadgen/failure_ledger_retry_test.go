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
