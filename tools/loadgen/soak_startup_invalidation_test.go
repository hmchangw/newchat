package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A configuration breach only warns, by design — the run starts and carries the
// verdict in its own evidence. That bargain depends on the verdict surviving
// the process. If the journal refuses the record, a crash before Close replays
// the run without it and the recovered evidence looks trustworthy, so the run
// must not get past startup on a ledger that cannot hold its own disqualifier.
func TestRecordSoakStartupInvalidations(t *testing.T) {
	now := time.Date(2026, 8, 22, 11, 0, 0, 0, time.UTC)

	newLedger := func(t *testing.T, journal failureJournal) *failureLedger {
		t.Helper()
		ledger, err := newFailureLedger(&failureLedgerConfig{
			Capacity: 2, Journal: journal, Now: func() time.Time { return now },
		})
		require.NoError(t, err)
		return ledger
	}

	t.Run("a journaled breach starts the run and keeps the verdict", func(t *testing.T) {
		journal := &memoryFailureJournal{}
		ledger := newLedger(t, journal)

		err := recordSoakStartupInvalidations(
			ledger, []string{invalidReasonReconcileCapacity})

		require.NoError(t, err)
		assert.Equal(t, invalidReasonReconcileCapacity, ledger.Snapshot().InvalidReason)
		assert.Empty(t, ledger.UnpersistedInvalidations())
	})

	t.Run("a breach the journal refused stops the run", func(t *testing.T) {
		ledger := newLedger(t, &appendFailingFailureJournal{
			failFor: failureLedgerEventInvalidated,
		})

		err := recordSoakStartupInvalidations(
			ledger, []string{invalidReasonReconcileCapacity})

		require.Error(t, err)
		assert.ErrorContains(t, err, invalidReasonReconcileCapacity)
		assert.ErrorContains(t, err, invalidReasonWAL)
	})

	t.Run("no breach needs no journal write", func(t *testing.T) {
		ledger := newLedger(t, &appendFailingFailureJournal{
			failFor: failureLedgerEventInvalidated,
		})

		require.NoError(t, recordSoakStartupInvalidations(ledger, nil))
		assert.Empty(t, ledger.Snapshot().InvalidReason)
	})

	// The in-memory fallback is already an invalid run by construction and has
	// no journal to refuse anything. Reporting it as a persistence failure
	// would turn the degraded mode this tool deliberately supports into a
	// startup refusal.
	t.Run("a ledger with no journal is not a persistence failure", func(t *testing.T) {
		ledger := newLedger(t, nil)

		require.NoError(t, recordSoakStartupInvalidations(
			ledger, []string{invalidReasonReconcileLagRange}))
		assert.Equal(t, invalidReasonReconcileLagRange, ledger.Snapshot().InvalidReason)
	})
}
