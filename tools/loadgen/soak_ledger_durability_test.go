package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A journal that refuses an invalidation leaves the run producing evidence it
// can no longer disqualify: the cause is in memory, the counter and the log
// say so, but a kill before the debt is paid replays the run as trustworthy.
// The retry pays a transient failure on the next append; a debt still standing
// a whole sweep later is a journal that is not coming back, and the run has
// nothing to gain by continuing.
func TestWatchSoakLedgerDurability(t *testing.T) {
	now := time.Date(2026, 8, 22, 16, 0, 0, 0, time.UTC)
	const grace = 30 * time.Second

	newLedger := func(t *testing.T, journal failureJournal) *failureLedger {
		t.Helper()
		ledger, err := newFailureLedger(&failureLedgerConfig{
			Capacity: 2, Journal: journal, Now: func() time.Time { return now },
		})
		require.NoError(t, err)
		return ledger
	}

	t.Run("a verdict the journal will not take stops the run", func(t *testing.T) {
		ledger := newLedger(t, &appendFailingFailureJournal{
			failFor: failureLedgerEventInvalidated,
		})
		ledger.Invalidate(invalidReasonReconcileCapacity)

		ticks := make(chan time.Time, 2)
		ticks <- now
		ticks <- now.Add(grace)
		var reported []string
		watchSoakLedgerDurability(context.Background(), ledger, ticks, grace,
			func(reasons []string) { reported = reasons })

		assert.Equal(t,
			[]string{invalidReasonReconcileCapacity, invalidReasonWAL}, reported,
			"the run is told what the journal never took")
	})

	t.Run("a paid debt does not stop the run", func(t *testing.T) {
		// One refusal, then the journal recovers: the retry on the next append
		// settles it, so the sweep finds nothing owed.
		journal := &flakyInvalidationJournal{failures: 1}
		ledger := newLedger(t, journal)
		ledger.Invalidate(invalidReasonReconcileCapacity)
		operation := testFailureOperation("m1", now)
		operation.LifecycleState = failureOperationJournaled
		require.NoError(t, ledger.Start(operation))

		ticks := make(chan time.Time)
		stopped := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		var reported []string
		go func() {
			defer close(stopped)
			watchSoakLedgerDurability(ctx, ledger, ticks, grace,
				func(reasons []string) { reported = reasons })
		}()
		ticks <- now
		// A second tick can only be received once the first was handled, which
		// is what makes the assertion below meaningful without a timer.
		ticks <- now
		cancel()
		<-stopped

		assert.Nil(t, reported)
	})

	t.Run("a closed tick channel ends the watch", func(t *testing.T) {
		ledger := newLedger(t, &memoryFailureJournal{})
		ticks := make(chan time.Time)
		close(ticks)

		watchSoakLedgerDurability(context.Background(), ledger, ticks, grace,
			func([]string) { t.Fatal("nothing was owed") })
	})

	// The retry pays a transient debt on the next write, so a debt seen once is
	// not yet evidence that the journal is gone. Stopping on the first sighting
	// gives an append that failed a moment before the tick no grace at all,
	// which is not what a run should be ended on.
	t.Run("a debt seen once is given the grace to be paid", func(t *testing.T) {
		ledger := newLedger(t, &appendFailingFailureJournal{
			failFor: failureLedgerEventInvalidated,
		})
		ledger.Invalidate(invalidReasonReconcileCapacity)

		ticks := make(chan time.Time, 1)
		ticks <- now
		close(ticks)
		watchSoakLedgerDurability(context.Background(), ledger, ticks, grace,
			func([]string) { t.Fatal("one sighting is not a whole interval") })
	})

	// A debt that clears within the grace leaves nothing to report, and the
	// next one starts its own interval rather than inheriting this one.
	t.Run("a debt paid inside the grace resets the clock", func(t *testing.T) {
		journal := &flakyInvalidationJournal{failures: 1}
		ledger := newLedger(t, journal)
		ledger.Invalidate(invalidReasonReconcileCapacity)

		ticks := make(chan time.Time, 3)
		ticks <- now
		// Between the ticks the journal recovers and a write settles the debt.
		operation := testFailureOperation("m1", now)
		operation.LifecycleState = failureOperationJournaled
		require.NoError(t, ledger.Start(operation))
		ticks <- now.Add(grace)
		ticks <- now.Add(2 * grace)
		close(ticks)

		watchSoakLedgerDurability(context.Background(), ledger, ticks, grace,
			func([]string) { t.Fatal("the debt was paid") })
	})
}
