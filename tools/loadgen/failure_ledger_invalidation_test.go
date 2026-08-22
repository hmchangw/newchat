package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// InvalidReason answers "when did this run stop being trustworthy", so the
// first cause keeps it. The counter answers "what went wrong", which is a
// different question: a run invalidated at startup can still lose its WAL an
// hour later, and that is the failure an operator has to act on. Collapsing
// both into one first-wins gate means the startup reason silently disables
// every runtime invalidation metric for the rest of the run.
func TestFailureLedger_AStartupInvalidationDoesNotMaskALaterRuntimeCause(t *testing.T) {
	metrics := NewMetrics()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 8, Recorder: newFailureLedgerPromRecorder(metrics),
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ledger.Close()) })

	ledger.Invalidate(invalidReasonReconcileCapacity)
	ledger.Invalidate("wal")

	assert.Equal(t, invalidReasonReconcileCapacity, ledger.Snapshot().InvalidReason,
		"the first cause is when the evidence stopped standing, and it keeps the field")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues(invalidReasonReconcileCapacity)))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues("wal")),
		"a later runtime cause must still be counted, or the startup reason hides it")
}

// The counter says which causes occurred, not how many times each fired, so a
// reason that repeats must not inflate it.
func TestFailureLedger_RepeatingOneInvalidationReasonCountsItOnce(t *testing.T) {
	metrics := NewMetrics()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 8, Recorder: newFailureLedgerPromRecorder(metrics),
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ledger.Close()) })

	ledger.Invalidate("wal")
	ledger.Invalidate("wal")

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues("wal")))
}

// A run that starts below the reconcile floor is invalid for the whole window,
// and the window outlives the process: the operator restarts the pod, the
// journal is replayed, and the verdicts from before the restart are still in
// it. An invalidation that lives only in memory is forgotten exactly when the
// evidence it disqualifies is recovered.
func TestFailureLedger_AnInvalidationSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalidation.wal")

	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 8, Journal: wal})
	require.NoError(t, err)
	ledger.Invalidate(invalidReasonReconcileCapacity)
	require.NoError(t, ledger.Close())

	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	metrics := NewMetrics()
	recovered, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 8, Journal: reopened, Recorder: newFailureLedgerPromRecorder(metrics),
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, recovered.Close()) })

	assert.Equal(t, invalidReasonReconcileCapacity, recovered.Snapshot().InvalidReason,
		"the recovered run must know its evidence was already in question")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues(invalidReasonReconcileCapacity)),
		"and must say so through the same counter a live invalidation raises")
}

// Compaction rewrites the journal from live state. An invalidation that is not
// part of that state is dropped by the first reclamation, which is the one
// thing guaranteed to happen on a long run.
func TestFailureLedger_CompactionKeepsEveryInvalidationReasonInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compacted.wal")

	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	// Any journal is over a one-byte budget, so this compacts on demand.
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 8, Journal: wal, MaxJournalBytes: 1,
	})
	require.NoError(t, err)
	ledger.Invalidate(invalidReasonReconcileCapacity)
	ledger.Invalidate("wal")
	require.NoError(t, ledger.MaybeCompact(time.Unix(2000, 0).UTC()))
	require.NoError(t, ledger.Close())

	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	metrics := NewMetrics()
	recovered, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 8, Journal: reopened, Recorder: newFailureLedgerPromRecorder(metrics),
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, recovered.Close()) })

	assert.Equal(t, invalidReasonReconcileCapacity, recovered.Snapshot().InvalidReason,
		"compaction must not reorder which cause came first")
	for _, reason := range []string{invalidReasonReconcileCapacity, "wal"} {
		assert.Equal(t, float64(1), testutil.ToFloat64(
			metrics.FailureInvalidations.WithLabelValues(reason)), "reason %q", reason)
	}
}

// An unknown reason is folded to "other" rather than widening the label set,
// and that has to hold on the replay path too — a journal written by a newer
// build is exactly where an unregistered reason arrives.
//
// The record is appended raw rather than through Invalidate, which folds before
// it writes: going through the API would put "other" in the file and replay
// would only have to read it back, exercising nothing.
func TestFailureLedger_AReplayedUnknownReasonBecomesOther(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unknown.wal")

	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.Append(&failureLedgerEvent{
		Type:          failureLedgerEventInvalidated,
		InvalidReason: "a_reason_this_build_does_not_know",
		At:            time.Unix(1000, 0).UTC(),
	}))
	require.NoError(t, wal.Close())

	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	metrics := NewMetrics()
	recovered, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 8, Journal: reopened, Recorder: newFailureLedgerPromRecorder(metrics),
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, recovered.Close()) })

	assert.Equal(t, "other", recovered.Snapshot().InvalidReason)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues("other")),
		"the counter must carry the folded label, not the one from the file")
}
