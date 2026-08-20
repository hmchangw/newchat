package main

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startExpiryOperation puts one operation in the ledger and its expectation in
// the evidence, which is the pairing the sweep has to keep in step.
func startExpiryOperation(
	t *testing.T,
	ledger *failureLedger,
	evidence *recipientEvidence,
	id string,
	at time.Time,
) {
	t.Helper()
	require.NoError(t, ledger.Start(&failureOperation{
		ID: id, Scenario: soakFailureScenario, Lane: soakFailureLaneMessageSend,
		StartedAt: at, VerifyAfter: at, Deadline: at.Add(time.Minute),
		Expected: []failureObserver{failureObserverRecipient},
	}))
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: id, Recipients: []string{"a"}, Complete: true,
	}))
}

// The evidence map is emptied only by Finalize, which runs inside the
// reconciler. An operation that reaches its deadline without being reconciled
// is finalized by failureLedger.Expire — which drops it from the ledger and,
// before this, never told the evidence. At 55 sends/s with 100 recipients each
// that is ~20M per-recipient route maps an hour, and it is what an 8GB pod
// dies of.
func TestRecipientEvidence_ForgetAllReleasesTheGivenOperations(t *testing.T) {
	evidence := newRecipientEvidence(false)
	for _, id := range []string{"op-1", "op-2", "op-3"} {
		require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
			OperationID: id, Recipients: []string{"a"}, Complete: true,
		}))
	}

	assert.Equal(t, 2, evidence.ForgetAll([]string{"op-1", "op-3"}))
	assert.Equal(t, 1, evidence.Len())
}

// Releasing the outer entry has to release the observations with it: the
// per-recipient route maps incrementRecipientRoute allocates are where the
// bytes actually are.
func TestRecipientEvidence_ForgetAllReleasesObservedRoutes(t *testing.T) {
	evidence := newRecipientEvidence(false)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op", Recipients: []string{"a", "b", "c"}, Complete: true,
	}))
	for _, recipient := range []string{"a", "b", "c"} {
		require.True(t, evidence.Observe("op", recipient))
	}

	require.Equal(t, 1, evidence.ForgetAll([]string{"op"}))

	assert.Zero(t, evidence.Len())
	assert.Equal(t, recipientEvidenceUntracked,
		evidence.ObserveDelivery("op", "a", "", "", recipientDeliveryRouteRoomGlobal),
		"a released expectation must not be resurrected by a late delivery")
}

func TestRecipientEvidence_ForgetAllIgnoresWhatItDoesNotHold(t *testing.T) {
	var absent *recipientEvidence
	assert.Zero(t, absent.ForgetAll([]string{"op"}))
	assert.Zero(t, absent.Len())

	evidence := newRecipientEvidence(false)
	assert.Zero(t, evidence.ForgetAll([]string{"never-registered"}))
	assert.Zero(t, evidence.ForgetAll(nil))
}

// Expire is the only thing that knows which operations it actually retired, so
// it has to say. A count leaves every caller holding per-operation state to
// guess, and guessing is what the two findings on this PR were about.
func TestFailureLedger_ExpireReportsTheOperationsItFinalized(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return at },
	})
	require.NoError(t, err)
	evidence := newRecipientEvidence(false)
	startExpiryOperation(t, ledger, evidence, "op-1", at)
	startExpiryOperation(t, ledger, evidence, "op-2", at)

	expired, err := ledger.Expire(at.Add(2 * time.Minute))

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"op-1", "op-2"}, expired)
}

func TestRunSoakFailureExpiry_ForgetsExactlyWhatTheLedgerFinalized(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return at },
	})
	require.NoError(t, err)
	evidence := newRecipientEvidence(false)
	startExpiryOperation(t, ledger, evidence, "op", at)

	ticks := make(chan time.Time, 1)
	ticks <- at.Add(2 * time.Minute)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, ticks, nil)

	assert.Zero(t, evidence.Len(),
		"the sweep that finalizes an operation must release its evidence")
}

// Expire stops at LEDGER_EXPIRE_BATCH and returns no error, so a successful
// call is not a complete one. Releasing every overdue expectation on that tick
// strands the operations the batch did not reach: still active, still
// reconcilable, and with no recipient evidence left to reconcile against.
func TestRunSoakFailureExpiry_KeepsEvidenceTheLedgerBatchDeferred(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, ExpireBatch: 1, Now: func() time.Time { return at },
	})
	require.NoError(t, err)
	evidence := newRecipientEvidence(false)
	startExpiryOperation(t, ledger, evidence, "op-1", at)
	startExpiryOperation(t, ledger, evidence, "op-2", at)

	ticks := make(chan time.Time, 1)
	ticks <- at.Add(2 * time.Minute)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, ticks, nil)

	assert.Equal(t, 1, evidence.Len(),
		"the operation the batch limit deferred must keep its evidence")
}

// A claimed operation is mid-verification, so Expire skips it and the sweep
// completes without error. This is the ordinary path, not a configuration edge:
// the reconciler is about to call Finalize and needs the evidence still there.
func TestRunSoakFailureExpiry_KeepsEvidenceForAClaimedOperation(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return at },
	})
	require.NoError(t, err)
	evidence := newRecipientEvidence(false)
	startExpiryOperation(t, ledger, evidence, "op", at)
	_, claimed := ledger.ClaimDue(at.Add(2 * time.Minute))
	require.True(t, claimed, "the operation must be claimed for this to test anything")

	ticks := make(chan time.Time, 1)
	ticks <- at.Add(3 * time.Minute)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, ticks, nil)

	assert.Equal(t, 1, evidence.Len(),
		"evidence for an operation the reconciler is verifying must survive the sweep")
}

// A failed expiry finalizes nothing, so there is nothing to forget — and the
// operations stay live and reconcilable.
func TestRunSoakFailureExpiry_KeepsEvidenceWhenLedgerExpiryFails(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return at },
	})
	require.NoError(t, err)
	evidence := newRecipientEvidence(false)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op", Recipients: []string{"a"}, Complete: true,
	}))
	require.NoError(t, ledger.Close())

	var swept []error
	ticks := make(chan time.Time, 1)
	ticks <- at.Add(time.Hour)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, ticks,
		func(err error) { swept = append(swept, err) })

	require.NotEmpty(t, swept, "a failed ledger expiry must be reported")
	assert.Equal(t, 1, evidence.Len())
}

// Forgetting only what the ledger retired means a ledger that stops retiring
// stops bounding the map, so it needs a ceiling of its own — the ledger has
// LEDGER_CAPACITY and this had nothing.
func TestRecipientEvidence_RefusesToGrowPastItsCapacity(t *testing.T) {
	evidence := newRecipientEvidence(false)
	evidence.capacity = 2
	for _, id := range []string{"op-1", "op-2"} {
		require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
			OperationID: id, Recipients: []string{"a"}, Complete: true,
		}))
	}

	err := evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op-3", Recipients: []string{"a"}, Complete: true,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "capacity")
	assert.Equal(t, 2, evidence.Len())
}

// Without a gauge the only way to see this map growing again is a heap profile,
// which is how it went unnoticed for the whole investigation. A healthy run
// tracks the ledger's own in-flight count; a run climbing past it is this
// leaking again.
func TestRunSoakFailureExpiry_PublishesTheRetainedExpectationCount(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return at },
	})
	require.NoError(t, err)
	metrics := NewMetrics()
	evidence := newRecipientEvidence(false)
	for _, id := range []string{"op-1", "op-2", "op-3"} {
		require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
			OperationID: id, Recipients: []string{"a"}, Complete: true,
		}))
	}

	ticks := make(chan time.Time, 1)
	ticks <- at.Add(time.Minute)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, ticks, nil,
		withSoakFailureExpiryMetrics(metrics))

	assert.Equal(t, float64(3), testutil.ToFloat64(metrics.FailureRecipientExpectations))
}

// The sweep frees ledger slots before ForgetAll frees the matching evidence, so
// for a moment the ledger has room the evidence does not. Sized equal, that
// window refuses to register a send the ledger would have admitted. Headroom of
// one full ledger generation closes it by construction: even a sweep that
// finalizes every operation before cleanup runs cannot fill the gap.
func TestFailureRecipientObserver_EvidenceCarriesHeadroomOverTheLedger(t *testing.T) {
	observer := newFailureRecipientObserver(nil, nil, 1, nil,
		withFailureRecipientEvidenceCapacity(10))

	require.Greater(t, observer.evidence.capacity, 10,
		"an evidence map sized exactly like the ledger rejects during the cleanup lag")
	assert.Equal(t, 20, observer.evidence.capacity)
}

// finalizeLocked removes the operation from l.active and compacts afterwards,
// so a compaction failure returns an error on an operation that has already
// been retired. Returning before recording that ID strands its evidence for the
// life of the run: nothing is left in l.active for any later sweep to report.
func TestFailureLedger_ExpireReportsAnOperationRetiredBeforeTheFailure(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, CompactEvery: 1,
		Journal: &compactFailingJournal{},
		Now:     func() time.Time { return at },
	})
	require.NoError(t, err)
	evidence := newRecipientEvidence(false)
	startExpiryOperation(t, ledger, evidence, "op", at)

	expired, err := ledger.Expire(at.Add(2 * time.Minute))

	require.Error(t, err, "a failed compaction must still surface")
	assert.NotContains(t, ledger.active, "op",
		"the operation was retired before the compaction failed")
	assert.Equal(t, []string{"op"}, expired,
		"an operation that left the ledger must be reported or its evidence is stranded")
}

// AbandonUnsent finalizes the operation, which removes it from l.active — so no
// later Expire can report it either. The expectation registered before the
// publish attempt would then outlive the run that created it.
func TestSoakFailureTracker_AbandonUnsentReleasesTheRecipientExpectation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 4})
	require.NoError(t, err)
	observer := newFailureRecipientObserver(ledger, nil, 4, func() time.Time { return now })
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
		withSoakFailureRecipientObserver(observer),
	)
	pending := testSoakFailurePending(now)
	pending.Target.Recipients = []string{"alice"}
	require.NoError(t, tracker.Start(pending))
	require.Equal(t, 1, observer.evidence.Len())

	require.NoError(t, tracker.AbandonUnsent(pending))

	assert.Zero(t, observer.evidence.Len(),
		"an abandoned operation leaves the ledger, so its expectation must go with it")
}

// The error branch of AbandonUnsent kept the expectation on the reasoning that
// a failed call leaves the operation reconcilable. That holds for a failure
// before the commit and not for one after it: finalizeLocked deletes from
// l.active and compacts afterwards, so a compaction error arrives on an
// operation the ledger has already retired — and no sweep will ever name it.
func TestSoakFailureTracker_AbandonUnsentReleasesTheExpectationItRetired(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 4, CompactEvery: 1, Journal: &compactFailingJournal{},
	})
	require.NoError(t, err)
	observer := newFailureRecipientObserver(ledger, nil, 4, func() time.Time { return now })
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
		withSoakFailureRecipientObserver(observer),
	)
	pending := testSoakFailurePending(now)
	pending.Target.Recipients = []string{"alice"}
	require.NoError(t, tracker.Start(pending))
	require.Equal(t, 1, observer.evidence.Len())

	err = tracker.AbandonUnsent(pending)

	require.Error(t, err, "the compaction failure must still be propagated")
	assert.NotContains(t, ledger.active, pending.MessageID,
		"the operation was retired before the compaction failed")
	assert.Zero(t, observer.evidence.Len(),
		"an expectation the ledger has retired must be released, error or not")
}
