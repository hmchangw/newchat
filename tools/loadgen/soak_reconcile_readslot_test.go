package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The gate exists to protect the production-like read mix from a large
// reconciliation backlog. A claim that issues no request to the system under
// test takes nothing from that mix, so the allowance it took has to go back —
// otherwise reconciliation throttles itself against reads it never performed.
func TestSoakShareGate_RefundsAnAllowanceThatBoughtNoRead(t *testing.T) {
	gate := newSoakShareGate(0.5)

	require.False(t, gate.Allow(), "half a share is not a whole slot yet")
	require.True(t, gate.Allow())

	gate.Refund()

	assert.True(t, gate.Allow(), "the refunded slot is available again immediately")
}

func TestSoakShareGate_RefundIsNilSafe(t *testing.T) {
	var gate *soakShareGate
	assert.NotPanics(t, gate.Refund)
}

// Try reports whether the claim it handled spent a read slot, which is what
// lets the caller give the allowance back and still perform the read the action
// was scheduled for. The recipient observer is event mode: its evidence arrives
// on NATS subscriptions and Finalize reads it from memory, so the claim reaches
// no service under test.
func TestSoakFailureReconciler_ReportsWhetherAClaimSpentAReadSlot(t *testing.T) {
	started := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	const grace = 10 * time.Second

	t.Run("a history poll spends the slot it took", func(t *testing.T) {
		ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
		require.NoError(t, err)
		tracker := newSoakFailureTracker(
			ledger, grace, 10*time.Minute, func() time.Time { return started },
		)
		pending := testSoakFailurePending(started)
		require.NoError(t, tracker.Start(pending))
		require.NoError(t, tracker.Activate(pending))
		require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
			Status: soakSendReplyAccepted, MessageID: pending.MessageID,
		}))
		now := started.Add(grace).Add(time.Minute)
		reconciler := newSoakFailureReconciler(
			ledger,
			&fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
			time.Second, func() time.Time { return now },
		)

		handled, spentRead, err := reconciler.Try(context.Background())

		require.NoError(t, err)
		assert.True(t, handled)
		assert.True(t, spentRead, "the history verifier queried the system under test")
	})

	t.Run("a claim with nothing due spends nothing", func(t *testing.T) {
		ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
		require.NoError(t, err)
		reconciler := newSoakFailureReconciler(
			ledger, &fakeSoakFailureVerifier{}, time.Second,
			func() time.Time { return started },
		)

		handled, spentRead, err := reconciler.Try(context.Background())

		require.NoError(t, err)
		assert.False(t, handled)
		assert.False(t, spentRead)
	})

	t.Run("a recipient finalize spends nothing", func(t *testing.T) {
		now := started
		ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
		require.NoError(t, err)
		recipientObserver := newFailureRecipientObserver(
			ledger, nil, 1, func() time.Time { return now },
		)
		tracker := newSoakFailureTracker(
			ledger, 0, 10*time.Second, func() time.Time { return now },
			withSoakFailureRecipientObserver(recipientObserver),
		)
		pending := testSoakFailurePending(now)
		require.NoError(t, tracker.Start(pending))
		require.NoError(t, tracker.Activate(pending))
		_, err = ledger.Observe(
			pending.MessageID, failureObserverAdmission, failureObservationGood, now)
		require.NoError(t, err)
		_, err = ledger.Observe(
			pending.MessageID, failureObserverHistory, failureObservationGood, now)
		require.NoError(t, err)

		reconciler := newSoakFailureReconciler(
			ledger,
			&fakeSoakFailureVerifier{err: errors.New("the history observer already resolved")},
			time.Second, func() time.Time { return now },
			withSoakFailureRecipientFinalizer(&fakeSoakRecipientFinalizer{
				result: recipientEvidenceResult{Observation: failureObservationGood},
			}),
		)
		now = now.Add(10 * time.Second)

		handled, spentRead, err := reconciler.Try(context.Background())

		require.NoError(t, err)
		assert.True(t, handled, "the recipient step was reconciled")
		assert.False(t, spentRead, "Finalize reads local evidence, not the system under test")
	})
}

// With the allowance refunded for event-mode claims, the recipient observer
// costs the read lane nothing, so counting it in the floor would demand read
// capacity for work that never reaches a service. The history poll and the
// search probe do query, and stay counted.
func TestSoakReconcileStepsPerMessage_CountsOnlyTheStepsThatQuery(t *testing.T) {
	tests := []struct {
		name      string
		recipient bool
		search    bool
		want      int
	}{
		{name: "history alone", want: 1},
		{name: "recipient is event mode and costs no read", recipient: true, want: 1},
		{name: "search queries the index", search: true, want: 2},
		{name: "all three", recipient: true, search: true, want: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validSoakConfig(t)
			cfg.RecipientObserverEnabled = tc.recipient
			cfg.SearchObserverEnabled = tc.search
			assert.Equal(t, tc.want, soakReconcileStepsPerMessage(&cfg))
		})
	}
}
