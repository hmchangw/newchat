package main

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The capacity rule can only model the healthy path — one claim per observer —
// because the poll's retries depend on how many messages are slow to persist,
// which no configuration knows. Separating the two at runtime is what turns
// "reconcile looks starved" into a number: claims that advanced an observer
// against claims spent re-polling, with idle claims showing whether the lane
// has any slack left at all.
func TestSoakFailureReconciler_SeparatesAdvancedFromRetriedClaims(t *testing.T) {
	started := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	const grace = 10 * time.Second
	const deadline = 10 * time.Minute

	newRun := func(t *testing.T, result soakFailureHistoryResult) (*Metrics, *soakFailureReconciler) {
		t.Helper()
		ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
		require.NoError(t, err)
		tracker := newSoakFailureTracker(
			ledger, grace, deadline, func() time.Time { return started },
		)
		pending := testSoakFailurePending(started)
		require.NoError(t, tracker.Start(pending))
		require.NoError(t, tracker.Activate(pending))
		require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
			Status: soakSendReplyAccepted, MessageID: pending.MessageID,
		}))
		metrics := NewMetrics()
		now := started.Add(grace).Add(time.Minute)
		return metrics, newSoakFailureReconciler(
			ledger,
			&fakeSoakFailureVerifier{results: []soakFailureHistoryResult{result}},
			time.Second,
			func() time.Time { return now },
			withSoakFailureReconcileMetrics(metrics),
		)
	}

	t.Run("a message still missing costs a retried claim", func(t *testing.T) {
		metrics, reconciler := newRun(t, soakFailureHistoryMissing)

		ran, err := reconciler.Try(context.Background())

		require.NoError(t, err)
		require.True(t, ran)
		require.Equal(t, float64(1), testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimRetried)))
		require.Zero(t, testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimAdvanced)))
	})

	t.Run("a message that landed costs an advanced claim", func(t *testing.T) {
		metrics, reconciler := newRun(t, soakFailureHistoryFound)

		ran, err := reconciler.Try(context.Background())

		require.NoError(t, err)
		require.True(t, ran)
		require.Equal(t, float64(1), testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimAdvanced)))
		require.Zero(t, testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimRetried)))
	})
}

// An idle claim is the one that says the lane still has slack. Without it a
// saturated reconciler and a quiet one look identical from the outside — both
// simply stop resolving operations.
func TestSoakFailureReconciler_CountsAClaimWithNothingDueAsIdle(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	metrics := NewMetrics()
	reconciler := newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{},
		time.Second,
		func() time.Time { return now },
		withSoakFailureReconcileMetrics(metrics),
	)

	ran, err := reconciler.Try(context.Background())

	require.NoError(t, err)
	require.False(t, ran, "an empty ledger has nothing to claim")
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimIdle)))
}
