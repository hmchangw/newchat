package main

import (
	"context"
	"errors"
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

// observeSearchIndex returns nil both when it persisted an observation and when
// it merely rescheduled the probe, so classifying the claim before the call
// counts every search retry as progress — which is the one thing the split
// exists to tell apart.
func TestSoakFailureReconciler_ARescheduledSearchProbeIsRetriedNotAdvanced(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := startedAt.Add(12 * time.Second)
	probe := &stubSoakSearchIndexProbe{result: soakSearchIndexTooEarly, settle: 30 * time.Second}
	ledger := newSoakSearchTestLedger(t, startedAt, now)
	metrics := NewMetrics()
	reconciler := newSoakFailureReconciler(
		ledger, &fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second, func() time.Time { return now },
		withSoakFailureSearchIndexProbe(probe),
		withSoakFailureReconcileMetrics(metrics),
	)

	// History resolves on the first claim, the search step runs on the second
	// and finds the message too early to be indexed yet.
	handled, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	require.True(t, handled)
	handled, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	require.True(t, handled)

	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimRetried)),
		"a rescheduled search probe advanced nothing")
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimAdvanced)),
		"only the history observation advanced")
}

// A claim whose observation could not be persisted advanced nothing either. The
// ledger is closed here, so recording the history verdict fails.
func TestSoakFailureReconciler_AClaimThatCouldNotPersistIsFailed(t *testing.T) {
	started := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return started },
	)
	pending := testSoakFailurePending(started)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, MessageID: pending.MessageID,
	}))
	metrics := NewMetrics()
	reconciler := newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second,
		func() time.Time { return started },
		withSoakFailureReconcileMetrics(metrics),
	)
	require.NoError(t, ledger.Close())

	_, err = reconciler.Try(context.Background())

	require.Error(t, err)
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimFailed)))
	require.Zero(t, testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimAdvanced)))
}

// A verifier that could not answer proves nothing about the message, so the
// claim it cost is not a healthy poll that found nothing. Counting it as
// retried is what makes a dependency outage read as "lots of messages are slow
// to persist" — the one reading the split exists to prevent, and the reading an
// operator would act on during the very campaign this metric serves.
func TestSoakFailureReconciler_AnUnreachableVerifierIsUnavailableNotRetried(t *testing.T) {
	started := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	const grace = 10 * time.Second
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
	metrics := NewMetrics()
	now := started.Add(grace).Add(time.Minute)
	reconciler := newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{err: errors.New("history service unreachable")},
		time.Second,
		func() time.Time { return now },
		withSoakFailureReconcileMetrics(metrics),
	)

	ran, err := reconciler.Try(context.Background())

	require.NoError(t, err)
	require.True(t, ran)
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimUnavailable)),
		"the verifier could not answer, so the claim bought no evidence")
	require.Zero(t, testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimRetried)),
		"retried is a healthy poll that found nothing, which this was not")
}

// The search probe has the same shape and the same consequence: an unreachable
// search-service is not a message that has not been indexed yet.
func TestSoakFailureReconciler_AnUnreachableSearchProbeIsUnavailableNotRetried(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := startedAt.Add(12 * time.Second)
	probe := &stubSoakSearchIndexProbe{
		err: errors.New("search service unreachable"), settle: 30 * time.Second,
	}
	ledger := newSoakSearchTestLedger(t, startedAt, now)
	metrics := NewMetrics()
	reconciler := newSoakFailureReconciler(
		ledger, &fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second, func() time.Time { return now },
		withSoakFailureSearchIndexProbe(probe),
		withSoakFailureReconcileMetrics(metrics),
	)

	// History resolves on the first claim; the search step runs on the second
	// and cannot reach the index.
	handled, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	require.True(t, handled)
	handled, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	require.True(t, handled)

	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimUnavailable)))
	require.Zero(t, testutil.ToFloat64(
		metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimRetried)))
}
