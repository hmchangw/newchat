package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Before the deadline an unanswered probe records unavailable, and at the
// deadline the same outage recorded advanced — the label that means an observer
// resolved. A dependency outage is therefore invisible in claim outcomes at
// exactly the point operations expire because of it, which is when the board is
// being read to tell an outage from a persistence backlog.
func TestSoakFailureReconciler_ATerminalProbeThatWentUnansweredIsUnavailable(t *testing.T) {
	startedAt := time.Date(2026, 8, 22, 19, 0, 0, 0, time.UTC)
	// Past the deadline, so both reconcilers take their terminal branch.
	now := startedAt.Add(2 * time.Minute)

	t.Run("an unreachable history service", func(t *testing.T) {
		ledger := newSoakSearchTestLedger(t, startedAt, now)
		metrics := NewMetrics()
		reconciler := newSoakFailureReconciler(
			ledger,
			&fakeSoakFailureVerifier{err: errors.New("history service unreachable")},
			time.Second, func() time.Time { return now },
			withSoakFailureReconcileMetrics(metrics),
		)

		_, _, err := reconciler.Try(context.Background())
		require.NoError(t, err)

		assert.Equal(t, float64(1), testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimUnavailable)))
		assert.Zero(t, testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimAdvanced)))
	})

	t.Run("a search probe that issued no query", func(t *testing.T) {
		ledger := newSoakSearchTestLedger(t, startedAt, now)
		metrics := NewMetrics()
		reconciler := newSoakFailureReconciler(
			ledger,
			&fakeSoakFailureVerifier{
				results: []soakFailureHistoryResult{soakFailureHistoryFound},
			},
			time.Second, func() time.Time { return now },
			withSoakFailureSearchIndexProbe(
				&stubSoakSearchIndexProbe{result: soakSearchIndexUnknown, settle: time.Hour}),
			withSoakFailureReconcileMetrics(metrics),
		)

		// History resolves first, then the search step takes the terminal path.
		_, _, err := reconciler.Try(context.Background())
		require.NoError(t, err)
		_, _, err = reconciler.Try(context.Background())
		require.NoError(t, err)

		assert.Equal(t, float64(1), testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimUnavailable)))
		assert.Equal(t, float64(1), testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimAdvanced)),
			"the answered history claim is the only one that advanced")
	})

	// An answered probe that found the message absent is the loss report the
	// ledger exists to produce, and that claim did advance an observer.
	t.Run("an answered probe still advances", func(t *testing.T) {
		ledger := newSoakSearchTestLedger(t, startedAt, now)
		metrics := NewMetrics()
		reconciler := newSoakFailureReconciler(
			ledger,
			&fakeSoakFailureVerifier{
				results: []soakFailureHistoryResult{soakFailureHistoryMissing},
			},
			time.Second, func() time.Time { return now },
			withSoakFailureReconcileMetrics(metrics),
		)

		_, _, err := reconciler.Try(context.Background())
		require.NoError(t, err)

		assert.Equal(t, float64(1), testutil.ToFloat64(
			metrics.FailureReconcileClaims.WithLabelValues(soakReconcileClaimAdvanced)))
	})
}
