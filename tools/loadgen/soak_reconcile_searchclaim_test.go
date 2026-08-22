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

// A search probe that never issued a query proves nothing about the message,
// and "retried" is defined as a probe that answered and found the effect
// absent. Counting an unqueried result as retried makes catalog eviction or a
// probe that was never configured read as persistence lag on the dashboard,
// which is the one thing the separate unavailable outcome exists to prevent.
// too_early is the exception: it is unqueried on purpose, because the settle
// window has not elapsed, so there is nothing unavailable about it.
func TestSoakFailureReconciler_ClassifiesUnqueriedSearchProbesAsUnavailable(t *testing.T) {
	tests := []struct {
		name    string
		result  soakSearchIndexResult
		queried bool
		err     error
		outcome string
	}{
		{
			name:    "a probe with no query to issue is unavailable",
			result:  soakSearchIndexUnknown,
			outcome: soakReconcileClaimUnavailable,
		},
		{
			name:    "a probe the search service could not answer is unavailable",
			result:  soakSearchIndexUnknown,
			queried: true,
			err:     errors.New("search timeout"),
			outcome: soakReconcileClaimUnavailable,
		},
		{
			name:    "an answered probe that did not find the message is retried",
			result:  soakSearchIndexMissing,
			queried: true,
			outcome: soakReconcileClaimRetried,
		},
		{
			name:    "an intentionally unqueried too-early probe is retried",
			result:  soakSearchIndexTooEarly,
			outcome: soakReconcileClaimRetried,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			startedAt := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
			now := startedAt.Add(time.Second)
			probe := &stubSoakSearchIndexProbe{
				result: test.result, queried: test.queried, err: test.err,
				settle: 30 * time.Second,
			}
			ledger := newSoakSearchTestLedger(t, startedAt, now)
			metrics := NewMetrics()
			reconciler := newSoakFailureReconciler(
				ledger,
				&fakeSoakFailureVerifier{
					results: []soakFailureHistoryResult{soakFailureHistoryFound},
				},
				time.Second, func() time.Time { return now },
				withSoakFailureSearchIndexProbe(probe),
				withSoakFailureReconcileMetrics(metrics),
			)

			// The first claim resolves history; the second reaches the search
			// step this test is about.
			_, _, err := reconciler.Try(context.Background())
			require.NoError(t, err)
			_, _, err = reconciler.Try(context.Background())
			require.NoError(t, err)
			require.Equal(t, 1, probe.calls)

			assert.Equal(t, float64(1), testutil.ToFloat64(
				metrics.FailureReconcileClaims.WithLabelValues(test.outcome)))
		})
	}
}
