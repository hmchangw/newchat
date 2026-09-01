package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The search probe answers several ways without issuing a request: before the
// settle window elapses, and whenever the operation or the catalogue cannot
// supply a query. IndexedAt's own contract says so — "the query would only
// spend a read slot to learn nothing" — so deriving spentRead from whether a
// probe is configured contradicts the design it was meant to serve. A claim
// that queried nothing must give its allowance back and let the action perform
// the read it was scheduled for.
func TestSoakFailureReconciler_ASearchProbeThatIssuedNoQuerySpendsNoReadSlot(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := startedAt.Add(12 * time.Second)
	probe := &stubSoakSearchIndexProbe{
		result: soakSearchIndexTooEarly, settle: 30 * time.Second,
	}
	ledger := newSoakSearchTestLedger(t, startedAt, now)
	reconciler := newSoakFailureReconciler(
		ledger, &fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second, func() time.Time { return now },
		withSoakFailureSearchIndexProbe(probe),
	)

	// History resolves on the first claim and does query.
	handled, spentRead, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, spentRead)

	handled, spentRead, err = reconciler.Try(context.Background())

	require.NoError(t, err)
	assert.True(t, handled, "the search step was claimed and rescheduled")
	assert.False(t, spentRead,
		"a too-early probe returns before issuing a request, so the action still owes its read")
}

func TestSoakFailureReconciler_ASearchProbeThatQueriedSpendsItsReadSlot(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := startedAt.Add(time.Minute)
	probe := &stubSoakSearchIndexProbe{
		result: soakSearchIndexFound, queried: true, settle: 30 * time.Second,
	}
	ledger := newSoakSearchTestLedger(t, startedAt, now)
	reconciler := newSoakFailureReconciler(
		ledger, &fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second, func() time.Time { return now },
		withSoakFailureSearchIndexProbe(probe),
	)

	handled, _, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	require.True(t, handled)

	handled, spentRead, err := reconciler.Try(context.Background())

	require.NoError(t, err)
	assert.True(t, handled)
	assert.True(t, spentRead, "the probe reached the search service")
}
