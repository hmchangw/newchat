package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A message that admission accepted and that never persists is the one verdict
// this whole tool exists to produce. Walking the real backoff schedule with the
// history verifier reporting missing throughout must end in
// missing_after_deadline with history_missing — not unverified, which says
// nobody looked.
//
// The expiry sweep runs alongside, as it does in production. It must not retire
// the operation before the reconciler has had its authoritative look at or
// after the deadline: expiry records unverified for every unresolved observer,
// so a sweep that gets there first converts data loss into "inconclusive".
func TestSoakFailureReconciler_APermanentlyMissingMessageIsLossNotUnverified(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	now := start
	const deadline = 10 * time.Minute
	const retry = time.Second

	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 4})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ledger.Close()) })

	tracker := newSoakFailureTracker(
		ledger, 0, deadline, func() time.Time { return now },
	)
	pending := testSoakFailurePending(now)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, MessageID: pending.MessageID,
	}))

	verifier := &alwaysMissingSoakHistoryVerifier{}
	reconciler := newSoakFailureReconciler(
		ledger, verifier, retry, func() time.Time { return now },
	)

	// Drive the run the way it actually runs: the reconciler claims whatever is
	// due, and the expiry sweep ticks on its own interval regardless.
	sweep := soakFailureExpiryInterval(deadline)
	nextSweep := start.Add(sweep)
	for step := 0; step < 4000 && ledger.Snapshot().Active > 0; step++ {
		if !now.Before(nextSweep) {
			_, expireErr := ledger.Expire(now)
			require.NoError(t, expireErr)
			nextSweep = now.Add(sweep)
			continue
		}
		if _, _, tryErr := reconciler.Try(context.Background()); tryErr != nil {
			require.NoError(t, tryErr)
		}
		now = now.Add(retry)
	}

	snapshot := ledger.Snapshot()
	require.Zero(t, snapshot.Active, "the operation must have been finalized")
	assert.Equal(t, uint64(1), snapshot.Results[failureResultMissingAfterDeadline],
		"an accepted message that never persisted is data loss, not an inconclusive window")
	assert.Zero(t, snapshot.Results[failureResultUnverified],
		"unverified here would mean the sweep retired it before anything looked")
	assert.Positive(t, verifier.calls, "the verdict must come from an actual query")
}

// The probe schedule must leave the reconciler a claim to make once the
// deadline arrives. Sending it far past the deadline hands the operation to the
// expiry sweep, which cannot query and so cannot tell loss from an unread
// window.
func TestNextReconcileProbe_LandsOnTheDeadlineWhenNoProbeStillFits(t *testing.T) {
	verifyAfter := time.Unix(1000, 0).UTC()
	deadline := verifyAfter.Add(10 * time.Minute)
	const retry = time.Second

	now := deadline.Add(-retry)

	got := nextReconcileProbe(now, verifyAfter, deadline, retry)

	assert.Equal(t, deadline, got,
		"the terminal probe belongs at the deadline, not beyond the sweep that follows it")
}

type alwaysMissingSoakHistoryVerifier struct{ calls int }

func (v *alwaysMissingSoakHistoryVerifier) Verify(
	context.Context, *failureOperation,
) (soakFailureHistoryResult, error) {
	v.calls++
	return soakFailureHistoryMissing, nil
}
