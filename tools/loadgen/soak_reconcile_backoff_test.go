package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The history probe reschedules itself whenever the message has not landed yet.
// At a flat RECONCILE_RETRY_INTERVAL of 1s against a 10m deadline that is ~590
// claims for a single unresolved operation, so a small fraction of late
// messages can demand more reconcile capacity than the whole read lane has.
//
// The interval instead grows to match the time already spent waiting, which
// doubles it on every probe without storing an attempt count anywhere — it is
// derived from VerifyAfter and now, both of which the ledger already persists,
// so recovery after a restart needs no new state.
func TestNextReconcileProbe_GrowsWithTheWaitAlreadySpent(t *testing.T) {
	verifyAfter := time.Unix(1000, 0).UTC()
	deadline := verifyAfter.Add(10 * time.Minute)
	const retry = time.Second

	tests := []struct {
		name string
		now  time.Time
		want time.Duration
	}{
		{name: "first probe waits the configured interval", now: verifyAfter, want: retry},
		{name: "a sub-interval wait still waits the floor", now: verifyAfter.Add(300 * time.Millisecond), want: retry},
		{name: "four seconds in, wait four", now: verifyAfter.Add(4 * time.Second), want: 4 * time.Second},
		{name: "two minutes in, wait two", now: verifyAfter.Add(2 * time.Minute), want: 2 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.now.Add(tc.want),
				nextReconcileProbe(tc.now, verifyAfter, deadline, retry))
		})
	}
}

// Backing off past the deadline would mean the operation expires without the
// probe that could still have found a late message, turning a delivered
// message into missing_after_deadline. The last probe is pulled back to sit
// one interval inside the deadline.
func TestNextReconcileProbe_KeepsAFinalProbeInsideTheDeadline(t *testing.T) {
	verifyAfter := time.Unix(1000, 0).UTC()
	deadline := verifyAfter.Add(10 * time.Minute)
	const retry = time.Second

	now := verifyAfter.Add(9 * time.Minute)

	got := nextReconcileProbe(now, verifyAfter, deadline, retry)

	assert.True(t, got.Before(deadline), "a doubled wait would overshoot the deadline")
	assert.Equal(t, deadline.Add(-retry), got)
}

// Once there is no room left for another probe the operation must be allowed to
// expire, rather than being rescheduled into a tight loop against a deadline it
// has already effectively reached.
func TestNextReconcileProbe_StopsWhenNoProbeStillFits(t *testing.T) {
	verifyAfter := time.Unix(1000, 0).UTC()
	deadline := verifyAfter.Add(10 * time.Minute)
	const retry = time.Second

	now := deadline.Add(-retry)

	got := nextReconcileProbe(now, verifyAfter, deadline, retry)

	assert.False(t, got.Before(deadline),
		"with no room for a probe the next attempt must fall at or past the deadline")
}

// The whole point is the count. A flat interval spends ~590 claims on one
// operation; the same walk under the doubling schedule has to cost an order of
// magnitude less, or the reconcile budget is still set by the miss rate.
func TestNextReconcileProbe_BoundsTheProbesOneOperationCosts(t *testing.T) {
	verifyAfter := time.Unix(1000, 0).UTC()
	deadline := verifyAfter.Add(10 * time.Minute)
	const retry = time.Second

	probes := 0
	for now := verifyAfter; now.Before(deadline); probes++ {
		next := nextReconcileProbe(now, verifyAfter, deadline, retry)
		if !next.After(now) {
			t.Fatalf("probe %d did not advance: %s", probes, next)
		}
		now = next
	}

	t.Logf("probes to deadline: %d", probes)
	assert.Less(t, probes, 20,
		"a doubling schedule must cost far less than the ~590 a flat 1s interval does")
	assert.Greater(t, probes, 5, "backing off this hard would find a late message too late")
}

// The schedule only matters if Try actually uses it. A reconciler whose message
// never lands must reschedule by the doubling walk, not by the flat interval —
// this is the assertion that fails if the call site is reverted while
// nextReconcileProbe keeps its own tests green.
func TestSoakFailureReconciler_BacksOffThePollForAMessageThatHasNotLanded(t *testing.T) {
	started := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	const grace = 10 * time.Second
	const deadline = 10 * time.Minute
	const retry = time.Second

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

	// A minute past the point the probe first became due, so a flat interval
	// would reschedule one second out and the doubling walk a minute out.
	verifyAfter := started.Add(grace)
	now := verifyAfter.Add(time.Minute)
	reconciler := newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryMissing}},
		retry,
		func() time.Time { return now },
	)

	ran, _, err := reconciler.Try(context.Background())

	require.NoError(t, err)
	require.True(t, ran)
	operation := ledger.active[pending.MessageID]
	require.NotNil(t, operation, "an operation before its deadline must stay active")
	assert.Equal(t, now.Add(time.Minute), operation.nextVerifyAt,
		"the poll must wait the time already spent waiting, not the flat interval")
}
