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

// A probe that could not be answered has established nothing about the message,
// so it is not a pending effect and must not inherit the pending effect's
// backoff. One timeout nine minutes into a ten-minute deadline would otherwise
// push the next look past the window, and the operation would resolve on the
// retry policy rather than on storage. The branch already tells the two apart
// for the claim metric; the schedule has to agree.
func TestSoakFailureReconciler_AnUnansweredProbeRetriesFlatNotBackedOff(t *testing.T) {
	started := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	const grace = 10 * time.Second
	const deadline = 10 * time.Minute
	const retry = time.Second

	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ledger.Close()) })
	tracker := newSoakFailureTracker(
		ledger, grace, deadline, func() time.Time { return started },
	)
	pending := testSoakFailurePending(started)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, MessageID: pending.MessageID,
	}))

	// Five minutes of waiting already banked, so the backoff would reschedule
	// five minutes out and the flat interval one second out.
	now := started.Add(grace).Add(5 * time.Minute)
	reconciler := newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{err: errors.New("history service unreachable")},
		retry,
		func() time.Time { return now },
	)

	_, _, err = reconciler.Try(context.Background())
	require.NoError(t, err)

	operation := ledger.active[pending.MessageID]
	require.NotNil(t, operation)
	assert.Equal(t, now.Add(retry), operation.nextVerifyAt,
		"an unanswered probe retries the call, and a failed call keeps the flat interval")
}

// The invalidation is only durable once the journal has it. Marking the reason
// recorded before the append succeeds, and then deduplicating on that mark,
// makes a transient append failure permanent: nothing retries it, Invalidate
// cannot report it, and a restart replays the evidence without the verdict that
// disqualifies it.
func TestFailureLedger_AnInvalidationWhoseAppendFailedIsRetriedNotForgotten(t *testing.T) {
	journal := &appendFailingFailureJournal{failFor: failureLedgerEventInvalidated}
	metrics := NewMetrics()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 4, Journal: journal, Recorder: newFailureLedgerPromRecorder(metrics),
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ledger.Close()) })

	ledger.Invalidate(invalidReasonReconcileCapacity)
	require.Equal(t, 1, journal.attempts, "the first append is attempted")

	journal.failFor = ""
	ledger.Invalidate(invalidReasonReconcileCapacity)

	assert.Equal(t, 2, journal.attempts,
		"a reason that never reached the journal must be retried, not deduplicated away")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues(invalidReasonReconcileCapacity)),
		"the counter still reports the cause once, however many appends it took")
}

type appendFailingFailureJournal struct {
	memoryFailureJournal
	failFor  string
	attempts int
}

// Non-zero so the byte-budget compaction trigger can fire; the embedded memory
// journal reports 0, which would make MaybeCompact a silent no-op.
func (*appendFailingFailureJournal) Size() int64 { return 1024 }

func (j *appendFailingFailureJournal) Append(event *failureLedgerEvent) error {
	if event.Type == failureLedgerEventInvalidated {
		j.attempts++
		if j.failFor == event.Type {
			return errors.New("no space left on device")
		}
	}
	return j.memoryFailureJournal.Append(event)
}
