package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func reviewFailureOperation(id string, now time.Time) *failureOperation {
	return &failureOperation{
		ID: id, Scenario: "cassandra_soak", Lane: "message_send",
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}
}

func TestFailureLedger_AbandonFinalizesWithoutDataLoss(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("never-sent", now)))

	require.NoError(t, ledger.Abandon("never-sent", failureResultNotSent, now))

	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[failureResultNotSent])
	assert.Zero(t, snapshot.Results[failureResultMissingAfterDeadline])

	expired, err := ledger.Expire(now.Add(time.Hour))
	require.NoError(t, err)
	assert.Zero(t, expired)
}

func TestFailureLedger_ActivatedPublishCannotBecomeNotSent(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	operation := testFailureOperation("message-1", now)
	operation.LifecycleState = failureOperationJournaled
	require.NoError(t, ledger.Start(operation))
	require.NoError(t, ledger.Activate(operation.ID, now))

	err = ledger.Abandon(operation.ID, failureResultNotSent, now)

	assert.ErrorContains(t, err, "publish was attempted")
	assert.Equal(t, 1, ledger.Snapshot().Active)
}

func TestFailureLedger_AbandonRejectsUnknownOperationAndResult(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("known", now)))

	require.Error(t, ledger.Abandon("missing", failureResultNotSent, now))
	require.Error(t, ledger.Abandon("known", failureResult("bogus"), now))
	assert.Equal(t, 1, ledger.Snapshot().Active)
}

func TestFailureLedger_ExpireSkipsClaimedOperation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("claimed", now)))

	_, ok := ledger.ClaimDue(now)
	require.True(t, ok)

	finalized, err := ledger.Expire(now.Add(time.Hour))
	require.NoError(t, err)
	assert.Zero(t, finalized, "an in-flight verification must not be finalized underneath the reconciler")
	assert.Equal(t, 1, ledger.Snapshot().Active)

	// The successful read-back still resolves the operation.
	_, err = ledger.Observe("claimed", failureObserverAdmission, failureObservationGood, now)
	require.NoError(t, err)
	_, err = ledger.Observe("claimed", failureObserverHistory, failureObservationGood, now)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ledger.Snapshot().Results[failureResultGood])
}

func TestFailureLedger_ExpireFinalizesReleasedClaim(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("released", now)))
	_, ok := ledger.ClaimDue(now)
	require.True(t, ok)
	require.NoError(t, ledger.ReleaseClaim("released", now))

	finalized, err := ledger.Expire(now.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, finalized)
}

func TestFailureLedger_HistoryObservationReleasesClaimForAdmissionExpiry(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("reply-missing", now)))

	_, claimed := ledger.ClaimDue(now)
	require.True(t, claimed)
	finalized, err := ledger.Observe(
		"reply-missing", failureObserverHistory, failureObservationGood, now,
	)
	require.NoError(t, err)
	assert.False(t, finalized)

	expired, err := ledger.Expire(now.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, expired)
	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[failureResultUnverified])
	assert.Zero(t, snapshot.Results[failureResultMissingAfterDeadline])
}

func TestFailureLedger_ClaimDueReturnsEarliestDueOperation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 4, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	for _, spec := range []struct {
		id     string
		verify time.Duration
	}{{"late", 30 * time.Second}, {"early", time.Second}, {"middle", 5 * time.Second}} {
		operation := reviewFailureOperation(spec.id, now)
		operation.VerifyAfter = now.Add(spec.verify)
		require.NoError(t, ledger.Start(operation))
	}

	_, ok := ledger.ClaimDue(now)
	assert.False(t, ok, "nothing is due before the earliest verify time")

	claimed, ok := ledger.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "early", claimed.ID)
	require.NoError(t, ledger.ReleaseClaim("early", now.Add(time.Minute)))

	claimed, ok = ledger.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "middle", claimed.ID)

	_, ok = ledger.ClaimDue(now.Add(10 * time.Second))
	assert.False(t, ok, "the remaining operations are not due yet")
}

func TestFailureLedger_ClaimDueSkipsObservedHistory(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("observed", now)))

	_, err = ledger.Observe("observed", failureObserverHistory, failureObservationGood, now)
	require.NoError(t, err)

	_, ok := ledger.ClaimDue(now)
	assert.False(t, ok)
}

func TestFailureLedger_ClaimDueIgnoresOperationsWithoutHistoryObserver(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	operation := reviewFailureOperation("admission-only", now)
	operation.Expected = []failureObserver{failureObserverAdmission}
	require.NoError(t, ledger.Start(operation))

	_, ok := ledger.ClaimDue(now.Add(time.Second))
	assert.False(t, ok)
}

func TestFailureLedger_UnverifiedObservationDoesNotReportDataLoss(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("unverifiable", now)))

	_, err = ledger.Observe("unverifiable", failureObserverAdmission, failureObservationGood, now)
	require.NoError(t, err)
	finalized, err := ledger.Observe(
		"unverifiable", failureObserverHistory, failureObservationUnverified, now,
	)
	require.NoError(t, err)
	require.True(t, finalized)

	snapshot := ledger.Snapshot()
	assert.Equal(t, uint64(1), snapshot.Results[failureResultUnverified])
	assert.Zero(t, snapshot.Results[failureResultMissingAfterDeadline])
}

func TestFailureOperationResult_Precedence(t *testing.T) {
	tests := []struct {
		name         string
		lifecycle    failureOperationLifecycle
		observations map[failureObserver]failureObservation
		want         failureResult
	}{
		{
			name: "all good",
			observations: map[failureObserver]failureObservation{
				failureObserverAdmission: failureObservationGood,
				failureObserverHistory:   failureObservationGood,
			},
			want: failureResultGood,
		},
		{
			name:      "accepted missing effect outranks everything",
			lifecycle: failureOperationActive,
			observations: map[failureObserver]failureObservation{
				failureObserverAdmission: failureObservationGood,
				failureObserverHistory:   failureObservationMissingAfterDeadline,
			},
			want: failureResultMissingAfterDeadline,
		},
		{
			name:      "rejected admission prevents missing claim",
			lifecycle: failureOperationActive,
			observations: map[failureObserver]failureObservation{
				failureObserverAdmission: failureObservationBad,
				failureObserverHistory:   failureObservationMissingAfterDeadline,
			},
			want: failureResultBad,
		},
		{
			name:      "ambiguous admission prevents missing claim",
			lifecycle: failureOperationActive,
			observations: map[failureObserver]failureObservation{
				failureObserverAdmission: failureObservationUnverified,
				failureObserverHistory:   failureObservationMissingAfterDeadline,
			},
			want: failureResultUnverified,
		},
		{
			name: "bad outranks unverified",
			observations: map[failureObserver]failureObservation{
				failureObserverAdmission: failureObservationBad,
				failureObserverHistory:   failureObservationUnverified,
			},
			want: failureResultBad,
		},
		{
			name: "unverified outranks good",
			observations: map[failureObserver]failureObservation{
				failureObserverAdmission: failureObservationGood,
				failureObserverHistory:   failureObservationUnverified,
			},
			want: failureResultUnverified,
		},
		{
			name: "missing admission reply with persisted history is unverified",
			observations: map[failureObserver]failureObservation{
				failureObserverAdmission: failureObservationMissingAfterDeadline,
				failureObserverHistory:   failureObservationGood,
			},
			want: failureResultUnverified,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, failureOperationResult(&failureOperation{
				LifecycleState: test.lifecycle,
				Observations:   test.observations,
			}))
		})
	}
}

func TestFailureOperationResult_MissingWithoutAcceptedPublishIsUnverified(t *testing.T) {
	operation := testFailureOperation("message-1", time.Now().UTC())
	operation.LifecycleState = failureOperationJournaled
	operation.Observations = map[failureObserver]failureObservation{
		failureObserverAdmission: failureObservationUnverified,
		failureObserverHistory:   failureObservationMissingAfterDeadline,
	}

	assert.Equal(t, failureResultUnverified, failureOperationResult(operation))
}

func TestFailureOperationResult_JournaledMissingDoesNotHideBadEvidence(t *testing.T) {
	operation := testFailureOperation("message-1", time.Now().UTC())
	operation.LifecycleState = failureOperationJournaled
	operation.Observations = map[failureObserver]failureObservation{
		failureObserverAdmission: failureObservationBad,
		failureObserverHistory:   failureObservationMissingAfterDeadline,
	}

	for range 100 {
		assert.Equal(t, failureResultBad, failureOperationResult(operation))
	}
}

func TestFailureLedger_ReplayDegradesInsteadOfCrashLoopingOverCapacity(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	directory := t.TempDir()
	walPath := filepath.Join(directory, "ledger.wal")

	wal, err := openFailureWAL(walPath)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 4, Journal: wal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	for _, id := range []string{"one", "two", "three"} {
		require.NoError(t, ledger.Start(reviewFailureOperation(id, now)))
	}
	require.NoError(t, ledger.Close())

	// A retained PVC keeps the WAL across a capacity reduction; startup must
	// degrade the ledger rather than fail and crash-loop the pod.
	reopenedWAL, err := openFailureWAL(walPath)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Journal: reopenedWAL, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })

	snapshot := recovered.Snapshot()
	assert.Equal(t, 1, snapshot.Active)
	assert.Equal(t, "capacity", snapshot.InvalidReason)
	assert.Equal(t, 2, snapshot.Dropped)
}

func TestFailureLedger_ReplayDropsFollowUpEventsForDroppedOperations(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	directory := t.TempDir()
	walPath := filepath.Join(directory, "ledger.wal")

	wal, err := openFailureWAL(walPath)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 4, Journal: wal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("kept", now)))
	require.NoError(t, ledger.Start(reviewFailureOperation("dropped", now)))
	_, err = ledger.Observe("dropped", failureObserverAdmission, failureObservationGood, now)
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	reopenedWAL, err := openFailureWAL(walPath)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Journal: reopenedWAL, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	assert.Equal(t, 1, recovered.Snapshot().Active)
	assert.Equal(t, 1, recovered.Snapshot().Dropped)
}
