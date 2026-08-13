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

type stubFailureHistoryVerifier struct {
	results []soakFailureHistoryResult
	errs    []error
	calls   int
}

func (s *stubFailureHistoryVerifier) Verify(
	context.Context,
	*failureOperation,
) (soakFailureHistoryResult, error) {
	index := s.calls
	s.calls++
	if index < len(s.errs) && s.errs[index] != nil {
		return "", s.errs[index]
	}
	if index < len(s.results) {
		return s.results[index], nil
	}
	return soakFailureHistoryFound, nil
}

func newReviewReconcilerLedger(t *testing.T, now time.Time) *failureLedger {
	t.Helper()
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 4, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(reviewFailureOperation("message-1", now)))
	_, err = ledger.Observe("message-1", failureObserverAdmission, failureObservationGood, now)
	require.NoError(t, err)
	return ledger
}

func TestSoakFailureReconciler_UnavailableVerifierIsNotDataLoss(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger := newReviewReconcilerLedger(t, now)
	verifier := &stubFailureHistoryVerifier{errs: []error{errors.New("history service unavailable")}}
	reconciler := newSoakFailureReconciler(
		ledger, verifier, time.Second,
		func() time.Time { return now.Add(2 * time.Minute) },
	)

	reconciled, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, reconciled)

	snapshot := ledger.Snapshot()
	assert.Equal(t, uint64(1), snapshot.Results[failureResultUnverified])
	assert.Zero(
		t, snapshot.Results[failureResultMissingAfterDeadline],
		"an unreachable history service must not be reported as data loss",
	)
}

func TestSoakFailureReconciler_ConfirmedMissingAfterDeadlineIsDataLoss(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger := newReviewReconcilerLedger(t, now)
	verifier := &stubFailureHistoryVerifier{
		results: []soakFailureHistoryResult{soakFailureHistoryMissing},
	}
	reconciler := newSoakFailureReconciler(
		ledger, verifier, time.Second,
		func() time.Time { return now.Add(2 * time.Minute) },
	)

	reconciled, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(
		t, uint64(1),
		ledger.Snapshot().Results[failureResultMissingAfterDeadline],
	)
}

func TestSoakFailureReconciler_RetriesBeforeDeadline(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	tests := []struct {
		name     string
		verifier *stubFailureHistoryVerifier
	}{
		{
			name:     "verifier unavailable",
			verifier: &stubFailureHistoryVerifier{errs: []error{errors.New("unavailable")}},
		},
		{
			name: "history missing",
			verifier: &stubFailureHistoryVerifier{
				results: []soakFailureHistoryResult{soakFailureHistoryMissing},
			},
		},
		{
			name: "content mismatch",
			verifier: &stubFailureHistoryVerifier{
				results: []soakFailureHistoryResult{soakFailureHistoryMismatch},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := newReviewReconcilerLedger(t, now)
			reconciler := newSoakFailureReconciler(
				ledger, test.verifier, time.Second,
				func() time.Time { return now.Add(time.Second) },
			)

			reconciled, err := reconciler.Try(context.Background())
			require.NoError(t, err)
			assert.True(t, reconciled)

			snapshot := ledger.Snapshot()
			assert.Equal(t, 1, snapshot.Active, "the operation stays open until its deadline")
			assert.Empty(t, snapshot.Results)

			// The claim must be released so a later pass can retry it.
			_, ok := ledger.ClaimDue(now.Add(time.Minute))
			assert.True(t, ok)
		})
	}
}

func TestSoakFailureReconciler_MismatchAfterDeadlineIsCorruption(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger := newReviewReconcilerLedger(t, now)
	verifier := &stubFailureHistoryVerifier{
		results: []soakFailureHistoryResult{soakFailureHistoryMismatch},
	}
	reconciler := newSoakFailureReconciler(
		ledger, verifier, time.Second,
		func() time.Time { return now.Add(2 * time.Minute) },
	)

	_, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ledger.Snapshot().Results[failureResultBad])
}

func TestSoakFailureTracker_AbandonMarksSendAsNeverPublished(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 2})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
	)
	pending := &soakPendingSend{
		Kind: soakSendTopLevel, MessageID: "message-1", RequestID: "request-1",
		Target:  soakSendTarget{Account: "alice", RoomID: "room-1"},
		Content: "hello", PublishedAt: now,
	}
	require.NoError(t, tracker.Start(pending))

	require.NoError(t, tracker.AbandonUnsent(pending))

	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[failureResultNotSent])
	assert.Zero(t, snapshot.Results[failureResultMissingAfterDeadline])
}

func TestSoakFailureTracker_ObserveReplyIgnoresUnmatchedReply(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
	)

	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyUnmatched, RequestID: "request-1",
	}))
}

func TestSoakFailureTracker_ObserveReplyToleratesUntrackedOperation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	metrics := NewMetrics()
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
		withSoakFailureMetrics(metrics),
	)

	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, RequestID: "request-1", MessageID: "never-started",
	}))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureUntracked.WithLabelValues(failureUntrackedReasonObserve),
	))
}

func TestSoakFailureTracker_ObserveReplyStillRejectsMissingMessageID(t *testing.T) {
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(ledger, 0, time.Minute, time.Now)

	require.Error(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, RequestID: "request-1",
	}))
}

func TestSoakShareGate_AdmitsConfiguredFraction(t *testing.T) {
	tests := []struct {
		name  string
		share float64
		calls int
		want  int
	}{
		{name: "half", share: 0.5, calls: 10, want: 5},
		{name: "quarter", share: 0.25, calls: 12, want: 3},
		{name: "all", share: 1, calls: 7, want: 7},
		{name: "clamped below zero", share: -1, calls: 5, want: 0},
		{name: "clamped above one", share: 4, calls: 5, want: 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gate := newSoakShareGate(test.share)
			allowed := 0
			for range test.calls {
				if gate.Allow() {
					allowed++
				}
			}
			assert.Equal(t, test.want, allowed)
		})
	}
}

func TestSoakFailureTracker_AbandonUnsentRejectsInvalidInput(t *testing.T) {
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)

	require.Error(t, newSoakFailureTracker(nil, 0, time.Minute, nil).AbandonUnsent(nil))

	tracker := newSoakFailureTracker(ledger, 0, time.Minute, time.Now)
	require.Error(t, tracker.AbandonUnsent(nil))
	require.Error(t, tracker.AbandonUnsent(&soakPendingSend{RequestID: "request-1"}))
}

func TestSoakFailureTracker_AbandonUnsentToleratesUntrackedOperation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	metrics := NewMetrics()
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
		withSoakFailureMetrics(metrics),
	)

	require.NoError(t, tracker.AbandonUnsent(&soakPendingSend{
		MessageID: "never-started", RequestID: "request-1",
	}))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureUntracked.WithLabelValues(failureUntrackedReasonAbandon),
	))
}

func TestSoakFailureTracker_AbandonUnsentSurfacesLedgerFailure(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
	)
	require.NoError(t, tracker.Start(&soakPendingSend{
		MessageID: "message-1", RequestID: "request-1",
		Target:  soakSendTarget{Account: "alice", RoomID: "room-1"},
		Content: "hello", PublishedAt: now,
	}))
	require.NoError(t, ledger.Close())

	require.Error(t, tracker.AbandonUnsent(&soakPendingSend{
		MessageID: "message-1", RequestID: "request-1",
	}))
}

func TestSoakFailureReconciler_ReleasesClaimWhenObservationFails(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger := newReviewReconcilerLedger(t, now)
	// A closed ledger fails the observation write after the claim was taken.
	require.NoError(t, ledger.Close())
	reconciler := newSoakFailureReconciler(
		ledger,
		&stubFailureHistoryVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second,
		func() time.Time { return now.Add(2 * time.Minute) },
	)

	reconciled, err := reconciler.Try(context.Background())
	assert.True(t, reconciled)
	require.Error(t, err)
}

func TestSoakFailureReconciler_TryRequiresConfiguration(t *testing.T) {
	_, err := newSoakFailureReconciler(nil, nil, 0, nil).Try(context.Background())
	require.Error(t, err)
}
