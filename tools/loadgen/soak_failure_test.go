package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestSoakFailureTracker_StartsDurableMessageOperation(t *testing.T) {
	assert.Equal(t, "message_soak", soakFailureScenario)
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	observer := newFailureRecipientObserver(ledger, nil, 1, func() time.Time { return now })
	tracker := newSoakFailureTracker(
		ledger, 10*time.Second, time.Minute, func() time.Time { return now },
		withSoakFailureRunID("run-1"),
		withSoakFailureRecipientObserver(observer),
	)

	require.NoError(t, tracker.Start(&soakPendingSend{
		Kind: soakSendTopLevel, MessageID: "message-1", RequestID: "request-1",
		Target:  soakSendTarget{Account: "alice", RoomID: "room-1", RoomType: model.RoomTypeChannel, Recipients: []string{"alice", "bob"}},
		Content: "secret message", PublishedAt: now,
	}))

	operation, ok := ledger.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "request-1", operation.CorrelationID)
	assert.Equal(t, "room-1", operation.Attributes[soakFailureAttributeRoomID])
	assert.Equal(t, "alice", operation.Attributes[soakFailureAttributeAccount])
	assert.NotEmpty(t, operation.Attributes[soakFailureAttributeContentSHA256])
	assert.Equal(t, string(recipientExpectedRouteRoom), operation.Attributes[soakFailureAttributeRecipientRoute])
	assert.NotContains(t, operation.Attributes, "content")
	assert.Equal(t, 2, operation.SchemaVersion)
	assert.Equal(t, "run-1", operation.RunID)
	assert.Equal(t, soakFailureScenario, operation.Scenario)
	assert.Equal(t, failureOperationMessageCreate, operation.OperationType)
	assert.Equal(t, "message-1", operation.Targets["messageId"])
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverHistory, failureObserverRecipient}, operation.Expected)
	assert.Len(t, operation.Effects, 3)
}

func TestSoakFailureExpiryLoop_FinalizesPastDeadline(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ticks := make(chan time.Time, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSoakFailureExpiry(ctx, ledger, ticks, nil)
	}()
	ticks <- now.Add(2 * time.Minute)

	require.Eventually(t, func() bool {
		snapshot := ledger.Snapshot()
		return snapshot.Active == 0 && snapshot.Results[failureResultUnverified] == 1
	}, time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestSoakFailureExpiryLoop_ReportsPersistenceFailureAndStopsOnCancel(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	journal := &failingFailureJournal{err: errors.New("disk full")}
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))
	ledger.journal = journal

	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time, 1)
	errorsSeen := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSoakFailureExpiry(ctx, ledger, ticks, func(err error) { errorsSeen <- err })
	}()
	ticks <- now.Add(2 * time.Minute)
	require.ErrorContains(t, <-errorsSeen, "expire failure operations")
	cancel()
	<-done
}

func TestSoakFailureTracker_ForgetsRecipientExpectationWhenLedgerRejectsStart(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("occupied", now)))
	observer := newFailureRecipientObserver(ledger, nil, 1, func() time.Time { return now })
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
		withSoakFailureRecipientObserver(observer),
	)
	pending := testSoakFailurePending(now)
	pending.Target.Recipients = []string{"alice"}

	require.Error(t, tracker.Start(pending))
	assert.False(t, observer.evidence.Observe(pending.MessageID, "alice"))
}

func TestSoakFailureTracker_RegistersRecipientExpectation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	observer := newFailureRecipientObserver(ledger, NewMetrics(), 2, func() time.Time { return now })
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
		withSoakFailureRecipientObserver(observer),
	)
	pending := testSoakFailurePending(now)
	pending.Target.Recipients = []string{"alice", "bob"}
	require.NoError(t, tracker.Start(pending))
	assert.True(t, observer.evidence.Observe("message-1", "alice"))
	assert.False(t, observer.evidence.Complete("message-1"))
}

func TestSoakFailureTracker_PersistsRecipientExpectationSemantics(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	observer := newFailureRecipientObserver(ledger, nil, 1, func() time.Time { return now })
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
		withSoakFailureRecipientObserver(observer),
	)
	pending := testSoakFailurePending(now)
	pending.Kind = soakSendThreadReply
	pending.Target.RoomType = model.RoomTypeChannel
	pending.Target.Recipients = []string{"alice", "bob"}
	pending.Target.RecipientSetSource = recipientSetSourceThreadFollowers
	pending.Target.RecipientSetComplete = true
	pending.Target.RecipientRoute = recipientExpectedRouteUser

	require.NoError(t, tracker.Start(pending))
	operation, ok := ledger.Active(pending.MessageID)
	require.True(t, ok)
	assert.Equal(t, string(recipientSetSourceThreadFollowers), operation.Attributes[soakFailureAttributeRecipientSource])
	assert.Equal(t, "true", operation.Attributes[soakFailureAttributeRecipientComplete])
	assert.Equal(t, string(recipientExpectedRouteUser), operation.Attributes[soakFailureAttributeRecipientRoute])
}

func TestSoakFailureTracker_DisabledRecipientObserverOmitsExactSetFromWAL(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(ledger, 0, time.Minute, func() time.Time { return now })
	pending := testSoakFailurePending(now)
	pending.Target.Recipients = []string{"alice", "bob"}

	require.NoError(t, tracker.Start(pending))
	operation, ok := ledger.Active(pending.MessageID)
	require.True(t, ok)
	assert.NotContains(t, operation.Attributes, "expected_recipients")
	assert.NotContains(t, operation.Attributes, soakFailureAttributeRecipientEvent)
}

func TestSoakOpenFailureLedger_PersistentRecovery(t *testing.T) {
	now := time.Now().UTC()
	cfg := validSoakConfig(t)
	cfg.LedgerDir = t.TempDir()
	ledger, err := openSoakFailureLedger(&cfg, NewMetrics(), func() time.Time { return now })
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))
	require.NoError(t, ledger.Close())

	recovered, err := openSoakFailureLedger(
		&cfg,
		NewMetrics(),
		func() time.Time { return now.Add(time.Second) },
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	assert.Equal(t, 1, recovered.Snapshot().Recovered)
}

func TestSoakOpenFailureLedger_RejectsInvalidConfiguration(t *testing.T) {
	_, err := openSoakFailureLedger(nil, NewMetrics(), time.Now)
	require.Error(t, err)

	directory := t.TempDir()
	filePath := filepath.Join(directory, "not-a-directory")
	require.NoError(t, os.WriteFile(filePath, []byte("occupied"), 0o600))
	cfg := validSoakConfig(t)
	cfg.LedgerDir = filePath
	_, err = openSoakFailureLedger(&cfg, NewMetrics(), time.Now)
	require.Error(t, err)
}

func TestSoakFailureTracker_RejectsInvalidInputs(t *testing.T) {
	tracker := newSoakFailureTracker(nil, 0, 0, nil)
	require.Error(t, tracker.Start(nil))
	require.Error(t, tracker.ObserveReply(nil))

	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker = newSoakFailureTracker(ledger, 0, time.Minute, time.Now)
	require.Error(t, tracker.Start(nil))
	require.EqualError(t, tracker.ObserveReply(nil), "soak send result is required")
	require.Error(t, tracker.ObserveReply(&soakSendReplyResult{}))
}

func TestSoakFailureReconciler_AcceptedAndPersistedIsGood(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
	)
	pending := testSoakFailurePending(now)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, MessageID: "message-1",
	}))
	reconciler := newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second,
		func() time.Time { return now },
	)

	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[failureResultGood])
}

func TestSoakFailureReconciler_FinalizesRecipientAtDeadlineWithoutRepeatingHistoryQuery(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	recipientObserver := newFailureRecipientObserver(ledger, nil, 1, func() time.Time { return now })
	tracker := newSoakFailureTracker(
		ledger, 0, 10*time.Second, func() time.Time { return now },
		withSoakFailureRecipientObserver(recipientObserver),
	)
	pending := testSoakFailurePending(now)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	_, err = ledger.Observe("message-1", failureObserverAdmission, failureObservationGood, now)
	require.NoError(t, err)
	_, err = ledger.Observe("message-1", failureObserverHistory, failureObservationGood, now)
	require.NoError(t, err)

	finalizer := &fakeSoakRecipientFinalizer{result: recipientEvidenceResult{
		Observation: failureObservationMissingAfterDeadline,
		Missing:     []string{"bob"},
	}}
	reconciler := newSoakFailureReconciler(
		ledger, &fakeSoakFailureVerifier{err: errors.New("must not query")}, time.Second,
		func() time.Time { return now }, withSoakFailureRecipientFinalizer(finalizer),
	)
	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.False(t, ran)
	now = now.Add(10 * time.Second)
	ran, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, 1, finalizer.calls)
	assert.Equal(t, uint64(1), ledger.Snapshot().Results[failureResultMissingAfterDeadline])
}

func TestSoakFailureReconciler_UnverifiedRecipientDoesNotUseNegativeReason(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	recipientObserver := newFailureRecipientObserver(ledger, nil, 1, func() time.Time { return now })
	tracker := newSoakFailureTracker(
		ledger, 0, 10*time.Second, func() time.Time { return now },
		withSoakFailureRecipientObserver(recipientObserver),
	)
	pending := testSoakFailurePending(now)
	pending.Target.Recipients = []string{"alice", "bob"}
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	_, err = ledger.Observe("message-1", failureObserverAdmission, failureObservationGood, now)
	require.NoError(t, err)
	_, err = ledger.Observe("message-1", failureObserverHistory, failureObservationGood, now)
	require.NoError(t, err)

	finalizer := &fakeSoakRecipientFinalizer{result: recipientEvidenceResult{
		Observation: failureObservationUnverified,
		Missing:     []string{"bob"},
	}}
	reconciler := newSoakFailureReconciler(
		ledger, &fakeSoakFailureVerifier{err: errors.New("must not query")}, time.Second,
		func() time.Time { return now }, withSoakFailureRecipientFinalizer(finalizer),
	)
	now = now.Add(10 * time.Second)

	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, uint64(1), ledger.Snapshot().Results[failureResultUnverified])
}

func TestSoakFailureReconciler_TimeoutButPersistedPreservesAvailabilityFailure(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
	)
	pending := testSoakFailurePending(now)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyRejected, MessageID: "message-1",
		ErrorClass: soakErrorTimeout,
	}))
	reconciler := newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second,
		func() time.Time { return now },
	)

	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, uint64(1), ledger.Snapshot().Results[failureResultUnverified])
}

func TestSoakFailureTracker_ActivatesDurableIntent(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	journal := &memoryFailureJournal{}
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1, Journal: journal})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(ledger, 0, time.Minute, func() time.Time { return now })
	pending := testSoakFailurePending(now)
	require.NoError(t, tracker.Start(pending))

	require.NoError(t, tracker.Activate(pending))

	operation, ok := ledger.Active(pending.MessageID)
	require.True(t, ok)
	assert.Equal(t, failureOperationActive, operation.LifecycleState)
	require.Len(t, journal.events, 2)
	assert.Equal(t, failureLedgerEventActivated, journal.events[1].Type)
}

func TestSoakFailureTracker_ActivateRejectsInvalidInputs(t *testing.T) {
	assert.Error(t, (*soakFailureTracker)(nil).Activate(nil))
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(ledger, 0, time.Minute, time.Now)
	assert.Error(t, tracker.Activate(nil))
	assert.Error(t, tracker.Activate(&soakPendingSend{}))
	assert.Error(t, tracker.Activate(&soakPendingSend{MessageID: "unknown"}))
}

func TestSoakFailureReconciler_RetriesMissingUntilDeadline(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, 10*time.Second, func() time.Time { return now },
	)
	pending := testSoakFailurePending(now)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, MessageID: "message-1",
	}))
	verifier := &fakeSoakFailureVerifier{results: []soakFailureHistoryResult{
		soakFailureHistoryMissing,
		soakFailureHistoryMissing,
	}}
	reconciler := newSoakFailureReconciler(
		ledger, verifier, 5*time.Second, func() time.Time { return now },
	)

	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, 1, ledger.Snapshot().Active)
	ran, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.False(t, ran)

	now = now.Add(10 * time.Second)
	ran, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(
		t, uint64(1),
		ledger.Snapshot().Results[failureResultMissingAfterDeadline],
	)
}

func TestSoakFailureReconciler_RPCFailureReleasesClaim(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, time.Minute, func() time.Time { return now },
	)
	pending := testSoakFailurePending(now)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	verifier := &fakeSoakFailureVerifier{err: errors.New("NATS unavailable")}
	reconciler := newSoakFailureReconciler(
		ledger, verifier, time.Second, func() time.Time { return now },
	)

	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, 1, ledger.Snapshot().Active)
	now = now.Add(time.Second)
	ran, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
}

func TestSoakFailureReconciler_MismatchRetriesUntilDeadlineThenFails(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(
		ledger, 0, 10*time.Second, func() time.Time { return now },
	)
	pending := testSoakFailurePending(now)
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, MessageID: "message-1",
	}))
	verifier := &fakeSoakFailureVerifier{results: []soakFailureHistoryResult{
		soakFailureHistoryMismatch,
		soakFailureHistoryMismatch,
	}}
	reconciler := newSoakFailureReconciler(
		ledger, verifier, time.Second, func() time.Time { return now },
	)

	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, 1, ledger.Snapshot().Active)

	now = now.Add(10 * time.Second)
	ran, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, uint64(1), ledger.Snapshot().Results[failureResultBad])
}

func TestSoakFailureRPCVerifier_ComparesPersistedMessage(t *testing.T) {
	contentHash := sha256.Sum256([]byte("message"))
	operation := failureOperation{
		ID: "message-1",
		Attributes: map[string]string{
			soakFailureAttributeRoomID:        "room-1",
			soakFailureAttributeAccount:       "alice",
			soakFailureAttributeContentSHA256: hex.EncodeToString(contentHash[:]),
		},
	}
	tests := []struct {
		name    string
		message soakVerifyMessage
		want    soakFailureHistoryResult
	}{
		{
			name: "matching",
			message: soakVerifyMessage{
				RoomID: "room-1", MessageID: "message-1", Msg: "message",
				Sender: cassandra.Participant{Account: "alice"},
			},
			want: soakFailureHistoryFound,
		},
		{
			name: "content mismatch",
			message: soakVerifyMessage{
				RoomID: "room-1", MessageID: "message-1", Msg: "different",
				Sender: cassandra.Participant{Account: "alice"},
			},
			want: soakFailureHistoryMismatch,
		},
		{
			name: "message ID mismatch",
			message: soakVerifyMessage{
				RoomID: "room-1", MessageID: "other", Msg: "message",
				Sender: cassandra.Participant{Account: "alice"},
			},
			want: soakFailureHistoryMismatch,
		},
		{
			name: "room mismatch",
			message: soakVerifyMessage{
				RoomID: "room-2", MessageID: "message-1", Msg: "message",
				Sender: cassandra.Participant{Account: "alice"},
			},
			want: soakFailureHistoryMismatch,
		},
		{
			name: "author mismatch",
			message: soakVerifyMessage{
				RoomID: "room-1", MessageID: "message-1", Msg: "message",
				Sender: cassandra.Participant{Account: "bob"},
			},
			want: soakFailureHistoryMismatch,
		},
		{
			name: "unexpected deletion",
			message: soakVerifyMessage{
				RoomID: "room-1", MessageID: "message-1", Deleted: true,
				Sender: cassandra.Participant{Account: "alice"},
			},
			want: soakFailureHistoryMismatch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.message)
			require.NoError(t, err)
			transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{data: data}}}
			verifier := newSoakFailureRPCVerifier(
				"site-1",
				newSoakRPCClient(
					transport,
					soakRetryConfig{MaxAttempts: 1},
					&soakRecordingSleeper{},
					nil,
				),
				nil,
				nil,
				time.Now,
			)

			got, err := verifier.Verify(context.Background(), &operation)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSoakFailureRPCVerifier_UsesCurrentCatalogStateAfterEdit(t *testing.T) {
	now := time.Now().UTC()
	catalog := newSoakCatalog(8, 100, 0, nil)
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: "message-1", RoomID: "room-1", Author: "alice",
		Content: "original", CreatedAt: now,
	}))
	require.True(t, catalog.Accept("room-1", "message-1"))
	require.True(t, catalog.MarkEdited("room-1", "message-1", "edited"))
	response, err := json.Marshal(soakVerifyMessage{
		RoomID: "room-1", MessageID: "message-1", Msg: "edited",
		Sender: cassandra.Participant{Account: "alice"}, EditedAt: &now,
	})
	require.NoError(t, err)
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{data: response}}}
	originalHash := sha256.Sum256([]byte("original"))
	operation := failureOperation{
		ID: "message-1",
		Attributes: map[string]string{
			soakFailureAttributeRoomID:        "room-1",
			soakFailureAttributeAccount:       "alice",
			soakFailureAttributeContentSHA256: hex.EncodeToString(originalHash[:]),
		},
	}
	verifier := newSoakFailureRPCVerifier(
		"site-1",
		newSoakRPCClient(
			transport,
			soakRetryConfig{MaxAttempts: 1},
			&soakRecordingSleeper{},
			nil,
		),
		catalog,
		nil,
		time.Now,
	)

	result, err := verifier.Verify(context.Background(), &operation)
	require.NoError(t, err)
	assert.Equal(t, soakFailureHistoryFound, result)
}

func TestSoakFailureRPCVerifier_ClassifiesMissingAndTransientErrors(t *testing.T) {
	contentHash := sha256.Sum256([]byte("message"))
	operation := failureOperation{
		ID: "message-1",
		Attributes: map[string]string{
			soakFailureAttributeRoomID:        "room-1",
			soakFailureAttributeAccount:       "alice",
			soakFailureAttributeContentSHA256: hex.EncodeToString(contentHash[:]),
		},
	}
	tests := []struct {
		name      string
		reply     soakRPCFakeReply
		want      soakFailureHistoryResult
		wantError bool
	}{
		{
			name:  "not found",
			reply: soakRPCFakeReply{data: []byte(`{"error":"missing","code":"not_found"}`)},
			want:  soakFailureHistoryMissing,
		},
		{
			name:      "transient timeout",
			reply:     soakRPCFakeReply{err: nats.ErrTimeout},
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{tt.reply}}
			verifier := newSoakFailureRPCVerifier(
				"site-1",
				newSoakRPCClient(
					transport,
					soakRetryConfig{MaxAttempts: 1},
					&soakRecordingSleeper{},
					nil,
				),
				nil,
				nil,
				time.Now,
			)

			got, err := verifier.Verify(context.Background(), &operation)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSoakFailureRPCVerifier_RejectsMissingDependenciesAndAttributes(t *testing.T) {
	verifier := newSoakFailureRPCVerifier("site-1", nil, nil, nil, nil)
	_, err := verifier.Verify(context.Background(), &failureOperation{})
	require.Error(t, err)

	verifier.rpc = newSoakRPCClient(
		&soakRPCFakeTransport{},
		soakRetryConfig{MaxAttempts: 1},
		&soakRecordingSleeper{},
		nil,
	)
	_, err = verifier.Verify(context.Background(), &failureOperation{ID: "message-1"})
	require.Error(t, err)
}

func TestSoakFailureReconciler_HandlesNoDueAndInvalidConfiguration(t *testing.T) {
	reconciler := newSoakFailureReconciler(nil, nil, 0, nil)
	_, err := reconciler.Try(context.Background())
	require.Error(t, err)

	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	reconciler = newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{},
		time.Second,
		time.Now,
	)
	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.False(t, ran)
}

func testSoakFailurePending(now time.Time) *soakPendingSend {
	return &soakPendingSend{
		Kind: soakSendTopLevel, MessageID: "message-1", RequestID: "request-1",
		Target:  soakSendTarget{Account: "alice", RoomID: "room-1"},
		Content: "message", PublishedAt: now,
	}
}

type fakeSoakFailureVerifier struct {
	results []soakFailureHistoryResult
	err     error
}

type fakeSoakRecipientFinalizer struct {
	result recipientEvidenceResult
	calls  int
}

func (f *fakeSoakRecipientFinalizer) Finalize(string, time.Time, time.Time) recipientEvidenceResult {
	f.calls++
	return f.result
}

func (v *fakeSoakFailureVerifier) Verify(
	context.Context,
	*failureOperation,
) (soakFailureHistoryResult, error) {
	if v.err != nil {
		return "", v.err
	}
	result := v.results[0]
	v.results = v.results[1:]
	return result, nil
}

type stubSoakSearchIndexProbe struct {
	result soakSearchIndexResult
	err    error
	settle time.Duration
	calls  int
}

func (p *stubSoakSearchIndexProbe) Indexed(
	context.Context,
	*failureOperation,
) (soakSearchIndexResult, error) {
	p.calls++
	return p.result, p.err
}

func (p *stubSoakSearchIndexProbe) SettleBoundary(publishedAt time.Time) time.Time {
	return publishedAt.Add(p.settle)
}

// A too-early operation must be rescheduled to the settle boundary. Retrying it
// every retry interval instead would spend several times the whole
// reconciliation budget on queries that cannot succeed yet.
func TestSoakFailureReconciler_TooEarlySearchProbeWaitsForTheSettleBoundary(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := startedAt.Add(12 * time.Second)
	probe := &stubSoakSearchIndexProbe{result: soakSearchIndexTooEarly, settle: 30 * time.Second}
	ledger := newSoakSearchTestLedger(t, startedAt, now)
	reconciler := newSoakFailureReconciler(
		ledger, &fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second, func() time.Time { return now },
		withSoakFailureSearchIndexProbe(probe),
	)

	// History resolves first, then the search step runs and finds it too early.
	handled, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	require.True(t, handled)
	handled, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	require.True(t, handled)
	assert.Equal(t, 1, probe.calls)

	// Nothing is claimable again until the settle boundary, not one retry
	// interval later.
	_, claimable := ledger.ClaimDue(startedAt.Add(29 * time.Second))
	assert.False(t, claimable, "polling through the settle window burns the reconcile budget")
	_, claimable = ledger.ClaimDue(startedAt.Add(31 * time.Second))
	assert.True(t, claimable)
}

// Too-early at the deadline means the settle window outlived the deadline. That
// is a configuration problem, not evidence of loss.
func TestSoakFailureReconciler_TooEarlyAtTheDeadlineIsUnverified(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	probe := &stubSoakSearchIndexProbe{result: soakSearchIndexTooEarly, settle: time.Hour}
	now := startedAt.Add(time.Second)
	ledger := newSoakSearchTestLedger(t, startedAt, now)
	reconciler := newSoakFailureReconciler(
		ledger, &fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second, func() time.Time { return now },
		withSoakFailureSearchIndexProbe(probe),
	)

	_, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	now = startedAt.Add(2 * time.Minute)
	_, err = reconciler.Try(context.Background())
	require.NoError(t, err)

	assert.Equal(t, uint64(1), ledger.Snapshot().Results[failureResultUnverified])
}

func newSoakSearchTestLedger(t *testing.T, startedAt, now time.Time) *failureLedger {
	t.Helper()
	contract := newFailureObserverContract(false, true)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 4, Now: func() time.Time { return now }, ObserverContract: &contract,
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		ID: "m1", Scenario: soakFailureScenario, Lane: soakFailureLaneMessageSend,
		OperationType:  failureOperationMessageCreate,
		LifecycleState: failureOperationJournaled,
		StartedAt:      startedAt, VerifyAfter: startedAt,
		Deadline:   startedAt.Add(time.Minute),
		Targets:    map[string]string{"roomId": "room-1", "messageId": "m1"},
		Attributes: map[string]string{soakFailureAttributeAccount: "user-a"},
		Effects:    messageCreateExpectedEffectsForObservers(false, true, 0, ""),
		Expected: []failureObserver{
			failureObserverAdmission, failureObserverHistory, failureObserverSearchIndex,
		},
	}))
	_, err = ledger.Observe("m1", failureObserverAdmission, failureObservationGood, startedAt)
	require.NoError(t, err)
	return ledger
}

// The message reconciler and the room reconciler each understand only their own
// lane's effects. A lane-blind claim hands a room mutation to the Cassandra
// history verifier, which cannot observe room_state and records its verdict
// against an observer the operation never declared.
func TestSoakFailureReconciler_LeavesRoomLaneOperationsAlone(t *testing.T) {
	now := time.Date(2026, 8, 16, 4, 5, 6, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 4, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		SchemaVersion: 2, ID: "room-operation", RunID: "run-1",
		Scenario: soakFailureScenario, Lane: soakFailureLaneMemberMutation,
		OperationType: failureOperationMemberAdd, LifecycleState: failureOperationJournaled,
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Targets: map[string]string{"roomId": "room-1", "account": "user-a"},
		Effects: memberMutationExpectedEffects(),
	}))

	reconciler := newSoakFailureReconciler(
		ledger,
		&fakeSoakFailureVerifier{err: errors.New("the message verifier must never see a room operation")},
		time.Second,
		func() time.Time { return now },
	)

	ran, err := reconciler.Try(context.Background())

	require.NoError(t, err)
	assert.False(t, ran, "a room operation is not the message reconciler's to claim")
	operations := ledger.ActiveOperations()
	require.Len(t, operations, 1)
	assert.Empty(t, operations[0].Observations,
		"the room operation must still be waiting for its own observer")
}
