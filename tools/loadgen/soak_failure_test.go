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

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestSoakFailureTracker_StartsDurableMessageOperation(t *testing.T) {
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
		Target:  soakSendTarget{Account: "alice", RoomID: "room-1", Recipients: []string{"alice", "bob"}},
		Content: "secret message", PublishedAt: now,
	}))

	operation, ok := ledger.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "request-1", operation.CorrelationID)
	assert.Equal(t, "room-1", operation.Attributes[soakFailureAttributeRoomID])
	assert.Equal(t, "alice", operation.Attributes[soakFailureAttributeAccount])
	assert.NotEmpty(t, operation.Attributes[soakFailureAttributeContentSHA256])
	assert.NotContains(t, operation.Attributes, "content")
	assert.Equal(t, 2, operation.SchemaVersion)
	assert.Equal(t, "run-1", operation.RunID)
	assert.Equal(t, soakFailureScenario, operation.Scenario)
	assert.Equal(t, failureOperationMessageCreate, operation.OperationType)
	assert.Equal(t, "message-1", operation.Targets["messageId"])
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverHistory, failureObserverRecipient}, operation.Expected)
	assert.Len(t, operation.Effects, 3)
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
