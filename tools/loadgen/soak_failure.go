package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"time"

	"github.com/hmchangw/chat/pkg/subject"
)

const (
	soakFailureScenario               = "cassandra_soak"
	soakFailureLaneMessageSend        = "message_send"
	soakFailureAttributeRoomID        = "room_id"
	soakFailureAttributeAccount       = "account"
	soakFailureAttributeContentSHA256 = "content_sha256"
)

func openSoakFailureLedger(
	cfg *soakConfig,
	metrics *Metrics,
	now func() time.Time,
) (*failureLedger, error) {
	if cfg == nil {
		return nil, fmt.Errorf("soak configuration is required")
	}
	var journal failureJournal
	if cfg.LedgerDir != "" {
		wal, err := openFailureWAL(filepath.Join(cfg.LedgerDir, cfg.RunID+".wal"))
		if err != nil {
			return nil, err
		}
		journal = wal
	}
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: cfg.LedgerCapacity,
		Journal:  journal,
		Now:      now,
		Recorder: newFailureLedgerPromRecorder(metrics),
	})
	if err != nil {
		if journal != nil {
			_ = journal.Close()
		}
		return nil, fmt.Errorf("open soak failure ledger: %w", err)
	}
	return ledger, nil
}

type soakFailureTracker struct {
	ledger       *failureLedger
	persistGrace time.Duration
	deadline     time.Duration
	now          func() time.Time
}

func newSoakFailureTracker(
	ledger *failureLedger,
	persistGrace time.Duration,
	deadline time.Duration,
	now func() time.Time,
) *soakFailureTracker {
	if now == nil {
		now = time.Now
	}
	return &soakFailureTracker{
		ledger: ledger, persistGrace: max(0, persistGrace),
		deadline: max(time.Second, deadline), now: now,
	}
}

func (t *soakFailureTracker) Start(pending *soakPendingSend) error {
	if t == nil || t.ledger == nil {
		return fmt.Errorf("soak failure ledger is required")
	}
	if pending == nil {
		return fmt.Errorf("soak pending send is required")
	}
	startedAt := pending.PublishedAt.UTC()
	if startedAt.IsZero() {
		startedAt = t.now().UTC()
	}
	contentHash := sha256.Sum256([]byte(pending.Content))
	if err := t.ledger.Start(&failureOperation{
		ID: pending.MessageID, CorrelationID: pending.RequestID,
		Scenario: soakFailureScenario, Lane: soakFailureLaneMessageSend,
		StartedAt: startedAt, VerifyAfter: startedAt.Add(t.persistGrace),
		Deadline: startedAt.Add(t.deadline),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
		Attributes: map[string]string{
			soakFailureAttributeRoomID:        pending.Target.RoomID,
			soakFailureAttributeAccount:       pending.Target.Account,
			soakFailureAttributeContentSHA256: hex.EncodeToString(contentHash[:]),
		},
	}); err != nil {
		return fmt.Errorf("start soak failure operation: %w", err)
	}
	return nil
}

func (t *soakFailureTracker) ObserveReply(result *soakSendReplyResult) error {
	if t == nil || t.ledger == nil {
		return fmt.Errorf("soak failure ledger is required")
	}
	if result.MessageID == "" {
		return fmt.Errorf("soak send result requires message ID")
	}
	observation := failureObservationBad
	if result.Status == soakSendReplyAccepted {
		observation = failureObservationGood
	}
	if _, err := t.ledger.Observe(
		result.MessageID,
		failureObserverAdmission,
		observation,
		t.now().UTC(),
	); err != nil {
		return fmt.Errorf("record soak admission observation: %w", err)
	}
	return nil
}

type soakFailureHistoryResult string

const (
	soakFailureHistoryFound    soakFailureHistoryResult = "found"
	soakFailureHistoryMissing  soakFailureHistoryResult = "missing"
	soakFailureHistoryMismatch soakFailureHistoryResult = "mismatch"
)

type soakFailureHistoryVerifier interface {
	Verify(context.Context, *failureOperation) (soakFailureHistoryResult, error)
}

type soakFailureRPCVerifier struct {
	siteID   string
	rpc      *soakRPCClient
	catalog  *soakCatalog
	recorder soakVerifyResultRecorder
	now      func() time.Time
}

func newSoakFailureRPCVerifier(
	siteID string,
	rpc *soakRPCClient,
	catalog *soakCatalog,
	recorder soakVerifyResultRecorder,
	now func() time.Time,
) *soakFailureRPCVerifier {
	if now == nil {
		now = time.Now
	}
	return &soakFailureRPCVerifier{
		siteID: siteID, rpc: rpc, catalog: catalog, recorder: recorder, now: now,
	}
}

func (v *soakFailureRPCVerifier) Verify(
	ctx context.Context,
	operation *failureOperation,
) (soakFailureHistoryResult, error) {
	if v == nil || v.rpc == nil {
		return "", fmt.Errorf("soak failure RPC verifier is not configured")
	}
	roomID := operation.Attributes[soakFailureAttributeRoomID]
	account := operation.Attributes[soakFailureAttributeAccount]
	expectedHash := operation.Attributes[soakFailureAttributeContentSHA256]
	if operation.ID == "" || roomID == "" || account == "" || expectedHash == "" {
		return "", fmt.Errorf("soak failure operation is missing message verification attributes")
	}
	expectedDeleted := false
	if v.catalog != nil {
		if current, ok := v.catalog.Get(roomID, operation.ID); ok {
			currentHash := sha256.Sum256([]byte(current.Content))
			expectedHash = hex.EncodeToString(currentHash[:])
			expectedDeleted = current.Deleted
		}
	}

	result := soakVerifyResult{
		Class: soakVerifyOK, Action: soakRPCGetMessage,
		ExpectedAction: soakRPCReadBack, RoomID: roomID, MessageID: operation.ID,
	}
	startedAt := v.now()
	var response soakVerifyMessage
	rpcResult, err := v.rpc.Call(ctx, soakRPCRequest{
		Action:    soakRPCGetMessage,
		Subject:   subject.MsgGet(account, roomID, v.siteID),
		Body:      soakGetMessageByIDRequest{MessageID: operation.ID},
		Timeout:   soakRequestTimeout,
		RetryMode: soakRetrySafe,
	}, &response)
	result.Latency = v.now().Sub(startedAt)
	result.Retries = rpcResult.Retries
	if err != nil {
		result.RPCErrorClass = rpcResult.ErrorClass
		if rpcResult.ErrorClass == soakErrorNotFound {
			result.Class = soakVerifyMissing
			v.record(&result)
			return soakFailureHistoryMissing, nil
		}
		if transientSoakError(rpcResult.ErrorClass) {
			result.Class = soakVerifyRetryable
		} else {
			result.Class = soakVerifyRPCError
		}
		v.record(&result)
		return "", fmt.Errorf("verify soak failure operation: %w", err)
	}

	actualHash := sha256.Sum256([]byte(response.Msg))
	switch {
	case response.MessageID != operation.ID:
		result.Class = soakVerifyMismatch
		result.Field = "message_id"
	case response.RoomID != roomID:
		result.Class = soakVerifyMismatch
		result.Field = "room_id"
	case response.Sender.Account != account:
		result.Class = soakVerifyMismatch
		result.Field = "author"
	case response.Deleted != expectedDeleted:
		result.Class = soakVerifyMismatch
		result.Field = "deleted"
	case !expectedDeleted && hex.EncodeToString(actualHash[:]) != expectedHash:
		result.Class = soakVerifyMismatch
		result.Field = "content"
	default:
		v.record(&result)
		return soakFailureHistoryFound, nil
	}
	v.record(&result)
	return soakFailureHistoryMismatch, nil
}

func (v *soakFailureRPCVerifier) record(result *soakVerifyResult) {
	if v.recorder != nil {
		v.recorder.Record(result)
	}
}

type soakFailureReconciler struct {
	ledger        *failureLedger
	verifier      soakFailureHistoryVerifier
	retryInterval time.Duration
	now           func() time.Time
}

func newSoakFailureReconciler(
	ledger *failureLedger,
	verifier soakFailureHistoryVerifier,
	retryInterval time.Duration,
	now func() time.Time,
) *soakFailureReconciler {
	if now == nil {
		now = time.Now
	}
	return &soakFailureReconciler{
		ledger: ledger, verifier: verifier,
		retryInterval: max(time.Millisecond, retryInterval), now: now,
	}
}

// Try consumes at most one due reconciliation operation. Callers use this in
// the existing read lane so 100% send verification does not add unbudgeted
// read traffic.
func (r *soakFailureReconciler) Try(ctx context.Context) (bool, error) {
	if r == nil || r.ledger == nil || r.verifier == nil {
		return false, fmt.Errorf("soak failure reconciler is not configured")
	}
	now := r.now().UTC()
	operation, ok := r.ledger.ClaimDue(now)
	if !ok {
		return false, nil
	}
	result, verifyErr := r.verifier.Verify(ctx, &operation)
	if verifyErr != nil || result == soakFailureHistoryMissing ||
		result == soakFailureHistoryMismatch {
		if now.Before(operation.Deadline) {
			if err := r.ledger.ReleaseClaim(
				operation.ID,
				now.Add(r.retryInterval),
			); err != nil {
				return true, err
			}
			return true, nil
		}
		observation := failureObservationMissingAfterDeadline
		if result == soakFailureHistoryMismatch {
			observation = failureObservationBad
		}
		_, err := r.ledger.Observe(
			operation.ID,
			failureObserverHistory,
			observation,
			now,
		)
		if err != nil {
			return true, fmt.Errorf("record missing soak history: %w", err)
		}
		return true, nil
	}

	if _, err := r.ledger.Observe(
		operation.ID,
		failureObserverHistory,
		failureObservationGood,
		now,
	); err != nil {
		return true, fmt.Errorf("record soak history observation: %w", err)
	}
	return true, nil
}
