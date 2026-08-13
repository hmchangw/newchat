package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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

const (
	failureUntrackedReasonStart   = "start"
	failureUntrackedReasonObserve = "observe"
	failureUntrackedReasonAbandon = "abandon"
)

type soakFailureTracker struct {
	ledger       *failureLedger
	persistGrace time.Duration
	deadline     time.Duration
	now          func() time.Time
	metrics      *Metrics
	runID        string
	recipient    *failureRecipientObserver
	scenario     string
}

type soakFailureTrackerOption func(*soakFailureTracker)

func withSoakFailureMetrics(metrics *Metrics) soakFailureTrackerOption {
	return func(tracker *soakFailureTracker) {
		tracker.metrics = metrics
	}
}

func withSoakFailureRunID(runID string) soakFailureTrackerOption {
	return func(tracker *soakFailureTracker) { tracker.runID = runID }
}

func withSoakFailureRecipientObserver(observer *failureRecipientObserver) soakFailureTrackerOption {
	return func(tracker *soakFailureTracker) { tracker.recipient = observer }
}

func withSoakFailureScenario(scenario string) soakFailureTrackerOption {
	return func(tracker *soakFailureTracker) { tracker.scenario = scenario }
}

func newSoakFailureTracker(
	ledger *failureLedger,
	persistGrace time.Duration,
	deadline time.Duration,
	now func() time.Time,
	options ...soakFailureTrackerOption,
) *soakFailureTracker {
	if now == nil {
		now = time.Now
	}
	tracker := &soakFailureTracker{
		ledger: ledger, persistGrace: max(0, persistGrace),
		deadline: max(time.Second, deadline), now: now, runID: "legacy",
		scenario: soakFailureScenario,
	}
	for _, option := range options {
		option(tracker)
	}
	return tracker
}

func (t *soakFailureTracker) countUntracked(reason string) {
	if t.metrics == nil {
		return
	}
	t.metrics.FailureUntracked.WithLabelValues(reason).Inc()
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
	recipients := append([]string(nil), pending.Target.Recipients...)
	if len(recipients) == 0 && pending.Target.Account != "" {
		recipients = []string{pending.Target.Account}
	}
	slices.Sort(recipients)
	recipientHash := sha256.Sum256([]byte(strings.Join(recipients, "\n")))
	if t.recipient != nil {
		if err := t.recipient.Expect(pending.MessageID, recipients); err != nil {
			return fmt.Errorf("register soak recipient expectation: %w", err)
		}
	}
	if err := t.ledger.Start(&failureOperation{
		SchemaVersion: 2, ID: pending.MessageID, CorrelationID: pending.RequestID,
		RunID: t.runID, OperationType: failureOperationMessageCreate,
		Scenario: t.scenario, Lane: soakFailureLaneMessageSend,
		StartedAt: startedAt, VerifyAfter: startedAt.Add(t.persistGrace),
		Deadline: startedAt.Add(t.deadline),
		Targets:  map[string]string{"messageId": pending.MessageID, "roomId": pending.Target.RoomID},
		Effects:  messageCreateExpectedEffects(len(recipients), hex.EncodeToString(recipientHash[:])),
		Attributes: map[string]string{
			soakFailureAttributeRoomID:        pending.Target.RoomID,
			soakFailureAttributeAccount:       pending.Target.Account,
			soakFailureAttributeContentSHA256: hex.EncodeToString(contentHash[:]),
			"expected_recipients":             strings.Join(recipients, "\n"),
		},
	}); err != nil {
		if t.recipient != nil {
			t.recipient.evidence.Forget(pending.MessageID)
		}
		return fmt.Errorf("start soak failure operation: %w", err)
	}
	return nil
}

// AbandonUnsent closes an operation whose publish never left the process. The
// intent is journaled before publishing, so without this the never-sent message
// would expire as missing history and be reported as data loss.
func (t *soakFailureTracker) AbandonUnsent(pending *soakPendingSend) error {
	if t == nil || t.ledger == nil {
		return fmt.Errorf("soak failure ledger is required")
	}
	if pending == nil || pending.MessageID == "" {
		return fmt.Errorf("soak pending send requires message ID")
	}
	err := t.ledger.Abandon(pending.MessageID, failureResultNotSent, t.now().UTC())
	if errors.Is(err, errFailureOperationNotActive) {
		t.countUntracked(failureUntrackedReasonAbandon)
		return nil
	}
	if err != nil {
		return fmt.Errorf("abandon unsent soak operation: %w", err)
	}
	return nil
}

func (t *soakFailureTracker) ObserveReply(result *soakSendReplyResult) error {
	if t == nil || t.ledger == nil {
		return fmt.Errorf("soak failure ledger is required")
	}
	if result == nil {
		return fmt.Errorf("soak send result is required")
	}
	// A reply with no matching pending send belongs to no ledger operation —
	// typically a late response whose send already expired.
	if result.Status == soakSendReplyUnmatched {
		return nil
	}
	if result.MessageID == "" {
		return fmt.Errorf("soak send result requires message ID")
	}
	observation := failureObservationBad
	if result.Status == soakSendReplyAccepted {
		observation = failureObservationGood
	}
	_, err := t.ledger.Observe(
		result.MessageID,
		failureObserverAdmission,
		observation,
		t.now().UTC(),
	)
	// A send the ledger never accepted (or already finalized) still has to move
	// traffic; the counter keeps the accounting gap visible instead of silent.
	if errors.Is(err, errFailureOperationNotActive) {
		t.countUntracked(failureUntrackedReasonObserve)
		return nil
	}
	if err != nil {
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
	recipient     soakFailureRecipientFinalizer
}

type soakFailureRecipientFinalizer interface {
	Finalize(string, time.Time, time.Time) recipientEvidenceResult
}

type soakFailureReconcilerOption func(*soakFailureReconciler)

func withSoakFailureRecipientFinalizer(finalizer soakFailureRecipientFinalizer) soakFailureReconcilerOption {
	return func(reconciler *soakFailureReconciler) { reconciler.recipient = finalizer }
}

func newSoakFailureReconciler(
	ledger *failureLedger,
	verifier soakFailureHistoryVerifier,
	retryInterval time.Duration,
	now func() time.Time,
	options ...soakFailureReconcilerOption,
) *soakFailureReconciler {
	if now == nil {
		now = time.Now
	}
	reconciler := &soakFailureReconciler{
		ledger: ledger, verifier: verifier,
		retryInterval: max(time.Millisecond, retryInterval), now: now,
	}
	for _, option := range options {
		option(reconciler)
	}
	return reconciler
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
	if _, historyObserved := operation.Observations[failureObserverHistory]; historyObserved {
		if _, recipientObserved := operation.Observations[failureObserverRecipient]; !recipientObserved &&
			slices.Contains(operation.Expected, failureObserverRecipient) {
			observation := failureObservationUnverified
			if r.recipient != nil {
				observation = r.recipient.Finalize(
					operation.ID,
					operation.StartedAt,
					operation.Deadline,
				).Observation
			}
			if _, err := r.ledger.Observe(
				operation.ID,
				failureObserverRecipient,
				observation,
				now,
			); err != nil {
				_ = r.ledger.ReleaseClaim(operation.ID, now.Add(r.retryInterval))
				return true, fmt.Errorf("record soak recipient observation: %w", err)
			}
			return true, nil
		}
		for _, observer := range operation.Expected {
			if _, observed := operation.Observations[observer]; observed {
				continue
			}
			if _, err := r.ledger.Observe(
				operation.ID,
				observer,
				failureObservationUnverified,
				now,
			); err != nil {
				_ = r.ledger.ReleaseClaim(operation.ID, now.Add(r.retryInterval))
				return true, fmt.Errorf("record unresolved soak observation: %w", err)
			}
			return true, nil
		}
		return true, fmt.Errorf("failure operation %q was queued without an unresolved observer", operation.ID)
	}
	result, verifyErr := r.verifier.Verify(ctx, &operation)
	if verifyErr == nil && result == soakFailureHistoryFound {
		return true, r.observe(operation.ID, failureObservationGood, now)
	}
	if now.Before(operation.Deadline) {
		if err := r.ledger.ReleaseClaim(
			operation.ID,
			now.Add(r.retryInterval),
		); err != nil {
			return true, err
		}
		return true, nil
	}

	// Past the deadline the verdict depends on what the verifier could actually
	// establish. An unreachable history service proves nothing, so it must not
	// be recorded as a missing message: that would turn every dependency outage
	// longer than the deadline into a data-loss report.
	observation := failureObservationMissingAfterDeadline
	switch {
	case verifyErr != nil:
		observation = failureObservationUnverified
	case result == soakFailureHistoryMismatch:
		observation = failureObservationBad
	}
	return true, r.observe(operation.ID, observation, now)
}

func (r *soakFailureReconciler) observe(
	operationID string,
	observation failureObservation,
	now time.Time,
) error {
	if _, err := r.ledger.Observe(
		operationID,
		failureObserverHistory,
		observation,
		now,
	); err != nil {
		// Leave the operation claimable so a later pass can retry it rather
		// than stranding it in-flight forever.
		_ = r.ledger.ReleaseClaim(operationID, now.Add(r.retryInterval))
		return fmt.Errorf("record soak history observation: %w", err)
	}
	return nil
}

// soakShareGate admits a fixed fraction of calls. Reconciliation runs inside the
// read lane, so without a cap a large unresolved backlog would consume every
// read slot and stop the production-like read mix during the fault window.
type soakShareGate struct {
	mu     sync.Mutex
	share  float64
	credit float64
}

func newSoakShareGate(share float64) *soakShareGate {
	return &soakShareGate{share: min(max(share, 0), 1)}
}

func (g *soakShareGate) Allow() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.credit += g.share
	if g.credit < 1 {
		return false
	}
	g.credit--
	return true
}
