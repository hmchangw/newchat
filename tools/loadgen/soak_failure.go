package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	soakFailureScenario                   = "message_soak"
	soakFailureTrafficProfile             = "cassandra-soak-v1"
	soakFailureLaneMessageSend            = "message_send"
	soakFailureLaneMemberMutation         = "member_mutation"
	soakFailureLaneRoomMutation           = "room_mutation"
	soakFailureLaneRoomCreate             = "room_create"
	soakFailureLaneReadReceipt            = "read_receipt"
	soakFailureDefaultLedgerEpoch         = "v1"
	soakFailureAttributeRoomID            = "room_id"
	soakFailureAttributeAccount           = "account"
	soakFailureAttributeContentSHA256     = "content_sha256"
	soakFailureAttributeRecipientEvent    = "expected_recipient_event"
	soakFailureAttributeRecipientSource   = "expected_recipient_source"
	soakFailureAttributeRecipientComplete = "expected_recipient_complete"
	soakFailureAttributeRecipientRoute    = "expected_recipient_route"
	soakFailureWALGroupCommitDelay        = 10 * time.Millisecond
	soakFailureWALGroupCommitBatchSize    = 256
)

func setSoakRunInfo(metrics *Metrics, environment string) {
	if metrics == nil {
		return
	}
	metrics.RunInfo.WithLabelValues(
		environment,
		soakFailureScenario,
		soakFailureTrafficProfile,
	).Set(1)
}

func soakFailureExpiryInterval(deadline time.Duration) time.Duration {
	return min(30*time.Second, max(time.Second, deadline/10))
}

func runSoakFailureExpiry(
	ctx context.Context,
	ledger *failureLedger,
	ticks <-chan time.Time,
	onError func(error),
) {
	if ledger == nil || ticks == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case at, ok := <-ticks:
			if !ok {
				return
			}
			if _, err := ledger.Expire(at); err != nil && onError != nil {
				onError(fmt.Errorf("expire failure operations: %w", err))
			}
			// Reclaiming on the same tick means a run that finalizes nothing
			// still bounds the journal; the finalize counter alone would let it
			// grow until the next restart made recovery more expensive.
			if err := ledger.MaybeCompact(at); err != nil && onError != nil {
				onError(fmt.Errorf("compact failure journal: %w", err))
			}
		}
	}
}

// failureWALPath separates the two identities the journal name used to
// conflate: the run ID owns the seeded topology, the epoch owns the evidence
// journal. Bumping the epoch starts a fresh journal on an unchanged topology,
// so a contract change no longer forces a re-seed.
func failureWALPath(dir, runID, epoch string) string {
	if epoch == "" {
		epoch = soakFailureDefaultLedgerEpoch
	}
	return filepath.Join(dir, runID+"."+epoch+".wal")
}

// recordAbandonedFailureJournals counts retained journals from earlier epochs of
// this run. They stay on disk as evidence but belong to an incompatible
// contract and are never replayed, so the boundary has to be visible rather
// than silent.
//
// The pre-epoch release wrote {runId}.wal, which the epoch glob cannot match.
// Bumping the epoch while keeping the run ID is the documented upgrade path, so
// that file is precisely what an in-place upgrade inherits and it is counted
// explicitly rather than left to disappear.
func recordAbandonedFailureJournals(metrics *Metrics, dir, runID, epoch string) {
	if metrics == nil {
		return
	}
	active := failureWALPath(dir, runID, epoch)
	matches, err := filepath.Glob(filepath.Join(dir, runID+".*.wal"))
	if err != nil {
		slog.Error("scan retained failure journals", "runId", runID, "error", err)
		return
	}
	legacy := filepath.Join(dir, runID+".wal")
	if _, statErr := os.Stat(legacy); statErr == nil {
		matches = append(matches, legacy)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		slog.Error("stat pre-epoch failure journal",
			"runId", runID, "path", legacy, "error", statErr)
	}
	abandoned := 0
	for _, match := range matches {
		if match != active {
			abandoned++
		}
	}
	metrics.FailureAbandonedJournals.Set(float64(abandoned))
	if abandoned > 0 {
		slog.Warn("retained failure journals from earlier epochs are not replayed",
			"runId", runID, "epoch", epoch, "abandoned", abandoned)
	}
}

func openSoakFailureLedger(
	cfg *soakConfig,
	metrics *Metrics,
	now func() time.Time,
) (*failureLedger, error) {
	if cfg == nil {
		return nil, fmt.Errorf("soak configuration is required")
	}
	var journal failureJournal
	contract := newFailureObserverContract(
		cfg.RecipientObserverEnabled, cfg.SearchObserverEnabled,
	)
	if cfg.LedgerDir != "" {
		wal, err := openFailureWAL(failureWALPath(cfg.LedgerDir, cfg.RunID, cfg.LedgerEpoch))
		if err != nil {
			return nil, fmt.Errorf(
				"open soak failure WAL for run %q epoch %q: %w",
				cfg.RunID, cfg.LedgerEpoch, err,
			)
		}
		recordAbandonedFailureJournals(metrics, cfg.LedgerDir, cfg.RunID, cfg.LedgerEpoch)
		journal = newFailureJournalMetrics(
			newFailureJournalGroupCommit(
				wal,
				soakFailureWALGroupCommitDelay,
				soakFailureWALGroupCommitBatchSize,
				metrics,
			),
			metrics,
		)
	}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity:         cfg.LedgerCapacity,
		CompactEvery:     cfg.LedgerCompactEvery,
		MaxJournalBytes:  cfg.LedgerMaxBytes,
		ExpireBatch:      cfg.LedgerExpireBatch,
		Journal:          journal,
		Now:              now,
		Recorder:         newFailureLedgerPromRecorder(metrics),
		ObserverContract: &contract,
	})
	if err != nil {
		if journal != nil {
			_ = journal.Close()
		}
		return nil, fmt.Errorf("open soak failure ledger: %w", err)
	}
	recordFailureObserverConfiguration(metrics, contract)
	return ledger, nil
}

func recordFailureObserverConfiguration(metrics *Metrics, contract failureObserverContract) {
	if metrics == nil {
		return
	}
	for _, observer := range []failureObserver{
		failureObserverAdmission, failureObserverHistory, failureObserverRecipient,
	} {
		configured := 0.0
		if slices.Contains(contract.Observers, observer) {
			configured = 1
		}
		metrics.FailureObserverConfigured.WithLabelValues(string(observer)).Set(configured)
	}
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
	// searchIndexed is set when the search-index observer is enabled, which
	// makes every admitted message additionally require an index hit.
	searchIndexed bool
}

func withSoakFailureSearchObserver(enabled bool) soakFailureTrackerOption {
	return func(tracker *soakFailureTracker) { tracker.searchIndexed = enabled }
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
	recipientEvent := model.RoomEventNewMessage
	if pending.Kind == soakSendThreadReply {
		recipientEvent = model.RoomEventNewThreadMessage
	}
	attributes := map[string]string{
		soakFailureAttributeRoomID:        pending.Target.RoomID,
		soakFailureAttributeAccount:       pending.Target.Account,
		soakFailureAttributeContentSHA256: hex.EncodeToString(contentHash[:]),
	}
	if t.recipient != nil {
		source := pending.Target.RecipientSetSource
		complete := pending.Target.RecipientSetComplete
		if source == "" {
			source = recipientSetSourceTopology
			complete = true
		}
		route := pending.Target.RecipientRoute
		if route == "" {
			route = recipientExpectedRouteUser
			if pending.Kind != soakSendThreadReply && pending.Target.RoomType == model.RoomTypeChannel {
				route = recipientExpectedRouteRoom
			}
		}
		if err := t.recipient.ExpectDelivery(&recipientExpectationConfig{
			OperationID: pending.MessageID,
			Recipients:  recipients,
			RoomID:      pending.Target.RoomID,
			EventType:   recipientEvent,
			Route:       route,
			Source:      source,
			Complete:    complete,
		}); err != nil {
			return fmt.Errorf("register soak recipient expectation: %w", err)
		}
		attributes[soakFailureAttributeRecipientEvent] = string(recipientEvent)
		attributes[soakFailureAttributeRecipientSource] = string(source)
		attributes[soakFailureAttributeRecipientComplete] = fmt.Sprintf("%t", complete)
		attributes[soakFailureAttributeRecipientRoute] = string(route)
		attributes["expected_recipients"] = strings.Join(recipients, "\n")
	}
	if err := t.ledger.Start(&failureOperation{
		SchemaVersion: 2, ID: pending.MessageID, CorrelationID: pending.RequestID,
		RunID: t.runID, OperationType: failureOperationMessageCreate,
		Scenario: soakFailureScenario, Lane: soakFailureLaneMessageSend,
		LifecycleState: failureOperationJournaled,
		StartedAt:      startedAt, VerifyAfter: startedAt.Add(t.persistGrace),
		Deadline: startedAt.Add(t.deadline),
		Targets:  map[string]string{"messageId": pending.MessageID, "roomId": pending.Target.RoomID},
		Effects: messageCreateExpectedEffectsForObservers(
			t.recipient != nil,
			t.searchIndexed,
			len(recipients),
			hex.EncodeToString(recipientHash[:]),
		),
		Attributes: attributes,
	}); err != nil {
		if t.recipient != nil {
			t.recipient.evidence.Forget(pending.MessageID)
		}
		return fmt.Errorf("start soak failure operation: %w", err)
	}
	return nil
}

func (t *soakFailureTracker) Activate(pending *soakPendingSend) error {
	if t == nil || t.ledger == nil {
		return fmt.Errorf("soak failure ledger is required")
	}
	if pending == nil || pending.MessageID == "" {
		return fmt.Errorf("soak pending send with message ID is required")
	}
	if err := t.ledger.Activate(pending.MessageID, t.now().UTC()); err != nil {
		return fmt.Errorf("activate soak failure operation: %w", err)
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
	} else if result.ErrorClass == soakErrorTimeout || transientSoakError(result.ErrorClass) {
		observation = failureObservationUnverified
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
			expectedHash = current.ContentSHA256
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
		result.RPCErrorReason = rpcResult.ErrorReason
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
		result.Field = soakVerifyFieldMessageID
	case response.RoomID != roomID:
		result.Class = soakVerifyMismatch
		result.Field = soakVerifyFieldRoomID
	case response.Sender.Account != account:
		result.Class = soakVerifyMismatch
		result.Field = soakVerifyFieldAuthor
	case response.Deleted != expectedDeleted:
		result.Class = soakVerifyMismatch
		result.Field = soakVerifyFieldDeleted
	case !expectedDeleted && hex.EncodeToString(actualHash[:]) != expectedHash:
		result.Class = soakVerifyMismatch
		result.Field = soakVerifyFieldContent
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
	searchIndex   soakFailureSearchIndexProbe
}

type soakFailureRecipientFinalizer interface {
	Finalize(string, time.Time, time.Time) recipientEvidenceResult
}

// soakFailureSearchIndexProbe answers whether one admitted message reached the
// search index. Defined here rather than with the search reader so the
// reconciler depends only on the question it asks.
type soakFailureSearchIndexProbe interface {
	Indexed(context.Context, *failureOperation) (soakSearchIndexResult, error)
	// SettleBoundary is when a too-early operation can first produce a usable
	// answer, so it is rescheduled there rather than polled meanwhile.
	SettleBoundary(publishedAt time.Time) time.Time
}

func withSoakFailureSearchIndexProbe(
	probe soakFailureSearchIndexProbe,
) soakFailureReconcilerOption {
	return func(reconciler *soakFailureReconciler) { reconciler.searchIndex = probe }
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
	// Scoped to the message lane. This reconciler only understands history,
	// recipient and search effects, and it records its verdict against the
	// history observer; a room mutation claimed here would be verified by a
	// Cassandra message lookup and closed against an observer it never
	// declared, while its own room_state observer never resolves.
	operation, ok := r.ledger.ClaimDueLanes(now, []string{soakFailureLaneMessageSend})
	if !ok {
		return false, nil
	}
	if _, historyObserved := operation.Observations[failureObserverHistory]; historyObserved {
		if _, recipientObserved := operation.Observations[failureObserverRecipient]; !recipientObserved &&
			slices.Contains(operation.Expected, failureObserverRecipient) {
			observation := failureObservationUnverified
			reason := failureReasonNone
			if r.recipient != nil {
				result := r.recipient.Finalize(
					operation.ID,
					operation.StartedAt,
					operation.Deadline,
				)
				observation = result.Observation
				if observation == failureObservationBad ||
					observation == failureObservationMissingAfterDeadline {
					switch {
					case len(result.Mismatches) > 0:
						reason = failureReasonRecipientIdentityMismatch
					case len(result.Unexpected) > 0:
						reason = failureReasonRecipientUnexpected
					case len(result.Duplicates) > 0 && observation == failureObservationBad:
						reason = failureReasonRecipientDuplicate
					case len(result.Missing) > 0:
						reason = failureReasonRecipientMissing
					}
				}
			}
			if _, err := r.ledger.ObserveWithReason(
				operation.ID,
				failureObserverRecipient,
				observation,
				reason,
				now,
			); err != nil {
				_ = r.ledger.ReleaseClaim(operation.ID, now.Add(r.retryInterval))
				return true, fmt.Errorf("record soak recipient observation: %w", err)
			}
			return true, nil
		}
		if _, searchObserved := operation.Observations[failureObserverSearchIndex]; !searchObserved &&
			slices.Contains(operation.Expected, failureObserverSearchIndex) {
			return true, r.observeSearchIndex(ctx, &operation, now)
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
		if err := r.ledger.ReleaseClaim(operation.ID, now.Add(r.retryInterval)); err != nil {
			return true, fmt.Errorf("release malformed failure operation %q: %w", operation.ID, err)
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

	// Past the deadline the result depends on what the verifier could actually
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

// observeSearchIndex asks the query side whether the message reached the index.
//
// Three outcomes, deliberately distinct. A hit is good. An unreachable
// search-service proves nothing and stays claimable until the deadline, then
// resolves unverified — reporting missing there would turn every dependency
// outage longer than the deadline into a data-loss claim. Only an answer from a
// healthy search-service that does not contain the message, past the deadline,
// is loss; and it is only loss because admission already recorded good, which
// the ledger enforces.
func (r *soakFailureReconciler) observeSearchIndex(
	ctx context.Context,
	operation *failureOperation,
	now time.Time,
) error {
	result := soakSearchIndexUnknown
	var probeErr error
	if r.searchIndex != nil {
		result, probeErr = r.searchIndex.Indexed(ctx, operation)
	}
	if probeErr == nil && result == soakSearchIndexFound {
		return r.observeAs(operation.ID, failureObserverSearchIndex, failureObservationGood, now)
	}
	if now.Before(operation.Deadline) {
		// A too-early operation is rescheduled to the settle boundary, not the
		// retry interval. Polling through the settle window would spend several
		// times the whole reconciliation budget on queries that cannot succeed,
		// starving the lanes that share it.
		retryAt := now.Add(r.retryInterval)
		if result == soakSearchIndexTooEarly && r.searchIndex != nil {
			if boundary := r.searchIndex.SettleBoundary(operation.StartedAt); boundary.After(retryAt) {
				retryAt = boundary
			}
		}
		if err := r.ledger.ReleaseClaim(operation.ID, retryAt); err != nil {
			return fmt.Errorf(
				"reschedule soak search index probe for %q: %w", operation.ID, err,
			)
		}
		return nil
	}
	observation := failureObservationMissingAfterDeadline
	if probeErr != nil || result != soakSearchIndexMissing {
		// Only a healthy search-service that answered without the message is
		// loss. Too-early at the deadline means the settle window outlived it,
		// which is a configuration problem, not evidence.
		observation = failureObservationUnverified
	}
	return r.observeAs(operation.ID, failureObserverSearchIndex, observation, now)
}

func (r *soakFailureReconciler) observeAs(
	operationID string,
	observer failureObserver,
	observation failureObservation,
	now time.Time,
) error {
	if _, err := r.ledger.Observe(operationID, observer, observation, now); err != nil {
		_ = r.ledger.ReleaseClaim(operationID, now.Add(r.retryInterval))
		return fmt.Errorf("record soak %s observation: %w", observer, err)
	}
	return nil
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
