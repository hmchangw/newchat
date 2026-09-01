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

// watchSoakLedgerDurability stops a run whose ledger can no longer record the
// verdict that disqualifies its own evidence. A refused invalidation is settled
// by the next write, so a transient failure clears on its own; the grace is
// measured from the tick that first saw the debt, because an append that failed
// a moment before a tick would otherwise be given no time at all to be paid. A
// debt still standing a full grace later is a journal that is not coming back.
// It reports once and returns — the caller stops the workload, and the ordinary
// shutdown path decides the exit code.
func watchSoakLedgerDurability(
	ctx context.Context,
	ledger *failureLedger,
	ticks <-chan time.Time,
	grace time.Duration,
	onUnrecordedVerdict func([]string),
) {
	if ledger == nil || ticks == nil || onUnrecordedVerdict == nil {
		return
	}
	var owedSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case at, ok := <-ticks:
			if !ok {
				return
			}
			owed := ledger.UnpersistedInvalidations()
			if len(owed) == 0 {
				// Paid. The next debt starts its own interval rather than
				// inheriting the time this one spent.
				owedSince = time.Time{}
				continue
			}
			if owedSince.IsZero() {
				owedSince = at
				continue
			}
			if at.Sub(owedSince) >= grace {
				onUnrecordedVerdict(owed)
				return
			}
		}
	}
}

func runSoakFailureExpiry(
	ctx context.Context,
	ledger *failureLedger,
	evidence *recipientEvidence,
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
			// The IDs the ledger actually finalized, which is not the same as
			// "everything overdue": Expire stops at its batch limit and skips
			// claimed operations mid-verification. Forgetting exactly these
			// keeps the evidence and the ledger in step, and taking them after
			// Expire returns creates no ordering between the two locks.
			expired, err := ledger.Expire(at)
			if err != nil {
				reportSoakFailureSweepError(
					onError, fmt.Errorf("expire failure operations: %w", err))
			}
			evidence.ForgetAll(expired)
			// Reclaiming on the same tick means a run that finalizes nothing
			// still bounds the journal; the finalize counter alone would let it
			// grow until the next restart made recovery more expensive.
			if err := ledger.MaybeCompact(at); err != nil {
				reportSoakFailureSweepError(
					onError, fmt.Errorf("compact failure journal: %w", err))
			}
		}
	}
}

// reportSoakFailureSweepError makes sure a sweep failure is seen. Both of these
// invalidate the ledger, so losing one because no callback happened to be
// configured would hide the moment the run stopped being trustworthy.
func reportSoakFailureSweepError(onError func(error), err error) {
	if onError != nil {
		onError(err)
		return
	}
	slog.Error("Cassandra soak failure sweep", soakErrorAttrs(err)...)
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
		t.forgetRecipientExpectation(pending.MessageID)
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
	// One decision, and the ledger makes it. Abandoning finalizes the operation
	// out of the ledger, putting it beyond the expiry sweep that releases
	// evidence by ID — but so does a failure that lands after the commit, so
	// "did the call succeed" is the wrong question. An operation still held is
	// still reconcilable and keeps its evidence; one the ledger has let go must
	// release it here or nothing will.
	if t.ledger.Retired(pending.MessageID) {
		t.forgetRecipientExpectation(pending.MessageID)
	}
	if errors.Is(err, errFailureOperationNotActive) {
		t.countUntracked(failureUntrackedReasonAbandon)
		return nil
	}
	if err != nil {
		return fmt.Errorf("abandon unsent soak operation: %w", err)
	}
	return nil
}

// forgetRecipientExpectation releases an expectation the ledger will never
// report, on the paths that retire an operation without going through expiry.
func (t *soakFailureTracker) forgetRecipientExpectation(operationID string) {
	if t.recipient != nil {
		t.recipient.evidence.Forget(operationID)
	}
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
		Account:   account,
		RoomID:    roomID,
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
	metrics       *Metrics
}

// What a reconcile claim achieved. The split exists because the startup
// capacity rule can only model one claim per observer: the poll's retries scale
// with how many messages are slow to persist, which no configuration knows, and
// idle claims are the only evidence that the lane still has slack.
const (
	soakReconcileClaimAdvanced    = "advanced"
	soakReconcileClaimRetried     = "retried"
	soakReconcileClaimIdle        = "idle"
	soakReconcileClaimFailed      = "failed"
	soakReconcileClaimUnavailable = "unavailable"
	// deferred is a claim that issued no query on purpose: the settle window
	// has not elapsed, so there was nothing to ask yet. It is separate from
	// retried because retried is the poll cost of an effect that has not
	// landed, and counting a scheduled wait there manufactures a retry
	// baseline in a healthy run.
	soakReconcileClaimDeferred = "deferred"
)

// soakReconcileClaimOutcomes is the closed label set, listed so the metric can
// publish every outcome at zero rather than only the ones that have happened.
func soakReconcileClaimOutcomes() []string {
	return []string{
		soakReconcileClaimAdvanced,
		soakReconcileClaimRetried,
		soakReconcileClaimIdle,
		soakReconcileClaimFailed,
		soakReconcileClaimUnavailable,
		soakReconcileClaimDeferred,
	}
}

// reconcileProbeOutcome names a claim that reached a probe but not a verdict.
// A probe that could not be answered proves nothing about the message, so it
// must not share a label with one that answered and found nothing — that is how
// a dependency outage reads as a persistence backlog.
func reconcileProbeOutcome(probeErr error) string {
	if probeErr != nil {
		return soakReconcileClaimUnavailable
	}
	return soakReconcileClaimRetried
}

// searchProbeOutcome separates a probe that answered from one that never ran.
// Indexed reports unknown without an error when it had no query to issue — an
// unconfigured probe, an operation missing its targets, or a catalog entry
// evicted before the probe was due — and "retried" is defined as a probe that
// answered and found the effect absent. Counting an unissued query as retried
// would show the dashboard persistence lag where there was no query at all.
// too_early is unqueried on purpose — the settle window has not elapsed — and
// is neither of those: it is a scheduled wait, so it gets its own outcome
// rather than inflating the retry baseline of a healthy run.
func searchProbeOutcome(
	result soakSearchIndexResult,
	probed bool,
	probeErr error,
) string {
	if probeErr != nil {
		return soakReconcileClaimUnavailable
	}
	if result == soakSearchIndexTooEarly {
		return soakReconcileClaimDeferred
	}
	if !probed {
		return soakReconcileClaimUnavailable
	}
	return soakReconcileClaimRetried
}

func withSoakFailureReconcileMetrics(metrics *Metrics) soakFailureReconcilerOption {
	return func(reconciler *soakFailureReconciler) { reconciler.metrics = metrics }
}

// recordObserved classifies a claim by whether the observation it tried to
// persist actually landed. Recording before the call counts a failed write as
// progress, which is the opposite of what the split is for.
func (r *soakFailureReconciler) recordObserved(err error) error {
	return r.recordObservedProbe(err, true)
}

// recordObservedProbe is the terminal form: the observation always lands, but
// what the claim bought depends on whether anything answered. An unreachable
// dependency at the deadline resolves the observer as unverified, and calling
// that advanced hides the outage in the one window where the board is being
// read to tell an outage from a persistence backlog. It is the same rule the
// pre-deadline branches already follow.
func (r *soakFailureReconciler) recordObservedProbe(err error, answered bool) error {
	if err != nil {
		r.recordClaim(soakReconcileClaimFailed)
		return err
	}
	if !answered {
		r.recordClaim(soakReconcileClaimUnavailable)
		return nil
	}
	r.recordClaim(soakReconcileClaimAdvanced)
	return nil
}

// recordClaimLag reports how far past its scheduled probe an operation was when
// the lane finally reached it. A claim taken early is not negative progress, so
// it floors at zero.
func (r *soakFailureReconciler) recordClaimLag(now, scheduled time.Time) {
	if r == nil || r.metrics == nil || scheduled.IsZero() {
		return
	}
	r.metrics.FailureReconcileLag.Observe(max(0, now.Sub(scheduled).Seconds()))
}

// recordClaim is nil-safe so the tests that only assert reconciliation do not
// have to build a registry.
func (r *soakFailureReconciler) recordClaim(outcome string) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.FailureReconcileClaims.WithLabelValues(outcome).Inc()
}

type soakFailureRecipientFinalizer interface {
	Finalize(string, time.Time, time.Time) recipientEvidenceResult
}

// soakFailureSearchIndexProbe answers whether one admitted message reached the
// search index. Defined here rather than with the search reader so the
// reconciler depends only on the question it asks.
type soakFailureSearchIndexProbe interface {
	// Indexed reports the verdict and whether it issued a query. Several paths
	// answer from local state alone — before the settle window, or when the
	// operation or catalogue cannot supply a term — and those spend no read
	// slot, so the caller must not be told they did.
	Indexed(context.Context, *failureOperation) (soakSearchIndexResult, bool, error)
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
// Try takes at most one claim. It reports whether it handled anything and
// whether that claim spent a read slot: an event-mode observer finalizes from
// evidence already in memory, so its claim reaches no service under test and
// the caller can give the allowance back and take its scheduled read.
func (r *soakFailureReconciler) Try(ctx context.Context) (bool, bool, error) {
	if r == nil || r.ledger == nil || r.verifier == nil {
		return false, false, fmt.Errorf("soak failure reconciler is not configured")
	}
	now := r.now().UTC()
	// Scoped to the message lane. This reconciler only understands history,
	// recipient and search effects, and it records its verdict against the
	// history observer; a room mutation claimed here would be verified by a
	// Cassandra message lookup and closed against an observer it never
	// declared, while its own room_state observer never resolves.
	operation, ok := r.ledger.ClaimDueLanes(now, []string{soakFailureLaneMessageSend})
	if !ok {
		r.recordClaim(soakReconcileClaimIdle)
		return false, false, nil
	}
	r.recordClaimLag(now, operation.nextVerifyAt)
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
				r.recordClaim(soakReconcileClaimFailed)
				return true, false, fmt.Errorf("record soak recipient observation: %w", err)
			}
			r.recordClaim(soakReconcileClaimAdvanced)
			return true, false, nil
		}
		if _, searchObserved := operation.Observations[failureObserverSearchIndex]; !searchObserved &&
			slices.Contains(operation.Expected, failureObserverSearchIndex) {
			probed, err := r.observeSearchIndex(ctx, &operation, now)
			return true, probed, err
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
				r.recordClaim(soakReconcileClaimFailed)
				return true, false, fmt.Errorf("record unresolved soak observation: %w", err)
			}
			r.recordClaim(soakReconcileClaimAdvanced)
			return true, false, nil
		}
		r.recordClaim(soakReconcileClaimFailed)
		if err := r.ledger.ReleaseClaim(operation.ID, now.Add(r.retryInterval)); err != nil {
			return true, false, fmt.Errorf("release malformed failure operation %q: %w", operation.ID, err)
		}
		return true, false, fmt.Errorf("failure operation %q was queued without an unresolved observer", operation.ID)
	}
	result, verifyErr := r.verifier.Verify(ctx, &operation)
	if verifyErr == nil && result == soakFailureHistoryFound {
		return true, true, r.recordObserved(r.observe(operation.ID, failureObservationGood, now))
	}
	if now.Before(operation.Deadline) {
		// Two different retries share this branch. A probe that answered and
		// found nothing is a pending effect, and it repeats for the whole
		// deadline, so it is the one that has to back off. A probe that could
		// not be answered established nothing about the message: backing it off
		// would let one timeout push the next look minutes away, and the
		// operation could reach its deadline unverified because of the retry
		// policy rather than because of storage. A failed call keeps the flat
		// interval, which is what the claim outcome recorded below already says.
		retryAt := now.Add(r.retryInterval)
		if verifyErr == nil {
			retryAt = nextReconcileProbe(
				now, operation.VerifyAfter, operation.Deadline, r.retryInterval)
		}
		if err := r.ledger.ReleaseClaim(operation.ID, retryAt); err != nil {
			r.recordClaim(soakReconcileClaimFailed)
			return true, true, fmt.Errorf(
				"reschedule pending soak history probe for %q: %w", operation.ID, err)
		}
		r.recordClaim(reconcileProbeOutcome(verifyErr))
		return true, true, nil
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
	return true, true, r.recordObservedProbe(
		r.observe(operation.ID, observation, now), verifyErr == nil)
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
) (bool, error) {
	result := soakSearchIndexUnknown
	var probeErr error
	// Only a probe that issued a query reached a service, so only that one
	// spends a read slot.
	probed := false
	if r.searchIndex != nil {
		result, probed, probeErr = r.searchIndex.Indexed(ctx, operation)
	}
	if probeErr == nil && result == soakSearchIndexFound {
		return probed, r.recordObserved(
			r.observeAs(operation.ID, failureObserverSearchIndex, failureObservationGood, now))
	}
	if now.Before(operation.Deadline) {
		// A too-early operation is rescheduled to the settle boundary, not the
		// retry interval. Polling through the settle window would spend several
		// times the whole reconciliation budget on queries that cannot succeed,
		// starving the lanes that share it.
		retryAt := now.Add(r.retryInterval)
		switch {
		case result == soakSearchIndexTooEarly && r.searchIndex != nil:
			if boundary := r.searchIndex.SettleBoundary(operation.StartedAt); boundary.After(retryAt) {
				retryAt = boundary
			}
		case probeErr == nil && result == soakSearchIndexMissing:
			// The same amplification the history poll was backed off for: one
			// message delayed in the index would otherwise cost a full-text
			// search every retry interval until its deadline, and during an
			// indexing backlog those probes saturate the reconcile share while
			// newer messages expire for want of a look. A probe that could not
			// answer keeps the flat interval — that retries a call, not a
			// pending effect.
			retryAt = nextReconcileProbe(
				now, r.searchProbeAnchor(operation), operation.Deadline, r.retryInterval)
		}
		if err := r.ledger.ReleaseClaim(operation.ID, retryAt); err != nil {
			r.recordClaim(soakReconcileClaimFailed)
			return probed, fmt.Errorf(
				"reschedule soak search index probe for %q: %w", operation.ID, err,
			)
		}
		r.recordClaim(searchProbeOutcome(result, probed, probeErr))
		return probed, nil
	}
	observation := failureObservationMissingAfterDeadline
	if probeErr != nil || result != soakSearchIndexMissing {
		// Only a healthy search-service that answered without the message is
		// loss. Too-early at the deadline means the settle window outlived it,
		// which is a configuration problem, not evidence.
		observation = failureObservationUnverified
	}
	return probed, r.recordObservedProbe(
		r.observeAs(operation.ID, failureObserverSearchIndex, observation, now),
		probed && probeErr == nil)
}

// searchProbeAnchor is where the backoff measures elapsed wait from. No probe
// before the settle boundary can find the message, so anchoring at VerifyAfter
// would open the sequence already a settle window wide and put the first two
// probes minutes apart.
func (r *soakFailureReconciler) searchProbeAnchor(operation *failureOperation) time.Time {
	if r.searchIndex == nil {
		return operation.VerifyAfter
	}
	if boundary := r.searchIndex.SettleBoundary(operation.StartedAt); boundary.After(operation.VerifyAfter) {
		return boundary
	}
	return operation.VerifyAfter
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

// soakReconcileLagCeiling is the largest reconcile deadline the lag histogram
// resolves. Lag is read against SOAK_RECONCILE_DEADLINE, so a deadline beyond
// this leaves the region the validity rule actually reads in the +Inf bucket,
// where no quantile can place it.
const soakReconcileLagCeiling = time.Hour

// soakReconcileLagBuckets spans a claim taken on schedule through a lane a full
// hour behind. The top bucket is deliberately past soakReconcileLagCeiling so a
// deadline at the ceiling still falls inside a finite bucket rather than on its
// edge.
func soakReconcileLagBuckets() []float64 {
	return []float64{
		0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 5400,
	}
}

// warnSoakReconcileLagRange reports a deadline the lag histogram cannot resolve
// and returns whether it found one. It warns rather than refuses for the same
// reason the capacity floor does: the run is the operator's to judge. But lag
// is the signal that says whether a window counted, so a deadline that outruns
// it leaves that question unanswerable, and the returned breach is what marks
// the run rather than leaving a log line to close it.
func warnSoakReconcileLagRange(cfg *soakConfig) bool {
	if cfg == nil || cfg.ReconcileDeadline <= soakReconcileLagCeiling {
		return false
	}
	slog.Warn("soak reconcile deadline outruns the lag histogram",
		"soakReconcileDeadline", cfg.ReconcileDeadline.String(),
		"resolvableCeiling", soakReconcileLagCeiling.String(),
		"consequence", "loadgen_failure_reconcile_lag_seconds cannot place a quantile near the deadline",
		"remedy", "lower SOAK_RECONCILE_DEADLINE or widen the lag buckets",
	)
	return true
}

// soakReadLaneReconciler is the reconciler seen from the read lane: one claim
// per call, reporting whether that claim queried the system under test.
type soakReadLaneReconciler interface {
	Try(context.Context) (bool, bool, error)
}

// soakReconcileFreeClaimBurst bounds how many claims one read action may drain,
// so a backlog cannot monopolise a single action.
const soakReconcileFreeClaimBurst = 8

// reconcileReadAction spends one read-lane action on reconciliation and reports
// whether the action queried the system under test, in which case the caller
// skips the read it was scheduled for.
//
// Try advances one observer per claim and a read action is the only way to take
// one, so a claim that costs no read still costs a callback. With the recipient
// observer on that is a second callback for every message — at the configured
// rates, the whole read lane, leaving nothing for the history poll's retries.
// Draining the free claims inside one admission is what keeps the callback
// budget matched to the claims that actually query, which is what the startup
// floor models.
func reconcileReadAction(
	ctx context.Context,
	reconciler soakReadLaneReconciler,
	gate *soakShareGate,
) bool {
	if !gate.Allow() {
		return false
	}
	for range soakReconcileFreeClaimBurst {
		handled, spentRead, err := reconciler.Try(ctx)
		if err != nil {
			slog.Error("reconcile Cassandra soak operation", soakErrorAttrs(err)...)
		}
		if spentRead {
			return true
		}
		// Nothing was due, so there is no free claim left to drain either.
		if !handled {
			break
		}
	}
	gate.Refund()
	return false
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

// Refund returns an allowance taken by a claim that issued no request to the
// system under test. The gate exists to protect the production-like read mix,
// so a claim that read nothing must not be charged against it.
//
// The credit is capped at one whole allowance. Allow bounds its own credit
// below one, so an uncapped refund would be the only way the gate could bank
// it — and a lane that sat idle would then admit a long run of consecutive
// reconciliation reads the moment a backlog arrived, which is the fault window
// the cap exists to protect. One allowance is what the refunded claim was
// entitled to and no more.
func (g *soakShareGate) Refund() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.credit = min(g.credit+1, 1)
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
