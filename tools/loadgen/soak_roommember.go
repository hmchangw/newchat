package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
)

type soakRoomLaneConfig struct {
	RunID            string
	PersistGrace     time.Duration
	Deadline         time.Duration
	RetryInterval    time.Duration
	RoomCreateBudget int
	CreateRoomSize   int
}

// soakRoomPending remembers which pool reservation a ledger operation came
// from, so the pool is settled with the terminal result rather than a guess
// made at send time.
type soakRoomPending struct {
	kind   failureOperationType
	member soakMemberIntent
	rename soakRenameIntent
	mute   soakMuteIntent
	roomID string
}

// soakRoomLanes journals every room and member mutation before it is sent and
// settles it from the ledger's terminal result.
//
// Unlike the message lane, a mutation the ledger refuses is not sent at all:
// message traffic must keep flowing to hold the offered load, but an untracked
// room mutation would leave state drift nothing can reconcile.
type soakRoomLanes struct {
	cfg      soakRoomLaneConfig
	pool     *soakRoomStatePool
	mutator  *soakRoomMutator
	ledger   *failureLedger
	reader   *soakRoomReader
	store    soakRoomStateStore
	metrics  *Metrics
	recorder soakMutationSampleRecorder
	now      func() time.Time

	mu         sync.Mutex
	pending    map[string]soakRoomPending
	budget     int
	renameTurn bool
}

var soakRoomLaneNames = []string{
	soakFailureLaneMemberMutation,
	soakFailureLaneRoomMutation,
	soakFailureLaneRoomCreate,
}

func newSoakRoomLanes(
	cfg soakRoomLaneConfig,
	pool *soakRoomStatePool,
	mutator *soakRoomMutator,
	ledger *failureLedger,
	reader *soakRoomReader,
	store soakRoomStateStore,
	metrics *Metrics,
	recorder soakMutationSampleRecorder,
	now func() time.Time,
) *soakRoomLanes {
	if now == nil {
		now = time.Now
	}
	if cfg.Deadline <= 0 {
		cfg.Deadline = 10 * time.Minute
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = time.Second
	}
	if cfg.CreateRoomSize < 2 {
		cfg.CreateRoomSize = 2
	}
	lanes := &soakRoomLanes{
		cfg: cfg, pool: pool, mutator: mutator, ledger: ledger, reader: reader,
		store: store, metrics: metrics, recorder: recorder, now: now,
		pending: make(map[string]soakRoomPending), budget: max(0, cfg.RoomCreateBudget),
	}
	lanes.setBudgetGauge()
	return lanes
}

func (l *soakRoomLanes) MemberMutation(ctx context.Context) error {
	intent, ok := l.pool.NextMemberIntent()
	if !ok {
		return nil
	}
	expectMember := "false"
	if intent.Add {
		expectMember = "true"
	}
	operation := l.newOperation(
		soakFailureLaneMemberMutation, intent.OperationType(),
		map[string]string{"roomId": intent.RoomID, "account": intent.Account},
		map[string]string{
			soakFailureAttributeTargetAccount:  intent.Account,
			soakFailureAttributeExpectedMember: expectMember,
			soakFailureAttributeRequester:      intent.Requester,
		},
		memberMutationExpectedEffects(),
	)
	pending := soakRoomPending{
		kind: intent.OperationType(), member: intent, roomID: intent.RoomID,
	}
	if !l.journal(operation, &pending) {
		l.pool.SettleMember(intent, failureResultNotSent)
		return nil
	}

	var (
		outcome soakRoomMutationOutcome
		err     error
	)
	if intent.Add {
		outcome, err = l.mutator.AddMember(ctx, intent.Requester, intent.RoomID, intent.Account)
	} else {
		outcome, err = l.mutator.RemoveMember(ctx, intent.Requester, intent.RoomID, intent.Account)
	}
	return l.observeAdmission(operation.ID, &outcome, err)
}

func (l *soakRoomLanes) RoomMutation(ctx context.Context) error {
	if l.pickRenameTurn() {
		return l.rename(ctx)
	}
	return l.muteToggle(ctx)
}

func (l *soakRoomLanes) rename(ctx context.Context) error {
	intent, ok := l.pool.NextRenameIntent()
	if !ok {
		return nil
	}
	// An unknown previous name is passed through as empty on purpose: asserting
	// a stale one would let the observer call a merely lost rename a name nobody
	// asked for, reporting corruption that never happened.
	previous, _ := l.pool.RoomName(intent.RoomID)
	operation := l.newOperation(
		soakFailureLaneRoomMutation, failureOperationRoomRename,
		map[string]string{"roomId": intent.RoomID},
		map[string]string{
			soakFailureAttributeExpectedName: intent.NewName,
			soakFailureAttributePreviousName: previous,
			soakFailureAttributeRequester:    intent.Requester,
		},
		roomMutationExpectedEffects(failureOperationRoomRename),
	)
	pending := soakRoomPending{
		kind: failureOperationRoomRename, rename: intent, roomID: intent.RoomID,
	}
	if !l.journal(operation, &pending) {
		l.pool.SettleRename(intent, failureResultNotSent)
		return nil
	}

	outcome, err := l.mutator.Rename(ctx, intent.Requester, intent.RoomID, intent.NewName)
	return l.observeAdmission(operation.ID, &outcome, err)
}

func (l *soakRoomLanes) muteToggle(ctx context.Context) error {
	intent, ok := l.pool.NextMuteIntent()
	if !ok {
		return nil
	}
	expectMuted := "false"
	if intent.TargetMuted {
		expectMuted = "true"
	}
	operation := l.newOperation(
		soakFailureLaneRoomMutation, failureOperationMuteToggle,
		map[string]string{"roomId": intent.RoomID, "account": intent.Account},
		map[string]string{
			soakFailureAttributeTargetAccount: intent.Account,
			soakFailureAttributeExpectedMuted: expectMuted,
			soakFailureAttributeRequester:     intent.Account,
		},
		roomMutationExpectedEffects(failureOperationMuteToggle),
	)
	pending := soakRoomPending{
		kind: failureOperationMuteToggle, mute: intent, roomID: intent.RoomID,
	}
	if !l.journal(operation, &pending) {
		l.pool.SettleMute(intent, failureResultNotSent)
		return nil
	}

	outcome, err := l.mutator.ToggleMute(ctx, intent.Account, intent.RoomID)
	return l.observeAdmission(operation.ID, &outcome, err)
}

// RoomCreate stops once the run's budget is spent. Rooms accumulate in MongoDB
// with no safe delete path before teardown, so the lane is capped while the
// other room and member lanes keep running.
func (l *soakRoomLanes) RoomCreate(ctx context.Context) error {
	if !l.takeCreateBudget() {
		l.countExhausted("create_budget")
		return nil
	}
	requester, members, ok := l.pool.CreateRoomMembers(l.cfg.CreateRoomSize)
	if !ok {
		l.releaseCreateBudget()
		l.countExhausted(soakRoomPoolReasonNoCandidate)
		return nil
	}
	name := fmt.Sprintf("soak-%s-created-%s", l.cfg.RunID, idgen.GenerateID())

	outcome, err := l.mutator.CreateRoom(ctx, requester, name, members)
	l.record(soakRPCRoomCreate, &outcome)
	if err != nil || outcome.RoomID == "" {
		// Without a room ID there is no target to reconcile. The attempt is
		// still counted as spent budget so a broken create lane cannot spin.
		if err != nil {
			return fmt.Errorf("create soak room: %w", err)
		}
		return nil
	}

	operation := l.newOperation(
		soakFailureLaneRoomCreate, failureOperationRoomCreate,
		map[string]string{"roomId": outcome.RoomID},
		map[string]string{soakFailureAttributeRequester: requester},
		roomCreateExpectedEffects(),
	)
	pending := soakRoomPending{kind: failureOperationRoomCreate, roomID: outcome.RoomID}
	if !l.journal(operation, &pending) {
		// room-service accepted the create, so the room will exist without this
		// run being able to claim or verify it. Name it in the log: teardown
		// only removes rooms carrying the run marker, and this one will not.
		slog.Warn("created soak room is untracked and will outlive teardown",
			"runId", l.cfg.RunID, "roomId", outcome.RoomID)
		l.countUntracked("ownership")
		return nil
	}
	// The reply already arrived, so the operation is activated and its
	// admission recorded from the same outcome the create used.
	return l.observeAdmission(operation.ID, &outcome, nil)
}

// Reconcile retires one due room or member operation. Callers run it inside the
// room read lane so verification adds no unbudgeted request rate.
func (l *soakRoomLanes) Reconcile(
	ctx context.Context,
	verifier *soakRoomStateVerifier,
) (bool, error) {
	if l.ledger == nil || verifier == nil {
		return false, fmt.Errorf("soak room reconciliation is not configured")
	}
	now := l.now().UTC()
	operation, ok := l.ledger.ClaimDueLanes(now, soakRoomLaneNames)
	if !ok {
		return false, nil
	}

	result, reason, err := verifier.Verify(ctx, &operation)
	switch {
	case err != nil, result == soakRoomStateUnknown:
		if now.Before(operation.Deadline) {
			return true, l.release(operation.ID, now)
		}
		return true, l.observe(operation.ID, failureObservationUnverified, failureReasonNone, now)
	case result == soakRoomStateMatched:
		return true, l.observe(operation.ID, failureObservationGood, failureReasonNone, now)
	case result == soakRoomStateMismatch:
		return true, l.observe(operation.ID, failureObservationBad, reason, now)
	default:
		if now.Before(operation.Deadline) {
			return true, l.release(operation.ID, now)
		}
		// Past the deadline with both sources agreeing the effect is absent.
		// failureOperationResult still refuses to call this data loss unless
		// admission accepted the request.
		return true, l.observe(operation.ID, failureObservationMissingAfterDeadline, reason, now)
	}
}

// SettleFinalized returns pool reservations whose operation the ledger already
// closed without this lane seeing it. The background expiry sweep finalizes any
// unclaimed operation once its deadline passes, which is exactly what happens
// when a fault leaves more unresolved work than the reconcile budget retires;
// without this the room keeps its lease forever and stops mutating.
func (l *soakRoomLanes) SettleFinalized() int {
	if l.ledger == nil {
		return 0
	}
	l.mu.Lock()
	operationIDs := make([]string, 0, len(l.pending))
	for operationID := range l.pending {
		operationIDs = append(operationIDs, operationID)
	}
	l.mu.Unlock()

	settled := 0
	for _, operationID := range operationIDs {
		if _, active := l.ledger.Active(operationID); active {
			continue
		}
		// Expiry means nothing was verified, so the effect's real state is
		// unknown and the pair has to be re-probed rather than assumed.
		l.settle(operationID, failureResultUnverified)
		settled++
	}
	return settled
}

// ProbeQuarantine re-reads one parked room/account pair so a candidate whose
// real state became unknown can rejoin the cycle instead of leaking.
func (l *soakRoomLanes) ProbeQuarantine(
	ctx context.Context,
	verifier *soakRoomStateVerifier,
) (bool, error) {
	if verifier == nil {
		return false, fmt.Errorf("soak room probing is not configured")
	}
	probe, ok := l.pool.NextProbe()
	if !ok {
		return false, nil
	}
	if probe.Mute {
		muted, found, err := verifier.store.SubscriptionMuted(ctx, probe.RoomID, probe.Account)
		if err != nil || !found {
			l.pool.ReleaseProbe(probe)
			l.countProbe("unresolved")
			if err != nil {
				return true, fmt.Errorf("probe soak mute state: %w", err)
			}
			return true, nil
		}
		l.pool.ResolveMuteProbe(probe.RoomID, probe.Account, muted)
		return true, nil
	}
	member, err := verifier.store.IsRoomMember(ctx, probe.RoomID, probe.Account)
	if err != nil {
		l.pool.ReleaseProbe(probe)
		l.countProbe("unresolved")
		return true, fmt.Errorf("probe soak room membership: %w", err)
	}
	l.pool.ResolveMemberProbe(probe.RoomID, probe.Account, member)
	return true, nil
}

func (l *soakRoomLanes) newOperation(
	lane string,
	operationType failureOperationType,
	targets map[string]string,
	attributes map[string]string,
	effects []failureExpectedEffect,
) *failureOperation {
	startedAt := l.now().UTC()
	return &failureOperation{
		SchemaVersion: 2, ID: idgen.GenerateUUIDv7(), RunID: l.cfg.RunID,
		Scenario: soakFailureScenario, Lane: lane, OperationType: operationType,
		LifecycleState: failureOperationJournaled, StartedAt: startedAt,
		VerifyAfter: startedAt.Add(max(0, l.cfg.PersistGrace)),
		Deadline:    startedAt.Add(l.cfg.Deadline),
		Targets:     targets, Attributes: attributes, Effects: effects,
	}
}

// journal makes the intent durable before the request goes out. A refusal
// stops the request: an unjournaled room mutation would change state nothing
// can later reconcile, which is worse than a missed dispatch.
func (l *soakRoomLanes) journal(operation *failureOperation, pending *soakRoomPending) bool {
	if err := l.ledger.Start(operation); err != nil {
		l.countUntracked(failureUntrackedReasonStart)
		return false
	}
	l.mu.Lock()
	l.pending[operation.ID] = *pending
	l.mu.Unlock()
	return true
}

func (l *soakRoomLanes) observeAdmission(
	operationID string,
	outcome *soakRoomMutationOutcome,
	err error,
) error {
	l.record(outcome.Action, outcome)

	if err != nil && soakRoomMutationNeverSent(outcome, err) {
		// Proven local failure: nothing left the process, so no downstream
		// effect is expected and no reconciliation is owed.
		if abandonErr := l.ledger.Abandon(operationID, failureResultNotSent, l.now().UTC()); abandonErr != nil {
			return fmt.Errorf("abandon unsent soak room mutation: %w", abandonErr)
		}
		l.settle(operationID, failureResultNotSent)
		return nil
	}
	if activateErr := l.ledger.Activate(operationID, l.now().UTC()); activateErr != nil {
		return fmt.Errorf("activate soak room mutation: %w", activateErr)
	}

	switch {
	case err == nil && outcome.Accepted:
		if _, observeErr := l.ledger.Observe(
			operationID, failureObserverAdmission, failureObservationGood, l.now().UTC(),
		); observeErr != nil {
			return fmt.Errorf("record soak room admission: %w", observeErr)
		}
		return nil
	case err != nil && transientSoakError(outcome.ErrorClass):
		// Ambiguous: the request may or may not have been accepted. It is never
		// resent and never called not_sent; reconciliation decides.
		if _, observeErr := l.ledger.Observe(
			operationID, failureObserverAdmission, failureObservationUnverified, l.now().UTC(),
		); observeErr != nil {
			return fmt.Errorf("record ambiguous soak room admission: %w", observeErr)
		}
		return nil
	default:
		// An explicit rejection has no expected effect left to observe, so the
		// operation is closed now instead of holding a slot until the deadline.
		if abandonErr := l.ledger.AbandonWithReason(
			operationID, failureResultBad, failureReasonAdmissionRejected, l.now().UTC(),
		); abandonErr != nil {
			return fmt.Errorf("close rejected soak room mutation: %w", abandonErr)
		}
		l.settle(operationID, failureResultBad)
		return nil
	}
}

func (l *soakRoomLanes) observe(
	operationID string,
	observation failureObservation,
	reason failureReason,
	now time.Time,
) error {
	finalized, err := l.ledger.ObserveWithReason(
		operationID, failureObserverRoomState, observation, reason, now,
	)
	if err != nil {
		_ = l.ledger.ReleaseClaim(operationID, now.Add(l.cfg.RetryInterval))
		return fmt.Errorf("record soak room state observation: %w", err)
	}
	if finalized {
		l.settle(operationID, soakRoomTerminalResult(observation))
	}
	return nil
}

func (l *soakRoomLanes) release(operationID string, now time.Time) error {
	if err := l.ledger.ReleaseClaim(operationID, now.Add(l.cfg.RetryInterval)); err != nil {
		return fmt.Errorf("reschedule soak room operation: %w", err)
	}
	return nil
}

// settle returns the pool reservation the operation held. Until this runs the
// room keeps its lease, which is what stops a second mutation from racing an
// unresolved one.
func (l *soakRoomLanes) settle(operationID string, result failureResult) {
	l.mu.Lock()
	pending, ok := l.pending[operationID]
	delete(l.pending, operationID)
	l.mu.Unlock()
	if !ok {
		return
	}
	switch pending.kind {
	case failureOperationMemberAdd, failureOperationMemberRemove:
		l.pool.SettleMember(pending.member, result)
	case failureOperationRoomRename:
		l.pool.SettleRename(pending.rename, result)
	case failureOperationMuteToggle:
		l.pool.SettleMute(pending.mute, result)
	case failureOperationRoomCreate:
		l.finishCreate(pending.roomID, result)
	case failureOperationMessageCreate:
		// The message lane keeps no pool reservation, so it never settles here.
	}
}

// finishCreate takes ownership of a confirmed room. Teardown only deletes rooms
// carrying the run marker, so a created room that is never claimed would
// outlive the run.
func (l *soakRoomLanes) finishCreate(roomID string, result failureResult) {
	if roomID == "" || result != failureResultGood {
		return
	}
	if l.store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), soakRequestTimeout)
		defer cancel()
		if err := l.store.AppendOwnedRooms(ctx, l.cfg.RunID, []string{roomID}); err != nil {
			l.countUntracked("ownership")
			return
		}
	}
	if l.reader != nil {
		if requester, ok := l.pool.AnyOwner(); ok {
			l.reader.RegisterCreatedRoom(roomID, requester)
		}
	}
}

func (l *soakRoomLanes) takeCreateBudget() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.budget <= 0 {
		return false
	}
	l.budget--
	l.setBudgetGaugeLocked()
	return true
}

func (l *soakRoomLanes) releaseCreateBudget() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.budget++
	l.setBudgetGaugeLocked()
}

func (l *soakRoomLanes) setBudgetGauge() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.setBudgetGaugeLocked()
}

func (l *soakRoomLanes) setBudgetGaugeLocked() {
	if l.metrics == nil {
		return
	}
	l.metrics.SoakRoomCreateBudgetRemaining.Set(float64(l.budget))
}

// pickRenameTurn alternates the two room-mutation shapes so one rate covers
// both without a second lane.
func (l *soakRoomLanes) pickRenameTurn() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.renameTurn = !l.renameTurn
	return l.renameTurn
}

func (l *soakRoomLanes) record(action soakRPCAction, outcome *soakRoomMutationOutcome) {
	if l.recorder == nil {
		return
	}
	l.recorder.Record(soakMutationSample{
		Action: action, Latency: outcome.Latency, Retries: outcome.Retries,
		ErrorClass: outcome.ErrorClass,
	})
}

func (l *soakRoomLanes) countUntracked(reason string) {
	if l.metrics == nil {
		return
	}
	l.metrics.FailureUntracked.WithLabelValues(reason).Inc()
}

func (l *soakRoomLanes) countExhausted(reason string) {
	if l.metrics == nil {
		return
	}
	l.metrics.SoakRoomPoolExhausted.WithLabelValues(reason).Inc()
}

func (l *soakRoomLanes) countProbe(result string) {
	if l.metrics == nil {
		return
	}
	l.metrics.SoakRoomQuarantineProbes.WithLabelValues(result).Inc()
}

// soakRoomMutationNeverSent reports whether a failed mutation is proven not to
// have left the process. Only a body that could not be encoded and a request
// that was never attempted qualify: a timeout or a dropped connection may have
// been delivered, so calling those not_sent would erase a real effect from the
// accounting.
func soakRoomMutationNeverSent(outcome *soakRoomMutationOutcome, err error) bool {
	if err == nil {
		return false
	}
	if outcome.ErrorClass == soakErrorDecode {
		return true
	}
	return outcome.ErrorClass == "" && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}

func soakRoomTerminalResult(observation failureObservation) failureResult {
	switch observation {
	case failureObservationGood:
		return failureResultGood
	case failureObservationBad:
		return failureResultBad
	case failureObservationMissingAfterDeadline:
		return failureResultMissingAfterDeadline
	default:
		return failureResultUnverified
	}
}
