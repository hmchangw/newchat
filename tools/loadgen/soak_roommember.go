package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
)

type soakRoomLaneConfig struct {
	RunID            string
	SiteID           string
	PersistGrace     time.Duration
	Deadline         time.Duration
	RetryInterval    time.Duration
	RoomCreateBudget int
	CreateRoomSize   int
}

// soakRoomStateStore is the authoritative final-state source for the room and
// member lanes. Its reads are the tiebreaker that turns "room-service did not
// return it" into a defensible absence claim. It is declared here, with the
// lanes and the verifier that consume it, rather than beside mongoSoakStore.
type soakRoomStateStore interface {
	RoomName(ctx context.Context, roomID string) (string, bool, error)
	IsRoomMember(ctx context.Context, roomID, account string) (bool, error)
	SubscriptionMuted(ctx context.Context, roomID, account string) (muted bool, found bool, err error)
	SubscriptionLastSeen(ctx context.Context, roomID, account string) (lastSeenAt time.Time, found bool, err error)
	RoomIDByName(ctx context.Context, siteID, name string) (roomID string, found bool, err error)
	CountCreatedRooms(ctx context.Context, runID string) (int, error)
	AppendOwnedRooms(ctx context.Context, runID string, roomIDs []string) error
}

// soakRoomPending remembers which pool reservation a ledger operation came
// from, so the pool is settled with the terminal result rather than a guess
// made at send time.
type soakRoomPending struct {
	kind   failureOperationType
	member soakMemberIntent
	rename soakRenameIntent
	mute   soakMuteIntent
	read   soakReadIntent
	roomID string
	// roomName identifies a created room before its server-generated ID is
	// known, and remains the only handle when the reply is lost.
	roomName string
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

// Outcomes of loadgen_soak_lane_attempts_total. Only soakLaneAttemptSent is
// offered load; the other two are the two distinct ways a scheduler slot can
// pass without a request leaving the process.
const (
	soakLaneAttemptSent     = "sent"
	soakLaneAttemptNoTarget = "no_target"
	soakLaneAttemptRefused  = "refused"
)

var soakRoomLaneNames = []string{
	soakFailureLaneMemberMutation,
	soakFailureLaneRoomMutation,
	soakFailureLaneRoomCreate,
	soakFailureLaneReadReceipt,
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

// Rehydrate restores the reservations an earlier process held for operations
// the ledger recovered from the WAL, and reports how much of the run's create
// budget those in-flight creates already spent.
//
// Both matter after a pod replacement. Without the reservations a new process
// can race a mutation that is still unresolved; without the budget accounting
// the create lane restarts at a full allowance and the per-run cap becomes a
// per-process one, so a crash loop grows MongoDB without bound.
//
// A reservation that cannot be retaken is not fatal: the operation stays in the
// ledger and still reconciles, it just no longer blocks the pool. That is
// counted rather than hidden.
func (l *soakRoomLanes) Rehydrate(operations []failureOperation) int {
	inFlightCreates := 0
	for i := range operations {
		operation := &operations[i]
		if !slices.Contains(soakRoomLaneNames, operation.Lane) {
			continue
		}
		if operation.OperationType == failureOperationRoomCreate {
			inFlightCreates++
			l.mu.Lock()
			l.pending[operation.ID] = soakRoomPending{
				kind:     failureOperationRoomCreate,
				roomName: operation.Targets["roomName"],
			}
			l.mu.Unlock()
			continue
		}
		roomID := operation.Targets["roomId"]
		account := operation.Targets["account"]
		if !l.pool.ReserveInFlight(operation.OperationType, roomID, account) {
			l.countUntracked("reservation")
			continue
		}
		// The intents are reconstructed from the journaled attributes, not from
		// zero values: settle writes the pool's next expectation from them, so
		// a defaulted Add or TargetMuted would record the opposite of what the
		// operation actually asked for.
		baseline, hasBaseline, _ := soakReadBaseline(operation)
		l.mu.Lock()
		l.pending[operation.ID] = soakRoomPending{
			kind:   operation.OperationType,
			roomID: roomID,
			member: soakMemberIntent{
				RoomID: roomID, Account: account,
				Add:       operation.OperationType == failureOperationMemberAdd,
				Requester: operation.Attributes[soakFailureAttributeRequester],
			},
			rename: soakRenameIntent{
				RoomID:    roomID,
				NewName:   operation.Attributes[soakFailureAttributeExpectedName],
				Requester: operation.Attributes[soakFailureAttributeRequester],
			},
			mute: soakMuteIntent{
				RoomID: roomID, Account: account,
				TargetMuted: operation.Attributes[soakFailureAttributeExpectedMuted] == "true",
			},
			read: soakReadIntent{
				RoomID: roomID, Account: account,
				Baseline: baseline, Known: hasBaseline,
			},
		}
		l.mu.Unlock()
	}
	return inFlightCreates
}

// SpendCreateBudget lowers the remaining allowance by rooms this run already
// created, so the cap is per run rather than per process.
func (l *soakRoomLanes) SpendCreateBudget(spent int) {
	if spent <= 0 {
		return
	}
	l.mu.Lock()
	l.budget = max(0, l.budget-spent)
	l.mu.Unlock()
	l.setBudgetGauge()
}

func (l *soakRoomLanes) MemberMutation(ctx context.Context) error {
	intent, ok := l.pool.NextMemberIntent()
	if !ok {
		l.countAttempt(soakFailureLaneMemberMutation, soakLaneAttemptNoTarget)
		l.countExhausted(soakRoomPoolReasonNoCandidate)
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
		l.countAttempt(soakFailureLaneRoomMutation, soakLaneAttemptNoTarget)
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
		l.countAttempt(soakFailureLaneRoomMutation, soakLaneAttemptNoTarget)
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

// ReadReceipt marks one subscription read. The expected effect is that the
// server-written read cursor moves past the baseline the pool captured, which
// is what makes a lost write detectable without comparing clocks.
func (l *soakRoomLanes) ReadReceipt(ctx context.Context) error {
	intent, ok := l.pool.NextReadIntent()
	if !ok {
		l.countAttempt(soakFailureLaneReadReceipt, soakLaneAttemptNoTarget)
		return nil
	}
	attributes := map[string]string{
		soakFailureAttributeTargetAccount: intent.Account,
		soakFailureAttributeRequester:     intent.Account,
	}
	if intent.Known {
		attributes[soakFailureAttributeReadBaseline] =
			strconv.FormatInt(intent.Baseline.UTC().UnixMilli(), 10)
	}
	operation := l.newOperation(
		soakFailureLaneReadReceipt, failureOperationMessageRead,
		map[string]string{"roomId": intent.RoomID, "account": intent.Account},
		attributes,
		readReceiptExpectedEffects(),
	)
	pending := soakRoomPending{
		kind: failureOperationMessageRead, read: intent, roomID: intent.RoomID,
	}
	if !l.journal(operation, &pending) {
		l.pool.SettleRead(intent, failureResultNotSent, time.Time{})
		return nil
	}

	outcome, err := l.mutator.MarkRead(ctx, intent.Account, intent.RoomID)
	return l.observeAdmission(operation.ID, &outcome, err)
}

// RoomCreate stops once the run's budget is spent. Rooms accumulate in MongoDB
// with no safe delete path before teardown, so the lane is capped while the
// other room and member lanes keep running.
func (l *soakRoomLanes) RoomCreate(ctx context.Context) error {
	if !l.takeCreateBudget() {
		l.countAttempt(soakFailureLaneRoomCreate, soakLaneAttemptNoTarget)
		l.countExhausted("create_budget")
		return nil
	}
	requester, members, ok := l.pool.CreateRoomMembers(l.cfg.CreateRoomSize)
	if !ok {
		l.releaseCreateBudget()
		l.countAttempt(soakFailureLaneRoomCreate, soakLaneAttemptNoTarget)
		l.countExhausted(soakRoomPoolReasonNoCandidate)
		return nil
	}
	// The room ID is server-generated, so the name is the only identifier that
	// exists before the request. Journaling it first is what makes a lost reply
	// recoverable: the room may already exist, and the name is how a later pass
	// — in this process or a replacement one — finds, verifies and claims it.
	name := soakCreatedRoomPrefix(l.cfg.RunID) + idgen.GenerateID()
	operation := l.newOperation(
		soakFailureLaneRoomCreate, failureOperationRoomCreate,
		map[string]string{"roomName": name},
		map[string]string{soakFailureAttributeRequester: requester},
		roomCreateExpectedEffects(),
	)
	pending := soakRoomPending{kind: failureOperationRoomCreate, roomName: name}
	if !l.journal(operation, &pending) {
		l.releaseCreateBudget()
		return nil
	}

	outcome, err := l.mutator.CreateRoom(ctx, requester, name, members)
	return l.observeAdmission(operation.ID, &outcome, err)
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
		// Ownership is a durable prerequisite of finishing a create, not an
		// afterthought: once the ledger records the terminal observation the
		// operation is gone and nothing is left to retry, so a room that could
		// not be claimed would be invisible to both teardown and the create
		// budget. Hold the operation open and try again while the deadline
		// allows it.
		if operation.OperationType == failureOperationRoomCreate {
			if err := l.claimCreatedRoom(ctx, &operation); err != nil {
				if now.Before(operation.Deadline) {
					return true, l.release(operation.ID, now)
				}
				slog.Error("created soak room could not be claimed before its deadline",
					"runId", l.cfg.RunID,
					"roomName", operation.Targets["roomName"], "error", err)
				l.countUntracked("ownership")
			}
		}
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
		l.countAttempt(operation.Lane, soakLaneAttemptRefused)
		return false
	}
	l.mu.Lock()
	l.pending[operation.ID] = *pending
	l.mu.Unlock()
	// Counted here rather than at the top of the lane: the request is only sent
	// once the ledger has accepted the intent, so counting it earlier would let a
	// refusing ledger keep the lane looking fully loaded.
	l.countAttempt(operation.Lane, soakLaneAttemptSent)
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
	case err == nil:
		// The responder answered, but not with a verdict this run recognises —
		// an unknown status, or one a newer service version introduced. It may
		// well have applied the mutation, so calling it a rejection would close
		// the operation, hand the candidate back as untouched, and leave a real
		// effect with nothing left to reconcile it. Only a bounded error
		// envelope below is a refusal.
		if _, observeErr := l.ledger.Observe(
			operationID, failureObserverAdmission, failureObservationUnverified, l.now().UTC(),
		); observeErr != nil {
			return fmt.Errorf("record unreadable soak room admission: %w", observeErr)
		}
		return nil
	case transientSoakError(outcome.ErrorClass):
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
		// Ownership already ran, as a prerequisite of finalizing the operation.
		// There is no pool reservation to return: the create lane holds budget,
		// not a lease.
	case failureOperationMessageRead:
		l.pool.SettleRead(pending.read, result, l.observedReadCursor(&pending))
	case failureOperationMessageCreate:
		// The message lane keeps no pool reservation, so it never settles here.
	}
}

// claimCreatedRoom takes ownership of a confirmed room and brings it into the
// read mix. Teardown only deletes rooms carrying the run marker, so a created
// room that is never claimed would outlive the run.
//
// The caller runs this before finalizing the operation, so returning an error
// leaves the ledger entry open and the work retryable.
func (l *soakRoomLanes) claimCreatedRoom(ctx context.Context, operation *failureOperation) error {
	roomName := operation.Targets["roomName"]
	if roomName == "" {
		return fmt.Errorf("room create operation %q has no room name", operation.ID)
	}
	if l.store == nil {
		return fmt.Errorf("soak room ownership requires a store")
	}
	// The ID is resolved from the name rather than remembered from the reply,
	// so a room whose reply was lost is still claimable — including by a
	// replacement process, whose in-memory reservation is gone.
	roomID, found, err := l.store.RoomIDByName(ctx, l.cfg.SiteID, roomName)
	if err != nil {
		return fmt.Errorf("resolve created soak room %q: %w", roomName, err)
	}
	if !found || roomID == "" {
		return fmt.Errorf("created soak room %q is not visible yet", roomName)
	}
	if err := l.store.AppendOwnedRooms(ctx, l.cfg.RunID, []string{roomID}); err != nil {
		return fmt.Errorf("take ownership of created soak room %q: %w", roomName, err)
	}
	// Reads are issued as the account that created the room. Any other account
	// is not a member, so the read lane would measure authorization failures
	// rather than the reads it exists to exercise. The requester is journaled,
	// so this survives a replacement process.
	if l.reader != nil {
		if requester := operation.Attributes[soakFailureAttributeRequester]; requester != "" {
			l.reader.RegisterCreatedRoom(roomID, requester)
		}
	}
	return nil
}

// observedReadCursor fetches the cursor the run just confirmed so it becomes
// the next operation's baseline. A failure to read it only costs the baseline,
// which the pool then treats as unknown.
func (l *soakRoomLanes) observedReadCursor(pending *soakRoomPending) time.Time {
	if l.store == nil || pending.read.RoomID == "" {
		return time.Time{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), soakRequestTimeout)
	defer cancel()
	lastSeenAt, found, err := l.store.SubscriptionLastSeen(
		ctx, pending.read.RoomID, pending.read.Account,
	)
	if err != nil {
		// Losing the cursor costs the next operation its baseline, which
		// weakens that verification to "some timestamp exists". That is a
		// silent downgrade of the evidence, so it is reported rather than
		// folded into the same zero a missing subscription returns.
		slog.Warn("soak read cursor could not be re-read for the next baseline",
			"runId", l.cfg.RunID, "error", err)
		l.countUntracked("read_baseline")
		return time.Time{}
	}
	if !found {
		return time.Time{}
	}
	return lastSeenAt
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

// countAttempt separates a lane slot that produced a request from one that did
// not, and says why it did not. loadgen_soak_dispatched_total counts scheduler
// slots and is consumed either way, so without this a lane whose pool is
// exhausted — or whose mutations the ledger is refusing — still looks like it is
// offering its configured load. Only sent is offered load: a refused mutation is
// never sent, because untracked room state cannot be reconciled afterwards.
func (l *soakRoomLanes) countAttempt(lane, outcome string) {
	if l.metrics == nil {
		return
	}
	l.metrics.SoakLaneAttempts.WithLabelValues(lane, outcome).Inc()
}

func (l *soakRoomLanes) countProbe(result string) {
	if l.metrics == nil {
		return
	}
	l.metrics.SoakRoomQuarantineProbes.WithLabelValues(result).Inc()
}

// soakRoomMutationNeverSent reports whether a failed mutation is proven not to
// have left the process. Only a body that could not be encoded and a request
// that was never attempted qualify: a timeout, a dropped connection, or a
// reply that failed to decode may all have been delivered, so calling those
// not_sent would erase a real effect from the accounting.
func soakRoomMutationNeverSent(outcome *soakRoomMutationOutcome, err error) bool {
	if err == nil {
		return false
	}
	if outcome.ErrorClass == soakErrorRequestEncode {
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
