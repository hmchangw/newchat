package main

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

type failureObserver string

const (
	failureObserverAdmission failureObserver = "admission"
	failureObserverHistory   failureObserver = "cassandra_history"
)

// failureObserverContractSchemaVersion is 2 because the contract became
// per-lane: version 1 declared one observer set for the whole scenario, which
// no longer describes a run whose lanes require different observers.
const failureObserverContractSchemaVersion = 2

type failureObserverContract struct {
	SchemaVersion            int                          `json:"schemaVersion"`
	Scenario                 string                       `json:"scenario"`
	Observers                []failureObserver            `json:"observers"`
	Lanes                    map[string][]failureObserver `json:"lanes"`
	RecipientObserverEnabled bool                         `json:"recipientObserverEnabled"`
}

// newFailureObserverContract builds the contract the WAL header stores. Both
// optional observers are additive: with each disabled the contract is identical
// to one built without them ever existing, so enabling search observation is
// the only thing that requires a new ledger epoch.
func newFailureObserverContract(recipientEnabled, searchEnabled bool) failureObserverContract {
	messageObservers := []failureObserver{failureObserverAdmission, failureObserverHistory}
	if recipientEnabled {
		messageObservers = append(messageObservers, failureObserverRecipient)
	}
	if searchEnabled {
		messageObservers = append(messageObservers, failureObserverSearchIndex)
	}
	roomObservers := []failureObserver{failureObserverAdmission, failureObserverRoomState}
	observers := []failureObserver{
		failureObserverAdmission, failureObserverHistory, failureObserverRoomState,
	}
	if recipientEnabled {
		observers = append(observers, failureObserverRecipient)
	}
	if searchEnabled {
		observers = append(observers, failureObserverSearchIndex)
	}
	slices.Sort(observers)
	return failureObserverContract{
		SchemaVersion: failureObserverContractSchemaVersion,
		Scenario:      soakFailureScenario, Observers: observers,
		Lanes: map[string][]failureObserver{
			soakFailureLaneMessageSend:    messageObservers,
			soakFailureLaneMemberMutation: slices.Clone(roomObservers),
			soakFailureLaneRoomMutation:   slices.Clone(roomObservers),
			soakFailureLaneRoomCreate:     slices.Clone(roomObservers),
			soakFailureLaneReadReceipt:    slices.Clone(roomObservers),
		},
		RecipientObserverEnabled: recipientEnabled,
	}
}

type failureOperationType string

const (
	failureOperationMessageCreate failureOperationType = "message_create"
	failureOperationMemberAdd     failureOperationType = "member_add"
	failureOperationMemberRemove  failureOperationType = "member_remove"
	failureOperationRoomRename    failureOperationType = "room_rename"
	failureOperationMuteToggle    failureOperationType = "mute_toggle"
	failureOperationRoomCreate    failureOperationType = "room_create"
	failureOperationMessageRead   failureOperationType = "message_read"
)

var failureOperationTypeRegistry = map[failureOperationType]struct{}{
	failureOperationMessageCreate: {}, failureOperationMemberAdd: {},
	failureOperationMemberRemove: {}, failureOperationRoomRename: {},
	failureOperationMuteToggle: {}, failureOperationRoomCreate: {},
	failureOperationMessageRead: {},
}

type failureOperationLifecycle string

const (
	failureOperationJournaled failureOperationLifecycle = "journaled"
	failureOperationActive    failureOperationLifecycle = "active"
)

type failureEffect string

const (
	failureEffectAdmission        failureEffect = "admission"
	failureEffectMessagePersisted failureEffect = "message_persisted"
	failureEffectRecipientEvent   failureEffect = "recipient_event"
	failureEffectMemberState      failureEffect = "member_state"
	failureEffectRoomName         failureEffect = "room_name"
	failureEffectSubscriptionMute failureEffect = "subscription_mute"
	failureEffectRoomCreated      failureEffect = "room_created"
	failureEffectSubscriptionRead failureEffect = "subscription_read"
	failureEffectMessageIndexed   failureEffect = "message_indexed"
)

type failureCardinality struct {
	Mode   string `json:"mode"`
	Count  int    `json:"count"`
	SHA256 string `json:"sha256"`
}

type failureExpectedEffect struct {
	Effect      failureEffect       `json:"effect"`
	Observer    failureObserver     `json:"observer"`
	Required    bool                `json:"required"`
	Cardinality *failureCardinality `json:"cardinality,omitempty"`
}

func messageCreateExpectedEffectsForObservers(
	recipientEnabled bool,
	searchEnabled bool,
	recipientCount int,
	recipientHash string,
) []failureExpectedEffect {
	effects := []failureExpectedEffect{
		{Effect: failureEffectAdmission, Observer: failureObserverAdmission, Required: true},
		{Effect: failureEffectMessagePersisted, Observer: failureObserverHistory, Required: true},
	}
	if recipientEnabled {
		effects = append(effects, failureExpectedEffect{
			Effect: failureEffectRecipientEvent, Observer: failureObserverRecipient, Required: true,
			Cardinality: &failureCardinality{Mode: "exact_set_hash", Count: recipientCount, SHA256: recipientHash},
		})
	}
	if searchEnabled {
		effects = append(effects, failureExpectedEffect{
			Effect: failureEffectMessageIndexed, Observer: failureObserverSearchIndex,
			Required: true,
		})
	}
	return effects
}

func memberMutationExpectedEffects() []failureExpectedEffect {
	return []failureExpectedEffect{
		{Effect: failureEffectAdmission, Observer: failureObserverAdmission, Required: true},
		{Effect: failureEffectMemberState, Observer: failureObserverRoomState, Required: true},
	}
}

func roomMutationExpectedEffects(operationType failureOperationType) []failureExpectedEffect {
	effect := failureEffectRoomName
	if operationType == failureOperationMuteToggle {
		effect = failureEffectSubscriptionMute
	}
	return []failureExpectedEffect{
		{Effect: failureEffectAdmission, Observer: failureObserverAdmission, Required: true},
		{Effect: effect, Observer: failureObserverRoomState, Required: true},
	}
}

func readReceiptExpectedEffects() []failureExpectedEffect {
	return []failureExpectedEffect{
		{Effect: failureEffectAdmission, Observer: failureObserverAdmission, Required: true},
		{Effect: failureEffectSubscriptionRead, Observer: failureObserverRoomState, Required: true},
	}
}

func roomCreateExpectedEffects() []failureExpectedEffect {
	return []failureExpectedEffect{
		{Effect: failureEffectAdmission, Observer: failureObserverAdmission, Required: true},
		{Effect: failureEffectRoomCreated, Observer: failureObserverRoomState, Required: true},
	}
}

type failureObservation string

const (
	failureObservationGood failureObservation = "good"
	failureObservationBad  failureObservation = "bad"
	// failureObservationUnverified records that the observer itself could not
	// answer. It is an availability signal, never evidence of data loss.
	failureObservationUnverified           failureObservation = "unverified"
	failureObservationMissingAfterDeadline failureObservation = "missing_after_deadline"
)

type failureReason string

const (
	failureReasonNone                      failureReason = ""
	failureReasonAdmissionRejected         failureReason = "admission_rejected"
	failureReasonHistoryContentMismatch    failureReason = "history_content_mismatch"
	failureReasonHistoryMissing            failureReason = "history_missing"
	failureReasonRecipientDuplicate        failureReason = "recipient_duplicate"
	failureReasonRecipientUnexpected       failureReason = "recipient_unexpected"
	failureReasonRecipientIdentityMismatch failureReason = "recipient_identity_mismatch"
	failureReasonRecipientMissing          failureReason = "recipient_missing"
	failureReasonPublishLocalError         failureReason = "publish_local_error"
	failureReasonMemberStateMismatch       failureReason = "member_state_mismatch"
	failureReasonRoomNameMismatch          failureReason = "room_name_mismatch"
	failureReasonMuteStateMismatch         failureReason = "mute_state_mismatch"
	failureReasonRoomStateMissing          failureReason = "room_state_missing"
	failureReasonReadStateRegressed        failureReason = "read_state_regressed"
)

var failureReasonRegistry = map[failureReason]struct{}{
	failureReasonNone: {}, failureReasonAdmissionRejected: {},
	failureReasonHistoryContentMismatch: {}, failureReasonHistoryMissing: {},
	failureReasonRecipientDuplicate: {}, failureReasonRecipientUnexpected: {},
	failureReasonRecipientIdentityMismatch: {}, failureReasonRecipientMissing: {},
	failureReasonPublishLocalError: {}, failureReasonMemberStateMismatch: {},
	failureReasonRoomNameMismatch: {}, failureReasonMuteStateMismatch: {},
	failureReasonRoomStateMissing: {}, failureReasonReadStateRegressed: {},
}

var errFailureObserverContractMismatch = errors.New("failure observer contract mismatch")

type failureResult string

const (
	failureResultGood       failureResult = "good"
	failureResultBad        failureResult = "bad"
	failureResultUnverified failureResult = "unverified"
	// failureResultNotSent terminates an operation whose intent was journaled
	// but whose publish never left the process, so no side effect is expected.
	failureResultNotSent              failureResult = "not_sent"
	failureResultMissingAfterDeadline failureResult = "missing_after_deadline"
)

var (
	errFailureLedgerCapacity     = errors.New("failure ledger capacity exceeded")
	errFailureOperationNotActive = errors.New("failure operation is not active")
)

const invalidReasonCapacity = "capacity"

// invalidReasonWAL marks evidence the journal could not be trusted to hold.
const invalidReasonWAL = "wal"

// invalidReasonReconcileCapacity marks a run started below the reconciliation
// floor. Nothing has failed yet, but every message will expire unverified for
// want of a claim, so the evidence is in question from the first second.
const invalidReasonReconcileCapacity = "reconcile_capacity"

// invalidReasonReconcileLagRange marks a run whose reconcile deadline outruns
// the lag histogram. Nothing is wrong with the lane, but the rule that says
// whether a window counted reads lag against the deadline, and past the
// ceiling that reading cannot be made — so the window is unreadable rather
// than merely imprecise.
const invalidReasonReconcileLagRange = "reconcile_lag_range"

// invalidReasonLeaseAbort marks evidence abandoned when an uncooperative lane
// could not drain before the heartbeat lease's shutdown margin. The process
// must exit to preserve the teardown fence, so those in-flight observations
// cannot be treated as ordinary unverified results.
const invalidReasonLeaseAbort = "lease_abort"

var failureInvalidationReasonRegistry = map[string]struct{}{
	"capacity": {}, "wal": {}, "accounting_invariant": {}, "observer_queue": {},
	invalidReasonReconcileCapacity: {}, invalidReasonReconcileLagRange: {},
	invalidReasonLeaseAbort: {},
	"observer_malformed":    {}, "recipient_recovery": {}, "recipient_observer": {},
	"timeline": {}, "other": {},
	"sidecar": {},
}

var failureOperationScenarioRegistry = map[string]struct{}{
	soakFailureScenario: {},
}

var failureOperationLaneRegistry = map[string]struct{}{
	soakFailureLaneMessageSend:    {},
	soakFailureLaneMemberMutation: {},
	soakFailureLaneRoomMutation:   {},
	soakFailureLaneRoomCreate:     {},
	soakFailureLaneReadReceipt:    {},
}

type failureOperation struct {
	SchemaVersion      int                                    `json:"schemaVersion,omitempty"`
	ID                 string                                 `json:"operationId"`
	CorrelationID      string                                 `json:"correlationId,omitempty"`
	RunID              string                                 `json:"runId,omitempty"`
	Scenario           string                                 `json:"scenario"`
	Lane               string                                 `json:"lane"`
	OperationType      failureOperationType                   `json:"operationType,omitempty"`
	StartedAt          time.Time                              `json:"startedAt"`
	VerifyAfter        time.Time                              `json:"verifyAfter"`
	Deadline           time.Time                              `json:"deadline"`
	Targets            map[string]string                      `json:"targets,omitempty"`
	Effects            []failureExpectedEffect                `json:"expectedEffects,omitempty"`
	Expected           []failureObserver                      `json:"expected,omitempty"`
	Attributes         map[string]string                      `json:"attributes,omitempty"`
	Observations       map[failureObserver]failureObservation `json:"observations,omitempty"`
	ObservationReasons map[failureObserver]failureReason      `json:"observationReasons,omitempty"`
	FinalResult        failureResult                          `json:"finalResult,omitempty"`
	FinalReason        failureReason                          `json:"finalReason,omitempty"`
	EvidenceRefs       []string                               `json:"evidenceRefs,omitempty"`
	LifecycleState     failureOperationLifecycle              `json:"lifecycleState,omitempty"`

	nextVerifyAt time.Time
	claimed      bool
	// heapIndex is the operation's position in the ledger's verification queue,
	// or -1 when it is not queued.
	heapIndex int
}

type failureLedgerEvent struct {
	SchemaVersion     int                                               `json:"schemaVersion,omitempty"`
	Type              string                                            `json:"type"`
	Operation         *failureOperation                                 `json:"operation,omitempty"`
	OperationID       string                                            `json:"operationId,omitempty"`
	Observer          failureObserver                                   `json:"observer,omitempty"`
	Observation       failureObservation                                `json:"observation,omitempty"`
	Reason            failureReason                                     `json:"reason,omitempty"`
	Result            failureResult                                     `json:"result,omitempty"`
	Results           map[failureResult]uint64                          `json:"results,omitempty"`
	ObservationCounts map[failureObserver]map[failureObservation]uint64 `json:"observationCounts,omitempty"`
	NotSent           []string                                          `json:"notSent,omitempty"`
	InvalidReason     string                                            `json:"invalidReason,omitempty"`
	At                time.Time                                         `json:"at"`
}

//nolint:gocritic // A value receiver preserves json.Marshaler behavior for operation values and pointers.
func (o failureOperation) MarshalJSON() ([]byte, error) {
	type operationAlias failureOperation
	if o.SchemaVersion == 0 {
		legacy := struct {
			ID string `json:"id"`
			operationAlias
		}{ID: o.ID, operationAlias: operationAlias(o)}
		legacy.operationAlias.ID = ""
		return json.Marshal(legacy)
	}
	return json.Marshal(operationAlias(o))
}

func (o *failureOperation) UnmarshalJSON(data []byte) error {
	type operationAlias failureOperation
	var decoded struct {
		LegacyID string `json:"id"`
		operationAlias
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*o = failureOperation(decoded.operationAlias)
	if o.ID == "" {
		o.ID = decoded.LegacyID
	}
	return nil
}

const (
	failureLedgerEventStarted    = "started"
	failureLedgerEventActivated  = "activated"
	failureLedgerEventObserved   = "observed"
	failureLedgerEventFinalized  = "finalized"
	failureLedgerEventCheckpoint = "checkpoint"
	failureLedgerEventInvariant  = "accounting_invariant"
	// failureLedgerEventInvalidated carries the reason a run's evidence stopped
	// standing. It is durable because the window a run invalidates outlives the
	// process: the pod restarts, the journal replays, and the verdicts from
	// before the restart are still in it.
	failureLedgerEventInvalidated = "invalidated"
)

// streamingFailureJournal is the recovery path a journal should offer: the
// buffering Replay holds the whole file, which is what turns a restart of a
// long-lived run into an OOM. Journals that cannot stream still work through
// Replay, so this stays an optional capability rather than a breaking change.
type streamingFailureJournal interface {
	ReplayEach(func(*failureLedgerEvent) error) error
}

type failureJournal interface {
	Replay() ([]failureLedgerEvent, error)
	Append(*failureLedgerEvent) error
	Compact([]failureLedgerEvent) error
	Size() int64
	Close() error
}

type failureLedgerConfig struct {
	Capacity int
	// MaxJournalBytes reclaims the journal once it passes this size, regardless
	// of how many operations have finalized. Waiting on a finalize count alone
	// means a run whose operations all outlive their deadline never compacts,
	// and the file grows without bound. Zero disables the size trigger.
	MaxJournalBytes int64
	// ExpireBatch bounds one sweep. The sweep holds the ledger lock while it
	// writes two or three journal records per expired operation, so an unbounded
	// pass over a large backlog stalls every lane at once. Zero leaves it
	// unbounded.
	ExpireBatch      int
	CompactEvery     int
	Journal          failureJournal
	Now              func() time.Time
	Recorder         failureLedgerRecorder
	ObserverContract *failureObserverContract
}

type failureLedgerRecorder interface {
	OperationStarted(*failureOperation)
	ObservationRecorded(*failureOperation, failureObserver, failureObservation)
	OperationFinalized(*failureOperation, failureResult)
	Recovered(int)
	Invalidated(string)
	JournalSize(int64)
}

type failureLedgerSnapshot struct {
	Active        int
	Recovered     int
	Dropped       int
	InvalidReason string
	Results       map[failureResult]uint64
	Observations  map[failureObserver]map[failureObservation]uint64
	JournalBytes  int64
}

type failureLedger struct {
	mu         sync.Mutex
	startingWG sync.WaitGroup

	capacity        int
	compactEvery    int
	maxJournalBytes int64
	expireBatch     int
	journal         failureJournal
	now             func() time.Time
	recorder        failureLedgerRecorder

	active        map[string]*failureOperation
	starting      map[string]struct{}
	verifyQueues  map[string]*failureVerifyQueue
	results       map[failureResult]uint64
	observations  map[failureObserver]map[failureObservation]uint64
	notSent       map[string]struct{}
	notSentOrder  []string
	recovered     int
	dropped       int
	invalidReason string
	// invalidReasons is every distinct cause, in the order observed.
	// invalidReason keeps the first — that is when the evidence stopped
	// standing — while the counter reports them all, because a run invalidated
	// at startup can still lose its WAL an hour later and that is the failure
	// an operator has to act on.
	invalidReasons []string
	// journalClosed marks the point after which nothing can be written. It is
	// distinct from closed: Close flushes owed causes while still open, and a
	// cause arriving after this point is neither persistable nor a WAL fault.
	journalClosed bool
	// persistedInvalidReasons is the subset the journal actually accepted. A
	// reason absent here is retried on the next invalidation rather than being
	// treated as durably recorded.
	persistedInvalidReasons []string
	// replaying suppresses the journal write while recovery re-applies records
	// that are already in the file.
	replaying             bool
	closed                bool
	finalizedSinceCompact int
	recoveredEvents       int
}

// failureVerifyQueue orders unclaimed operations by their next verification
// time so ClaimDue is O(log n) instead of scanning every in-flight operation
// under the lock that also serializes fsync-bearing journal appends.
type failureVerifyQueue []*failureOperation

func (q failureVerifyQueue) Len() int { return len(q) }

func (q failureVerifyQueue) Less(i, j int) bool {
	return q[i].nextVerifyAt.Before(q[j].nextVerifyAt)
}

func (q failureVerifyQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].heapIndex = i
	q[j].heapIndex = j
}

func (q *failureVerifyQueue) Push(item any) {
	operation, ok := item.(*failureOperation)
	if !ok {
		return
	}
	operation.heapIndex = len(*q)
	*q = append(*q, operation)
}

func (q *failureVerifyQueue) Pop() any {
	old := *q
	last := len(old) - 1
	operation := old[last]
	old[last] = nil
	operation.heapIndex = -1
	*q = old[:last]
	return operation
}

func newFailureLedger(cfg *failureLedgerConfig) (*failureLedger, error) {
	if cfg.Capacity <= 0 {
		return nil, fmt.Errorf("failure ledger capacity must be greater than zero")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.CompactEvery <= 0 {
		cfg.CompactEvery = 10000
	}
	ledger := &failureLedger{
		capacity:        cfg.Capacity,
		compactEvery:    cfg.CompactEvery,
		maxJournalBytes: max(0, cfg.MaxJournalBytes),
		expireBatch:     max(0, cfg.ExpireBatch),
		journal:         cfg.Journal,
		now:             cfg.Now,
		recorder:        cfg.Recorder,
		active:          make(map[string]*failureOperation, cfg.Capacity),
		starting:        make(map[string]struct{}),
		verifyQueues:    make(map[string]*failureVerifyQueue),
		results:         make(map[failureResult]uint64),
		observations:    make(map[failureObserver]map[failureObservation]uint64),
		notSent:         make(map[string]struct{}),
	}
	if cfg.Journal == nil {
		return ledger, nil
	}
	if err := ledger.recoverFrom(cfg.Journal); err != nil {
		return nil, err
	}
	if cfg.ObserverContract != nil {
		configurer, ok := cfg.Journal.(interface {
			ConfigureObserverContract(failureObserverContract, []failureOperation) error
		})
		if !ok {
			return nil, fmt.Errorf("failure journal does not support observer contracts")
		}
		active := make([]failureOperation, 0, len(ledger.active))
		for _, operation := range ledger.active {
			active = append(active, *cloneFailureOperation(operation))
		}
		if err := configurer.ConfigureObserverContract(*cfg.ObserverContract, active); err != nil {
			return nil, fmt.Errorf("validate failure observer contract; start a new SOAK_RUN_ID: %w", err)
		}
	}
	upgraded := false
	if upgrade, ok := cfg.Journal.(interface{ NeedsUpgrade() bool }); ok && upgrade.NeedsUpgrade() {
		if err := ledger.compactLocked(cfg.Now().UTC()); err != nil {
			return nil, fmt.Errorf("upgrade legacy failure ledger journal: %w", err)
		}
		upgraded = true
	}
	// Reclaim the inherited journal once, before the run resumes. A journal that
	// has been running for hours is mostly retired evidence, and nothing else
	// reclaims it until CompactEvery more operations finalize — which a process
	// that keeps restarting never reaches, so the file would grow on every
	// restart and make the next recovery more expensive than the last.
	// Skipped when replay dropped operations: they exist nowhere else, and
	// leaving the file intact is what lets an operator raise the capacity,
	// restart, and recover them. The size trigger still reclaims it later.
	//
	// A failure here is logged rather than returned. The journal replayed
	// cleanly, so the recovered state is sound; failing would discard it and
	// downgrade the run to an invalid in-memory ledger over what is only an
	// optimisation.
	if !upgraded && ledger.recoveredEvents > 0 && ledger.dropped == 0 &&
		ledger.canCompactLocked() {
		if err := ledger.compactLocked(cfg.Now().UTC()); err != nil {
			slog.Error("could not reclaim recovered failure ledger journal",
				"error", err)
		}
	}
	ledger.recovered = len(ledger.active)
	if ledger.recorder != nil {
		ledger.recorder.Recovered(ledger.recovered)
		ledger.recorder.JournalSize(cfg.Journal.Size())
		for _, operation := range ledger.active {
			ledger.recorder.OperationStarted(cloneFailureOperation(operation))
		}
	}
	return ledger, nil
}

func (l *failureLedger) Start(operation *failureOperation) error {
	tracked := cloneFailureOperation(operation)
	if err := validateFailureOperation(tracked); err != nil {
		return fmt.Errorf("start failure operation: %w", err)
	}
	tracked.Observations = make(map[failureObserver]failureObservation)
	tracked.ObservationReasons = make(map[failureObserver]failureReason)
	tracked.nextVerifyAt = tracked.VerifyAfter

	l.mu.Lock()
	if err := l.ensureOpen(); err != nil {
		l.mu.Unlock()
		return err
	}
	_, active := l.active[tracked.ID]
	_, starting := l.starting[tracked.ID]
	if active || starting {
		l.mu.Unlock()
		return fmt.Errorf("failure operation %q is already active", tracked.ID)
	}
	if len(l.active)+len(l.starting) >= l.capacity {
		l.invalidateLocked(invalidReasonCapacity)
		l.mu.Unlock()
		return fmt.Errorf("start failure operation %q: %w", tracked.ID, errFailureLedgerCapacity)
	}
	// Before the append below, which happens off the mutex so concurrent starts
	// can share one fsync through the group-commit barrier: no evidence may
	// reach the file while a verdict that disqualifies it is already owed.
	//
	// The barrier is why this is a check and not a critical section. Holding
	// the mutex across the append would serialise the starts the barrier exists
	// to batch, so a verdict raised *during* the append still lands behind the
	// record; it is settled on the far side instead, leaving a kill in that
	// window as the residual.
	if err := l.settleBeforeAppendLocked(); err != nil {
		l.mu.Unlock()
		return fmt.Errorf("start failure operation %q: %w", tracked.ID, err)
	}
	l.starting[tracked.ID] = struct{}{}
	l.startingWG.Add(1)
	l.mu.Unlock()

	event := failureLedgerEvent{
		Type: failureLedgerEventStarted, Operation: cloneFailureOperation(tracked),
		At: l.now().UTC(),
	}
	var appendErr error
	if l.journal != nil {
		appendErr = l.journal.Append(&event)
	}

	l.mu.Lock()
	delete(l.starting, tracked.ID)
	defer func() {
		l.mu.Unlock()
		l.startingWG.Done()
	}()
	if appendErr != nil {
		l.noteInvalidationLocked(invalidReasonWAL)
		return fmt.Errorf("persist failure operation %q: %w", tracked.ID, appendErr)
	}
	if l.recorder != nil && l.journal != nil {
		l.recorder.JournalSize(l.journal.Size())
	}
	// Again, on the far side of an append that ran off the mutex: a verdict
	// raised while this record was being written could not be settled before
	// it, so it is settled the moment the lock is back. The record is already
	// on disk by then, which is the residual this leaves — see the comment on
	// the settle above the append.
	l.retryPendingInvalidationsLocked()
	l.active[tracked.ID] = tracked
	l.enqueueLocked(tracked)
	if l.recorder != nil {
		l.recorder.OperationStarted(cloneFailureOperation(tracked))
	}
	return nil
}

func (l *failureLedger) Activate(operationID string, at time.Time) error {
	if at.IsZero() {
		at = l.now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return err
	}
	operation := l.active[operationID]
	if operation == nil {
		return fmt.Errorf("activate failure operation %q: %w", operationID, errFailureOperationNotActive)
	}
	if operation.LifecycleState == failureOperationActive {
		return nil
	}
	if operation.LifecycleState != failureOperationJournaled {
		return fmt.Errorf("activate failure operation %q: invalid lifecycle state %q", operationID, operation.LifecycleState)
	}
	event := failureLedgerEvent{
		Type: failureLedgerEventActivated, OperationID: operationID, At: at.UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.noteInvalidationLocked(invalidReasonWAL)
		return fmt.Errorf("persist activated failure operation %q: %w", operationID, err)
	}
	operation.LifecycleState = failureOperationActive
	return nil
}

// Abandon terminates an active operation with an explicit result. It exists for
// outcomes that no observer can report, most importantly a send whose intent was
// journaled before a publish that never left the process: without this the
// unresolved history observer would later expire into the data-loss bucket.
func (l *failureLedger) Abandon(
	operationID string,
	result failureResult,
	at time.Time,
) error {
	reason := failureReasonNone
	if result == failureResultNotSent {
		reason = failureReasonPublishLocalError
	}
	return l.AbandonWithReason(operationID, result, reason, at)
}

func (l *failureLedger) AbandonWithReason(
	operationID string,
	result failureResult,
	reason failureReason,
	at time.Time,
) error {
	if !validFailureResult(result) {
		return fmt.Errorf("invalid failure result %q", result)
	}
	if _, known := failureReasonRegistry[reason]; !known {
		return fmt.Errorf("unsupported failure final reason %q", reason)
	}
	if result == failureResultBad && reason == failureReasonNone {
		return fmt.Errorf("bad failure result requires a bounded reason")
	}
	if result == failureResultNotSent && reason != failureReasonPublishLocalError {
		return fmt.Errorf("not_sent requires reason %q", failureReasonPublishLocalError)
	}
	if at.IsZero() {
		at = l.now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return err
	}
	operation := l.active[operationID]
	if operation == nil {
		return fmt.Errorf("abandon failure operation %q: %w", operationID, errFailureOperationNotActive)
	}
	if result == failureResultNotSent && operation.LifecycleState == failureOperationActive {
		return fmt.Errorf("abandon failure operation %q as not_sent: publish was attempted", operationID)
	}
	if err := l.finalizeLocked(operation, result, reason, at); err != nil {
		return err
	}
	return nil
}

// Retired reports that the ledger no longer holds the operation. An error from
// a finalizing call does not answer that question: finalizeLocked commits the
// removal and compacts afterwards, so a compaction failure returns an error on
// an operation that is already gone — and one nothing reports can never be
// reported again.
func (l *failureLedger) Retired(operationID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, active := l.active[operationID]
	return !active
}

func (l *failureLedger) Observe(
	operationID string,
	observer failureObserver,
	observation failureObservation,
	at time.Time,
) (bool, error) {
	return l.ObserveWithReason(
		operationID,
		observer,
		observation,
		defaultFailureReason(observer, observation),
		at,
	)
}

func (l *failureLedger) ObserveWithReason(
	operationID string,
	observer failureObserver,
	observation failureObservation,
	reason failureReason,
	at time.Time,
) (bool, error) {
	if _, known := failureReasonRegistry[reason]; !known {
		return false, fmt.Errorf("unsupported failure observation reason %q", reason)
	}
	if (observation == failureObservationBad ||
		observation == failureObservationMissingAfterDeadline) && reason == failureReasonNone {
		return false, fmt.Errorf("failure observation %q requires a bounded reason", observation)
	}
	if observation != failureObservationBad &&
		observation != failureObservationMissingAfterDeadline &&
		reason != failureReasonNone {
		return false, fmt.Errorf("failure observation %q cannot use negative reason %q", observation, reason)
	}
	if at.IsZero() {
		at = l.now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return false, err
	}
	operation := l.active[operationID]
	if operation == nil {
		if _, notSent := l.notSent[operationID]; notSent {
			event := failureLedgerEvent{
				Type: failureLedgerEventInvariant, OperationID: operationID,
				Observer: observer, Observation: observation, At: at.UTC(),
			}
			if err := l.appendLocked(&event); err != nil {
				l.noteInvalidationLocked(invalidReasonWAL)
				return false, fmt.Errorf("persist accounting invariant for %q: %w", operationID, err)
			}
			l.invalidateLocked("accounting_invariant")
			return false, fmt.Errorf("failure operation %q accounting invariant: downstream effect observed after not_sent", operationID)
		}
		return false, fmt.Errorf(
			"observe failure operation %q: %w", operationID, errFailureOperationNotActive,
		)
	}
	if !slices.Contains(operation.Expected, observer) {
		return false, fmt.Errorf(
			"failure operation %q does not expect observer %q",
			operationID,
			observer,
		)
	}
	if !validFailureObservation(observation) {
		return false, fmt.Errorf("invalid failure observation %q", observation)
	}
	if existing, exists := operation.Observations[observer]; exists {
		if existing == observation {
			return false, nil
		}
		return false, fmt.Errorf(
			"failure operation %q observer %q is already %q",
			operationID,
			observer,
			existing,
		)
	}
	if at.IsZero() {
		at = l.now()
	}
	event := failureLedgerEvent{
		Type: failureLedgerEventObserved, OperationID: operationID,
		Observer: observer, Observation: observation, Reason: reason, At: at.UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.noteInvalidationLocked(invalidReasonWAL)
		return false, fmt.Errorf("persist failure observation for %q: %w", operationID, err)
	}
	operation.Observations[observer] = observation
	operation.ObservationReasons[observer] = reason
	l.countObservationLocked(observer, observation)
	operation.claimed = false
	l.dequeueLocked(operation)
	if l.recorder != nil {
		l.recorder.ObservationRecorded(
			cloneFailureOperation(operation), observer, observation,
		)
		if recorder, ok := l.recorder.(interface {
			ObservationReasonRecorded(*failureOperation, failureObserver, failureObservation, failureReason)
		}); ok {
			recorder.ObservationReasonRecorded(
				cloneFailureOperation(operation), observer, observation, reason,
			)
		}
	}
	if len(operation.Observations) != len(operation.Expected) {
		l.enqueueLocked(operation)
		return false, nil
	}
	result := failureOperationResult(operation)
	if err := l.finalizeLocked(operation, result, failureOperationFinalReason(operation, result), at); err != nil {
		return false, err
	}
	return true, nil
}

func defaultFailureReason(
	observer failureObserver,
	observation failureObservation,
) failureReason {
	switch {
	case observer == failureObserverAdmission && observation == failureObservationBad:
		return failureReasonAdmissionRejected
	case observer == failureObserverHistory && observation == failureObservationBad:
		return failureReasonHistoryContentMismatch
	case observer == failureObserverHistory && observation == failureObservationMissingAfterDeadline:
		return failureReasonHistoryMissing
	case observer == failureObserverRecipient && observation == failureObservationBad:
		return failureReasonRecipientIdentityMismatch
	case observer == failureObserverRecipient && observation == failureObservationMissingAfterDeadline:
		return failureReasonRecipientMissing
	default:
		return failureReasonNone
	}
}

// Expire finalizes every operation past its deadline and returns the IDs it
// actually finalized. The IDs matter to callers holding per-operation state:
// a successful call is not a complete one — it stops at expireBatch and skips
// claimed operations mid-verification — so "the sweep succeeded" is not a
// licence to discard anything else that looks overdue.
// failureExpiryGrace is how long the sweep leaves a scheduled probe alone
// before retiring the operation anyway, derived from the operation's own
// verification window so it needs no configuration and scales with the run.
func failureExpiryGrace(operation *failureOperation) time.Duration {
	return soakFailureExpiryInterval(operation.Deadline.Sub(operation.StartedAt))
}

func (l *failureLedger) Expire(now time.Time) ([]string, error) {
	if now.IsZero() {
		now = l.now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return nil, err
	}
	var finalized []string
	for _, operation := range l.active {
		if l.expireBatch > 0 && len(finalized) >= l.expireBatch {
			break
		}
		if now.Before(operation.Deadline) {
			continue
		}
		// An operation whose next probe is still owed its turn is not the
		// sweep's to take. The reconciler schedules that probe on the deadline
		// itself, and only a probe can tell a lost message from one nobody
		// looked for — this sweep queries nothing and records unverified, so
		// getting there first would report genuine data loss as an unread
		// window. One sweep interval of grace lets a healthy lane deliver the
		// verdict; past that the lane really did not look, and unverified
		// becomes the honest answer.
		if now.Before(operation.nextVerifyAt.Add(failureExpiryGrace(operation))) {
			continue
		}
		// A claimed operation is mid-verification. Finalizing it here would
		// discard a read-back that is about to succeed and report the message
		// as missing; the next pass picks it up once the claim is released.
		if operation.claimed {
			continue
		}
		for _, observer := range operation.Expected {
			if _, exists := operation.Observations[observer]; exists {
				continue
			}
			event := failureLedgerEvent{
				Type: failureLedgerEventObserved, OperationID: operation.ID,
				Observer: observer, Observation: failureObservationUnverified,
				At: now.UTC(),
			}
			if err := l.appendLocked(&event); err != nil {
				l.noteInvalidationLocked(invalidReasonWAL)
				return finalized, fmt.Errorf(
					"persist expired failure observation for %q: %w", operation.ID, err,
				)
			}
			operation.Observations[observer] = failureObservationUnverified
			operation.ObservationReasons[observer] = failureReasonNone
			l.countObservationLocked(observer, failureObservationUnverified)
			if l.recorder != nil {
				l.recorder.ObservationRecorded(
					cloneFailureOperation(operation), observer,
					failureObservationUnverified,
				)
			}
		}
		result := failureOperationResult(operation)
		operationID := operation.ID
		err := l.finalizeLocked(
			operation, result, failureOperationFinalReason(operation, result), now,
		)
		// Report by what left the ledger, not by whether the call succeeded.
		// finalizeLocked drops the operation from l.active and compacts
		// afterwards, so it can fail on one it has already retired — and an ID
		// this loop does not report can never be reported by a later sweep.
		if _, stillActive := l.active[operationID]; !stillActive {
			finalized = append(finalized, operationID)
		}
		if err != nil {
			return finalized, err
		}
	}
	return finalized, nil
}

func (l *failureLedger) ClaimDue(now time.Time) (failureOperation, bool) {
	return l.claimDue(now, nil)
}

// ClaimDueLanes restricts a claim to the given lanes. Each lane is reconciled
// by the observer that understands its effects, so a lane-blind claim would
// hand a room mutation to the message-history verifier and vice versa.
func (l *failureLedger) ClaimDueLanes(now time.Time, lanes []string) (failureOperation, bool) {
	if len(lanes) == 0 {
		return failureOperation{}, false
	}
	return l.claimDue(now, lanes)
}

func (l *failureLedger) claimDue(now time.Time, lanes []string) (failureOperation, bool) {
	if now.IsZero() {
		now = l.now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if lanes == nil {
		lanes = make([]string, 0, len(l.verifyQueues))
		for lane := range l.verifyQueues {
			lanes = append(lanes, lane)
		}
		slices.Sort(lanes)
	}
	var earliest *failureVerifyQueue
	for _, lane := range lanes {
		queue := l.verifyQueues[lane]
		if queue == nil || queue.Len() == 0 || now.Before((*queue)[0].nextVerifyAt) {
			continue
		}
		if earliest == nil || (*queue)[0].nextVerifyAt.Before((*earliest)[0].nextVerifyAt) {
			earliest = queue
		}
	}
	if earliest == nil {
		return failureOperation{}, false
	}
	selected, ok := heap.Pop(earliest).(*failureOperation)
	if !ok {
		return failureOperation{}, false
	}
	selected.claimed = true
	return *cloneFailureOperation(selected), true
}

func (l *failureLedger) ReleaseClaim(operationID string, next time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	operation := l.active[operationID]
	if operation == nil {
		return fmt.Errorf(
			"release failure operation %q: %w", operationID, errFailureOperationNotActive,
		)
	}
	operation.claimed = false
	operation.nextVerifyAt = next.UTC()
	l.enqueueLocked(operation)
	return nil
}

func (l *failureLedger) enqueueLocked(operation *failureOperation) {
	if operation.heapIndex >= 0 || operation.claimed {
		return
	}
	if !l.scheduleNextLocked(operation) {
		return
	}
	queue := l.verifyQueues[operation.Lane]
	if queue == nil {
		queue = &failureVerifyQueue{}
		l.verifyQueues[operation.Lane] = queue
	}
	heap.Push(queue, operation)
}

// scheduleNextLocked decides when an operation becomes claimable again. A
// query observer polls from its verify time so a converged effect is seen
// early; an event observer has nothing to poll, so it waits for the deadline
// and is resolved from what arrived by then.
func (l *failureLedger) scheduleNextLocked(operation *failureOperation) bool {
	for _, observer := range operation.Expected {
		if _, observed := operation.Observations[observer]; observed {
			continue
		}
		if failureObserverRegistry[observer].Mode == failureObserverQuery {
			if operation.nextVerifyAt.IsZero() {
				operation.nextVerifyAt = operation.VerifyAfter
			}
			return true
		}
	}
	for _, observer := range operation.Expected {
		if _, observed := operation.Observations[observer]; !observed {
			operation.nextVerifyAt = operation.Deadline
			return true
		}
	}
	return false
}

func (l *failureLedger) dequeueLocked(operation *failureOperation) {
	queue := l.verifyQueues[operation.Lane]
	if operation.heapIndex < 0 || queue == nil {
		return
	}
	heap.Remove(queue, operation.heapIndex)
}

func (l *failureLedger) Snapshot() failureLedgerSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	results := make(map[failureResult]uint64, len(l.results))
	for result, count := range l.results {
		results[result] = count
	}
	observations := cloneFailureObservationCounts(l.observations)
	journalBytes := int64(0)
	if l.journal != nil {
		journalBytes = l.journal.Size()
	}
	return failureLedgerSnapshot{
		Active: len(l.active), Recovered: l.recovered, Dropped: l.dropped,
		InvalidReason: l.invalidReason, Results: results, Observations: observations,
		JournalBytes: journalBytes,
	}
}

func (l *failureLedger) Active(operationID string) (failureOperation, bool) {
	if l == nil {
		return failureOperation{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	operation := l.active[operationID]
	if operation == nil {
		return failureOperation{}, false
	}
	return *cloneFailureOperation(operation), true
}

func (l *failureLedger) ActiveOperations() []failureOperation {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	operationIDs := make([]string, 0, len(l.active))
	for operationID := range l.active {
		operationIDs = append(operationIDs, operationID)
	}
	slices.Sort(operationIDs)
	operations := make([]failureOperation, 0, len(operationIDs))
	for _, operationID := range operationIDs {
		operations = append(operations, *cloneFailureOperation(l.active[operationID]))
	}
	return operations
}

func (l *failureLedger) Invalidate(reason string) {
	if l == nil || reason == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.invalidateLocked(registeredInvalidationReason(reason))
}

// registeredInvalidationReason keeps the label set bounded. An unregistered
// reason becomes "other" rather than adding a series nobody declared.
func registeredInvalidationReason(reason string) string {
	if _, known := failureInvalidationReasonRegistry[reason]; !known {
		return "other"
	}
	return reason
}

// invalidateReplayedLocked applies a cause read back from the journal. It folds
// through the registry because a journal written by a newer build is exactly
// where a reason this one does not know arrives.
//
// A record with no reason at all is a different case: the reason is the event's
// only payload, so skipping it would drop the invalidation itself and let a run
// that had disowned its evidence come back looking sound. It fails the replay
// like every other malformed record, which degrades the run to an in-memory
// ledger already invalidated for "wal" rather than taking it down. The caller
// names the record, as it does for every other replay failure.
func (l *failureLedger) invalidateReplayedLocked(reason string) error {
	if reason == "" {
		return fmt.Errorf("invalidation is missing its reason")
	}
	l.invalidateLocked(registeredInvalidationReason(reason))
	return nil
}

// UnpersistedInvalidations reports the causes held only in memory. A caller
// that cannot afford to continue on evidence whose disqualifier the journal
// never accepted asks before it commits to the run.
func (l *failureLedger) UnpersistedInvalidations() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.unpersistedInvalidationsLocked()
}

func (l *failureLedger) unpersistedInvalidationsLocked() []string {
	var unpersisted []string
	for _, reason := range l.invalidReasons {
		if !slices.Contains(l.persistedInvalidReasons, reason) {
			unpersisted = append(unpersisted, reason)
		}
	}
	return unpersisted
}

// flushInvalidationsLocked makes one last attempt at every cause the journal
// never accepted, and reports what still did not land. A run that closes with
// an invalidation only in memory would leave the next replay presenting
// evidence this run had already disowned.
func (l *failureLedger) flushInvalidationsLocked() error {
	l.retryPendingInvalidationsLocked()
	unpersisted := l.unpersistedInvalidationsLocked()
	if len(unpersisted) == 0 {
		return nil
	}
	return fmt.Errorf(
		"failure ledger could not persist invalidation %s",
		strings.Join(unpersisted, ", "),
	)
}

func (l *failureLedger) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()
	// Before the flush, not after. Start appends outside the mutex, so a starter
	// already past the closed check can still be writing; if that append fails
	// it raises the wal cause, and a flush that ran first would have declared
	// the journal complete without it.
	l.startingWG.Wait()

	l.mu.Lock()
	// Before the journal goes away, not after: this is the last chance to land
	// a cause the file never accepted.
	flushErr := l.flushInvalidationsLocked()
	// Taken out under the mutex so a late invalidation cannot append to a file
	// that is being closed: after this, no locked path holds a journal to write
	// to, and journalClosed says why.
	journal := l.journal
	l.journal = nil
	l.journalClosed = true
	l.mu.Unlock()
	if journal == nil {
		return flushErr
	}
	if err := journal.Close(); err != nil {
		return errors.Join(flushErr, fmt.Errorf("close failure ledger journal: %w", err))
	}
	return flushErr
}

func (l *failureLedger) finalizeLocked(
	operation *failureOperation,
	result failureResult,
	reason failureReason,
	at time.Time,
) error {
	event := failureLedgerEvent{
		Type: failureLedgerEventFinalized, OperationID: operation.ID,
		Result: result, Reason: reason, At: at.UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.noteInvalidationLocked(invalidReasonWAL)
		return fmt.Errorf("persist finalized failure operation %q: %w", operation.ID, err)
	}
	l.results[result]++
	operation.FinalResult = result
	operation.FinalReason = reason
	if result == failureResultNotSent {
		l.rememberNotSentLocked(operation.ID)
	}
	l.dequeueLocked(operation)
	delete(l.active, operation.ID)
	if l.recorder != nil {
		l.recorder.OperationFinalized(cloneFailureOperation(operation), result)
		if recorder, ok := l.recorder.(interface {
			FinalizationReasonRecorded(*failureOperation, failureResult, failureReason)
		}); ok {
			recorder.FinalizationReasonRecorded(cloneFailureOperation(operation), result, reason)
		}
	}
	l.finalizedSinceCompact++
	if l.journal != nil && l.canCompactLocked() &&
		(l.finalizedSinceCompact >= l.compactEvery || l.journalOverBudgetLocked()) {
		if err := l.compactLocked(at); err != nil {
			l.noteInvalidationLocked(invalidReasonWAL)
			return fmt.Errorf("compact failure ledger: %w", err)
		}
		l.finalizedSinceCompact = 0
	}
	return nil
}

func (l *failureLedger) compactLocked(at time.Time) error {
	operationIDs := make([]string, 0, len(l.active))
	for operationID := range l.active {
		operationIDs = append(operationIDs, operationID)
	}
	slices.Sort(operationIDs)
	checkpointResults := make(map[failureResult]uint64, len(l.results))
	for result, count := range l.results {
		checkpointResults[result] = count
	}
	events := make([]failureLedgerEvent, 0, 1+len(l.active)*2)
	events = append(events, failureLedgerEvent{
		Type: failureLedgerEventCheckpoint, Results: checkpointResults,
		ObservationCounts: cloneFailureObservationCounts(l.observations),
		NotSent:           append([]string(nil), l.notSentOrder...), At: at.UTC(),
	})
	// Compaction rewrites the journal from live state, so a cause that is not
	// re-emitted here is dropped by the first reclamation — the one thing a
	// long run is guaranteed to do. Order is preserved because the first cause
	// is the one InvalidReason keeps.
	for _, reason := range l.invalidReasons {
		events = append(events, failureLedgerEvent{
			Type: failureLedgerEventInvalidated, InvalidReason: reason, At: at.UTC(),
		})
	}
	for _, operationID := range operationIDs {
		operation := l.active[operationID]
		started := cloneFailureOperation(operation)
		started.Observations = nil
		started.ObservationReasons = nil
		if started.LifecycleState == failureOperationActive {
			started.LifecycleState = failureOperationJournaled
		}
		events = append(events, failureLedgerEvent{
			Type: failureLedgerEventStarted, Operation: started,
			At: operation.StartedAt.UTC(),
		})
		if operation.LifecycleState == failureOperationActive {
			events = append(events, failureLedgerEvent{
				Type: failureLedgerEventActivated, OperationID: operation.ID, At: at.UTC(),
			})
		}
		for _, observer := range operation.Expected {
			observation, exists := operation.Observations[observer]
			if !exists {
				continue
			}
			events = append(events, failureLedgerEvent{
				Type: failureLedgerEventObserved, OperationID: operation.ID,
				Observer: observer, Observation: observation,
				Reason: operation.ObservationReasons[observer], At: at.UTC(),
			})
		}
	}
	if err := l.journal.Compact(events); err != nil {
		return err
	}
	// The rewritten journal carries every invalidation above, so all of them are
	// durable now — including any the original appends never landed. Without
	// this, Close would keep retrying appends the file no longer needs, and
	// report a lost verdict if one of those retries failed.
	l.persistedInvalidReasons = slices.Clone(l.invalidReasons)
	if l.recorder != nil {
		l.recorder.JournalSize(l.journal.Size())
	}
	return nil
}

func (l *failureLedger) appendLocked(event *failureLedgerEvent) error {
	if l.journal == nil {
		return nil
	}
	if err := l.settleBeforeAppendLocked(); err != nil {
		return err
	}
	if err := l.journal.Append(event); err != nil {
		return err
	}
	if l.recorder != nil {
		l.recorder.JournalSize(l.journal.Size())
	}
	return nil
}

// settleBeforeAppendLocked pays verdicts the journal refused earlier, before
// new evidence is written rather than after. The order is the point: a record
// written first and the verdict settled after leaves a kill in between with
// durable evidence and an in-memory disqualifier, which is the replay this
// ledger exists to prevent.
//
// A debt that cannot be paid refuses the record. Refusing before the write is
// what makes the error true — nothing was journaled — where refusing after it
// would tell the caller a record failed that the file actually holds.
func (l *failureLedger) settleBeforeAppendLocked() error {
	l.retryPendingInvalidationsLocked()
	owed := l.unpersistedInvalidationsLocked()
	if len(owed) == 0 {
		return nil
	}
	return fmt.Errorf(
		"failure ledger owes the journal invalidation %s",
		strings.Join(owed, ", "),
	)
}

// retryPendingInvalidationsLocked lands verdicts the journal refused earlier.
// A successful append is proof the file is accepting records again, and it is
// the cheapest such proof there is: waiting for the next invalidation, a
// compaction or Close leaves the cause in memory only, and a run that is killed
// rather than closed — OOM, node drain, SIGKILL — replays without it.
func (l *failureLedger) retryPendingInvalidationsLocked() {
	if len(l.persistedInvalidReasons) == len(l.invalidReasons) {
		return
	}
	// In order, and stopping at the first refusal. InvalidReason is the first
	// cause and replay rebuilds it from the order the file holds, so stepping
	// over a reason that would not land lets a later one be journaled ahead of
	// it and a restart report the wrong cause as the first.
	// Over a copy: a failed attempt appends the wal cause to the slice below.
	for _, reason := range slices.Clone(l.invalidReasons) {
		if !l.persistInvalidationLocked(reason) {
			return
		}
	}
}

// recoverFrom rebuilds ledger state from the journal, streaming the records
// when the journal supports it. Streaming is what keeps a restart affordable: a
// journal that has been growing for hours is mostly retired evidence, and
// reading it into one slice costs roughly twice the file on the heap before the
// surviving operations are even built.
func (l *failureLedger) recoverFrom(journal failureJournal) error {
	l.replaying = true
	defer func() { l.replaying = false }()
	dropped := make(map[string]struct{})
	if streaming, ok := journal.(streamingFailureJournal); ok {
		index := 0
		if err := streaming.ReplayEach(func(event *failureLedgerEvent) error {
			err := l.replayEvent(index, event, dropped)
			index++
			return err
		}); err != nil {
			return fmt.Errorf("replay failure ledger journal: %w", err)
		}
		l.recoveredEvents = index
		return l.finishReplay()
	}
	events, err := journal.Replay()
	if err != nil {
		return fmt.Errorf("replay failure ledger journal: %w", err)
	}
	l.recoveredEvents = len(events)
	return l.replay(events)
}

// MaybeCompact reclaims the journal when it has outgrown its byte budget. The
// caller drives it on a timer so a run that finalizes nothing still bounds the
// file; it is a no-op when no budget is configured.
func (l *failureLedger) MaybeCompact(at time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return err
	}
	if !l.journalOverBudgetLocked() {
		return nil
	}
	if err := l.compactLocked(at); err != nil {
		l.noteInvalidationLocked(invalidReasonWAL)
		return fmt.Errorf("compact failure ledger: %w", err)
	}
	l.finalizedSinceCompact = 0
	return nil
}

// journalOverBudgetLocked also enforces the safety gate compaction has always
// had: an operation between claiming its slot and having its start record
// written is not in the active set a compaction rewrites from, so compacting
// now would erase it.
func (l *failureLedger) journalOverBudgetLocked() bool {
	if l.journal == nil || l.maxJournalBytes <= 0 || !l.canCompactLocked() {
		return false
	}
	return l.journal.Size() >= l.maxJournalBytes
}

// canCompactLocked guards the way compaction can destroy live state: an
// operation between claiming its slot and having its start record written is
// not in the active set a compaction rewrites from, so compacting then erases
// it.
//
// It deliberately does not consider l.dropped. Operations replay dropped over
// capacity exist only in the journal, and recovery leaves the inherited file
// alone so an immediate restart with a raised SOAK_LEDGER_CAPACITY can still
// get them back — but that window cannot be indefinite. l.dropped only ever
// moves during replay, so gating every compaction on it would disable
// reclamation for the whole process and grow the journal until the volume
// filled, which takes the run down as surely as losing the records does.
func (l *failureLedger) canCompactLocked() bool {
	return len(l.starting) == 0
}

func (l *failureLedger) replay(events []failureLedgerEvent) error {
	// A retained volume outlives any single configuration, so a journal can
	// legitimately hold more unresolved operations than the current capacity
	// admits. Dropping the excess degrades observation for this run; failing
	// would crash-loop the pod with no way out.
	dropped := make(map[string]struct{})
	for index := range events {
		if err := l.replayEvent(index, &events[index], dropped); err != nil {
			return err
		}
	}
	return l.finishReplay()
}

// replayEvent applies one journal record. It is split out of replay so recovery
// can drive it straight off the reader and keep no more than one record alive
// at a time.
func (l *failureLedger) replayEvent(
	index int,
	event *failureLedgerEvent,
	dropped map[string]struct{},
) error {
	{
		switch event.Type {
		case failureLedgerEventCheckpoint:
			if index != 0 {
				return fmt.Errorf("replay failure ledger event %d: checkpoint must be first", index)
			}
			for result, count := range event.Results {
				if !validFailureResult(result) {
					return fmt.Errorf("replay failure ledger event %d: invalid checkpoint result %q", index, result)
				}
				l.results[result] = count
			}
			for observer, counts := range event.ObservationCounts {
				if _, known := failureObserverRegistry[observer]; !known {
					return fmt.Errorf("replay failure ledger event %d: invalid checkpoint observer %q", index, observer)
				}
				for observation, count := range counts {
					if !validFailureObservation(observation) {
						return fmt.Errorf("replay failure ledger event %d: invalid checkpoint observation %q", index, observation)
					}
					l.countObservationByLocked(observer, observation, count)
				}
			}
			for _, operationID := range event.NotSent {
				l.rememberNotSentLocked(operationID)
			}
		case failureLedgerEventStarted:
			if event.Operation == nil {
				return fmt.Errorf("replay failure ledger event %d: started operation is missing", index)
			}
			operation := cloneFailureOperation(event.Operation)
			if err := validateFailureOperation(operation); err != nil {
				return fmt.Errorf("replay failure ledger event %d: %w", index, err)
			}
			if _, duplicate := l.active[operation.ID]; duplicate {
				return fmt.Errorf("replay failure ledger event %d: operation %q is already active", index, operation.ID)
			}
			if len(l.active) >= l.capacity {
				dropped[operation.ID] = struct{}{}
				l.dropped++
				return nil
			}
			operation.nextVerifyAt = operation.VerifyAfter
			operation.claimed = false
			l.active[operation.ID] = operation
		case failureLedgerEventActivated:
			if _, skipped := dropped[event.OperationID]; skipped {
				return nil
			}
			operation := l.active[event.OperationID]
			if operation == nil {
				return fmt.Errorf(
					"replay failure ledger event %d: operation %q is not active",
					index,
					event.OperationID,
				)
			}
			if operation.LifecycleState == failureOperationActive {
				return fmt.Errorf("replay failure ledger event %d: operation %q is already activated", index, event.OperationID)
			}
			operation.LifecycleState = failureOperationActive
		case failureLedgerEventObserved:
			if _, skipped := dropped[event.OperationID]; skipped {
				return nil
			}
			operation := l.active[event.OperationID]
			if operation == nil {
				return fmt.Errorf(
					"replay failure ledger event %d: operation %q is not active",
					index,
					event.OperationID,
				)
			}
			if !slices.Contains(operation.Expected, event.Observer) || !validFailureObservation(event.Observation) {
				return fmt.Errorf("replay failure ledger event %d: invalid observation", index)
			}
			if existing, duplicate := operation.Observations[event.Observer]; duplicate {
				return fmt.Errorf("replay failure ledger event %d: observer %q already recorded as %q", index, event.Observer, existing)
			}
			operation.Observations[event.Observer] = event.Observation
			if _, known := failureReasonRegistry[event.Reason]; !known {
				return fmt.Errorf("replay failure ledger event %d: invalid observation reason %q", index, event.Reason)
			}
			if operation.ObservationReasons == nil {
				operation.ObservationReasons = make(map[failureObserver]failureReason)
			}
			operation.ObservationReasons[event.Observer] = event.Reason
			l.countObservationLocked(event.Observer, event.Observation)
		case failureLedgerEventFinalized:
			if _, skipped := dropped[event.OperationID]; skipped {
				delete(dropped, event.OperationID)
				l.dropped--
				return nil
			}
			operation, exists := l.active[event.OperationID]
			if !exists {
				return fmt.Errorf(
					"replay failure ledger event %d: operation %q is not active",
					index,
					event.OperationID,
				)
			}
			if !validFailureResult(event.Result) {
				return fmt.Errorf("replay failure ledger event %d: invalid result %q", index, event.Result)
			}
			l.results[event.Result]++
			if event.Result == failureResultNotSent {
				l.rememberNotSentLocked(operation.ID)
			}
			delete(l.active, event.OperationID)
		case failureLedgerEventInvariant:
			l.invalidateLocked("accounting_invariant")
		case failureLedgerEventInvalidated:
			// Through Invalidate rather than invalidateLocked so a reason this
			// build does not know folds to "other" instead of widening the
			// label set — a journal from a newer build is exactly where one
			// arrives.
			if err := l.invalidateReplayedLocked(event.InvalidReason); err != nil {
				return fmt.Errorf("replay failure ledger event %d: %w", index, err)
			}
		default:
			return fmt.Errorf("replay failure ledger event %d: unknown type %q", index, event.Type)
		}
	}
	return nil
}

// finishReplay runs once the last record has been applied: it reports whatever
// the capacity forced us to drop and queues the surviving operations for
// verification.
func (l *failureLedger) finishReplay() error {
	if l.dropped > 0 {
		slog.Warn(
			"failure ledger dropped recovered operations over capacity",
			"dropped", l.dropped,
			"capacity", l.capacity,
		)
		// Noted rather than invalidated: this verdict is derived here, not read
		// from the journal, and replay suppresses the append — claiming it
		// persisted would retire a debt no write ever paid. The next successful
		// append settles it.
		l.noteInvalidationLocked(invalidReasonCapacity)
	}
	for _, operation := range l.active {
		l.enqueueLocked(operation)
	}
	return nil
}

func (l *failureLedger) rememberNotSentLocked(operationID string) {
	if operationID == "" {
		return
	}
	if _, exists := l.notSent[operationID]; exists {
		return
	}
	if len(l.notSentOrder) >= l.capacity {
		oldest := l.notSentOrder[0]
		l.notSentOrder = l.notSentOrder[1:]
		delete(l.notSent, oldest)
	}
	l.notSent[operationID] = struct{}{}
	l.notSentOrder = append(l.notSentOrder, operationID)
}

func (l *failureLedger) countObservationLocked(observer failureObserver, observation failureObservation) {
	l.countObservationByLocked(observer, observation, 1)
}

func (l *failureLedger) countObservationByLocked(observer failureObserver, observation failureObservation, count uint64) {
	if l.observations[observer] == nil {
		l.observations[observer] = make(map[failureObservation]uint64)
	}
	l.observations[observer][observation] += count
}

func cloneFailureObservationCounts(
	input map[failureObserver]map[failureObservation]uint64,
) map[failureObserver]map[failureObservation]uint64 {
	cloned := make(map[failureObserver]map[failureObservation]uint64, len(input))
	for observer, counts := range input {
		cloned[observer] = make(map[failureObservation]uint64, len(counts))
		for observation, count := range counts {
			cloned[observer][observation] = count
		}
	}
	return cloned
}

// errFailureLedgerClosed is a sentinel so callers can recognise a closed ledger
// with errors.Is rather than by matching the message.
var errFailureLedgerClosed = errors.New("failure ledger is closed")

func (l *failureLedger) ensureOpen() error {
	if l.closed {
		return errFailureLedgerClosed
	}
	return nil
}

// invalidateLocked records one cause. The first keeps InvalidReason, since that
// is the point the run's evidence stopped standing; every distinct cause is
// counted and journaled, so a startup invalidation cannot silence the runtime
// failures an operator still has to act on.
func (l *failureLedger) invalidateLocked(reason string) {
	l.noteInvalidationLocked(reason)
	// Outside the note above: a cause already counted in memory can still be
	// missing from the journal, and durability is what the retry is for.
	l.retryPendingInvalidationsLocked()
}

// noteInvalidationLocked records a cause in memory and counts it once. It
// deliberately does not touch the journal, so the paths that report a journal
// failure can use it without attempting the append that just failed.
func (l *failureLedger) noteInvalidationLocked(reason string) {
	if slices.Contains(l.invalidReasons, reason) {
		return
	}
	l.invalidReasons = append(l.invalidReasons, reason)
	if l.invalidReason == "" {
		l.invalidReason = reason
	}
	if l.recorder != nil {
		l.recorder.Invalidated(reason)
	}
}

// persistInvalidationLocked writes the cause to the journal and remembers only
// what actually landed. A reason held in memory but missing from the file is
// not durable, so it stays eligible for the next attempt rather than being
// deduplicated away — otherwise one transient append failure would be
// permanent, and a restart would replay the run's evidence without the verdict
// that disqualifies it.
func (l *failureLedger) persistInvalidationLocked(reason string) bool {
	if slices.Contains(l.persistedInvalidReasons, reason) {
		return true
	}
	if l.journalClosed {
		// Nothing can be written any more, and nothing failed: the ledger closed
		// before this cause arrived. Recording wal here would report a fault the
		// file never had, so the cause stays owed and says so.
		slog.Error("failure ledger closed before an invalidation could be recorded",
			"reason", reason,
			"consequence", "this cause exists only in memory and will not survive the run",
		)
		return false
	}
	if err := l.journalInvalidationLocked(reason); err != nil {
		// A journal that will not hold the verdict is a cause in its own right,
		// and every other write path in this ledger records it. Noted rather
		// than invalidated, because invalidating would attempt the append that
		// just failed and recurse.
		l.noteInvalidationLocked(invalidReasonWAL)
		return false
	}
	l.persistedInvalidReasons = append(l.persistedInvalidReasons, reason)
	return true
}

// journalInvalidationLocked persists the cause on a best effort. It cannot
// route its own failure back through invalidateLocked — that is how the WAL
// paths report, and it would recurse — and a lost record is strictly better
// than losing the in-memory verdict too, so the error is logged and dropped.
func (l *failureLedger) journalInvalidationLocked(reason string) error {
	if l.replaying || l.journal == nil {
		return nil
	}
	err := l.journal.Append(&failureLedgerEvent{
		Type: failureLedgerEventInvalidated, InvalidReason: reason, At: l.now().UTC(),
	})
	if err != nil {
		slog.Error("persist failure ledger invalidation",
			"reason", reason,
			"consequence", "a restart will not know this run's evidence was in question "+
				"unless a later invalidation or compaction rewrites it",
			"error", err,
		)
		return err
	}
	if l.recorder != nil {
		l.recorder.JournalSize(l.journal.Size())
	}
	return nil
}

func validateFailureOperation(operation *failureOperation) error {
	if operation == nil {
		return fmt.Errorf("failure operation is required")
	}
	if operation.ID == "" || operation.Scenario == "" || operation.Lane == "" {
		return fmt.Errorf("failure operation requires ID, scenario, and lane")
	}
	if _, known := failureOperationScenarioRegistry[operation.Scenario]; !known {
		return fmt.Errorf("failure operation %q has unsupported scenario %q", operation.ID, operation.Scenario)
	}
	if _, known := failureOperationLaneRegistry[operation.Lane]; !known {
		return fmt.Errorf("failure operation %q has unsupported lane %q", operation.ID, operation.Lane)
	}
	if operation.SchemaVersion != 0 && operation.SchemaVersion != 2 {
		return fmt.Errorf("failure operation %q has unsupported schema version %d", operation.ID, operation.SchemaVersion)
	}
	if operation.SchemaVersion == 2 {
		if operation.LifecycleState != failureOperationJournaled &&
			operation.LifecycleState != failureOperationActive {
			return fmt.Errorf("failure operation %q has invalid lifecycle state %q", operation.ID, operation.LifecycleState)
		}
		if _, known := failureOperationTypeRegistry[operation.OperationType]; !known {
			return fmt.Errorf("version 2 failure operation %q has unsupported type %q", operation.ID, operation.OperationType)
		}
		if operation.RunID == "" || len(operation.Targets) == 0 || len(operation.Effects) == 0 {
			return fmt.Errorf("version 2 failure operation %q requires run, type, targets, and effects", operation.ID)
		}
		operation.Expected = make([]failureObserver, 0, len(operation.Effects))
		seenEffects := make(map[string]struct{}, len(operation.Effects))
		for _, effect := range operation.Effects {
			definition, known := failureObserverRegistry[effect.Observer]
			if !known || !slices.Contains(definition.Effects, effect.Effect) {
				return fmt.Errorf("failure operation %q has unsupported effect %q for observer %q", operation.ID, effect.Effect, effect.Observer)
			}
			key := string(effect.Effect) + "\x00" + string(effect.Observer)
			if _, duplicate := seenEffects[key]; duplicate {
				return fmt.Errorf("failure operation %q repeats effect %q", operation.ID, effect.Effect)
			}
			seenEffects[key] = struct{}{}
			if effect.Cardinality != nil && (effect.Cardinality.Mode != "exact_set_hash" || effect.Cardinality.Count <= 0 || effect.Cardinality.SHA256 == "") {
				return fmt.Errorf("failure operation %q has invalid effect cardinality", operation.ID)
			}
			if effect.Required {
				operation.Expected = append(operation.Expected, effect.Observer)
			}
		}
	}
	if operation.StartedAt.IsZero() || operation.VerifyAfter.IsZero() || operation.Deadline.IsZero() {
		return fmt.Errorf("failure operation %q requires timestamps", operation.ID)
	}
	if operation.VerifyAfter.Before(operation.StartedAt) {
		return fmt.Errorf("failure operation %q verify time precedes start", operation.ID)
	}
	if operation.Deadline.Before(operation.VerifyAfter) {
		return fmt.Errorf("failure operation %q deadline precedes verification", operation.ID)
	}
	if len(operation.Expected) == 0 {
		return fmt.Errorf("failure operation %q requires an observer", operation.ID)
	}
	seen := make(map[failureObserver]struct{}, len(operation.Expected))
	for _, observer := range operation.Expected {
		if observer == "" {
			return fmt.Errorf("failure operation %q has an empty observer", operation.ID)
		}
		if _, duplicate := seen[observer]; duplicate {
			return fmt.Errorf("failure operation %q repeats observer %q", operation.ID, observer)
		}
		seen[observer] = struct{}{}
	}
	if operation.Observations == nil {
		operation.Observations = make(map[failureObserver]failureObservation)
	}
	operation.heapIndex = -1
	return nil
}

func validFailureObservation(observation failureObservation) bool {
	switch observation {
	case failureObservationGood,
		failureObservationBad,
		failureObservationUnverified,
		failureObservationMissingAfterDeadline:
		return true
	default:
		return false
	}
}

func validFailureResult(result failureResult) bool {
	switch result {
	case failureResultGood,
		failureResultBad,
		failureResultUnverified,
		failureResultNotSent,
		failureResultMissingAfterDeadline:
		return true
	default:
		return false
	}
}

// failureOperationResult collapses an operation's observations into one result.
// Precedence runs from strongest evidence to weakest: a confirmed absence
// outranks an ambiguous observation, which outranks "the observer could not
// answer", which outranks success.
func failureOperationResult(operation *failureOperation) failureResult {
	result := failureResultGood
	admissionAccepted := operation.Observations[failureObserverAdmission] == failureObservationGood
	for observer, observation := range operation.Observations {
		switch observation {
		case failureObservationGood:
		case failureObservationMissingAfterDeadline:
			// Missing downstream evidence is a correctness failure only after
			// admission authoritatively accepted an active publish. Otherwise the
			// absence is ambiguous and must not hide stronger bad evidence.
			if observer == failureObserverAdmission ||
				operation.LifecycleState == failureOperationJournaled ||
				!admissionAccepted {
				if result == failureResultGood {
					result = failureResultUnverified
				}
				continue
			}
			return failureResultMissingAfterDeadline
		case failureObservationBad:
			result = failureResultBad
		case failureObservationUnverified:
			if result == failureResultGood {
				result = failureResultUnverified
			}
		}
	}
	return result
}

func failureOperationFinalReason(
	operation *failureOperation,
	result failureResult,
) failureReason {
	var terminalObservation failureObservation
	switch result {
	case failureResultBad:
		terminalObservation = failureObservationBad
	case failureResultMissingAfterDeadline:
		terminalObservation = failureObservationMissingAfterDeadline
	default:
		return failureReasonNone
	}
	for _, observer := range operation.Expected {
		if operation.Observations[observer] != terminalObservation {
			continue
		}
		if reason := operation.ObservationReasons[observer]; reason != failureReasonNone {
			return reason
		}
	}
	return failureReasonNone
}

func cloneFailureOperation(operation *failureOperation) *failureOperation {
	if operation == nil {
		return nil
	}
	cloned := *operation
	cloned.heapIndex = -1
	cloned.Expected = append([]failureObserver(nil), operation.Expected...)
	cloned.Effects = append([]failureExpectedEffect(nil), operation.Effects...)
	for index := range cloned.Effects {
		if operation.Effects[index].Cardinality != nil {
			cardinality := *operation.Effects[index].Cardinality
			cloned.Effects[index].Cardinality = &cardinality
		}
	}
	cloned.Targets = make(map[string]string, len(operation.Targets))
	for key, value := range operation.Targets {
		cloned.Targets[key] = value
	}
	cloned.EvidenceRefs = append([]string(nil), operation.EvidenceRefs...)
	cloned.Attributes = make(map[string]string, len(operation.Attributes))
	for key, value := range operation.Attributes {
		cloned.Attributes[key] = value
	}
	cloned.Observations = make(map[failureObserver]failureObservation, len(operation.Observations))
	for observer, observation := range operation.Observations {
		cloned.Observations[observer] = observation
	}
	cloned.ObservationReasons = make(map[failureObserver]failureReason, len(operation.ObservationReasons))
	for observer, reason := range operation.ObservationReasons {
		cloned.ObservationReasons[observer] = reason
	}
	return &cloned
}

type fileFailureWAL struct {
	mu               sync.Mutex
	path             string
	file             *os.File
	size             int64
	legacy           bool
	observerContract *failureObserverContract
	syncDirectory    func(string) error
}

type failureWALHeader struct {
	RecordType       string                   `json:"recordType"`
	SchemaVersion    int                      `json:"schemaVersion"`
	ObserverContract *failureObserverContract `json:"observerContract,omitempty"`
}

const failureWALSchemaVersion = 2

func openFailureWAL(path string) (*fileFailureWAL, error) {
	if path == "" {
		return nil, fmt.Errorf("failure WAL path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create failure WAL directory: %w", err)
	}
	backupPath := path + ".bak"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if _, backupErr := os.Stat(backupPath); backupErr == nil {
			if err := os.Rename(backupPath, path); err != nil {
				return nil, fmt.Errorf("restore failure WAL backup: %w", err)
			}
		}
	}
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open failure WAL %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat failure WAL %q: %w", path, err)
	}
	return &fileFailureWAL{
		path: path, file: file, size: info.Size(), syncDirectory: syncFailureWALDirectory,
	}, nil
}

// Replay buffers the whole journal. It exists for tests and for callers that
// genuinely want every record at once; recovery uses ReplayEach so a journal
// that has been growing for hours does not have to fit in memory to be read.
func (w *fileFailureWAL) Replay() ([]failureLedgerEvent, error) {
	events := make([]failureLedgerEvent, 0)
	if err := w.ReplayEach(func(event *failureLedgerEvent) error {
		events = append(events, *event)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("buffer replayed failure WAL: %w", err)
	}
	return events, nil
}

// ReplayEach hands each durable record to emit in write order and keeps none of
// them, so recovery costs what the surviving operations cost rather than what
// the file weighs. A torn final record is repaired exactly as before, after the
// last intact record has been emitted.
func (w *fileFailureWAL) ReplayEach(emit func(*failureLedgerEvent) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("open failure WAL for replay: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			// Discarded deliberately: this path only runs when replay already
			// failed, and the close error would mask the one worth reporting.
			_ = file.Close()
		}
	}()

	reader := bufio.NewReader(file)
	line := 0
	headerSeen := false
	w.observerContract = nil
	durableBytes := int64(0)
	tornFinalRecord := false
	for {
		encoded, readErr := reader.ReadBytes('\n')
		if errors.Is(readErr, io.EOF) {
			// Append writes one JSON event and its newline in a single syscall.
			// A final unterminated record is therefore a torn crash write and
			// must not prevent recovery of every prior durable event.
			tornFinalRecord = len(encoded) > 0
			break
		}
		if readErr != nil {
			return fmt.Errorf("read failure WAL line %d: %w", line+1, readErr)
		}
		line++
		var header failureWALHeader
		if err := json.Unmarshal(encoded, &header); err == nil && header.RecordType != "" {
			if line != 1 || header.RecordType != "header" || header.SchemaVersion != failureWALSchemaVersion {
				return fmt.Errorf("decode failure WAL line %d: unsupported header version %d", line, header.SchemaVersion)
			}
			durableBytes += int64(len(encoded))
			headerSeen = true
			w.observerContract = cloneFailureObserverContract(header.ObserverContract)
			continue
		}
		if line == 1 {
			w.legacy = true
		}
		var event failureLedgerEvent
		if err := json.Unmarshal(encoded, &event); err != nil {
			return fmt.Errorf("decode failure WAL line %d: %w", line, err)
		}
		switch {
		case headerSeen && event.SchemaVersion != failureWALSchemaVersion:
			return fmt.Errorf(
				"decode failure WAL line %d: versioned WAL requires record version %d",
				line,
				failureWALSchemaVersion,
			)
		case !headerSeen && event.SchemaVersion != 0:
			return fmt.Errorf(
				"decode failure WAL line %d: record version %d requires a versioned header",
				line,
				event.SchemaVersion,
			)
		}
		if err := emit(&event); err != nil {
			return fmt.Errorf("apply failure WAL line %d: %w", line, err)
		}
		durableBytes += int64(len(encoded))
	}
	if line == 0 && !headerSeen {
		w.legacy = false
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close replayed failure WAL: %w", err)
	}
	closed = true
	if tornFinalRecord {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("close failure WAL before repair: %w", err)
		}
		w.file = nil
		if err := os.Truncate(w.path, durableBytes); err != nil {
			return w.reopenAfterCompactFailure(
				fmt.Errorf("truncate torn failure WAL record: %w", err),
			)
		}
		// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
		// nosemgrep: gosec.G304-1
		reopened, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o600)
		if err != nil {
			return fmt.Errorf("reopen repaired failure WAL: %w", err)
		}
		w.file = reopened
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync repaired failure WAL: %w", err)
		}
		w.size = durableBytes
	}
	return nil
}

func (w *fileFailureWAL) NeedsUpgrade() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.legacy
}

func (w *fileFailureWAL) ConfigureObserverContract(
	contract failureObserverContract,
	active []failureOperation,
) error {
	if err := validateFailureObserverContract(contract); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.observerContract != nil {
		if !equalFailureObserverContract(*w.observerContract, contract) {
			return fmt.Errorf("%w: stored observer contract differs from current configuration", errFailureObserverContractMismatch)
		}
		return nil
	}
	for index := range active {
		if !failureOperationMatchesObserverContract(&active[index], contract) {
			return fmt.Errorf(
				"%w: pending operation %q is incompatible with the current observer contract",
				errFailureObserverContractMismatch,
				active[index].ID,
			)
		}
	}
	w.observerContract = cloneFailureObserverContract(&contract)
	if w.size == 0 {
		return w.writeHeaderLocked()
	}
	w.legacy = true
	return nil
}

func validateFailureObserverContract(contract failureObserverContract) error {
	if contract.SchemaVersion != failureObserverContractSchemaVersion {
		return fmt.Errorf("unsupported observer contract schema version %d", contract.SchemaVersion)
	}
	if contract.Scenario != soakFailureScenario {
		return fmt.Errorf("observer contract scenario must be %q", soakFailureScenario)
	}
	if err := validateRegisteredObservers(contract.Observers); err != nil {
		return err
	}
	if len(contract.Lanes) == 0 {
		return fmt.Errorf("observer contract must declare at least one lane")
	}
	for lane, observers := range contract.Lanes {
		if _, known := failureOperationLaneRegistry[lane]; !known {
			return fmt.Errorf("observer contract declares unsupported lane %q", lane)
		}
		if len(observers) == 0 {
			return fmt.Errorf("observer contract lane %q declares no observer", lane)
		}
		if err := validateRegisteredObservers(observers); err != nil {
			return fmt.Errorf("observer contract lane %q: %w", lane, err)
		}
	}
	// Recipient observation only applies to the message lane, so its enablement
	// flag is checked there rather than against the scenario-wide union.
	hasRecipient := slices.Contains(contract.Lanes[soakFailureLaneMessageSend], failureObserverRecipient)
	if hasRecipient != contract.RecipientObserverEnabled {
		return fmt.Errorf("recipient observer enablement does not match configured observers")
	}
	return nil
}

func equalFailureObserverContract(left, right failureObserverContract) bool {
	return left.SchemaVersion == right.SchemaVersion &&
		left.Scenario == right.Scenario &&
		left.RecipientObserverEnabled == right.RecipientObserverEnabled &&
		slices.Equal(left.Observers, right.Observers) &&
		maps.EqualFunc(left.Lanes, right.Lanes, slices.Equal)
}

func cloneFailureObserverContract(contract *failureObserverContract) *failureObserverContract {
	if contract == nil {
		return nil
	}
	cloned := *contract
	cloned.Observers = slices.Clone(contract.Observers)
	if contract.Lanes != nil {
		cloned.Lanes = make(map[string][]failureObserver, len(contract.Lanes))
		for lane, observers := range contract.Lanes {
			cloned.Lanes[lane] = slices.Clone(observers)
		}
	}
	return &cloned
}

func failureOperationMatchesObserverContract(
	operation *failureOperation,
	contract failureObserverContract,
) bool {
	if operation == nil || operation.Scenario != contract.Scenario {
		return false
	}
	configured, known := contract.Lanes[operation.Lane]
	if !known {
		return false
	}
	expected := slices.Clone(operation.Expected)
	slices.Sort(expected)
	laneObservers := slices.Clone(configured)
	slices.Sort(laneObservers)
	return slices.Equal(expected, laneObservers)
}

func (w *fileFailureWAL) writeHeaderLocked() error {
	header, err := json.Marshal(failureWALHeader{
		RecordType: "header", SchemaVersion: failureWALSchemaVersion,
		ObserverContract: cloneFailureObserverContract(w.observerContract),
	})
	if err != nil {
		return fmt.Errorf("encode failure WAL header: %w", err)
	}
	header = append(header, '\n')
	written, err := w.file.Write(header)
	if err != nil {
		return fmt.Errorf("append failure WAL header: %w", err)
	}
	if written != len(header) {
		return fmt.Errorf("append failure WAL header: wrote %d of %d bytes", written, len(header))
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync failure WAL header: %w", err)
	}
	if err := w.syncDirectory(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("sync failure WAL header directory: %w", err)
	}
	w.size += int64(written)
	return nil
}

func (w *fileFailureWAL) Append(event *failureLedgerEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.appendBufferedLocked(event); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync failure WAL event: %w", err)
	}
	return nil
}

func (w *fileFailureWAL) AppendBuffered(event *failureLedgerEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendBufferedLocked(event)
}

func (w *fileFailureWAL) appendBufferedLocked(event *failureLedgerEvent) error {
	if w.file == nil {
		return fmt.Errorf("failure WAL is closed")
	}
	if w.size == 0 {
		if err := w.writeHeaderLocked(); err != nil {
			return err
		}
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = failureWALSchemaVersion
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode failure WAL event: %w", err)
	}
	encoded = append(encoded, '\n')
	written, err := w.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("append failure WAL event: %w", err)
	}
	if written != len(encoded) {
		return fmt.Errorf("append failure WAL event: wrote %d of %d bytes", written, len(encoded))
	}
	w.size += int64(written)
	return nil
}

func (w *fileFailureWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("failure WAL is closed")
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync failure WAL event: %w", err)
	}
	return nil
}

func (w *fileFailureWAL) Compact(events []failureLedgerEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("failure WAL is closed")
	}
	temporaryPath := w.path + ".compact"
	backupPath := w.path + ".bak"
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	temporary, err := os.OpenFile(
		temporaryPath,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0o600,
	)
	if err != nil {
		return fmt.Errorf("create compacted failure WAL: %w", err)
	}
	closeTemporary := true
	defer func() {
		if closeTemporary {
			_ = temporary.Close()
		}
	}()
	var compactedSize int64
	header, err := json.Marshal(failureWALHeader{
		RecordType: "header", SchemaVersion: failureWALSchemaVersion,
		ObserverContract: cloneFailureObserverContract(w.observerContract),
	})
	if err != nil {
		return fmt.Errorf("encode compacted failure WAL header: %w", err)
	}
	header = append(header, '\n')
	written, err := temporary.Write(header)
	if err != nil {
		return fmt.Errorf("write compacted failure WAL header: %w", err)
	}
	if written != len(header) {
		return fmt.Errorf("write compacted failure WAL header: wrote %d of %d bytes", written, len(header))
	}
	compactedSize += int64(written)
	for index := range events {
		event := &events[index]
		if event.SchemaVersion == 0 {
			event.SchemaVersion = failureWALSchemaVersion
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode compacted failure WAL event: %w", err)
		}
		encoded = append(encoded, '\n')
		written, err := temporary.Write(encoded)
		if err != nil {
			return fmt.Errorf("write compacted failure WAL event: %w", err)
		}
		if written != len(encoded) {
			return fmt.Errorf(
				"write compacted failure WAL event: wrote %d of %d bytes",
				written,
				len(encoded),
			)
		}
		compactedSize += int64(written)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync compacted failure WAL: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close compacted failure WAL: %w", err)
	}
	closeTemporary = false

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close failure WAL before compaction: %w", err)
	}
	w.file = nil
	if err := os.Remove(backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return w.reopenAfterCompactFailure(fmt.Errorf("remove stale failure WAL backup: %w", err))
	}
	if err := os.Rename(w.path, backupPath); err != nil {
		return w.reopenAfterCompactFailure(fmt.Errorf("backup failure WAL: %w", err))
	}
	if err := os.Rename(temporaryPath, w.path); err != nil {
		if restoreErr := os.Rename(backupPath, w.path); restoreErr != nil {
			return fmt.Errorf(
				"install compacted failure WAL: %w; restore backup: %v",
				err,
				restoreErr,
			)
		}
		return w.reopenAfterCompactFailure(fmt.Errorf("install compacted failure WAL: %w", err))
	}
	if err := w.syncDirectory(filepath.Dir(w.path)); err != nil {
		return w.reopenAfterCompactFailure(fmt.Errorf("sync installed failure WAL directory: %w", err))
	}
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen compacted failure WAL: %w", err)
	}
	w.file = file
	w.size = compactedSize
	w.legacy = false
	if err := os.Remove(backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove failure WAL backup: %w", err)
	}
	if err := w.syncDirectory(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("sync failure WAL directory: %w", err)
	}
	return nil
}

func syncFailureWALDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		// Windows does not support fsync on directory handles. The retained
		// production PVC runs on Linux, where the sync below makes rename durable.
		return nil
	}
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open directory: %w", err)
	}
	defer directoryFile.Close()
	if err := directoryFile.Sync(); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}

func (w *fileFailureWAL) reopenAfterCompactFailure(compactErr error) error {
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, reopenErr := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if reopenErr == nil {
		w.file = file
		if info, statErr := file.Stat(); statErr == nil {
			w.size = info.Size()
		}
		return compactErr
	}
	return fmt.Errorf("%v; reopen failure WAL: %w", compactErr, reopenErr)
}

func (w *fileFailureWAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

func (w *fileFailureWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close failure WAL: %w", err)
	}
	w.file = nil
	return nil
}
