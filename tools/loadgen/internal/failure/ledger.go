package failure

import (
	"container/heap"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	ErrLedgerCapacity     = errors.New("failure ledger capacity exceeded")
	ErrOperationNotActive = errors.New("failure operation is not active")
)

const InvalidReasonCapacity = "capacity"

// InvalidReasonWAL marks evidence the journal could not be trusted to hold.
const InvalidReasonWAL = "wal"

// InvalidReasonReconcileCapacity marks a run started below the reconciliation
// floor. Nothing has failed yet, but every message will expire unverified for
// want of a claim, so the evidence is in question from the first second.
const InvalidReasonReconcileCapacity = "reconcile_capacity"

// InvalidReasonReconcileLagRange marks a run whose reconcile deadline outruns
// the lag histogram. Nothing is wrong with the lane, but the rule that says
// whether a window counted reads lag against the deadline, and past the
// ceiling that reading cannot be made — so the window is unreadable rather
// than merely imprecise.
const InvalidReasonReconcileLagRange = "reconcile_lag_range"

// InvalidReasonLeaseAbort marks evidence abandoned when an uncooperative lane
// could not drain before the heartbeat lease's shutdown margin. The process
// must exit to preserve the teardown fence, so those in-flight observations
// cannot be treated as ordinary unverified results.
const InvalidReasonLeaseAbort = "lease_abort"

var invalidationReasonRegistry = map[string]struct{}{
	"capacity": {}, "wal": {}, "accounting_invariant": {}, "observer_queue": {},
	InvalidReasonReconcileCapacity: {}, InvalidReasonReconcileLagRange: {},
	InvalidReasonLeaseAbort: {},
	"observer_malformed":    {}, "recipient_recovery": {}, "recipient_observer": {},
	"timeline": {}, "other": {},
	"sidecar": {},
}

func ValidInvalidationReason(reason string) bool {
	_, ok := invalidationReasonRegistry[reason]
	return ok
}

type LedgerConfig struct {
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
	Journal          Journal
	Now              func() time.Time
	Recorder         LedgerRecorder
	ObserverContract *ObserverContract
}

type LedgerRecorder interface {
	OperationStarted(*Operation)
	ObservationRecorded(*Operation, Observer, Observation)
	OperationFinalized(*Operation, Result)
	Recovered(int)
	Invalidated(string)
	JournalSize(int64)
}

type LedgerSnapshot struct {
	Active        int
	Recovered     int
	Dropped       int
	InvalidReason string
	Results       map[Result]uint64
	Observations  map[Observer]map[Observation]uint64
	JournalBytes  int64
}

type Ledger struct {
	mu         sync.Mutex
	startingWG sync.WaitGroup

	capacity        int
	compactEvery    int
	maxJournalBytes int64
	expireBatch     int
	journal         Journal
	now             func() time.Time
	recorder        LedgerRecorder

	active        map[string]*Operation
	starting      map[string]struct{}
	verifyQueues  map[string]*verifyQueue
	results       map[Result]uint64
	observations  map[Observer]map[Observation]uint64
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

// verifyQueue orders unclaimed operations by their next verification
// time so ClaimDue is O(log n) instead of scanning every in-flight operation
// under the lock that also serializes fsync-bearing journal appends.
type verifyQueue []*Operation

func (q verifyQueue) Len() int { return len(q) }

func (q verifyQueue) Less(i, j int) bool {
	return q[i].NextVerifyAt().Before(q[j].NextVerifyAt())
}

func (q verifyQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].SetHeapIndex(i)
	q[j].SetHeapIndex(j)
}

func (q *verifyQueue) Push(item any) {
	operation, ok := item.(*Operation)
	if !ok {
		return
	}
	operation.SetHeapIndex(len(*q))
	*q = append(*q, operation)
}

func (q *verifyQueue) Pop() any {
	old := *q
	last := len(old) - 1
	operation := old[last]
	old[last] = nil
	operation.SetHeapIndex(-1)
	*q = old[:last]
	return operation
}

func NewLedger(cfg *LedgerConfig) (*Ledger, error) {
	if cfg.Capacity <= 0 {
		return nil, fmt.Errorf("failure ledger capacity must be greater than zero")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.CompactEvery <= 0 {
		cfg.CompactEvery = 10000
	}
	ledger := &Ledger{
		capacity:        cfg.Capacity,
		compactEvery:    cfg.CompactEvery,
		maxJournalBytes: max(0, cfg.MaxJournalBytes),
		expireBatch:     max(0, cfg.ExpireBatch),
		journal:         cfg.Journal,
		now:             cfg.Now,
		recorder:        cfg.Recorder,
		active:          make(map[string]*Operation, cfg.Capacity),
		starting:        make(map[string]struct{}),
		verifyQueues:    make(map[string]*verifyQueue),
		results:         make(map[Result]uint64),
		observations:    make(map[Observer]map[Observation]uint64),
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
			ConfigureObserverContract(ObserverContract, []Operation) error
		})
		if !ok {
			return nil, fmt.Errorf("failure journal does not support observer contracts")
		}
		active := make([]Operation, 0, len(ledger.active))
		for _, operation := range ledger.active {
			active = append(active, *CloneOperation(operation))
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
			ledger.recorder.OperationStarted(CloneOperation(operation))
		}
	}
	return ledger, nil
}

func (l *Ledger) Start(operation *Operation) error {
	tracked := CloneOperation(operation)
	if err := ValidateOperation(tracked); err != nil {
		return fmt.Errorf("start failure operation: %w", err)
	}
	tracked.Observations = make(map[Observer]Observation)
	tracked.ObservationReasons = make(map[Observer]Reason)
	tracked.SetNextVerifyAt(tracked.VerifyAfter)

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
		l.invalidateLocked(InvalidReasonCapacity)
		l.mu.Unlock()
		return fmt.Errorf("start failure operation %q: %w", tracked.ID, ErrLedgerCapacity)
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

	event := Event{
		Type: EventStarted, Operation: CloneOperation(tracked),
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
		l.noteInvalidationLocked(InvalidReasonWAL)
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
		l.recorder.OperationStarted(CloneOperation(tracked))
	}
	return nil
}

func (l *Ledger) Activate(operationID string, at time.Time) error {
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
		return fmt.Errorf("activate failure operation %q: %w", operationID, ErrOperationNotActive)
	}
	if operation.LifecycleState == OperationActive {
		return nil
	}
	if operation.LifecycleState != OperationJournaled {
		return fmt.Errorf("activate failure operation %q: invalid lifecycle state %q", operationID, operation.LifecycleState)
	}
	event := Event{
		Type: EventActivated, OperationID: operationID, At: at.UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.noteInvalidationLocked(InvalidReasonWAL)
		return fmt.Errorf("persist activated failure operation %q: %w", operationID, err)
	}
	operation.LifecycleState = OperationActive
	return nil
}

// Abandon terminates an active operation with an explicit result. It exists for
// outcomes that no observer can report, most importantly a send whose intent was
// journaled before a publish that never left the process: without this the
// unresolved history observer would later expire into the data-loss bucket.
func (l *Ledger) Abandon(
	operationID string,
	result Result,
	at time.Time,
) error {
	reason := ReasonNone
	if result == ResultNotSent {
		reason = ReasonPublishLocalError
	}
	return l.AbandonWithReason(operationID, result, reason, at)
}

func (l *Ledger) AbandonWithReason(
	operationID string,
	result Result,
	reason Reason,
	at time.Time,
) error {
	if !ValidResult(result) {
		return fmt.Errorf("invalid failure result %q", result)
	}
	if !ValidReason(reason) {
		return fmt.Errorf("unsupported failure final reason %q", reason)
	}
	if result == ResultBad && reason == ReasonNone {
		return fmt.Errorf("bad failure result requires a bounded reason")
	}
	if result == ResultNotSent && reason != ReasonPublishLocalError {
		return fmt.Errorf("not_sent requires reason %q", ReasonPublishLocalError)
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
		return fmt.Errorf("abandon failure operation %q: %w", operationID, ErrOperationNotActive)
	}
	if result == ResultNotSent && operation.LifecycleState == OperationActive {
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
func (l *Ledger) Retired(operationID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, active := l.active[operationID]
	return !active
}

func (l *Ledger) Observe(
	operationID string,
	observer Observer,
	observation Observation,
	at time.Time,
) (bool, error) {
	return l.ObserveWithReason(
		operationID,
		observer,
		observation,
		DefaultReason(observer, observation),
		at,
	)
}

func (l *Ledger) ObserveWithReason(
	operationID string,
	observer Observer,
	observation Observation,
	reason Reason,
	at time.Time,
) (bool, error) {
	if !ValidReason(reason) {
		return false, fmt.Errorf("unsupported failure observation reason %q", reason)
	}
	if (observation == ObservationBad ||
		observation == ObservationMissingAfterDeadline) && reason == ReasonNone {
		return false, fmt.Errorf("failure observation %q requires a bounded reason", observation)
	}
	if observation != ObservationBad &&
		observation != ObservationMissingAfterDeadline &&
		reason != ReasonNone {
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
			event := Event{
				Type: EventInvariant, OperationID: operationID,
				Observer: observer, Observation: observation, At: at.UTC(),
			}
			if err := l.appendLocked(&event); err != nil {
				l.noteInvalidationLocked(InvalidReasonWAL)
				return false, fmt.Errorf("persist accounting invariant for %q: %w", operationID, err)
			}
			l.invalidateLocked("accounting_invariant")
			return false, fmt.Errorf("failure operation %q accounting invariant: downstream effect observed after not_sent", operationID)
		}
		return false, fmt.Errorf(
			"observe failure operation %q: %w", operationID, ErrOperationNotActive,
		)
	}
	if !slices.Contains(operation.Expected, observer) {
		return false, fmt.Errorf(
			"failure operation %q does not expect observer %q",
			operationID,
			observer,
		)
	}
	if !ValidObservation(observation) {
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
	event := Event{
		Type: EventObserved, OperationID: operationID,
		Observer: observer, Observation: observation, Reason: reason, At: at.UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.noteInvalidationLocked(InvalidReasonWAL)
		return false, fmt.Errorf("persist failure observation for %q: %w", operationID, err)
	}
	operation.Observations[observer] = observation
	operation.ObservationReasons[observer] = reason
	l.countObservationLocked(observer, observation)
	operation.SetClaimed(false)
	l.dequeueLocked(operation)
	if l.recorder != nil {
		l.recorder.ObservationRecorded(
			CloneOperation(operation), observer, observation,
		)
		if recorder, ok := l.recorder.(interface {
			ObservationReasonRecorded(*Operation, Observer, Observation, Reason)
		}); ok {
			recorder.ObservationReasonRecorded(
				CloneOperation(operation), observer, observation, reason,
			)
		}
	}
	if len(operation.Observations) != len(operation.Expected) {
		l.enqueueLocked(operation)
		return false, nil
	}
	result := OperationResult(operation)
	if err := l.finalizeLocked(operation, result, OperationFinalReason(operation, result), at); err != nil {
		return false, err
	}
	return true, nil
}

// Expire finalizes every operation past its deadline and returns the IDs it
// actually finalized. The IDs matter to callers holding per-operation state:
// a successful call is not a complete one — it stops at expireBatch and skips
// claimed operations mid-verification — so "the sweep succeeded" is not a
// licence to discard anything else that looks overdue.
// expiryGrace is how long the sweep leaves a scheduled probe alone
// before retiring the operation anyway, derived from the operation's own
// verification window so it needs no configuration and scales with the run.
func ExpiryInterval(deadline time.Duration) time.Duration {
	return min(30*time.Second, max(time.Second, deadline/10))
}

func expiryGrace(operation *Operation) time.Duration {
	return ExpiryInterval(operation.Deadline.Sub(operation.StartedAt))
}

func (l *Ledger) Expire(now time.Time) ([]string, error) {
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
		if now.Before(operation.NextVerifyAt().Add(expiryGrace(operation))) {
			continue
		}
		// A claimed operation is mid-verification. Finalizing it here would
		// discard a read-back that is about to succeed and report the message
		// as missing; the next pass picks it up once the claim is released.
		if operation.Claimed() {
			continue
		}
		for _, observer := range operation.Expected {
			if _, exists := operation.Observations[observer]; exists {
				continue
			}
			event := Event{
				Type: EventObserved, OperationID: operation.ID,
				Observer: observer, Observation: ObservationUnverified,
				At: now.UTC(),
			}
			if err := l.appendLocked(&event); err != nil {
				l.noteInvalidationLocked(InvalidReasonWAL)
				return finalized, fmt.Errorf(
					"persist expired failure observation for %q: %w", operation.ID, err,
				)
			}
			operation.Observations[observer] = ObservationUnverified
			operation.ObservationReasons[observer] = ReasonNone
			l.countObservationLocked(observer, ObservationUnverified)
			if l.recorder != nil {
				l.recorder.ObservationRecorded(
					CloneOperation(operation), observer,
					ObservationUnverified,
				)
			}
		}
		result := OperationResult(operation)
		operationID := operation.ID
		err := l.finalizeLocked(
			operation, result, OperationFinalReason(operation, result), now,
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

func (l *Ledger) ClaimDue(now time.Time) (Operation, bool) {
	return l.claimDue(now, nil)
}

// ClaimDueLanes restricts a claim to the given lanes. Each lane is reconciled
// by the observer that understands its effects, so a lane-blind claim would
// hand a room mutation to the message-history verifier and vice versa.
func (l *Ledger) ClaimDueLanes(now time.Time, lanes []string) (Operation, bool) {
	if len(lanes) == 0 {
		return Operation{}, false
	}
	return l.claimDue(now, lanes)
}

func (l *Ledger) claimDue(now time.Time, lanes []string) (Operation, bool) {
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
	var earliest *verifyQueue
	for _, lane := range lanes {
		queue := l.verifyQueues[lane]
		if queue == nil || queue.Len() == 0 || now.Before((*queue)[0].NextVerifyAt()) {
			continue
		}
		if earliest == nil || (*queue)[0].NextVerifyAt().Before((*earliest)[0].NextVerifyAt()) {
			earliest = queue
		}
	}
	if earliest == nil {
		return Operation{}, false
	}
	selected, ok := heap.Pop(earliest).(*Operation)
	if !ok {
		return Operation{}, false
	}
	selected.SetClaimed(true)
	return *CloneOperation(selected), true
}

func (l *Ledger) ReleaseClaim(operationID string, next time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	operation := l.active[operationID]
	if operation == nil {
		return fmt.Errorf(
			"release failure operation %q: %w", operationID, ErrOperationNotActive,
		)
	}
	operation.SetClaimed(false)
	operation.SetNextVerifyAt(next.UTC())
	l.enqueueLocked(operation)
	return nil
}

func (l *Ledger) enqueueLocked(operation *Operation) {
	if operation.HeapIndex() >= 0 || operation.Claimed() {
		return
	}
	if !l.scheduleNextLocked(operation) {
		return
	}
	queue := l.verifyQueues[operation.Lane]
	if queue == nil {
		queue = &verifyQueue{}
		l.verifyQueues[operation.Lane] = queue
	}
	heap.Push(queue, operation)
}

// scheduleNextLocked decides when an operation becomes claimable again. A
// query observer polls from its verify time so a converged effect is seen
// early; an event observer has nothing to poll, so it waits for the deadline
// and is resolved from what arrived by then.
func (l *Ledger) scheduleNextLocked(operation *Operation) bool {
	for _, observer := range operation.Expected {
		if _, observed := operation.Observations[observer]; observed {
			continue
		}
		definition, _ := ObserverDefinitionFor(observer)
		if definition.Mode == ObserverQuery {
			if operation.NextVerifyAt().IsZero() {
				operation.SetNextVerifyAt(operation.VerifyAfter)
			}
			return true
		}
	}
	for _, observer := range operation.Expected {
		if _, observed := operation.Observations[observer]; !observed {
			operation.SetNextVerifyAt(operation.Deadline)
			return true
		}
	}
	return false
}

func (l *Ledger) dequeueLocked(operation *Operation) {
	queue := l.verifyQueues[operation.Lane]
	if operation.HeapIndex() < 0 || queue == nil {
		return
	}
	heap.Remove(queue, operation.HeapIndex())
}

func (l *Ledger) Snapshot() LedgerSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	results := make(map[Result]uint64, len(l.results))
	for result, count := range l.results {
		results[result] = count
	}
	observations := cloneFailureObservationCounts(l.observations)
	journalBytes := int64(0)
	if l.journal != nil {
		journalBytes = l.journal.Size()
	}
	return LedgerSnapshot{
		Active: len(l.active), Recovered: l.recovered, Dropped: l.dropped,
		InvalidReason: l.invalidReason, Results: results, Observations: observations,
		JournalBytes: journalBytes,
	}
}

func (l *Ledger) Active(operationID string) (Operation, bool) {
	if l == nil {
		return Operation{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	operation := l.active[operationID]
	if operation == nil {
		return Operation{}, false
	}
	return *CloneOperation(operation), true
}

func (l *Ledger) ActiveOperations() []Operation {
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
	operations := make([]Operation, 0, len(operationIDs))
	for _, operationID := range operationIDs {
		operations = append(operations, *CloneOperation(l.active[operationID]))
	}
	return operations
}

func (l *Ledger) Invalidate(reason string) {
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
	if _, known := invalidationReasonRegistry[reason]; !known {
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
func (l *Ledger) invalidateReplayedLocked(reason string) error {
	if reason == "" {
		return fmt.Errorf("invalidation is missing its reason")
	}
	l.invalidateLocked(registeredInvalidationReason(reason))
	return nil
}

// UnpersistedInvalidations reports the causes held only in memory. A caller
// that cannot afford to continue on evidence whose disqualifier the journal
// never accepted asks before it commits to the run.
func (l *Ledger) UnpersistedInvalidations() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.unpersistedInvalidationsLocked()
}

func (l *Ledger) unpersistedInvalidationsLocked() []string {
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
func (l *Ledger) flushInvalidationsLocked() error {
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

func (l *Ledger) Close() error {
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

func (l *Ledger) finalizeLocked(
	operation *Operation,
	result Result,
	reason Reason,
	at time.Time,
) error {
	event := Event{
		Type: EventFinalized, OperationID: operation.ID,
		Result: result, Reason: reason, At: at.UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.noteInvalidationLocked(InvalidReasonWAL)
		return fmt.Errorf("persist finalized failure operation %q: %w", operation.ID, err)
	}
	l.results[result]++
	operation.FinalResult = result
	operation.FinalReason = reason
	if result == ResultNotSent {
		l.rememberNotSentLocked(operation.ID)
	}
	l.dequeueLocked(operation)
	delete(l.active, operation.ID)
	if l.recorder != nil {
		l.recorder.OperationFinalized(CloneOperation(operation), result)
		if recorder, ok := l.recorder.(interface {
			FinalizationReasonRecorded(*Operation, Result, Reason)
		}); ok {
			recorder.FinalizationReasonRecorded(CloneOperation(operation), result, reason)
		}
	}
	l.finalizedSinceCompact++
	if l.journal != nil && l.canCompactLocked() &&
		(l.finalizedSinceCompact >= l.compactEvery || l.journalOverBudgetLocked()) {
		if err := l.compactLocked(at); err != nil {
			l.noteInvalidationLocked(InvalidReasonWAL)
			return fmt.Errorf("compact failure ledger: %w", err)
		}
		l.finalizedSinceCompact = 0
	}
	return nil
}

func (l *Ledger) compactLocked(at time.Time) error {
	operationIDs := make([]string, 0, len(l.active))
	for operationID := range l.active {
		operationIDs = append(operationIDs, operationID)
	}
	slices.Sort(operationIDs)
	checkpointResults := make(map[Result]uint64, len(l.results))
	for result, count := range l.results {
		checkpointResults[result] = count
	}
	events := make([]Event, 0, 1+len(l.active)*2)
	events = append(events, Event{
		Type: EventCheckpoint, Results: checkpointResults,
		ObservationCounts: cloneFailureObservationCounts(l.observations),
		NotSent:           append([]string(nil), l.notSentOrder...), At: at.UTC(),
	})
	// Compaction rewrites the journal from live state, so a cause that is not
	// re-emitted here is dropped by the first reclamation — the one thing a
	// long run is guaranteed to do. Order is preserved because the first cause
	// is the one InvalidReason keeps.
	for _, reason := range l.invalidReasons {
		events = append(events, Event{
			Type: EventInvalidated, InvalidReason: reason, At: at.UTC(),
		})
	}
	for _, operationID := range operationIDs {
		operation := l.active[operationID]
		started := CloneOperation(operation)
		started.Observations = nil
		started.ObservationReasons = nil
		if started.LifecycleState == OperationActive {
			started.LifecycleState = OperationJournaled
		}
		events = append(events, Event{
			Type: EventStarted, Operation: started,
			At: operation.StartedAt.UTC(),
		})
		if operation.LifecycleState == OperationActive {
			events = append(events, Event{
				Type: EventActivated, OperationID: operation.ID, At: at.UTC(),
			})
		}
		for _, observer := range operation.Expected {
			observation, exists := operation.Observations[observer]
			if !exists {
				continue
			}
			events = append(events, Event{
				Type: EventObserved, OperationID: operation.ID,
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

func (l *Ledger) appendLocked(event *Event) error {
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
func (l *Ledger) settleBeforeAppendLocked() error {
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
func (l *Ledger) retryPendingInvalidationsLocked() {
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
func (l *Ledger) recoverFrom(journal Journal) error {
	l.replaying = true
	defer func() { l.replaying = false }()
	dropped := make(map[string]struct{})
	if streaming, ok := journal.(StreamingJournal); ok {
		index := 0
		if err := streaming.ReplayEach(func(event *Event) error {
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
func (l *Ledger) MaybeCompact(at time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return err
	}
	if !l.journalOverBudgetLocked() {
		return nil
	}
	if err := l.compactLocked(at); err != nil {
		l.noteInvalidationLocked(InvalidReasonWAL)
		return fmt.Errorf("compact failure ledger: %w", err)
	}
	l.finalizedSinceCompact = 0
	return nil
}

// journalOverBudgetLocked also enforces the safety gate compaction has always
// had: an operation between claiming its slot and having its start record
// written is not in the active set a compaction rewrites from, so compacting
// now would erase it.
func (l *Ledger) journalOverBudgetLocked() bool {
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
func (l *Ledger) canCompactLocked() bool {
	return len(l.starting) == 0
}

func (l *Ledger) replay(events []Event) error {
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
func (l *Ledger) replayEvent(
	index int,
	event *Event,
	dropped map[string]struct{},
) error {
	{
		switch event.Type {
		case EventCheckpoint:
			if index != 0 {
				return fmt.Errorf("replay failure ledger event %d: checkpoint must be first", index)
			}
			for result, count := range event.Results {
				if !ValidResult(result) {
					return fmt.Errorf("replay failure ledger event %d: invalid checkpoint result %q", index, result)
				}
				l.results[result] = count
			}
			for observer, counts := range event.ObservationCounts {
				if _, known := ObserverDefinitionFor(observer); !known {
					return fmt.Errorf("replay failure ledger event %d: invalid checkpoint observer %q", index, observer)
				}
				for observation, count := range counts {
					if !ValidObservation(observation) {
						return fmt.Errorf("replay failure ledger event %d: invalid checkpoint observation %q", index, observation)
					}
					l.countObservationByLocked(observer, observation, count)
				}
			}
			for _, operationID := range event.NotSent {
				l.rememberNotSentLocked(operationID)
			}
		case EventStarted:
			if event.Operation == nil {
				return fmt.Errorf("replay failure ledger event %d: started operation is missing", index)
			}
			operation := CloneOperation(event.Operation)
			if err := ValidateOperation(operation); err != nil {
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
			operation.SetNextVerifyAt(operation.VerifyAfter)
			operation.SetClaimed(false)
			l.active[operation.ID] = operation
		case EventActivated:
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
			if operation.LifecycleState == OperationActive {
				return fmt.Errorf("replay failure ledger event %d: operation %q is already activated", index, event.OperationID)
			}
			operation.LifecycleState = OperationActive
		case EventObserved:
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
			if !slices.Contains(operation.Expected, event.Observer) || !ValidObservation(event.Observation) {
				return fmt.Errorf("replay failure ledger event %d: invalid observation", index)
			}
			if existing, duplicate := operation.Observations[event.Observer]; duplicate {
				return fmt.Errorf("replay failure ledger event %d: observer %q already recorded as %q", index, event.Observer, existing)
			}
			operation.Observations[event.Observer] = event.Observation
			if !ValidReason(event.Reason) {
				return fmt.Errorf("replay failure ledger event %d: invalid observation reason %q", index, event.Reason)
			}
			if operation.ObservationReasons == nil {
				operation.ObservationReasons = make(map[Observer]Reason)
			}
			operation.ObservationReasons[event.Observer] = event.Reason
			l.countObservationLocked(event.Observer, event.Observation)
		case EventFinalized:
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
			if !ValidResult(event.Result) {
				return fmt.Errorf("replay failure ledger event %d: invalid result %q", index, event.Result)
			}
			l.results[event.Result]++
			if event.Result == ResultNotSent {
				l.rememberNotSentLocked(operation.ID)
			}
			delete(l.active, event.OperationID)
		case EventInvariant:
			l.invalidateLocked("accounting_invariant")
		case EventInvalidated:
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
func (l *Ledger) finishReplay() error {
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
		l.noteInvalidationLocked(InvalidReasonCapacity)
	}
	for _, operation := range l.active {
		l.enqueueLocked(operation)
	}
	return nil
}

func (l *Ledger) rememberNotSentLocked(operationID string) {
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

func (l *Ledger) countObservationLocked(observer Observer, observation Observation) {
	l.countObservationByLocked(observer, observation, 1)
}

func (l *Ledger) countObservationByLocked(observer Observer, observation Observation, count uint64) {
	if l.observations[observer] == nil {
		l.observations[observer] = make(map[Observation]uint64)
	}
	l.observations[observer][observation] += count
}

func cloneFailureObservationCounts(
	input map[Observer]map[Observation]uint64,
) map[Observer]map[Observation]uint64 {
	cloned := make(map[Observer]map[Observation]uint64, len(input))
	for observer, counts := range input {
		cloned[observer] = make(map[Observation]uint64, len(counts))
		for observation, count := range counts {
			cloned[observer][observation] = count
		}
	}
	return cloned
}

// ErrLedgerClosed is a sentinel so callers can recognise a closed ledger
// with errors.Is rather than by matching the message.
var ErrLedgerClosed = errors.New("failure ledger is closed")

func (l *Ledger) ensureOpen() error {
	if l.closed {
		return ErrLedgerClosed
	}
	return nil
}

// invalidateLocked records one cause. The first keeps InvalidReason, since that
// is the point the run's evidence stopped standing; every distinct cause is
// counted and journaled, so a startup invalidation cannot silence the runtime
// failures an operator still has to act on.
func (l *Ledger) invalidateLocked(reason string) {
	l.noteInvalidationLocked(reason)
	// Outside the note above: a cause already counted in memory can still be
	// missing from the journal, and durability is what the retry is for.
	l.retryPendingInvalidationsLocked()
}

// noteInvalidationLocked records a cause in memory and counts it once. It
// deliberately does not touch the journal, so the paths that report a journal
// failure can use it without attempting the append that just failed.
func (l *Ledger) noteInvalidationLocked(reason string) {
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
func (l *Ledger) persistInvalidationLocked(reason string) bool {
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
		l.noteInvalidationLocked(InvalidReasonWAL)
		return false
	}
	l.persistedInvalidReasons = append(l.persistedInvalidReasons, reason)
	return true
}

// journalInvalidationLocked persists the cause on a best effort. It cannot
// route its own failure back through invalidateLocked — that is how the WAL
// paths report, and it would recurse — and a lost record is strictly better
// than losing the in-memory verdict too, so the error is logged and dropped.
func (l *Ledger) journalInvalidationLocked(reason string) error {
	if l.replaying || l.journal == nil {
		return nil
	}
	err := l.journal.Append(&Event{
		Type: EventInvalidated, InvalidReason: reason, At: l.now().UTC(),
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
