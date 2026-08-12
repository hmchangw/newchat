package main

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

type failureObserver string

const (
	failureObserverAdmission failureObserver = "admission"
	failureObserverHistory   failureObserver = "cassandra_history"
)

type failureObservation string

const (
	failureObservationGood failureObservation = "good"
	failureObservationBad  failureObservation = "bad"
	// failureObservationUnverified records that the observer itself could not
	// answer. It is an availability signal, never evidence of data loss.
	failureObservationUnverified           failureObservation = "unverified"
	failureObservationMissingAfterDeadline failureObservation = "missing_after_deadline"
)

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

type failureOperation struct {
	ID            string                                 `json:"id"`
	CorrelationID string                                 `json:"correlationId,omitempty"`
	Scenario      string                                 `json:"scenario"`
	Lane          string                                 `json:"lane"`
	StartedAt     time.Time                              `json:"startedAt"`
	VerifyAfter   time.Time                              `json:"verifyAfter"`
	Deadline      time.Time                              `json:"deadline"`
	Expected      []failureObserver                      `json:"expected"`
	Attributes    map[string]string                      `json:"attributes,omitempty"`
	Observations  map[failureObserver]failureObservation `json:"observations,omitempty"`

	nextVerifyAt time.Time
	claimed      bool
	// heapIndex is the operation's position in the ledger's verification queue,
	// or -1 when it is not queued.
	heapIndex int
}

type failureLedgerEvent struct {
	Type        string             `json:"type"`
	Operation   *failureOperation  `json:"operation,omitempty"`
	OperationID string             `json:"operationId,omitempty"`
	Observer    failureObserver    `json:"observer,omitempty"`
	Observation failureObservation `json:"observation,omitempty"`
	Result      failureResult      `json:"result,omitempty"`
	At          time.Time          `json:"at"`
}

const (
	failureLedgerEventStarted   = "started"
	failureLedgerEventObserved  = "observed"
	failureLedgerEventFinalized = "finalized"
)

type failureJournal interface {
	Replay() ([]failureLedgerEvent, error)
	Append(*failureLedgerEvent) error
	Compact([]failureLedgerEvent) error
	Size() int64
	Close() error
}

type failureLedgerConfig struct {
	Capacity     int
	CompactEvery int
	Journal      failureJournal
	Now          func() time.Time
	Recorder     failureLedgerRecorder
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
	JournalBytes  int64
}

type failureLedger struct {
	mu sync.Mutex

	capacity     int
	compactEvery int
	journal      failureJournal
	now          func() time.Time
	recorder     failureLedgerRecorder

	active                map[string]*failureOperation
	verifyQueue           failureVerifyQueue
	results               map[failureResult]uint64
	recovered             int
	dropped               int
	invalidReason         string
	closed                bool
	finalizedSinceCompact int
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

func newFailureLedger(cfg failureLedgerConfig) (*failureLedger, error) {
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
		capacity:     cfg.Capacity,
		compactEvery: cfg.CompactEvery,
		journal:      cfg.Journal,
		now:          cfg.Now,
		recorder:     cfg.Recorder,
		active:       make(map[string]*failureOperation, cfg.Capacity),
		results:      make(map[failureResult]uint64),
	}
	if cfg.Journal == nil {
		return ledger, nil
	}
	events, err := cfg.Journal.Replay()
	if err != nil {
		return nil, fmt.Errorf("replay failure ledger journal: %w", err)
	}
	if err := ledger.replay(events); err != nil {
		return nil, err
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
	if err := validateFailureOperation(operation); err != nil {
		return fmt.Errorf("start failure operation: %w", err)
	}
	tracked := cloneFailureOperation(operation)
	tracked.Observations = make(map[failureObserver]failureObservation)
	tracked.nextVerifyAt = tracked.VerifyAfter

	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return err
	}
	if _, exists := l.active[tracked.ID]; exists {
		return fmt.Errorf("failure operation %q is already active", tracked.ID)
	}
	if len(l.active) >= l.capacity {
		l.invalidateLocked(invalidReasonCapacity)
		return fmt.Errorf("start failure operation %q: %w", tracked.ID, errFailureLedgerCapacity)
	}
	event := failureLedgerEvent{
		Type: failureLedgerEventStarted, Operation: cloneFailureOperation(tracked),
		At: l.now().UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.invalidateLocked("wal")
		return fmt.Errorf("persist failure operation %q: %w", tracked.ID, err)
	}
	l.active[tracked.ID] = tracked
	l.enqueueLocked(tracked)
	if l.recorder != nil {
		l.recorder.OperationStarted(cloneFailureOperation(tracked))
	}
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
	if !validFailureResult(result) {
		return fmt.Errorf("invalid failure result %q", result)
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
	if err := l.finalizeLocked(operation, result, at); err != nil {
		return err
	}
	return nil
}

func (l *failureLedger) Observe(
	operationID string,
	observer failureObserver,
	observation failureObservation,
	at time.Time,
) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return false, err
	}
	operation := l.active[operationID]
	if operation == nil {
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
		Observer: observer, Observation: observation, At: at.UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.invalidateLocked("wal")
		return false, fmt.Errorf("persist failure observation for %q: %w", operationID, err)
	}
	operation.Observations[observer] = observation
	if observer == failureObserverHistory {
		operation.claimed = false
		l.dequeueLocked(operation)
	}
	if l.recorder != nil {
		l.recorder.ObservationRecorded(
			cloneFailureOperation(operation), observer, observation,
		)
	}
	if len(operation.Observations) != len(operation.Expected) {
		return false, nil
	}
	if err := l.finalizeLocked(operation, failureOperationResult(operation), at); err != nil {
		return false, err
	}
	return true, nil
}

func (l *failureLedger) Expire(now time.Time) (int, error) {
	if now.IsZero() {
		now = l.now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureOpen(); err != nil {
		return 0, err
	}
	finalized := 0
	for _, operation := range l.active {
		if now.Before(operation.Deadline) {
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
				Observer: observer, Observation: failureObservationMissingAfterDeadline,
				At: now.UTC(),
			}
			if err := l.appendLocked(&event); err != nil {
				l.invalidateLocked("wal")
				return finalized, fmt.Errorf(
					"persist expired failure observation for %q: %w", operation.ID, err,
				)
			}
			operation.Observations[observer] = failureObservationMissingAfterDeadline
			if l.recorder != nil {
				l.recorder.ObservationRecorded(
					cloneFailureOperation(operation), observer,
					failureObservationMissingAfterDeadline,
				)
			}
		}
		if err := l.finalizeLocked(
			operation,
			failureOperationResult(operation),
			now,
		); err != nil {
			return finalized, err
		}
		finalized++
	}
	return finalized, nil
}

func (l *failureLedger) ClaimDue(now time.Time) (failureOperation, bool) {
	if now.IsZero() {
		now = l.now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.verifyQueue.Len() == 0 || now.Before(l.verifyQueue[0].nextVerifyAt) {
		return failureOperation{}, false
	}
	selected, ok := heap.Pop(&l.verifyQueue).(*failureOperation)
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
	if !slices.Contains(operation.Expected, failureObserverHistory) {
		return
	}
	if _, observed := operation.Observations[failureObserverHistory]; observed {
		return
	}
	heap.Push(&l.verifyQueue, operation)
}

func (l *failureLedger) dequeueLocked(operation *failureOperation) {
	if operation.heapIndex < 0 {
		return
	}
	heap.Remove(&l.verifyQueue, operation.heapIndex)
}

func (l *failureLedger) Snapshot() failureLedgerSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	results := make(map[failureResult]uint64, len(l.results))
	for result, count := range l.results {
		results[result] = count
	}
	journalBytes := int64(0)
	if l.journal != nil {
		journalBytes = l.journal.Size()
	}
	return failureLedgerSnapshot{
		Active: len(l.active), Recovered: l.recovered, Dropped: l.dropped,
		InvalidReason: l.invalidReason, Results: results,
		JournalBytes: journalBytes,
	}
}

func (l *failureLedger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.journal == nil {
		return nil
	}
	if err := l.journal.Close(); err != nil {
		return fmt.Errorf("close failure ledger journal: %w", err)
	}
	return nil
}

func (l *failureLedger) finalizeLocked(
	operation *failureOperation,
	result failureResult,
	at time.Time,
) error {
	event := failureLedgerEvent{
		Type: failureLedgerEventFinalized, OperationID: operation.ID,
		Result: result, At: at.UTC(),
	}
	if err := l.appendLocked(&event); err != nil {
		l.invalidateLocked("wal")
		return fmt.Errorf("persist finalized failure operation %q: %w", operation.ID, err)
	}
	l.results[result]++
	l.dequeueLocked(operation)
	delete(l.active, operation.ID)
	if l.recorder != nil {
		l.recorder.OperationFinalized(cloneFailureOperation(operation), result)
	}
	l.finalizedSinceCompact++
	if l.journal != nil && l.finalizedSinceCompact >= l.compactEvery {
		if err := l.compactLocked(at); err != nil {
			l.invalidateLocked("wal")
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
	events := make([]failureLedgerEvent, 0, len(l.active)*2)
	for _, operationID := range operationIDs {
		operation := l.active[operationID]
		started := cloneFailureOperation(operation)
		started.Observations = nil
		events = append(events, failureLedgerEvent{
			Type: failureLedgerEventStarted, Operation: started,
			At: operation.StartedAt.UTC(),
		})
		for _, observer := range operation.Expected {
			observation, exists := operation.Observations[observer]
			if !exists {
				continue
			}
			events = append(events, failureLedgerEvent{
				Type: failureLedgerEventObserved, OperationID: operation.ID,
				Observer: observer, Observation: observation, At: at.UTC(),
			})
		}
	}
	if err := l.journal.Compact(events); err != nil {
		return err
	}
	if l.recorder != nil {
		l.recorder.JournalSize(l.journal.Size())
	}
	return nil
}

func (l *failureLedger) appendLocked(event *failureLedgerEvent) error {
	if l.journal == nil {
		return nil
	}
	if err := l.journal.Append(event); err != nil {
		return err
	}
	if l.recorder != nil {
		l.recorder.JournalSize(l.journal.Size())
	}
	return nil
}

func (l *failureLedger) replay(events []failureLedgerEvent) error {
	// A retained volume outlives any single configuration, so a journal can
	// legitimately hold more unresolved operations than the current capacity
	// admits. Dropping the excess degrades observation for this run; failing
	// would crash-loop the pod with no way out.
	dropped := make(map[string]struct{})
	for index, event := range events {
		switch event.Type {
		case failureLedgerEventStarted:
			if event.Operation == nil {
				return fmt.Errorf("replay failure ledger event %d: started operation is missing", index)
			}
			operation := cloneFailureOperation(event.Operation)
			if err := validateFailureOperation(operation); err != nil {
				return fmt.Errorf("replay failure ledger event %d: %w", index, err)
			}
			if len(l.active) >= l.capacity {
				dropped[operation.ID] = struct{}{}
				l.dropped++
				continue
			}
			operation.nextVerifyAt = operation.VerifyAfter
			operation.claimed = false
			l.active[operation.ID] = operation
		case failureLedgerEventObserved:
			if _, skipped := dropped[event.OperationID]; skipped {
				continue
			}
			operation := l.active[event.OperationID]
			if operation == nil {
				return fmt.Errorf(
					"replay failure ledger event %d: operation %q is not active",
					index,
					event.OperationID,
				)
			}
			operation.Observations[event.Observer] = event.Observation
		case failureLedgerEventFinalized:
			if _, skipped := dropped[event.OperationID]; skipped {
				delete(dropped, event.OperationID)
				l.dropped--
				continue
			}
			if _, exists := l.active[event.OperationID]; !exists {
				return fmt.Errorf(
					"replay failure ledger event %d: operation %q is not active",
					index,
					event.OperationID,
				)
			}
			delete(l.active, event.OperationID)
		default:
			return fmt.Errorf("replay failure ledger event %d: unknown type %q", index, event.Type)
		}
	}
	if l.dropped > 0 {
		slog.Warn(
			"failure ledger dropped recovered operations over capacity",
			"dropped", l.dropped,
			"capacity", l.capacity,
		)
		l.invalidateLocked(invalidReasonCapacity)
	}
	for _, operation := range l.active {
		l.enqueueLocked(operation)
	}
	return nil
}

func (l *failureLedger) ensureOpen() error {
	if l.closed {
		return fmt.Errorf("failure ledger is closed")
	}
	return nil
}

func (l *failureLedger) invalidateLocked(reason string) {
	if l.invalidReason != "" {
		return
	}
	l.invalidReason = reason
	if l.recorder != nil {
		l.recorder.Invalidated(reason)
	}
}

func validateFailureOperation(operation *failureOperation) error {
	if operation == nil {
		return fmt.Errorf("failure operation is required")
	}
	if operation.ID == "" || operation.Scenario == "" || operation.Lane == "" {
		return fmt.Errorf("failure operation requires ID, scenario, and lane")
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

// failureOperationResult collapses an operation's observations into one verdict.
// Precedence runs from strongest evidence to weakest: a confirmed absence
// outranks an ambiguous observation, which outranks "the observer could not
// answer", which outranks success.
func failureOperationResult(operation *failureOperation) failureResult {
	result := failureResultGood
	for observer, observation := range operation.Observations {
		switch observation {
		case failureObservationGood:
		case failureObservationMissingAfterDeadline:
			// A missing admission reply is an availability ambiguity, not proof
			// that a successfully persisted message was lost. Only a healthy
			// history read that confirms absence can make the current vertical
			// slice's data-loss claim.
			if observer == failureObserverAdmission {
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

func cloneFailureOperation(operation *failureOperation) *failureOperation {
	if operation == nil {
		return nil
	}
	cloned := *operation
	cloned.heapIndex = -1
	cloned.Expected = append([]failureObserver(nil), operation.Expected...)
	cloned.Attributes = make(map[string]string, len(operation.Attributes))
	for key, value := range operation.Attributes {
		cloned.Attributes[key] = value
	}
	cloned.Observations = make(map[failureObserver]failureObservation, len(operation.Observations))
	for observer, observation := range operation.Observations {
		cloned.Observations[observer] = observation
	}
	return &cloned
}

type fileFailureWAL struct {
	mu   sync.Mutex
	path string
	file *os.File
	size int64
}

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
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open failure WAL %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat failure WAL %q: %w", path, err)
	}
	return &fileFailureWAL{path: path, file: file, size: info.Size()}, nil
}

func (w *fileFailureWAL) Replay() ([]failureLedgerEvent, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	file, err := os.Open(w.path)
	if err != nil {
		return nil, fmt.Errorf("open failure WAL for replay: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()

	events := make([]failureLedgerEvent, 0)
	reader := bufio.NewReader(file)
	line := 0
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
			return nil, fmt.Errorf("read failure WAL line %d: %w", line+1, readErr)
		}
		line++
		var event failureLedgerEvent
		if err := json.Unmarshal(encoded, &event); err != nil {
			return nil, fmt.Errorf("decode failure WAL line %d: %w", line, err)
		}
		events = append(events, event)
		durableBytes += int64(len(encoded))
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close replayed failure WAL: %w", err)
	}
	closed = true
	if tornFinalRecord {
		if err := w.file.Close(); err != nil {
			return nil, fmt.Errorf("close failure WAL before repair: %w", err)
		}
		w.file = nil
		if err := os.Truncate(w.path, durableBytes); err != nil {
			return nil, w.reopenAfterCompactFailure(
				fmt.Errorf("truncate torn failure WAL record: %w", err),
			)
		}
		reopened, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o600)
		if err != nil {
			return nil, fmt.Errorf("reopen repaired failure WAL: %w", err)
		}
		w.file = reopened
		if err := w.file.Sync(); err != nil {
			return nil, fmt.Errorf("sync repaired failure WAL: %w", err)
		}
		w.size = durableBytes
	}
	return events, nil
}

func (w *fileFailureWAL) Append(event *failureLedgerEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("failure WAL is closed")
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
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync failure WAL event: %w", err)
	}
	w.size += int64(written)
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
	for _, event := range events {
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
	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen compacted failure WAL: %w", err)
	}
	w.file = file
	w.size = compactedSize
	if err := os.Remove(backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove failure WAL backup: %w", err)
	}
	return nil
}

func (w *fileFailureWAL) reopenAfterCompactFailure(compactErr error) error {
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
