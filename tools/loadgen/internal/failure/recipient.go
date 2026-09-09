package failure

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	sendmodel "github.com/hmchangw/chat/tools/loadgen/internal/soak/send"
)

type RecipientMetrics interface {
	SetObserverUp(Observer, bool)
	SetObserverQueueDepth(Observer, int)
	RecordObserverEvent(Observer, Observation)
	RecordUntracked(string)
	ObserveEvidenceFlush(string, string, time.Duration)
	RecordEvidence(string)
}

type RecipientExpectedRoute = sendmodel.RecipientExpectedRoute

const (
	RecipientRouteAny  = sendmodel.RecipientRouteAny
	RecipientRouteRoom = sendmodel.RecipientRouteRoom
	RecipientRouteUser = sendmodel.RecipientRouteUser
)

type RecipientSetSource = sendmodel.RecipientSetSource

const (
	RecipientSourceLegacy          = sendmodel.RecipientSourceLegacy
	RecipientSourceTopology        = sendmodel.RecipientSourceTopology
	RecipientSourceThreadFollowers = sendmodel.RecipientSourceThreadFollowers
)

type RecipientEvidenceResult struct {
	Observation    Observation      `json:"observation"`
	Missing        []string           `json:"missing,omitempty"`
	Unexpected     []string           `json:"unexpected,omitempty"`
	Duplicates     []string           `json:"duplicates,omitempty"`
	Mismatches     []string           `json:"mismatches,omitempty"`
	durableRecords map[string]map[string]struct{}
}

func (r RecipientEvidenceResult) Durable(kind, recipient string) bool {
	_, ok := r.durableRecords[kind][recipient]
	return ok
}

type RecipientDeliveryRoute string

const (
	RecipientDeliveryUnknown    RecipientDeliveryRoute = "unknown"
	RecipientDeliveryRoomGlobal RecipientDeliveryRoute = "room_global"
	RecipientDeliveryRoomLocal  RecipientDeliveryRoute = "room_local"
	RecipientDeliveryUser       RecipientDeliveryRoute = "user"
)

type RecipientExpectationConfig struct {
	OperationID string
	Recipients  []string
	RoomID      string
	EventType   model.RoomEventType
	Route       RecipientExpectedRoute
	Source      RecipientSetSource
	Complete    bool
}

type recipientExpectation struct {
	expected          map[string]struct{}
	observed          map[string]map[RecipientDeliveryRoute]int
	mismatches        map[string]struct{}
	roomID            string
	eventType         model.RoomEventType
	route             RecipientExpectedRoute
	source            RecipientSetSource
	complete          bool
	durableMissing    map[string]struct{}
	durableUnexpected map[string]struct{}
	durableDuplicates map[string]struct{}
	durableMismatches map[string]struct{}
}

type RecipientEvidence struct {
	mu              sync.Mutex
	allowDuplicates bool
	operations      map[string]*recipientExpectation
	// capacity bounds the map when expiry cannot run — a ledger that keeps
	// failing to expire would otherwise reinstate the growth this file exists
	// to stop. Zero means unbounded.
	capacity int
}

type RecipientEvidenceDisposition string

const (
	RecipientEvidenceUntracked  RecipientEvidenceDisposition = "untracked"
	RecipientEvidenceExpected   RecipientEvidenceDisposition = "expected"
	RecipientEvidenceDuplicate  RecipientEvidenceDisposition = "duplicate"
	RecipientEvidenceUnexpected RecipientEvidenceDisposition = "unexpected"
	RecipientEvidenceMismatch   RecipientEvidenceDisposition = "mismatch"
)

func NewRecipientEvidence(allowDuplicates bool) *RecipientEvidence {
	return &RecipientEvidence{
		allowDuplicates: allowDuplicates,
		operations:      make(map[string]*recipientExpectation),
	}
}

func (r *RecipientEvidence) SetCapacity(capacity int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capacity = max(0, capacity)
}

func (r *RecipientEvidence) SetAllowDuplicates(allow bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.allowDuplicates = allow
}

func (r *RecipientEvidence) Capacity() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capacity
}

func (r *RecipientEvidence) Expect(operationID string, recipients []string) error {
	return r.ExpectDelivery(&RecipientExpectationConfig{
		OperationID: operationID, Recipients: recipients,
		Route: RecipientRouteAny, Source: RecipientSourceLegacy, Complete: true,
	})
}

func (r *RecipientEvidence) ExpectEvent(
	operationID string,
	recipients []string,
	roomID string,
	eventType model.RoomEventType,
) error {
	return r.ExpectDelivery(&RecipientExpectationConfig{
		OperationID: operationID, Recipients: recipients, RoomID: roomID, EventType: eventType,
		Route: RecipientRouteAny, Source: RecipientSourceLegacy, Complete: true,
	})
}

func (r *RecipientEvidence) ExpectDelivery(config *RecipientExpectationConfig) error {
	if r == nil || config == nil || config.OperationID == "" || len(config.Recipients) == 0 {
		return fmt.Errorf("recipient expectation requires operation and recipients")
	}
	if (config.RoomID == "") != (config.EventType == "") {
		return fmt.Errorf("recipient expectation room and event type must be provided together")
	}
	if config.Route == "" {
		config.Route = RecipientRouteAny
	}
	if config.Source == "" {
		config.Source = RecipientSourceLegacy
	}
	if config.Route != RecipientRouteAny && config.Route != RecipientRouteRoom && config.Route != RecipientRouteUser {
		return fmt.Errorf("recipient expectation has unknown route %q", config.Route)
	}
	if config.Source != RecipientSourceLegacy && config.Source != RecipientSourceTopology && config.Source != RecipientSourceThreadFollowers {
		return fmt.Errorf("recipient expectation has unknown source %q", config.Source)
	}
	expected := make(map[string]struct{}, len(config.Recipients))
	for _, recipient := range config.Recipients {
		if recipient == "" {
			return fmt.Errorf("recipient expectation contains an empty recipient")
		}
		if _, duplicate := expected[recipient]; duplicate {
			return fmt.Errorf("recipient expectation repeats %q", recipient)
		}
		expected[recipient] = struct{}{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.operations[config.OperationID]; exists {
		return fmt.Errorf("recipient expectation %q already exists", config.OperationID)
	}
	if r.capacity > 0 && len(r.operations) >= r.capacity {
		return fmt.Errorf(
			"recipient expectation capacity %d exceeded; evidence is not being expired",
			r.capacity)
	}
	r.operations[config.OperationID] = &recipientExpectation{
		expected: expected, observed: make(map[string]map[RecipientDeliveryRoute]int), mismatches: make(map[string]struct{}),
		roomID: config.RoomID, eventType: config.EventType,
		route: config.Route, source: config.Source, complete: config.Complete,
		durableMissing: make(map[string]struct{}), durableUnexpected: make(map[string]struct{}),
		durableDuplicates: make(map[string]struct{}),
		durableMismatches: make(map[string]struct{}),
	}
	return nil
}

func (r *RecipientEvidence) Observe(operationID, recipient string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return false
	}
	incrementRecipientRoute(expectation, recipient, RecipientDeliveryUnknown)
	return true
}

func (r *RecipientEvidence) ObserveEvent(
	operationID,
	recipient,
	roomID string,
	eventType model.RoomEventType,
) bool {
	disposition := r.ObserveDelivery(operationID, recipient, roomID, eventType, RecipientDeliveryUnknown)
	return disposition != RecipientEvidenceUntracked
}

func (r *RecipientEvidence) ObserveDelivery(
	operationID,
	recipient,
	roomID string,
	eventType model.RoomEventType,
	route RecipientDeliveryRoute,
) RecipientEvidenceDisposition {
	if r == nil {
		return RecipientEvidenceUntracked
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return RecipientEvidenceUntracked
	}
	disposition := RecipientEvidenceExpected
	if expectation.roomID != "" &&
		(expectation.roomID != roomID || expectation.eventType != eventType) {
		disposition = RecipientEvidenceMismatch
	}
	if !recipientRouteMatches(expectation.route, route) {
		disposition = RecipientEvidenceMismatch
	}
	if disposition == RecipientEvidenceExpected && expectation.complete {
		if _, expected := expectation.expected[recipient]; !expected {
			disposition = RecipientEvidenceUnexpected
		}
	}
	if disposition == RecipientEvidenceMismatch {
		expectation.mismatches[recipient] = struct{}{}
		return disposition
	}
	if disposition == RecipientEvidenceUnexpected {
		incrementRecipientRoute(expectation, recipient, route)
		return disposition
	}
	if recipientRouteIsDuplicate(expectation, recipient, route) {
		disposition = RecipientEvidenceDuplicate
	}
	incrementRecipientRoute(expectation, recipient, route)
	return disposition
}

func recipientRouteMatches(expected RecipientExpectedRoute, actual RecipientDeliveryRoute) bool {
	switch expected {
	case RecipientRouteAny:
		return true
	case RecipientRouteRoom:
		return actual == RecipientDeliveryRoomGlobal || actual == RecipientDeliveryRoomLocal
	case RecipientRouteUser:
		return actual == RecipientDeliveryUser
	default:
		return false
	}
}

func recipientRouteIsDuplicate(expectation *recipientExpectation, recipient string, route RecipientDeliveryRoute) bool {
	routes := expectation.observed[recipient]
	if routes == nil {
		return false
	}
	return routes[route] > 0
}

func incrementRecipientRoute(expectation *recipientExpectation, recipient string, route RecipientDeliveryRoute) {
	if expectation.observed[recipient] == nil {
		expectation.observed[recipient] = make(map[RecipientDeliveryRoute]int)
	}
	expectation.observed[recipient][route]++
}

func (r *RecipientEvidence) ObserveMismatch(operationID, recipient string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return false
	}
	expectation.mismatches[recipient] = struct{}{}
	return true
}

func (r *RecipientEvidence) ReplayPositive(kind, operationID, recipient string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return false
	}
	switch kind {
	case "missing":
		expectation.durableMissing[recipient] = struct{}{}
	case "unexpected":
		expectation.durableUnexpected[recipient] = struct{}{}
	case "duplicate":
		expectation.durableDuplicates[recipient] = struct{}{}
	case "mismatch":
		expectation.mismatches[recipient] = struct{}{}
		expectation.durableMismatches[recipient] = struct{}{}
	default:
		return false
	}
	return true
}

func (r *RecipientEvidence) Forget(operationID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.operations, operationID)
}

// ForgetAll releases the expectations for the operations the ledger reports it
// finalized, and returns how many went. Taking the IDs from the ledger is what
// keeps the two aligned: a time-based bound cannot be, because Expire stops at
// its batch limit and skips claimed operations mid-verification, so "past its
// deadline" and "the ledger is done with it" are different sets.
//
// The caller passes IDs the ledger has already returned, so this takes only the
// evidence lock — the ledger's is released by then, and no ordering between the
// two is created.
func (r *RecipientEvidence) ForgetAll(operationIDs []string) int {
	if r == nil || len(operationIDs) == 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	forgotten := 0
	for _, operationID := range operationIDs {
		if _, tracked := r.operations[operationID]; !tracked {
			continue
		}
		delete(r.operations, operationID)
		forgotten++
	}
	return forgotten
}

// Len reports how many expectations are retained. The run needs this as a gauge:
// a count that tracks the ledger's own in-flight number is healthy, one that
// climbs past it is this map leaking again.
func (r *RecipientEvidence) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.operations)
}

func (r *RecipientEvidence) Complete(operationID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return false
	}
	if !expectation.complete {
		return false
	}
	for recipient := range expectation.expected {
		if !recipientWasObserved(expectation, recipient) {
			return false
		}
	}
	for recipient := range expectation.observed {
		if _, expected := expectation.expected[recipient]; !expected || (!r.allowDuplicates && recipientHasDuplicate(expectation, recipient)) {
			return false
		}
	}
	return len(expectation.mismatches) == 0
}

func (r *RecipientEvidence) Finalize(operationID string, observerHealthy bool) RecipientEvidenceResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return RecipientEvidenceResult{Observation: ObservationUnverified}
	}
	result := RecipientEvidenceResult{
		Observation:    ObservationGood,
		durableRecords: make(map[string]map[string]struct{}),
	}
	for recipient := range expectation.durableMissing {
		result.Missing = append(result.Missing, recipient)
		markRecipientEvidenceDurable(&result, "missing", recipient)
	}
	for recipient := range expectation.durableUnexpected {
		result.Unexpected = append(result.Unexpected, recipient)
		markRecipientEvidenceDurable(&result, "unexpected", recipient)
	}
	for recipient := range expectation.durableDuplicates {
		result.Duplicates = append(result.Duplicates, recipient)
		markRecipientEvidenceDurable(&result, "duplicate", recipient)
	}
	for recipient := range expectation.durableMismatches {
		result.Mismatches = append(result.Mismatches, recipient)
		markRecipientEvidenceDurable(&result, "mismatch", recipient)
	}
	durableClaim := len(result.Missing)+len(result.Unexpected)+len(result.Mismatches) > 0 ||
		(!r.allowDuplicates && len(result.Duplicates) > 0)
	if durableClaim {
		slices.Sort(result.Missing)
		slices.Sort(result.Unexpected)
		slices.Sort(result.Duplicates)
		slices.Sort(result.Mismatches)
		if len(result.Mismatches) > 0 || len(result.Unexpected) > 0 ||
			(!r.allowDuplicates && len(result.Duplicates) > 0) {
			result.Observation = ObservationBad
		} else if len(result.Missing) > 0 {
			result.Observation = ObservationMissingAfterDeadline
		}
		delete(r.operations, operationID)
		return result
	}
	if expectation.complete {
		for recipient := range expectation.expected {
			if !recipientWasObserved(expectation, recipient) {
				result.Missing = appendRecipientOnce(result.Missing, recipient)
			}
		}
	}
	for recipient := range expectation.observed {
		if _, expected := expectation.expected[recipient]; expectation.complete && !expected {
			result.Unexpected = appendRecipientOnce(result.Unexpected, recipient)
		}
		if recipientHasDuplicate(expectation, recipient) {
			result.Duplicates = appendRecipientOnce(result.Duplicates, recipient)
		}
	}
	for recipient := range expectation.mismatches {
		result.Mismatches = appendRecipientOnce(result.Mismatches, recipient)
	}
	slices.Sort(result.Missing)
	slices.Sort(result.Unexpected)
	slices.Sort(result.Duplicates)
	slices.Sort(result.Mismatches)
	switch {
	case len(result.Mismatches) > 0 || len(result.Unexpected) > 0 || (!r.allowDuplicates && len(result.Duplicates) > 0):
		result.Observation = ObservationBad
	case !observerHealthy || !expectation.complete:
		result.Observation = ObservationUnverified
	case len(result.Missing) > 0:
		result.Observation = ObservationMissingAfterDeadline
	}
	delete(r.operations, operationID)
	return result
}

func appendRecipientOnce(recipients []string, recipient string) []string {
	if slices.Contains(recipients, recipient) {
		return recipients
	}
	return append(recipients, recipient)
}

func markRecipientEvidenceDurable(result *RecipientEvidenceResult, kind, recipient string) {
	if result.durableRecords[kind] == nil {
		result.durableRecords[kind] = make(map[string]struct{})
	}
	result.durableRecords[kind][recipient] = struct{}{}
}

func recipientWasObserved(expectation *recipientExpectation, recipient string) bool {
	for _, count := range expectation.observed[recipient] {
		if count > 0 {
			return true
		}
	}
	return false
}

func recipientHasDuplicate(expectation *recipientExpectation, recipient string) bool {
	routes := expectation.observed[recipient]
	for _, count := range routes {
		if count > 1 {
			return true
		}
	}
	return false
}
