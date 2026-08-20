package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

type failureRecipientSubscription interface {
	Unsubscribe() error
}

type failureRecipientSource interface {
	Subscribe(string, string, nats.MsgHandler) (failureRecipientSubscription, error)
	Flush() error
}

type failureRecipientConnection interface {
	Subscribe(string, nats.MsgHandler) (failureRecipientSubscription, error)
	Flush() error
	Drain() error
}

type natsFailureRecipientConnection struct {
	nc        *nats.Conn
	closed    <-chan struct{}
	drain     func() error
	lastError func() error
}

func newNATSFailureRecipientConnection(nc *nats.Conn) *natsFailureRecipientConnection {
	if nc == nil {
		return &natsFailureRecipientConnection{}
	}
	closed := make(chan struct{})
	var closeOnce sync.Once
	markClosed := func() { closeOnce.Do(func() { close(closed) }) }
	previous := nc.ClosedHandler()
	nc.SetClosedHandler(func(connection *nats.Conn) {
		if previous != nil {
			previous(connection)
		}
		markClosed()
	})
	if nc.IsClosed() {
		markClosed()
	}
	return &natsFailureRecipientConnection{
		nc: nc, closed: closed, drain: nc.Drain, lastError: nc.LastError,
	}
}

func (c *natsFailureRecipientConnection) Subscribe(
	subjectName string,
	handler nats.MsgHandler,
) (failureRecipientSubscription, error) {
	return c.nc.Subscribe(subjectName, handler)
}

func (c *natsFailureRecipientConnection) Flush() error { return c.nc.Flush() }
func (c *natsFailureRecipientConnection) Drain() error {
	if c == nil || c.drain == nil {
		return fmt.Errorf("recipient observer connection is not configured")
	}
	if err := c.drain(); err != nil {
		return fmt.Errorf("drain recipient observer connection: %w", err)
	}
	if c.closed != nil {
		<-c.closed
	}
	if c.lastError != nil && errors.Is(c.lastError(), nats.ErrDrainTimeout) {
		return fmt.Errorf("drain recipient observer connection: %w", nats.ErrDrainTimeout)
	}
	return nil
}

type natsFailureRecipientSource struct {
	mu          sync.Mutex
	poolSize    int
	connect     func(int) (failureRecipientConnection, error)
	connections map[int]failureRecipientConnection
}

func newPooledNATSFailureRecipientSource(
	poolSize int,
	connect func(int) (failureRecipientConnection, error),
) *natsFailureRecipientSource {
	return &natsFailureRecipientSource{
		poolSize: poolSize, connect: connect, connections: make(map[int]failureRecipientConnection),
	}
}

func (s *natsFailureRecipientSource) Subscribe(
	subjectName string,
	recipient string,
	handler nats.MsgHandler,
) (failureRecipientSubscription, error) {
	if s == nil || s.connect == nil || s.poolSize <= 0 || recipient == "" {
		return nil, fmt.Errorf("recipient connection source is not configured")
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(recipient))
	connectionIndex := int(hasher.Sum32() % uint32(s.poolSize))
	s.mu.Lock()
	connection := s.connections[connectionIndex]
	if connection == nil {
		var err error
		connection, err = s.connect(connectionIndex)
		if err != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("connect recipient observer: %w", err)
		}
		if connection == nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("connect recipient observer: connector returned nil connection")
		}
		s.connections[connectionIndex] = connection
	}
	s.mu.Unlock()
	return connection.Subscribe(subjectName, handler)
}

func (s *natsFailureRecipientSource) Flush() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	indexes := make([]int, 0, len(s.connections))
	for index := range s.connections {
		indexes = append(indexes, index)
	}
	slices.Sort(indexes)
	connections := make([]failureRecipientConnection, 0, len(indexes))
	for _, index := range indexes {
		connections = append(connections, s.connections[index])
	}
	s.mu.Unlock()
	for _, connection := range connections {
		if err := connection.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (s *natsFailureRecipientSource) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	indexes := make([]int, 0, len(s.connections))
	for index := range s.connections {
		indexes = append(indexes, index)
	}
	slices.Sort(indexes)
	connections := make([]failureRecipientConnection, 0, len(indexes))
	for _, index := range indexes {
		connections = append(connections, s.connections[index])
	}
	clear(s.connections)
	s.mu.Unlock()
	var errs []error
	for _, connection := range connections {
		if err := connection.Drain(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type failureRecipientSubscriptions struct {
	subscriptions []failureRecipientSubscription
	source        failureRecipientSource
	closeOnce     sync.Once
	closeErr      error
}

func (s *failureRecipientSubscriptions) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if closer, ok := s.source.(interface{ Close() error }); ok {
			s.closeErr = closer.Close()
			return
		}
		var errs []error
		for _, subscription := range s.subscriptions {
			if err := subscription.Unsubscribe(); err != nil {
				errs = append(errs, err)
			}
		}
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

func shutdownFailureRecipientObserver(
	subscriptions *failureRecipientSubscriptions,
	stop func(),
	observer *failureRecipientObserver,
) error {
	var closeErr error
	if subscriptions != nil {
		closeErr = subscriptions.Close()
	}
	if stop != nil {
		stop()
	}
	if observer != nil {
		observer.Wait()
		observer.Drain()
		if err := observer.CloseEvidence(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	if closeErr != nil {
		return fmt.Errorf("drain recipient observer ingress: %w", closeErr)
	}
	return nil
}

func startFailureRecipientSubscriptions(
	source failureRecipientSource,
	topology *soakTopology,
	observer *failureRecipientObserver,
) (*failureRecipientSubscriptions, error) {
	if source == nil || topology == nil || observer == nil {
		return nil, fmt.Errorf("recipient subscription source, topology, and observer are required")
	}
	roomTypes := make(map[string]model.RoomType, len(topology.Rooms))
	for i := range topology.Rooms {
		roomTypes[topology.Rooms[i].ID] = topology.Rooms[i].Type
	}
	type target struct {
		subject   string
		recipient string
		route     recipientDeliveryRoute
	}
	targets := make([]target, 0, len(topology.Subscriptions)*3)
	seen := make(map[string]struct{}, len(topology.Subscriptions)*3)
	add := func(subjectName, recipient string, route recipientDeliveryRoute) {
		key := subjectName + "\x00" + recipient
		if _, duplicate := seen[key]; duplicate {
			return
		}
		seen[key] = struct{}{}
		targets = append(targets, target{subject: subjectName, recipient: recipient, route: route})
	}
	for i := range topology.Subscriptions {
		subscription := &topology.Subscriptions[i]
		account := subscription.User.Account
		if !isSoakRoomMember(subscription) || subscription.User.IsBot || account == "" {
			continue
		}
		add(subject.UserRoomEvent(account), account, recipientDeliveryRouteUser)
		if roomTypes[subscription.RoomID] == model.RoomTypeChannel {
			add(subject.RoomEvent(subscription.RoomID, true), account, recipientDeliveryRouteRoomGlobal)
			add(subject.RoomEvent(subscription.RoomID, false), account, recipientDeliveryRouteRoomLocal)
		}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("recipient observer has no eligible subscriptions")
	}
	slices.SortFunc(targets, func(a, b target) int {
		if a.subject != b.subject {
			return strings.Compare(a.subject, b.subject)
		}
		return strings.Compare(a.recipient, b.recipient)
	})
	result := &failureRecipientSubscriptions{source: source}
	for _, target := range targets {
		recipient := target.recipient
		route := target.route
		subscription, err := source.Subscribe(target.subject, recipient, func(message *nats.Msg) {
			observer.EnqueueRoute(recipient, route, message.Data)
		})
		if err != nil {
			_ = result.Close()
			observer.health.Set(false, observer.now().UTC(), "subscribe_error")
			return nil, fmt.Errorf("subscribe recipient observer: %w", err)
		}
		result.subscriptions = append(result.subscriptions, subscription)
	}
	if err := source.Flush(); err != nil {
		_ = result.Close()
		observer.health.Set(false, observer.now().UTC(), "flush_error")
		return nil, fmt.Errorf("flush recipient observer subscriptions: %w", err)
	}
	now := observer.now().UTC()
	observer.health.Set(true, now, "subscribed")
	if observer.metrics != nil {
		observer.metrics.FailureObserverUp.WithLabelValues(string(failureObserverRecipient)).Set(1)
	}
	return result, nil
}

type recipientEvidenceResult struct {
	Observation    failureObservation `json:"observation"`
	Missing        []string           `json:"missing,omitempty"`
	Unexpected     []string           `json:"unexpected,omitempty"`
	Duplicates     []string           `json:"duplicates,omitempty"`
	Mismatches     []string           `json:"mismatches,omitempty"`
	durableRecords map[string]map[string]struct{}
}

type recipientDeliveryRoute string

const (
	recipientDeliveryRouteUnknown    recipientDeliveryRoute = "unknown"
	recipientDeliveryRouteRoomGlobal recipientDeliveryRoute = "room_global"
	recipientDeliveryRouteRoomLocal  recipientDeliveryRoute = "room_local"
	recipientDeliveryRouteUser       recipientDeliveryRoute = "user"
)

type recipientExpectedRoute string

const (
	recipientExpectedRouteAny  recipientExpectedRoute = "any"
	recipientExpectedRouteRoom recipientExpectedRoute = "room"
	recipientExpectedRouteUser recipientExpectedRoute = "user"
)

type recipientSetSource string

const (
	recipientSetSourceLegacy          recipientSetSource = "legacy"
	recipientSetSourceTopology        recipientSetSource = "topology_subscriptions"
	recipientSetSourceThreadFollowers recipientSetSource = "catalog_thread_followers"
)

type recipientExpectationConfig struct {
	OperationID string
	Recipients  []string
	RoomID      string
	EventType   model.RoomEventType
	Route       recipientExpectedRoute
	Source      recipientSetSource
	Complete    bool
}

type recipientExpectation struct {
	registeredAt      time.Time
	expected          map[string]struct{}
	observed          map[string]map[recipientDeliveryRoute]int
	mismatches        map[string]struct{}
	roomID            string
	eventType         model.RoomEventType
	route             recipientExpectedRoute
	source            recipientSetSource
	complete          bool
	durableMissing    map[string]struct{}
	durableUnexpected map[string]struct{}
	durableDuplicates map[string]struct{}
	durableMismatches map[string]struct{}
}

type recipientEvidence struct {
	mu              sync.Mutex
	allowDuplicates bool
	operations      map[string]*recipientExpectation
	// now is injectable so expiry can be asserted without sleeping.
	now func() time.Time
}

func (r *recipientEvidence) clock() time.Time {
	if r.now == nil {
		return time.Now().UTC()
	}
	return r.now().UTC()
}

type recipientEvidenceDisposition string

const (
	recipientEvidenceUntracked  recipientEvidenceDisposition = "untracked"
	recipientEvidenceExpected   recipientEvidenceDisposition = "expected"
	recipientEvidenceDuplicate  recipientEvidenceDisposition = "duplicate"
	recipientEvidenceUnexpected recipientEvidenceDisposition = "unexpected"
	recipientEvidenceMismatch   recipientEvidenceDisposition = "mismatch"
)

func newRecipientEvidence(allowDuplicates bool) *recipientEvidence {
	return &recipientEvidence{
		allowDuplicates: allowDuplicates,
		operations:      make(map[string]*recipientExpectation),
		now:             func() time.Time { return time.Now().UTC() },
	}
}

func (r *recipientEvidence) Expect(operationID string, recipients []string) error {
	return r.ExpectDelivery(&recipientExpectationConfig{
		OperationID: operationID, Recipients: recipients,
		Route: recipientExpectedRouteAny, Source: recipientSetSourceLegacy, Complete: true,
	})
}

func (r *recipientEvidence) ExpectEvent(
	operationID string,
	recipients []string,
	roomID string,
	eventType model.RoomEventType,
) error {
	return r.ExpectDelivery(&recipientExpectationConfig{
		OperationID: operationID, Recipients: recipients, RoomID: roomID, EventType: eventType,
		Route: recipientExpectedRouteAny, Source: recipientSetSourceLegacy, Complete: true,
	})
}

func (r *recipientEvidence) ExpectDelivery(config *recipientExpectationConfig) error {
	if r == nil || config == nil || config.OperationID == "" || len(config.Recipients) == 0 {
		return fmt.Errorf("recipient expectation requires operation and recipients")
	}
	if (config.RoomID == "") != (config.EventType == "") {
		return fmt.Errorf("recipient expectation room and event type must be provided together")
	}
	if config.Route == "" {
		config.Route = recipientExpectedRouteAny
	}
	if config.Source == "" {
		config.Source = recipientSetSourceLegacy
	}
	if config.Route != recipientExpectedRouteAny && config.Route != recipientExpectedRouteRoom && config.Route != recipientExpectedRouteUser {
		return fmt.Errorf("recipient expectation has unknown route %q", config.Route)
	}
	if config.Source != recipientSetSourceLegacy && config.Source != recipientSetSourceTopology && config.Source != recipientSetSourceThreadFollowers {
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
	r.operations[config.OperationID] = &recipientExpectation{
		registeredAt: r.clock(),
		expected:     expected, observed: make(map[string]map[recipientDeliveryRoute]int), mismatches: make(map[string]struct{}),
		roomID: config.RoomID, eventType: config.EventType,
		route: config.Route, source: config.Source, complete: config.Complete,
		durableMissing: make(map[string]struct{}), durableUnexpected: make(map[string]struct{}),
		durableDuplicates: make(map[string]struct{}),
		durableMismatches: make(map[string]struct{}),
	}
	return nil
}

func (r *recipientEvidence) Observe(operationID, recipient string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return false
	}
	incrementRecipientRoute(expectation, recipient, recipientDeliveryRouteUnknown)
	return true
}

func (r *recipientEvidence) ObserveEvent(
	operationID,
	recipient,
	roomID string,
	eventType model.RoomEventType,
) bool {
	disposition := r.ObserveDelivery(operationID, recipient, roomID, eventType, recipientDeliveryRouteUnknown)
	return disposition != recipientEvidenceUntracked
}

func (r *recipientEvidence) ObserveDelivery(
	operationID,
	recipient,
	roomID string,
	eventType model.RoomEventType,
	route recipientDeliveryRoute,
) recipientEvidenceDisposition {
	if r == nil {
		return recipientEvidenceUntracked
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return recipientEvidenceUntracked
	}
	disposition := recipientEvidenceExpected
	if expectation.roomID != "" &&
		(expectation.roomID != roomID || expectation.eventType != eventType) {
		disposition = recipientEvidenceMismatch
	}
	if !recipientRouteMatches(expectation.route, route) {
		disposition = recipientEvidenceMismatch
	}
	if disposition == recipientEvidenceExpected && expectation.complete {
		if _, expected := expectation.expected[recipient]; !expected {
			disposition = recipientEvidenceUnexpected
		}
	}
	if disposition == recipientEvidenceMismatch {
		expectation.mismatches[recipient] = struct{}{}
		return disposition
	}
	if disposition == recipientEvidenceUnexpected {
		incrementRecipientRoute(expectation, recipient, route)
		return disposition
	}
	if recipientRouteIsDuplicate(expectation, recipient, route) {
		disposition = recipientEvidenceDuplicate
	}
	incrementRecipientRoute(expectation, recipient, route)
	return disposition
}

func recipientRouteMatches(expected recipientExpectedRoute, actual recipientDeliveryRoute) bool {
	switch expected {
	case recipientExpectedRouteAny:
		return true
	case recipientExpectedRouteRoom:
		return actual == recipientDeliveryRouteRoomGlobal || actual == recipientDeliveryRouteRoomLocal
	case recipientExpectedRouteUser:
		return actual == recipientDeliveryRouteUser
	default:
		return false
	}
}

func recipientRouteIsDuplicate(expectation *recipientExpectation, recipient string, route recipientDeliveryRoute) bool {
	routes := expectation.observed[recipient]
	if routes == nil {
		return false
	}
	return routes[route] > 0
}

func incrementRecipientRoute(expectation *recipientExpectation, recipient string, route recipientDeliveryRoute) {
	if expectation.observed[recipient] == nil {
		expectation.observed[recipient] = make(map[recipientDeliveryRoute]int)
	}
	expectation.observed[recipient][route]++
}

func (r *recipientEvidence) ObserveMismatch(operationID, recipient string) bool {
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

func (r *recipientEvidence) ReplayPositive(kind, operationID, recipient string) bool {
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

func (r *recipientEvidence) Forget(operationID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.operations, operationID)
}

// ExpireBefore releases every expectation registered at or before cutoff and
// returns how many went. Callers pass now minus the operation deadline, so an
// expectation cannot outlive the operation it describes.
//
// It exists because the ledger's own expiry finalizes an operation and forgets
// it without telling the evidence, while Finalize — the only other way out of
// this map — runs inside the reconciler. Anything the reconciler never reaches
// was therefore retained for the life of the process, and each entry holds one
// route map per recipient, which made this the largest object graph in the run.
//
// A recovered expectation is registered when the restart replays it rather than
// when its operation started, so it can outlive that operation by up to one
// deadline. That over-retention is bounded and costs one deadline of entries
// once per restart; treating a replayed entry as already aged would expire
// evidence the run can still resolve.
//
// It is deliberately not driven from the ledger's finalize path. That would put
// the evidence lock underneath the ledger lock, while the observer already
// takes them the other way round; a sweep that holds neither cannot deadlock
// however those paths change.
func (r *recipientEvidence) ExpireBefore(cutoff time.Time) int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expired := 0
	for operationID, expectation := range r.operations {
		if expectation.registeredAt.IsZero() || expectation.registeredAt.After(cutoff) {
			continue
		}
		delete(r.operations, operationID)
		expired++
	}
	return expired
}

// Len reports how many expectations are retained. The run needs this as a gauge:
// a count that tracks the ledger's own in-flight number is healthy, one that
// climbs past it is this map leaking again.
func (r *recipientEvidence) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.operations)
}

func (r *recipientEvidence) Complete(operationID string) bool {
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

func (r *recipientEvidence) Finalize(operationID string, observerHealthy bool) recipientEvidenceResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return recipientEvidenceResult{Observation: failureObservationUnverified}
	}
	result := recipientEvidenceResult{
		Observation:    failureObservationGood,
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
			result.Observation = failureObservationBad
		} else if len(result.Missing) > 0 {
			result.Observation = failureObservationMissingAfterDeadline
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
		result.Observation = failureObservationBad
	case !observerHealthy || !expectation.complete:
		result.Observation = failureObservationUnverified
	case len(result.Missing) > 0:
		result.Observation = failureObservationMissingAfterDeadline
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

func markRecipientEvidenceDurable(result *recipientEvidenceResult, kind, recipient string) {
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

type recipientDelivery struct {
	recipient string
	route     recipientDeliveryRoute
	payload   []byte
}

type failureRecipientObserver struct {
	ledger        *failureLedger
	metrics       *Metrics
	evidence      *recipientEvidence
	health        *failureObserverHealth
	queue         chan recipientDelivery
	now           func() time.Time
	wg            sync.WaitGroup
	evidenceDir   string
	syncDirectory func(string) error
	journal       failureRecipientEvidenceJournal
}

type failureRecipientEvidenceJournal interface {
	AppendBatch([]failureRecipientEvidenceRecord) error
	Close() error
}

type failureRecipientEvidenceRecord struct {
	Kind        string `json:"-"`
	OperationID string `json:"operationId"`
	Recipient   string `json:"recipient,omitempty"`
}

type fileFailureRecipientEvidenceJournal struct {
	mu            sync.Mutex
	directory     string
	syncDirectory func(string) error
}

type failureRecipientObserverOption func(*failureRecipientObserver)

func withFailureRecipientEvidenceDir(directory string) failureRecipientObserverOption {
	return func(observer *failureRecipientObserver) { observer.evidenceDir = directory }
}

func withFailureRecipientDuplicatePolicy(allow bool) failureRecipientObserverOption {
	return func(observer *failureRecipientObserver) {
		observer.evidence.allowDuplicates = allow
	}
}

func withFailureRecipientDirectorySync(syncDirectory func(string) error) failureRecipientObserverOption {
	return func(observer *failureRecipientObserver) { observer.syncDirectory = syncDirectory }
}

func withFailureRecipientEvidenceJournal(journal failureRecipientEvidenceJournal) failureRecipientObserverOption {
	return func(observer *failureRecipientObserver) { observer.journal = journal }
}

func newFailureRecipientObserver(
	ledger *failureLedger,
	metrics *Metrics,
	capacity int,
	now func() time.Time,
	options ...failureRecipientObserverOption,
) *failureRecipientObserver {
	if capacity <= 0 {
		capacity = 1
	}
	if now == nil {
		now = time.Now
	}
	startedAt := now().UTC()
	observer := &failureRecipientObserver{
		ledger: ledger, metrics: metrics, evidence: newRecipientEvidence(false),
		health: newFailureObserverHealth(failureObserverRecipient, startedAt),
		queue:  make(chan recipientDelivery, capacity), now: now,
		syncDirectory: syncFailureWALDirectory,
	}
	for _, option := range options {
		option(observer)
	}
	if observer.journal == nil && observer.evidenceDir != "" {
		observer.journal = &fileFailureRecipientEvidenceJournal{
			directory: observer.evidenceDir, syncDirectory: observer.syncDirectory,
		}
	}
	if metrics != nil {
		metrics.FailureObserverUp.WithLabelValues(string(failureObserverRecipient)).Set(0)
		metrics.FailureObserverQueueDepth.WithLabelValues(string(failureObserverRecipient)).Set(0)
	}
	return observer
}

func (o *failureRecipientObserver) Expect(operationID string, recipients []string) error {
	if o == nil {
		return fmt.Errorf("recipient observer is required")
	}
	return o.evidence.Expect(operationID, recipients)
}

func (o *failureRecipientObserver) ExpectEvent(
	operationID string,
	recipients []string,
	roomID string,
	eventType model.RoomEventType,
) error {
	if o == nil {
		return fmt.Errorf("recipient observer is required")
	}
	return o.evidence.ExpectEvent(operationID, recipients, roomID, eventType)
}

func (o *failureRecipientObserver) ExpectDelivery(config *recipientExpectationConfig) error {
	if o == nil {
		return fmt.Errorf("recipient observer is required")
	}
	return o.evidence.ExpectDelivery(config)
}

func (o *failureRecipientObserver) Recover(operations []failureOperation) error {
	if o == nil {
		return fmt.Errorf("recipient observer is required")
	}
	for i := range operations {
		operation := &operations[i]
		if !slices.Contains(operation.Expected, failureObserverRecipient) {
			continue
		}
		if _, observed := operation.Observations[failureObserverRecipient]; observed {
			continue
		}
		recipients := strings.FieldsFunc(operation.Attributes["expected_recipients"], func(r rune) bool {
			return r == '\n'
		})
		eventType := model.RoomEventType(operation.Attributes["expected_recipient_event"])
		var err error
		source := recipientSetSource(operation.Attributes[soakFailureAttributeRecipientSource])
		route := recipientExpectedRoute(operation.Attributes[soakFailureAttributeRecipientRoute])
		complete := operation.Attributes[soakFailureAttributeRecipientComplete] == "true"
		if source == "" && route == "" && eventType == "" {
			// Operations written before target-aware recipient correlation retain
			// their recipient list, but cannot make authoritative exact-set claims.
			err = o.ExpectDelivery(&recipientExpectationConfig{
				OperationID: operation.ID, Recipients: recipients,
				Route: recipientExpectedRouteAny, Source: recipientSetSourceLegacy, Complete: false,
			})
		} else {
			err = o.ExpectDelivery(&recipientExpectationConfig{
				OperationID: operation.ID, Recipients: recipients,
				RoomID: operation.Targets["roomId"], EventType: eventType,
				Route: route, Source: source, Complete: complete,
			})
		}
		if err != nil {
			return fmt.Errorf("recover recipient expectation %q: %w", operation.ID, err)
		}
	}
	return o.replayObservedEvidence()
}

func (o *failureRecipientObserver) replayObservedEvidence() error {
	if o.evidenceDir == "" {
		return nil
	}
	for _, kind := range []string{"missing", "unexpected", "duplicate", "mismatch"} {
		path := filepath.Join(o.evidenceDir, ".recipient-"+kind+".raw.jsonl")
		file, err := os.Open(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("open %s recipient evidence: %w", kind, err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var evidence failureRecipientRawEvidence
			if err := json.Unmarshal(scanner.Bytes(), &evidence); err != nil {
				_ = file.Close()
				return fmt.Errorf("decode %s recipient evidence: %w", kind, err)
			}
			o.evidence.ReplayPositive(kind, evidence.OperationID, evidence.Recipient)
		}
		if err := scanner.Err(); err != nil {
			_ = file.Close()
			return fmt.Errorf("scan %s recipient evidence: %w", kind, err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close %s recipient evidence: %w", kind, err)
		}
	}
	return nil
}

func (o *failureRecipientObserver) Enqueue(recipient string, payload []byte) bool {
	return o.EnqueueRoute(recipient, recipientDeliveryRouteUnknown, payload)
}

func (o *failureRecipientObserver) EnqueueRoute(
	recipient string,
	route recipientDeliveryRoute,
	payload []byte,
) bool {
	if o == nil {
		return false
	}
	delivery := recipientDelivery{recipient: recipient, route: route, payload: append([]byte(nil), payload...)}
	select {
	case o.queue <- delivery:
		if o.metrics != nil {
			o.metrics.FailureObserverQueueDepth.WithLabelValues(string(failureObserverRecipient)).Set(float64(len(o.queue)))
		}
		return true
	default:
		now := o.now().UTC()
		o.health.Set(false, now, "queue_overflow")
		if o.metrics != nil {
			o.metrics.FailureObserverUp.WithLabelValues(string(failureObserverRecipient)).Set(0)
			o.metrics.FailureObserverEvents.WithLabelValues(string(failureObserverRecipient), string(failureObservationUnverified)).Inc()
		}
		o.ledger.Invalidate("observer_queue")
		return false
	}
}

func (o *failureRecipientObserver) Run(ctx context.Context) {
	if o == nil {
		return
	}
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case delivery := <-o.queue:
				o.process(delivery)
				if o.metrics != nil {
					o.metrics.FailureObserverQueueDepth.WithLabelValues(string(failureObserverRecipient)).Set(float64(len(o.queue)))
				}
			}
		}
	}()
}

func (o *failureRecipientObserver) Wait() {
	if o != nil {
		o.wg.Wait()
	}
}

func (o *failureRecipientObserver) Drain() {
	if o == nil {
		return
	}
	for {
		select {
		case delivery := <-o.queue:
			o.process(delivery)
		default:
			if o.metrics != nil {
				o.metrics.FailureObserverQueueDepth.WithLabelValues(string(failureObserverRecipient)).Set(0)
			}
			return
		}
	}
}

func (o *failureRecipientObserver) process(delivery recipientDelivery) {
	var event model.RoomEvent
	if err := json.Unmarshal(delivery.payload, &event); err != nil {
		if o.metrics != nil {
			o.metrics.FailureObserverEvents.WithLabelValues(string(failureObserverRecipient), string(failureObservationBad)).Inc()
		}
		if o.ledger != nil {
			o.ledger.Invalidate("observer_malformed")
		}
		return
	}
	if event.Type != model.RoomEventNewMessage && event.Type != model.RoomEventNewThreadMessage {
		return
	}
	if event.LastMsgID == "" || event.RoomID == "" {
		if o.metrics != nil {
			o.metrics.FailureObserverEvents.WithLabelValues(string(failureObserverRecipient), string(failureObservationBad)).Inc()
		}
		if o.ledger != nil {
			o.ledger.Invalidate("observer_malformed")
		}
		return
	}
	disposition := o.evidence.ObserveDelivery(
		event.LastMsgID,
		delivery.recipient,
		event.RoomID,
		event.Type,
		delivery.route,
	)
	if o.journal != nil && (disposition == recipientEvidenceMismatch || disposition == recipientEvidenceUnexpected || disposition == recipientEvidenceDuplicate) {
		kind := string(disposition)
		record := failureRecipientEvidenceRecord{
			Kind: kind, OperationID: event.LastMsgID, Recipient: delivery.recipient,
		}
		if err := o.persistEvidenceRecords([]failureRecipientEvidenceRecord{record}, "positive"); err != nil {
			o.markSidecarFailure()
			return
		}
		o.evidence.ReplayPositive(kind, event.LastMsgID, delivery.recipient)
	}
	if disposition == recipientEvidenceUntracked {
		if o.metrics != nil {
			o.metrics.FailureUntracked.WithLabelValues(failureUntrackedReasonObserve).Inc()
		}
		return
	}
}

func (o *failureRecipientObserver) Finalize(operationID string, startedAt, deadline time.Time) recipientEvidenceResult {
	healthy := o.health.HealthyThroughout(startedAt, deadline)
	result := o.evidence.Finalize(operationID, healthy)
	if err := o.persistResult(operationID, &result); err != nil {
		o.markSidecarFailure()
		if result.Observation == failureObservationBad || result.Observation == failureObservationMissingAfterDeadline {
			result.Observation = failureObservationUnverified
		}
	}
	if o.metrics != nil {
		o.metrics.FailureObserverEvents.WithLabelValues(string(failureObserverRecipient), string(result.Observation)).Inc()
	}
	return result
}

func (o *failureRecipientObserver) markSidecarFailure() {
	now := o.now().UTC()
	o.health.Set(false, now, "sidecar_failure")
	if o.metrics != nil {
		o.metrics.FailureObserverUp.WithLabelValues(string(failureObserverRecipient)).Set(0)
	}
	if o.ledger != nil {
		o.ledger.Invalidate("sidecar")
	}
}

type failureRecipientRawEvidence = failureRecipientEvidenceRecord

func (o *failureRecipientObserver) persistResult(
	operationID string,
	result *recipientEvidenceResult,
) error {
	type evidenceItems struct {
		kind       string
		recipients []string
	}
	items := make([]evidenceItems, 0, 4)
	if result.Observation == failureObservationMissingAfterDeadline {
		items = append(items, evidenceItems{kind: "missing", recipients: result.Missing})
	}
	items = append(items,
		evidenceItems{kind: "unexpected", recipients: result.Unexpected},
		evidenceItems{kind: "duplicate", recipients: result.Duplicates},
		evidenceItems{kind: "mismatch", recipients: result.Mismatches},
	)
	records := make([]failureRecipientEvidenceRecord, 0)
	for _, item := range items {
		for _, recipient := range item.recipients {
			if _, durable := result.durableRecords[item.kind][recipient]; durable {
				continue
			}
			records = append(records, failureRecipientEvidenceRecord{
				Kind: item.kind, OperationID: operationID, Recipient: recipient,
			})
		}
	}
	if result.Observation == failureObservationUnverified {
		records = append(records, failureRecipientEvidenceRecord{Kind: "unverified", OperationID: operationID})
	}
	if len(records) == 0 || o.journal == nil {
		return nil
	}
	claim := "unverified"
	if result.Observation == failureObservationBad || result.Observation == failureObservationMissingAfterDeadline {
		claim = "positive"
	}
	return o.persistEvidenceRecords(records, claim)
}

func (o *failureRecipientObserver) persistEvidenceRecords(
	records []failureRecipientEvidenceRecord,
	claim string,
) error {
	if len(records) == 0 || o.journal == nil {
		return nil
	}
	startedAt := time.Now()
	err := o.journal.AppendBatch(records)
	if o.metrics != nil {
		flushResult := "success"
		if err != nil {
			flushResult = "error"
		}
		o.metrics.FailureEvidenceFlushDuration.WithLabelValues(claim, flushResult).Observe(time.Since(startedAt).Seconds())
		if err == nil {
			for _, record := range records {
				o.metrics.FailureEvidenceRecords.WithLabelValues(record.Kind).Inc()
			}
		}
	}
	return err
}

func (o *failureRecipientObserver) appendRawEvidence(kind, operationID, recipient string) error {
	if o == nil || o.evidenceDir == "" {
		return nil
	}
	if !knownRecipientEvidenceKind(kind) {
		return fmt.Errorf("unknown recipient evidence kind %q", kind)
	}
	if o.journal == nil {
		return nil
	}
	return o.journal.AppendBatch([]failureRecipientEvidenceRecord{{
		Kind: kind, OperationID: operationID, Recipient: recipient,
	}})
}

func (o *failureRecipientObserver) CloseEvidence() error {
	if o == nil || o.journal == nil {
		return nil
	}
	if err := o.journal.Close(); err != nil {
		return fmt.Errorf("close recipient evidence journal: %w", err)
	}
	return nil
}

func (j *fileFailureRecipientEvidenceJournal) AppendBatch(records []failureRecipientEvidenceRecord) error {
	if j == nil || len(records) == 0 {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := os.MkdirAll(j.directory, 0o750); err != nil {
		return fmt.Errorf("create recipient evidence directory: %w", err)
	}
	grouped := make(map[string][]failureRecipientEvidenceRecord)
	for _, record := range records {
		if !knownRecipientEvidenceKind(record.Kind) {
			return fmt.Errorf("unknown recipient evidence kind %q", record.Kind)
		}
		grouped[record.Kind] = append(grouped[record.Kind], record)
	}
	kinds := make([]string, 0, len(grouped))
	for kind := range grouped {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	createdAny := false
	for _, kind := range kinds {
		path := filepath.Join(j.directory, ".recipient-"+kind+".raw.jsonl")
		created := false
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			created = true
		} else if err != nil {
			return fmt.Errorf("stat recipient evidence journal: %w", err)
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return fmt.Errorf("open recipient evidence journal: %w", err)
		}
		for _, record := range grouped[kind] {
			encoded, encodeErr := json.Marshal(record)
			if encodeErr != nil {
				_ = file.Close()
				return fmt.Errorf("encode recipient evidence: %w", encodeErr)
			}
			if _, writeErr := file.Write(append(encoded, '\n')); writeErr != nil {
				_ = file.Close()
				return fmt.Errorf("append recipient evidence: %w", writeErr)
			}
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync recipient evidence: %w", err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close recipient evidence: %w", err)
		}
		createdAny = createdAny || created
	}
	if createdAny && j.syncDirectory != nil {
		if err := j.syncDirectory(j.directory); err != nil {
			return fmt.Errorf("sync recipient evidence directory: %w", err)
		}
	}
	return nil
}

func (*fileFailureRecipientEvidenceJournal) Close() error { return nil }

func knownRecipientEvidenceKind(kind string) bool {
	_, known := map[string]struct{}{
		"missing": {}, "unexpected": {}, "duplicate": {}, "mismatch": {},
		"unverified": {}, "untracked": {}, "observed": {},
	}[kind]
	return known
}
