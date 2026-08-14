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
	nc *nats.Conn
}

func (c *natsFailureRecipientConnection) Subscribe(
	subjectName string,
	handler nats.MsgHandler,
) (failureRecipientSubscription, error) {
	return c.nc.Subscribe(subjectName, handler)
}

func (c *natsFailureRecipientConnection) Flush() error { return c.nc.Flush() }
func (c *natsFailureRecipientConnection) Drain() error { return c.nc.Drain() }

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
	}
	targets := make([]target, 0, len(topology.Subscriptions)*3)
	seen := make(map[string]struct{}, len(topology.Subscriptions)*3)
	add := func(subjectName, recipient string) {
		key := subjectName + "\x00" + recipient
		if _, duplicate := seen[key]; duplicate {
			return
		}
		seen[key] = struct{}{}
		targets = append(targets, target{subject: subjectName, recipient: recipient})
	}
	for i := range topology.Subscriptions {
		subscription := &topology.Subscriptions[i]
		account := subscription.User.Account
		if !subscription.IsSubscribed || subscription.User.IsBot || account == "" {
			continue
		}
		add(subject.UserRoomEvent(account), account)
		if roomTypes[subscription.RoomID] == model.RoomTypeChannel {
			add(subject.RoomEvent(subscription.RoomID, true), account)
			add(subject.RoomEvent(subscription.RoomID, false), account)
		}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("recipient observer has no eligible subscriptions")
	}
	slices.SortFunc(targets, func(a, b target) int {
		if a.subject != b.subject {
			return stringsCompare(a.subject, b.subject)
		}
		return stringsCompare(a.recipient, b.recipient)
	})
	result := &failureRecipientSubscriptions{source: source}
	for _, target := range targets {
		recipient := target.recipient
		subscription, err := source.Subscribe(target.subject, recipient, func(message *nats.Msg) {
			observer.Enqueue(recipient, message.Data)
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

func stringsCompare(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

type recipientEvidenceResult struct {
	Observation failureObservation `json:"observation"`
	Missing     []string           `json:"missing,omitempty"`
	Unexpected  []string           `json:"unexpected,omitempty"`
	Duplicates  []string           `json:"duplicates,omitempty"`
	Mismatches  []string           `json:"mismatches,omitempty"`
}

type recipientExpectation struct {
	expected   map[string]struct{}
	observed   map[string]int
	mismatches map[string]struct{}
	roomID     string
	eventType  model.RoomEventType
}

type recipientEvidence struct {
	mu              sync.Mutex
	allowDuplicates bool
	operations      map[string]*recipientExpectation
}

type recipientEvidenceDisposition string

const (
	recipientEvidenceUntracked recipientEvidenceDisposition = "untracked"
	recipientEvidenceObserved  recipientEvidenceDisposition = "observed"
	recipientEvidenceMismatch  recipientEvidenceDisposition = "mismatch"
)

func newRecipientEvidence(allowDuplicates bool) *recipientEvidence {
	return &recipientEvidence{allowDuplicates: allowDuplicates, operations: make(map[string]*recipientExpectation)}
}

func (r *recipientEvidence) Expect(operationID string, recipients []string) error {
	return r.ExpectEvent(operationID, recipients, "", "")
}

func (r *recipientEvidence) ExpectEvent(
	operationID string,
	recipients []string,
	roomID string,
	eventType model.RoomEventType,
) error {
	if r == nil || operationID == "" || len(recipients) == 0 {
		return fmt.Errorf("recipient expectation requires operation and recipients")
	}
	if (roomID == "") != (eventType == "") {
		return fmt.Errorf("recipient expectation room and event type must be provided together")
	}
	expected := make(map[string]struct{}, len(recipients))
	for _, recipient := range recipients {
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
	if _, exists := r.operations[operationID]; exists {
		return fmt.Errorf("recipient expectation %q already exists", operationID)
	}
	r.operations[operationID] = &recipientExpectation{
		expected: expected, observed: make(map[string]int), mismatches: make(map[string]struct{}),
		roomID: roomID, eventType: eventType,
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
	expectation.observed[recipient]++
	return true
}

func (r *recipientEvidence) ObserveEvent(
	operationID,
	recipient,
	roomID string,
	eventType model.RoomEventType,
) bool {
	disposition, err := r.observeEventDurably(operationID, recipient, roomID, eventType, nil)
	return err == nil && disposition != recipientEvidenceUntracked
}

func (r *recipientEvidence) observeEventDurably(
	operationID,
	recipient,
	roomID string,
	eventType model.RoomEventType,
	persist func(recipientEvidenceDisposition) error,
) (recipientEvidenceDisposition, error) {
	if r == nil {
		return recipientEvidenceUntracked, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return recipientEvidenceUntracked, nil
	}
	disposition := recipientEvidenceObserved
	if expectation.roomID != "" &&
		(expectation.roomID != roomID || expectation.eventType != eventType) {
		disposition = recipientEvidenceMismatch
	}
	if persist != nil {
		if err := persist(disposition); err != nil {
			return disposition, err
		}
	}
	if disposition == recipientEvidenceMismatch {
		expectation.mismatches[recipient] = struct{}{}
		return disposition, nil
	}
	expectation.observed[recipient]++
	return disposition, nil
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

func (r *recipientEvidence) Forget(operationID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.operations, operationID)
}

func (r *recipientEvidence) Complete(operationID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	expectation := r.operations[operationID]
	if expectation == nil {
		return false
	}
	for recipient := range expectation.expected {
		if expectation.observed[recipient] == 0 {
			return false
		}
	}
	for recipient, count := range expectation.observed {
		if _, expected := expectation.expected[recipient]; !expected || (!r.allowDuplicates && count > 1) {
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
	result := recipientEvidenceResult{Observation: failureObservationGood}
	for recipient := range expectation.expected {
		if expectation.observed[recipient] == 0 {
			result.Missing = append(result.Missing, recipient)
		}
	}
	for recipient, count := range expectation.observed {
		if _, expected := expectation.expected[recipient]; !expected {
			result.Unexpected = append(result.Unexpected, recipient)
		}
		if count > 1 {
			result.Duplicates = append(result.Duplicates, recipient)
		}
	}
	for recipient := range expectation.mismatches {
		result.Mismatches = append(result.Mismatches, recipient)
	}
	slices.Sort(result.Missing)
	slices.Sort(result.Unexpected)
	slices.Sort(result.Duplicates)
	slices.Sort(result.Mismatches)
	switch {
	case !observerHealthy:
		result.Observation = failureObservationUnverified
	case len(result.Mismatches) > 0 || len(result.Unexpected) > 0 || (!r.allowDuplicates && len(result.Duplicates) > 0):
		result.Observation = failureObservationBad
	case len(result.Missing) > 0:
		result.Observation = failureObservationMissingAfterDeadline
	}
	delete(r.operations, operationID)
	return result
}

type recipientDelivery struct {
	recipient string
	payload   []byte
}

type failureRecipientObserver struct {
	ledger      *failureLedger
	metrics     *Metrics
	evidence    *recipientEvidence
	health      *failureObserverHealth
	queue       chan recipientDelivery
	now         func() time.Time
	wg          sync.WaitGroup
	sidecarMu   sync.Mutex
	evidenceDir string
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
	}
	for _, option := range options {
		option(observer)
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
		if eventType == "" {
			// Operations written before target-aware recipient correlation retain
			// their original message-ID and recipient-set replay semantics.
			err = o.Expect(operation.ID, recipients)
		} else {
			err = o.ExpectEvent(operation.ID, recipients, operation.Targets["roomId"], eventType)
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
	for _, kind := range []recipientEvidenceDisposition{recipientEvidenceObserved, recipientEvidenceMismatch} {
		path := filepath.Join(o.evidenceDir, ".recipient-"+string(kind)+".raw.jsonl")
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
			if kind == recipientEvidenceMismatch {
				o.evidence.ObserveMismatch(evidence.OperationID, evidence.Recipient)
			} else {
				o.evidence.Observe(evidence.OperationID, evidence.Recipient)
			}
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
	if o == nil {
		return false
	}
	delivery := recipientDelivery{recipient: recipient, payload: append([]byte(nil), payload...)}
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
	disposition, evidenceErr := o.evidence.observeEventDurably(
		event.LastMsgID,
		delivery.recipient,
		event.RoomID,
		event.Type,
		func(kind recipientEvidenceDisposition) error {
			return o.appendRawEvidence(string(kind), event.LastMsgID, delivery.recipient)
		},
	)
	if evidenceErr != nil {
		o.health.Set(false, o.now().UTC(), "sidecar_failure")
		if o.ledger != nil {
			o.ledger.Invalidate("sidecar")
		}
		return
	}
	if disposition == recipientEvidenceUntracked {
		if err := o.appendRawEvidence("untracked", event.LastMsgID, delivery.recipient); err != nil {
			if o.ledger != nil {
				o.ledger.Invalidate("sidecar")
			}
		}
		return
	}
}

func (o *failureRecipientObserver) Finalize(operationID string, startedAt, deadline time.Time) recipientEvidenceResult {
	healthy := o.health.HealthyThroughout(startedAt, deadline)
	result := o.evidence.Finalize(operationID, healthy)
	if err := o.persistResult(operationID, &result); err != nil {
		o.ledger.Invalidate("sidecar")
	}
	if o.metrics != nil {
		o.metrics.FailureObserverEvents.WithLabelValues(string(failureObserverRecipient), string(result.Observation)).Inc()
	}
	return result
}

type failureRecipientRawEvidence struct {
	OperationID string `json:"operationId"`
	Recipient   string `json:"recipient,omitempty"`
}

func (o *failureRecipientObserver) persistResult(
	operationID string,
	result *recipientEvidenceResult,
) error {
	for _, item := range []struct {
		kind       string
		recipients []string
	}{
		{kind: "missing", recipients: result.Missing},
		{kind: "unexpected", recipients: result.Unexpected},
		{kind: "duplicate", recipients: result.Duplicates},
		{kind: "mismatch", recipients: result.Mismatches},
	} {
		for _, recipient := range item.recipients {
			if err := o.appendRawEvidence(item.kind, operationID, recipient); err != nil {
				return err
			}
		}
	}
	if result.Observation == failureObservationUnverified {
		if err := o.appendRawEvidence("unverified", operationID, ""); err != nil {
			return err
		}
	}
	return nil
}

func (o *failureRecipientObserver) appendRawEvidence(kind, operationID, recipient string) error {
	if o == nil || o.evidenceDir == "" {
		return nil
	}
	if _, known := map[string]struct{}{
		"missing": {}, "unexpected": {}, "duplicate": {}, "mismatch": {}, "unverified": {}, "untracked": {}, "observed": {},
	}[kind]; !known {
		return fmt.Errorf("unknown recipient evidence kind %q", kind)
	}
	o.sidecarMu.Lock()
	defer o.sidecarMu.Unlock()
	if err := os.MkdirAll(o.evidenceDir, 0o750); err != nil {
		return fmt.Errorf("create recipient evidence directory: %w", err)
	}
	path := filepath.Join(o.evidenceDir, ".recipient-"+kind+".raw.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open recipient evidence journal: %w", err)
	}
	encoded, err := json.Marshal(failureRecipientRawEvidence{OperationID: operationID, Recipient: recipient})
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("encode recipient evidence: %w", err)
	}
	encoded = append(encoded, '\n')
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return fmt.Errorf("append recipient evidence: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync recipient evidence: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close recipient evidence: %w", err)
	}
	return nil
}
