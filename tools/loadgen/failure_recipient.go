package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type natsFailureRecipientSource struct {
	nc *nats.Conn
}

func (s *natsFailureRecipientSource) Subscribe(
	subjectName string,
	_ string,
	handler nats.MsgHandler,
) (failureRecipientSubscription, error) {
	return s.nc.Subscribe(subjectName, handler)
}

func (s *natsFailureRecipientSource) Flush() error {
	return s.nc.Flush()
}

type failureRecipientSubscriptions struct {
	subscriptions []failureRecipientSubscription
}

func (s *failureRecipientSubscriptions) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	for _, subscription := range s.subscriptions {
		if err := subscription.Unsubscribe(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
	result := &failureRecipientSubscriptions{}
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
}

type recipientExpectation struct {
	expected map[string]struct{}
	observed map[string]int
}

type recipientEvidence struct {
	mu              sync.Mutex
	allowDuplicates bool
	operations      map[string]*recipientExpectation
}

func newRecipientEvidence(allowDuplicates bool) *recipientEvidence {
	return &recipientEvidence{allowDuplicates: allowDuplicates, operations: make(map[string]*recipientExpectation)}
}

func (r *recipientEvidence) Expect(operationID string, recipients []string) error {
	if r == nil || operationID == "" || len(recipients) == 0 {
		return fmt.Errorf("recipient expectation requires operation and recipients")
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
	r.operations[operationID] = &recipientExpectation{expected: expected, observed: make(map[string]int)}
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
	return true
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
	slices.Sort(result.Missing)
	slices.Sort(result.Unexpected)
	slices.Sort(result.Duplicates)
	switch {
	case !observerHealthy:
		result.Observation = failureObservationUnverified
	case len(result.Unexpected) > 0 || (!r.allowDuplicates && len(result.Duplicates) > 0):
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
		if err := o.Expect(operation.ID, recipients); err != nil {
			return fmt.Errorf("recover recipient expectation %q: %w", operation.ID, err)
		}
	}
	return o.replayObservedEvidence()
}

func (o *failureRecipientObserver) replayObservedEvidence() error {
	if o.evidenceDir == "" {
		return nil
	}
	path := filepath.Join(o.evidenceDir, ".recipient-observed.raw.jsonl")
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open observed recipient evidence: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var evidence failureRecipientRawEvidence
		if err := json.Unmarshal(scanner.Bytes(), &evidence); err != nil {
			return fmt.Errorf("decode observed recipient evidence: %w", err)
		}
		o.evidence.Observe(evidence.OperationID, evidence.Recipient)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan observed recipient evidence: %w", err)
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

func (o *failureRecipientObserver) process(delivery recipientDelivery) {
	var event model.RoomEvent
	if err := json.Unmarshal(delivery.payload, &event); err != nil || event.LastMsgID == "" || (event.Type != model.RoomEventNewMessage && event.Type != model.RoomEventNewThreadMessage) {
		if o.metrics != nil {
			o.metrics.FailureObserverEvents.WithLabelValues(string(failureObserverRecipient), string(failureObservationBad)).Inc()
		}
		o.ledger.Invalidate("observer_malformed")
		return
	}
	if !o.evidence.Observe(event.LastMsgID, delivery.recipient) {
		if err := o.appendRawEvidence("untracked", event.LastMsgID, delivery.recipient); err != nil {
			o.ledger.Invalidate("sidecar")
		}
		return
	}
	if err := o.appendRawEvidence("observed", event.LastMsgID, delivery.recipient); err != nil {
		o.health.Set(false, o.now().UTC(), "sidecar_failure")
		if o.ledger != nil {
			o.ledger.Invalidate("sidecar")
		}
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
		"missing": {}, "unexpected": {}, "duplicate": {}, "unverified": {}, "untracked": {}, "observed": {},
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
