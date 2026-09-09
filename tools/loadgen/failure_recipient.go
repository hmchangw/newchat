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

// recipientEvidenceCapacityFactor is the headroom the expectation map keeps over
// the ledger's own capacity.
//
// The sweep frees ledger slots inside Expire and frees the matching evidence
// afterwards, in ForgetAll, so between the two the ledger has room the evidence
// does not. Sized exactly like the ledger, a send arriving in that window is
// refused registration even though the ledger would have admitted its
// operation. One extra generation closes the window by construction: an
// unbounded sweep can retire at most the whole ledger before cleanup runs, so
// the lag can never exceed one capacity.
const recipientEvidenceCapacityFactor = 2

// withFailureRecipientEvidenceCapacity bounds the expectation map. Callers pass
// the ledger's own capacity: an expectation exists per ledger operation, so the
// ledger's admission limit binds first in a healthy run and this only engages
// once expiry has stopped working.
func withFailureRecipientEvidenceCapacity(capacity int) failureRecipientObserverOption {
	return func(o *failureRecipientObserver) {
		if capacity > 0 {
			o.evidence.SetCapacity(capacity * recipientEvidenceCapacityFactor)
		}
	}
}

func withFailureRecipientDuplicatePolicy(allow bool) failureRecipientObserverOption {
	return func(observer *failureRecipientObserver) {
		observer.evidence.SetAllowDuplicates(allow)
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
		// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
		// nosemgrep: gosec.G304-1
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
			if result.Durable(item.kind, recipient) {
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
		// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
		// nosemgrep: gosec.G304-1
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
