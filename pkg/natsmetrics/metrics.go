// Package natsmetrics provides bounded application-side NATS and JetStream
// metrics. It observes existing publish and message-disposition behavior; it
// never retries, acknowledges, or changes a business result on its own.
//
// Every label value comes from a closed enum in this file, so the attribute
// sets are precomputed once per Consumer/Publisher and looked up by value on
// the hot path — fan-out records one publish per recipient and must not pay for
// attribute-set construction each time.
package natsmetrics

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flywindy/o11y"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// ScopeName is the instrumentation scope for every instrument in this package.
// Constructing through NewFromProvider keeps one scope per process so the
// instruments dedupe instead of splitting into per-caller series.
const ScopeName = "github.com/hmchangw/chat/pkg/natsmetrics"

type Outcome string

const (
	OutcomeAck              Outcome = "ack"
	OutcomeNak              Outcome = "nak"
	OutcomeTerm             Outcome = "term"
	OutcomeLeftPending      Outcome = "left_pending"
	OutcomeHandlerCancelled Outcome = "handler_cancelled"
)

var allOutcomes = []Outcome{OutcomeAck, OutcomeNak, OutcomeTerm, OutcomeLeftPending, OutcomeHandlerCancelled}

type EventType string

const (
	EventCreated          EventType = "created"
	EventUpdated          EventType = "updated"
	EventDeleted          EventType = "deleted"
	EventPinned           EventType = "pinned"
	EventUnpinned         EventType = "unpinned"
	EventReacted          EventType = "reacted"
	EventThreadReplyAdded EventType = "thread_reply_added"
	EventSend             EventType = "send"
	EventTeamsBatch       EventType = "teams_batch"
	EventUnknown          EventType = "unknown"
)

var allEventTypes = []EventType{
	EventCreated, EventUpdated, EventDeleted, EventPinned, EventUnpinned,
	EventReacted, EventThreadReplyAdded, EventSend, EventTeamsBatch, EventUnknown,
}

type PublishOutcome string

const (
	PublishSuccess         PublishOutcome = "success"
	PublishTimeout         PublishOutcome = "timeout"
	PublishNoResponders    PublishOutcome = "no_responders"
	PublishDisconnected    PublishOutcome = "disconnected"
	PublishBufferFull      PublishOutcome = "buffer_full"
	PublishPermission      PublishOutcome = "permission"
	PublishPayloadTooLarge PublishOutcome = "payload_too_large"
	PublishOtherError      PublishOutcome = "other_error"
)

var allPublishOutcomes = []PublishOutcome{
	PublishSuccess, PublishTimeout, PublishNoResponders, PublishDisconnected,
	PublishBufferFull, PublishPermission, PublishPayloadTooLarge, PublishOtherError,
}

type TerminalReason string

const (
	TerminalMaxDeliver        TerminalReason = "max_deliver"
	TerminalPermanent         TerminalReason = "permanent"
	TerminalPublishExhausted  TerminalReason = "publish_exhausted"
	TerminalConsumerDeleted   TerminalReason = "consumer_deleted"
	TerminalStreamUnavailable TerminalReason = "stream_unavailable"
	TerminalInvalidPayload    TerminalReason = "invalid_payload"
	TerminalInternal          TerminalReason = "internal"
)

var allTerminalReasons = []TerminalReason{
	TerminalMaxDeliver, TerminalPermanent, TerminalPublishExhausted, TerminalConsumerDeleted,
	TerminalStreamUnavailable, TerminalInvalidPayload, TerminalInternal,
}

type DestinationKind string

const (
	DestinationCanonical      DestinationKind = "canonical"
	DestinationRecipientEvent DestinationKind = "recipient_event"
	DestinationNotification   DestinationKind = "notification"
	DestinationPush           DestinationKind = "push"
	DestinationOutbox         DestinationKind = "outbox"
	DestinationInbox          DestinationKind = "inbox"
	DestinationClientResponse DestinationKind = "client_response"
	DestinationUserSync       DestinationKind = "user_sync"
	DestinationUnknown        DestinationKind = "unknown"
)

var allDestinations = []DestinationKind{
	DestinationCanonical, DestinationRecipientEvent, DestinationNotification, DestinationPush,
	DestinationOutbox, DestinationInbox, DestinationClientResponse, DestinationUserSync, DestinationUnknown,
}

type Operation string

const (
	OperationCanonicalPublish    Operation = "canonical_publish"
	OperationClientResponse      Operation = "client_response"
	OperationRecipientPublish    Operation = "recipient_publish"
	OperationNotificationPublish Operation = "notification_publish"
	OperationPushPublish         Operation = "push_publish"
	OperationHistoryGetMessage   Operation = "history_get_message"
	OperationPresenceLookup      Operation = "presence_lookup"
	OperationThreadTCount        Operation = "thread_tcount"
	OperationTeamsUserUpsert     Operation = "teams_user_upsert"
	OperationUnknown             Operation = "unknown"
)

var allOperations = []Operation{
	OperationCanonicalPublish, OperationClientResponse, OperationRecipientPublish,
	OperationNotificationPublish, OperationPushPublish, OperationHistoryGetMessage,
	OperationPresenceLookup, OperationThreadTCount, OperationTeamsUserUpsert, OperationUnknown,
}

// Metrics owns the shared instruments. Instrument-creation failures fall back
// to no-op instruments so telemetry can never block service startup or work.
type Metrics struct {
	loop               metric.Int64UpDownCounter
	messages           metric.Int64Counter
	redeliveries       metric.Int64Counter
	processingDuration metric.Float64Histogram
	publishAttempts    metric.Int64Counter
	publishRetries     metric.Int64Counter
	terminalFailures   metric.Int64Counter
	requests           metric.Int64Counter
	requestDuration    metric.Float64Histogram
}

// NewFromProvider builds the shared instruments on this package's own scope.
// Prefer it over New so every caller in a process lands on one scope.
func NewFromProvider(mp metric.MeterProvider) *Metrics { return New(mp.Meter(ScopeName)) }

func New(meter metric.Meter) *Metrics {
	noopMeter := noop.NewMeterProvider().Meter("natsmetrics-fallback")
	// Latency instruments carry the SDK's shared boundaries as an instrument
	// advisory. The o11y SDK only registers bucket views for its own instrument
	// names, so without this these two histograms fall back to the OTel default
	// boundaries (0..10000) and every sub-second duration lands in bucket one.
	latency := metric.WithExplicitBucketBoundaries(o11y.DefaultLatencyBuckets()...)

	loop, err := meter.Int64UpDownCounter("chat.nats.consumer.loop.up", metric.WithDescription("Whether an application consumer loop can receive messages."))
	if err != nil {
		loop, _ = noopMeter.Int64UpDownCounter("chat.nats.consumer.loop.up")
	}
	messages, err := meter.Int64Counter("chat.nats.consumer.messages", metric.WithDescription("JetStream delivery attempts by terminal application disposition."))
	if err != nil {
		messages, _ = noopMeter.Int64Counter("chat.nats.consumer.messages")
	}
	redeliveries, err := meter.Int64Counter("chat.nats.consumer.redeliveries", metric.WithDescription("JetStream delivery attempts whose delivery count is greater than one."))
	if err != nil {
		redeliveries, _ = noopMeter.Int64Counter("chat.nats.consumer.redeliveries")
	}
	processing, err := meter.Float64Histogram("chat.nats.consumer.processing.duration", metric.WithDescription("Time from handler start through the disposition attempt."), metric.WithUnit("s"), latency)
	if err != nil {
		processing, _ = noopMeter.Float64Histogram("chat.nats.consumer.processing.duration")
	}
	publishAttempts, err := meter.Int64Counter("chat.nats.publish.attempts", metric.WithDescription("Actual Core NATS or JetStream publish attempts."))
	if err != nil {
		publishAttempts, _ = noopMeter.Int64Counter("chat.nats.publish.attempts")
	}
	publishRetries, err := meter.Int64Counter("chat.nats.publish.retries", metric.WithDescription("Application-managed publish attempts after the first."))
	if err != nil {
		publishRetries, _ = noopMeter.Int64Counter("chat.nats.publish.retries")
	}
	terminal, err := meter.Int64Counter("chat.nats.terminal.failures", metric.WithDescription("Work that will receive no further application attempt."))
	if err != nil {
		terminal, _ = noopMeter.Int64Counter("chat.nats.terminal.failures")
	}
	requests, err := meter.Int64Counter("chat.nats.requests", metric.WithDescription("NATS request/reply results."))
	if err != nil {
		requests, _ = noopMeter.Int64Counter("chat.nats.requests")
	}
	requestDuration, err := meter.Float64Histogram("chat.nats.request.duration", metric.WithDescription("End-to-end NATS request/reply duration."), metric.WithUnit("s"), latency)
	if err != nil {
		requestDuration, _ = noopMeter.Float64Histogram("chat.nats.request.duration")
	}
	return &Metrics{loop: loop, messages: messages, redeliveries: redeliveries, processingDuration: processing, publishAttempts: publishAttempts, publishRetries: publishRetries, terminalFailures: terminal, requests: requests, requestDuration: requestDuration}
}

type ConsumerConfig struct {
	ServiceName string
	Site        string
	Stream      string
	Consumer    string
}

// consumerKey and publishKey index the precomputed attribute sets. Struct map
// keys are compared by value, so a lookup costs no allocation.
type consumerKey struct {
	event   EventType
	outcome Outcome
}

type terminalKey struct {
	event  EventType
	reason TerminalReason
}

type publishKey struct {
	destination DestinationKind
	operation   Operation
	outcome     PublishOutcome
}

type retryKey struct {
	destination DestinationKind
	operation   Operation
}

type requestKey struct {
	operation Operation
	outcome   PublishOutcome
}

type Consumer struct {
	metrics *Metrics
	loopOpt metric.MeasurementOption
	message map[consumerKey]metric.MeasurementOption
	redeliv map[EventType]metric.MeasurementOption
	termOpt map[terminalKey]metric.MeasurementOption
	up      atomic.Bool
}

func (m *Metrics) Consumer(cfg ConsumerConfig) *Consumer {
	base := []attribute.KeyValue{
		attribute.String("service_name", cfg.ServiceName),
		attribute.String("site", cfg.Site),
		attribute.String("stream", cfg.Stream),
		attribute.String("consumer", cfg.Consumer),
	}
	withEvent := func(event EventType, extra ...attribute.KeyValue) []attribute.KeyValue {
		attrs := make([]attribute.KeyValue, 0, len(base)+1+len(extra))
		attrs = append(attrs, base...)
		attrs = append(attrs, attribute.String("event_type", string(event)))
		return append(attrs, extra...)
	}
	c := &Consumer{
		metrics: m,
		loopOpt: metric.WithAttributes(base...),
		message: make(map[consumerKey]metric.MeasurementOption, len(allEventTypes)*len(allOutcomes)),
		redeliv: make(map[EventType]metric.MeasurementOption, len(allEventTypes)),
		termOpt: make(map[terminalKey]metric.MeasurementOption, len(allEventTypes)*len(allTerminalReasons)),
	}
	for _, event := range allEventTypes {
		c.redeliv[event] = metric.WithAttributes(withEvent(event)...)
		for _, outcome := range allOutcomes {
			c.message[consumerKey{event, outcome}] = metric.WithAttributes(withEvent(event, attribute.String("outcome", string(outcome)))...)
		}
		for _, reason := range allTerminalReasons {
			c.termOpt[terminalKey{event, reason}] = metric.WithAttributes(withEvent(event, attribute.String("reason", string(reason)))...)
		}
	}
	return c
}

func (c *Consumer) LoopStarted(ctx context.Context) {
	if c.up.CompareAndSwap(false, true) {
		c.metrics.loop.Add(ctx, 1, c.loopOpt)
	}
}

func (c *Consumer) LoopStopped(ctx context.Context) {
	if c.up.CompareAndSwap(true, false) {
		c.metrics.loop.Add(ctx, -1, c.loopOpt)
		return
	}
	// Establish an explicit zero-valued series before iterator creation.
	c.metrics.loop.Add(ctx, 0, c.loopOpt)
}

// IsUp reports whether the loop is currently able to receive messages.
func (c *Consumer) IsUp() bool { return c.up.Load() }

// LoopFailed marks the loop down before recording the bounded terminal cause.
func (c *Consumer) LoopFailed(ctx context.Context, err error) {
	wasUp := c.up.Load()
	c.LoopStopped(ctx)
	if !wasUp {
		return
	}
	reason := TerminalInternal
	switch {
	case errors.Is(err, jetstream.ErrConsumerDeleted):
		reason = TerminalConsumerDeleted
	case errors.Is(err, jetstream.ErrStreamNotFound), errors.Is(err, jetstream.ErrNoStreamResponse),
		errors.Is(err, jetstream.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionClosed),
		errors.Is(err, nats.ErrDisconnected), errors.Is(err, nats.ErrNoResponders):
		reason = TerminalStreamUnavailable
	}
	c.Terminal(ctx, EventUnknown, reason)
}

func (c *Consumer) Track(ctx context.Context, msg jetstream.Msg, eventType EventType, maxDeliver int) *Message {
	eventType = NormalizeEventType(string(eventType))
	tracked := &Message{Msg: msg, consumer: c, ctx: ctx, eventType: eventType, maxDeliver: maxDeliver, started: time.Now()}
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		tracked.numDelivered = meta.NumDelivered
		if meta.NumDelivered > 1 {
			c.metrics.redeliveries.Add(ctx, 1, c.redeliv[eventType])
		}
	}
	return tracked
}

func (c *Consumer) Terminal(ctx context.Context, eventType EventType, reason TerminalReason) {
	key := terminalKey{NormalizeEventType(string(eventType)), normalizeTerminalReason(reason)}
	c.metrics.terminalFailures.Add(ctx, 1, c.termOpt[key])
}

// Message intercepts disposition calls while preserving the wrapped message's
// exact method, return value, and call count.
type Message struct {
	jetstream.Msg
	consumer     *Consumer
	ctx          context.Context
	eventType    EventType
	maxDeliver   int
	numDelivered uint64
	started      time.Time
	disposeOnce  sync.Once
	terminalOnce sync.Once
}

type messageContextKey struct{}

// Context returns ctx carrying this delivery recorder so deeper fan-out code
// can report a swallowed terminal failure without accepting dynamic labels.
func (m *Message) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, messageContextKey{}, m)
}

// MarkTerminalFromContext records terminal evidence for the current delivery,
// if ctx belongs to a tracked delivery. It is a no-op for ordinary contexts.
func MarkTerminalFromContext(ctx context.Context, reason TerminalReason) {
	if msg, ok := ctx.Value(messageContextKey{}).(*Message); ok {
		msg.MarkTerminal(ctx, reason)
	}
}

// IsFinalDeliveryFromContext reports whether ctx carries metadata proving the
// current delivery is the configured final attempt.
func IsFinalDeliveryFromContext(ctx context.Context) bool {
	msg, ok := ctx.Value(messageContextKey{}).(*Message)
	return ok && msg.IsFinalDelivery()
}

// IsFinalDelivery reports whether metadata proves this is the configured last
// attempt. Missing metadata returns false rather than guessing.
func (m *Message) IsFinalDelivery() bool {
	return m.maxDeliver > 0 && m.numDelivered > 0 && m.numDelivered >= uint64(m.maxDeliver)
}

func (m *Message) Ack() error { err := m.Msg.Ack(); m.finish(OutcomeAck, err); return err }
func (m *Message) DoubleAck(ctx context.Context) error {
	err := m.Msg.DoubleAck(ctx)
	m.finish(OutcomeAck, err)
	return err
}
func (m *Message) Nak() error { err := m.Msg.Nak(); m.finish(OutcomeNak, err); return err }
func (m *Message) NakWithDelay(delay time.Duration) error {
	err := m.Msg.NakWithDelay(delay)
	m.finish(OutcomeNak, err)
	return err
}
func (m *Message) Term() error { err := m.Msg.Term(); m.finish(OutcomeTerm, err); return err }

// TermWithReason forwards the caller's free-text reason to JetStream but never
// to a label — the terminal metric uses the bounded `permanent` reason.
func (m *Message) TermWithReason(reason string) error {
	err := m.Msg.TermWithReason(reason)
	m.finish(OutcomeTerm, err)
	return err
}

func (m *Message) MarkTerminal(ctx context.Context, reason TerminalReason) {
	m.terminalOnce.Do(func() { m.consumer.Terminal(ctx, m.eventType, reason) })
}

// MarkHandlerPanic lets the shared panic guard classify its Ack-drop without
// importing this package or changing the Ack behavior.
func (m *Message) MarkHandlerPanic() { m.MarkTerminal(m.ctx, TerminalInternal) }

// Finish records the disposition for a delivery that returned without calling
// Ack/Nak/Term. A live loop and live context mean the handler is relying on
// AckWait redelivery (`left_pending`); a stopped loop or cancelled context mean
// shutdown took the delivery away before it settled (`handler_cancelled`).
func (m *Message) Finish(ctx context.Context) {
	outcome := OutcomeLeftPending
	if !m.consumer.IsUp() || ctx.Err() != nil {
		outcome = OutcomeHandlerCancelled
	}
	m.finishWithOutcome(ctx, outcome)
}

func (m *Message) finish(want Outcome, err error) {
	outcome := want
	if err != nil {
		outcome = OutcomeLeftPending
	}
	if want == OutcomeNak && m.maxDeliver > 0 && m.numDelivered >= uint64(m.maxDeliver) {
		m.MarkTerminal(m.ctx, TerminalMaxDeliver)
	}
	// An explicit Term is a deliberate drop. A handler that already classified
	// the drop keeps its own reason — MarkTerminal is once-only.
	if want == OutcomeTerm {
		m.MarkTerminal(m.ctx, TerminalPermanent)
	}
	m.finishWithOutcome(m.ctx, outcome)
}

func (m *Message) finishWithOutcome(ctx context.Context, outcome Outcome) {
	m.disposeOnce.Do(func() {
		opt := m.consumer.message[consumerKey{m.eventType, outcome}]
		m.consumer.metrics.messages.Add(ctx, 1, opt)
		m.consumer.metrics.processingDuration.Record(ctx, time.Since(m.started).Seconds(), opt)
	})
}

type Publisher struct {
	metrics *Metrics
	attempt map[publishKey]metric.MeasurementOption
	retry   map[retryKey]metric.MeasurementOption
	request map[requestKey]metric.MeasurementOption
}

func (m *Metrics) Publisher(serviceName, site string) Publisher {
	base := []attribute.KeyValue{attribute.String("service_name", serviceName), attribute.String("site", site)}
	build := func(extra ...attribute.KeyValue) metric.MeasurementOption {
		attrs := make([]attribute.KeyValue, 0, len(base)+len(extra))
		attrs = append(attrs, base...)
		return metric.WithAttributes(append(attrs, extra...)...)
	}
	p := Publisher{
		metrics: m,
		attempt: make(map[publishKey]metric.MeasurementOption, len(allDestinations)*len(allOperations)*len(allPublishOutcomes)),
		retry:   make(map[retryKey]metric.MeasurementOption, len(allDestinations)*len(allOperations)),
		request: make(map[requestKey]metric.MeasurementOption, len(allOperations)*len(allPublishOutcomes)),
	}
	for _, destination := range allDestinations {
		dst := attribute.String("destination_kind", string(destination))
		for _, operation := range allOperations {
			op := attribute.String("operation", string(operation))
			p.retry[retryKey{destination, operation}] = build(dst, op)
			for _, outcome := range allPublishOutcomes {
				p.attempt[publishKey{destination, operation, outcome}] = build(dst, op, attribute.String("outcome", string(outcome)))
			}
		}
	}
	for _, operation := range allOperations {
		op := attribute.String("operation", string(operation))
		for _, outcome := range allPublishOutcomes {
			p.request[requestKey{operation, outcome}] = build(op, attribute.String("outcome", string(outcome)))
		}
	}
	return p
}

// Attempt records one actual publish call.
//
// For Core NATS a nil error means the message entered the client's write or
// reconnect buffer, NOT that the broker accepted it: nats.go buffers publishes
// across a disconnect and only fails once the reconnect buffer overflows. Treat
// `outcome="success"` on a Core NATS destination as "handed to the client", and
// read it alongside the connection-state metrics in pkg/natsutil. JetStream
// destinations wait for a PubAck, so their success is broker-confirmed.
func (p Publisher) Attempt(ctx context.Context, destination DestinationKind, operation Operation, err error) {
	if p.metrics == nil {
		return
	}
	key := publishKey{normalizeDestination(destination), normalizeOperation(operation), ClassifyPublishError(err)}
	p.metrics.publishAttempts.Add(ctx, 1, p.attempt[key])
}

// Retry counts an application-managed publish attempt after the first. It is
// only meaningful for a caller that loops around its own publish; JetStream's
// internal PubAck retries and the consumer Nak path are not application retries
// and must not be counted here.
func (p Publisher) Retry(ctx context.Context, destination DestinationKind, operation Operation) {
	if p.metrics == nil {
		return
	}
	p.metrics.publishRetries.Add(ctx, 1, p.retry[retryKey{normalizeDestination(destination), normalizeOperation(operation)}])
}

func (p Publisher) Request(ctx context.Context, operation Operation, duration time.Duration, err error) {
	if p.metrics == nil {
		return
	}
	opt := p.request[requestKey{normalizeOperation(operation), ClassifyPublishError(err)}]
	p.metrics.requests.Add(ctx, 1, opt)
	p.metrics.requestDuration.Record(ctx, duration.Seconds(), opt)
}

func NormalizeEventType(value string) EventType {
	switch EventType(value) {
	case EventCreated, EventUpdated, EventDeleted, EventPinned, EventUnpinned, EventReacted, EventThreadReplyAdded, EventSend, EventTeamsBatch:
		return EventType(value)
	default:
		return EventUnknown
	}
}

func normalizeDestination(destination DestinationKind) DestinationKind {
	switch destination {
	case DestinationCanonical, DestinationRecipientEvent, DestinationNotification, DestinationPush, DestinationOutbox, DestinationInbox, DestinationClientResponse, DestinationUserSync:
		return destination
	default:
		return DestinationUnknown
	}
}

func normalizeOperation(operation Operation) Operation {
	switch operation {
	case OperationCanonicalPublish, OperationClientResponse, OperationRecipientPublish, OperationNotificationPublish, OperationPushPublish, OperationHistoryGetMessage, OperationPresenceLookup, OperationThreadTCount, OperationTeamsUserUpsert:
		return operation
	default:
		return OperationUnknown
	}
}

func normalizeTerminalReason(reason TerminalReason) TerminalReason {
	switch reason {
	case TerminalMaxDeliver, TerminalPermanent, TerminalPublishExhausted, TerminalConsumerDeleted, TerminalStreamUnavailable, TerminalInvalidPayload, TerminalInternal:
		return reason
	default:
		return TerminalInternal
	}
}

func ClassifyPublishError(err error) PublishOutcome {
	var jsErr jetstream.JetStreamError
	switch {
	case err == nil:
		return PublishSuccess
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		return PublishTimeout
	case errors.Is(err, nats.ErrNoResponders), errors.Is(err, jetstream.ErrNoStreamResponse):
		return PublishNoResponders
	case errors.Is(err, nats.ErrDisconnected), errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, jetstream.ErrConnectionClosed):
		return PublishDisconnected
	case errors.Is(err, nats.ErrReconnectBufExceeded):
		return PublishBufferFull
	case errors.Is(err, nats.ErrPermissionViolation), errors.Is(err, nats.ErrAuthorization):
		return PublishPermission
	case errors.As(err, &jsErr) && jsErr.APIError() != nil && jsErr.APIError().Code == 403:
		return PublishPermission
	case errors.Is(err, nats.ErrMaxPayload):
		return PublishPayloadTooLarge
	default:
		return PublishOtherError
	}
}
