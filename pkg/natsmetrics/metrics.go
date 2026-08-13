// Package natsmetrics provides bounded application-side NATS and JetStream
// metrics. It observes existing publish and message-disposition behavior; it
// never retries, acknowledges, or changes a business result on its own.
package natsmetrics

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type Outcome string

const (
	OutcomeAck              Outcome = "ack"
	OutcomeNak              Outcome = "nak"
	OutcomeTerm             Outcome = "term"
	OutcomeLeftPending      Outcome = "left_pending"
	OutcomeHandlerCancelled Outcome = "handler_cancelled"
)

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

func New(meter metric.Meter) *Metrics {
	noopMeter := noop.NewMeterProvider().Meter("natsmetrics-fallback")
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
	processing, err := meter.Float64Histogram("chat.nats.consumer.processing.duration", metric.WithDescription("Time from handler start through the disposition attempt."), metric.WithUnit("s"))
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
	requestDuration, err := meter.Float64Histogram("chat.nats.request.duration", metric.WithDescription("End-to-end NATS request/reply duration."), metric.WithUnit("s"))
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

type Consumer struct {
	metrics *Metrics
	attrs   []attribute.KeyValue
	up      atomic.Bool
}

func (m *Metrics) Consumer(cfg ConsumerConfig) *Consumer {
	return &Consumer{metrics: m, attrs: []attribute.KeyValue{
		attribute.String("service_name", cfg.ServiceName),
		attribute.String("site", cfg.Site),
		attribute.String("stream", cfg.Stream),
		attribute.String("consumer", cfg.Consumer),
	}}
}

func (c *Consumer) LoopStarted(ctx context.Context) {
	if c.up.CompareAndSwap(false, true) {
		c.metrics.loop.Add(ctx, 1, metric.WithAttributes(c.attrs...))
	}
}

func (c *Consumer) LoopStopped(ctx context.Context) {
	if c.up.CompareAndSwap(true, false) {
		c.metrics.loop.Add(ctx, -1, metric.WithAttributes(c.attrs...))
		return
	}
	// Establish an explicit zero-valued series before iterator creation.
	c.metrics.loop.Add(ctx, 0, metric.WithAttributes(c.attrs...))
}

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
			c.metrics.redeliveries.Add(ctx, 1, metric.WithAttributes(c.eventAttrs(eventType)...))
		}
	}
	return tracked
}

func (c *Consumer) eventAttrs(eventType EventType) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(c.attrs)+1)
	attrs = append(attrs, c.attrs...)
	return append(attrs, attribute.String("event_type", string(NormalizeEventType(string(eventType)))))
}

func (c *Consumer) Terminal(ctx context.Context, eventType EventType, reason TerminalReason) {
	attrs := c.eventAttrs(eventType)
	attrs = append(attrs, attribute.String("reason", string(normalizeTerminalReason(reason))))
	c.metrics.terminalFailures.Add(ctx, 1, metric.WithAttributes(attrs...))
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

func (m *Message) FinishPending(ctx context.Context) { m.finishWithOutcome(ctx, OutcomeLeftPending) }
func (m *Message) FinishCancelled(ctx context.Context) {
	m.finishWithOutcome(ctx, OutcomeHandlerCancelled)
}

func (m *Message) finish(want Outcome, err error) {
	outcome := want
	if err != nil {
		outcome = OutcomeLeftPending
	}
	if want == OutcomeNak && m.maxDeliver > 0 && m.numDelivered >= uint64(m.maxDeliver) {
		m.MarkTerminal(m.ctx, TerminalMaxDeliver)
	}
	if want == OutcomeTerm {
		m.MarkTerminal(m.ctx, TerminalInternal)
	}
	m.finishWithOutcome(m.ctx, outcome)
}

func (m *Message) finishWithOutcome(ctx context.Context, outcome Outcome) {
	m.disposeOnce.Do(func() {
		attrs := m.consumer.eventAttrs(m.eventType)
		attrs = append(attrs, attribute.String("outcome", string(outcome)))
		opts := metric.WithAttributes(attrs...)
		m.consumer.metrics.messages.Add(ctx, 1, opts)
		m.consumer.metrics.processingDuration.Record(ctx, time.Since(m.started).Seconds(), opts)
	})
}

type Publisher struct {
	metrics   *Metrics
	baseAttrs []attribute.KeyValue
}

func (m *Metrics) Publisher(serviceName, site string) Publisher {
	return Publisher{metrics: m, baseAttrs: []attribute.KeyValue{attribute.String("service_name", serviceName), attribute.String("site", site)}}
}

func (p Publisher) publishAttrs(destination DestinationKind, operation Operation) []attribute.KeyValue {
	attrs := append([]attribute.KeyValue{}, p.baseAttrs...)
	return append(attrs,
		attribute.String("destination_kind", string(normalizeDestination(destination))),
		attribute.String("operation", string(normalizeOperation(operation))),
	)
}

func (p Publisher) Attempt(ctx context.Context, destination DestinationKind, operation Operation, err error) {
	if p.metrics == nil {
		return
	}
	attrs := p.publishAttrs(destination, operation)
	attrs = append(attrs, attribute.String("outcome", string(ClassifyPublishError(err))))
	p.metrics.publishAttempts.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (p Publisher) Retry(ctx context.Context, destination DestinationKind, operation Operation) {
	if p.metrics == nil {
		return
	}
	p.metrics.publishRetries.Add(ctx, 1, metric.WithAttributes(p.publishAttrs(destination, operation)...))
}

func (p Publisher) Request(ctx context.Context, operation Operation, duration time.Duration, err error) {
	if p.metrics == nil {
		return
	}
	attrs := append([]attribute.KeyValue{}, p.baseAttrs...)
	attrs = append(attrs,
		attribute.String("operation", string(normalizeOperation(operation))),
		attribute.String("outcome", string(ClassifyPublishError(err))),
	)
	opts := metric.WithAttributes(attrs...)
	p.metrics.requests.Add(ctx, 1, opts)
	p.metrics.requestDuration.Record(ctx, duration.Seconds(), opts)
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
