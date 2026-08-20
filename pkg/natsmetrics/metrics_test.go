package natsmetrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeMsg struct {
	meta    *jetstream.MsgMetadata
	metaErr error
	ackErr  error
	nakErr  error
	termErr error
	acks    int
	naks    int
	terms   int
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, m.metaErr }
func (m *fakeMsg) Data() []byte                              { return nil }
func (m *fakeMsg) Headers() nats.Header                      { return nil }
func (m *fakeMsg) Subject() string                           { return "chat.msg.canonical.s1.created" }
func (m *fakeMsg) Reply() string                             { return "" }
func (m *fakeMsg) Ack() error                                { m.acks++; return m.ackErr }
func (m *fakeMsg) DoubleAck(context.Context) error           { m.acks++; return m.ackErr }
func (m *fakeMsg) Nak() error                                { m.naks++; return m.nakErr }
func (m *fakeMsg) NakWithDelay(time.Duration) error          { m.naks++; return m.nakErr }
func (m *fakeMsg) InProgress() error                         { return nil }
func (m *fakeMsg) Term() error                               { m.terms++; return m.termErr }
func (m *fakeMsg) TermWithReason(string) error               { m.terms++; return m.termErr }

func newTestMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return New(mp.Meter("test")), reader
}

func collect(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

func metricPoints[T int64 | float64](t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[T] {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[T]:
				return data.DataPoints
			case metricdata.Gauge[T]:
				return data.DataPoints
			default:
				t.Fatalf("metric %s has unexpected type %T", name, m.Data)
			}
		}
	}
	return nil
}

func histogramPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				h, ok := m.Data.(metricdata.Histogram[float64])
				require.True(t, ok)
				return h.DataPoints
			}
		}
	}
	return nil
}

func attrs(dp metricdata.DataPoint[int64]) map[string]string {
	out := map[string]string{}
	for _, kv := range dp.Attributes.ToSlice() {
		out[string(kv.Key)] = kv.Value.AsString()
	}
	return out
}

func TestConsumerLoopGaugeTransitions(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "MESSAGES_CANONICAL_s1", Consumer: "broadcast-worker"})

	c.LoopStopped(context.Background()) // iterator creation failure must expose zero
	c.LoopStarted(context.Background())
	c.LoopStarted(context.Background()) // idempotent
	rm := collect(t, reader)
	points := metricPoints[int64](t, rm, "chat.nats.consumer.loop.up")
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)

	c.LoopStopped(context.Background())
	c.LoopStopped(context.Background()) // idempotent
	rm = collect(t, reader)
	points = metricPoints[int64](t, rm, "chat.nats.consumer.loop.up")
	require.Len(t, points, 1)
	assert.Equal(t, int64(0), points[0].Value)
}

func TestConsumerLoopFailureUsesBoundedReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want TerminalReason
	}{
		{name: "consumer deleted", err: jetstream.ErrConsumerDeleted, want: TerminalConsumerDeleted},
		{name: "stream unavailable", err: jetstream.ErrNoStreamResponse, want: TerminalStreamUnavailable},
		{name: "unexpected iterator failure", err: errors.New("dynamic error text"), want: TerminalInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reader := newTestMetrics(t)
			c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
			c.LoopStarted(context.Background())

			c.LoopFailed(context.Background(), tt.err)

			rm := collect(t, reader)
			loop := metricPoints[int64](t, rm, "chat.nats.consumer.loop.up")
			require.Len(t, loop, 1)
			assert.Equal(t, int64(0), loop[0].Value)
			terminal := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
			require.Len(t, terminal, 1)
			assert.Equal(t, string(tt.want), attrs(terminal[0])["reason"])
		})
	}
}

func TestConsumerLoopFailureAfterShutdownDoesNotReportTerminalFailure(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
	c.LoopStopped(context.Background())

	c.LoopFailed(context.Background(), jetstream.ErrConsumerDeleted)

	rm := collect(t, reader)
	assert.Empty(t, metricPoints[int64](t, rm, "chat.nats.terminal.failures"))
}

func TestTrackedMessageDispositionAndRedelivery(t *testing.T) {
	tests := []struct {
		name       string
		msg        *fakeMsg
		dispose    func(*Message) error
		want       Outcome
		redelivery int64
	}{
		{name: "ack", msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, dispose: func(m *Message) error { return m.Ack() }, want: OutcomeAck},
		{name: "double ack", msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, dispose: func(m *Message) error { return m.DoubleAck(context.Background()) }, want: OutcomeAck},
		{name: "nak redelivery", msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 2}}, dispose: func(m *Message) error { return m.Nak() }, want: OutcomeNak, redelivery: 1},
		{name: "term", msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, dispose: func(m *Message) error { return m.Term() }, want: OutcomeTerm},
		{name: "term with reason", msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, dispose: func(m *Message) error { return m.TermWithReason("bounded by caller") }, want: OutcomeTerm},
		{name: "ack failure stays pending", msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}, ackErr: errors.New("ack failed")}, dispose: func(m *Message) error { return m.Ack() }, want: OutcomeLeftPending},
		{name: "nak failure stays pending", msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}, nakErr: errors.New("nak failed")}, dispose: func(m *Message) error { return m.Nak() }, want: OutcomeLeftPending},
		{name: "term failure stays pending", msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}, termErr: errors.New("term failed")}, dispose: func(m *Message) error { return m.Term() }, want: OutcomeLeftPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reader := newTestMetrics(t)
			c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
			tracked := c.Track(context.Background(), tt.msg, EventCreated, 5)
			_ = tt.dispose(tracked)
			tracked.Finish(context.Background()) // must not double count

			rm := collect(t, reader)
			points := metricPoints[int64](t, rm, "chat.nats.consumer.messages")
			require.Len(t, points, 1)
			assert.Equal(t, int64(1), points[0].Value)
			assert.Equal(t, string(tt.want), attrs(points[0])["outcome"])
			redeliveries := metricPoints[int64](t, rm, "chat.nats.consumer.redeliveries")
			if tt.redelivery == 0 {
				assert.Empty(t, redeliveries)
			} else {
				require.Len(t, redeliveries, 1)
				assert.Equal(t, tt.redelivery, redeliveries[0].Value)
			}
			durations := histogramPoints(t, rm, "chat.nats.consumer.processing.duration")
			require.Len(t, durations, 1)
			assert.Equal(t, uint64(1), durations[0].Count)
		})
	}
}

func TestTrackedMessageContextAndCancellation(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
	tracked := c.Track(context.Background(), &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 5}}, EventCreated, 5)
	ctx := tracked.Context(context.Background())

	assert.True(t, tracked.IsFinalDelivery())
	assert.True(t, IsFinalDeliveryFromContext(ctx))
	assert.False(t, IsFinalDeliveryFromContext(context.Background()))
	MarkTerminalFromContext(context.Background(), TerminalPublishExhausted)
	MarkTerminalFromContext(ctx, TerminalReason("dynamic reason"))
	tracked.MarkHandlerPanic()
	tracked.Finish(ctx)

	rm := collect(t, reader)
	terminal := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
	require.Len(t, terminal, 1)
	assert.Equal(t, string(TerminalInternal), attrs(terminal[0])["reason"])
	messages := metricPoints[int64](t, rm, "chat.nats.consumer.messages")
	require.Len(t, messages, 1)
	assert.Equal(t, string(OutcomeHandlerCancelled), attrs(messages[0])["outcome"])
}

func TestTerminalFailureAndMaxDeliver(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "MESSAGES_CANONICAL_s1", Consumer: "message-worker"})

	permanent := c.Track(context.Background(), &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, EventUnknown, 5)
	permanent.MarkTerminal(context.Background(), TerminalPermanent)
	require.NoError(t, permanent.Ack())
	exhausted := c.Track(context.Background(), &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 5}}, EventCreated, 5)
	require.NoError(t, exhausted.NakWithDelay(time.Second))

	rm := collect(t, reader)
	points := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
	require.Len(t, points, 2)
	reasons := map[string]int64{}
	for _, point := range points {
		reasons[attrs(point)["reason"]] = point.Value
	}
	assert.Equal(t, map[string]int64{"permanent": 1, "max_deliver": 1}, reasons)
}

func TestPublishAndRequestMetrics(t *testing.T) {
	m, reader := newTestMetrics(t)
	p := m.Publisher("s1")
	// A successful publish records nothing; only the failure lands.
	p.Failure(context.Background(), DestinationPush, OperationPushPublish, nil)
	p.Failure(context.Background(), DestinationPush, OperationPushPublish, nats.ErrNoResponders)
	p.Retry(context.Background(), DestinationPush, OperationPushPublish)
	p.Request(context.Background(), OperationPresenceLookup, 25*time.Millisecond, nats.ErrTimeout)

	rm := collect(t, reader)
	assert.Len(t, metricPoints[int64](t, rm, "chat.nats.publish.failures"), 1)
	assert.Len(t, metricPoints[int64](t, rm, "chat.nats.publish.retries"), 1)
	requestPoints := metricPoints[int64](t, rm, "chat.nats.requests")
	require.Len(t, requestPoints, 1)
	assert.Equal(t, "timeout", attrs(requestPoints[0])["outcome"])
	assert.Len(t, histogramPoints(t, rm, "chat.nats.request.duration"), 1)
}

func TestPublishAndRequestLabelsAreBounded(t *testing.T) {
	m, reader := newTestMetrics(t)
	p := m.Publisher("s1")
	p.Failure(context.Background(), DestinationKind("dynamic.destination"), Operation("dynamic.operation"), nats.ErrTimeout)
	p.Request(context.Background(), Operation("another.dynamic.operation"), time.Millisecond, nil)

	rm := collect(t, reader)
	publishPoints := metricPoints[int64](t, rm, "chat.nats.publish.failures")
	require.Len(t, publishPoints, 1)
	assert.Equal(t, string(DestinationUnknown), attrs(publishPoints[0])["destination_kind"])
	assert.Equal(t, string(OperationUnknown), attrs(publishPoints[0])["operation"])
	requestPoints := metricPoints[int64](t, rm, "chat.nats.requests")
	require.Len(t, requestPoints, 1)
	assert.Equal(t, string(OperationUnknown), attrs(requestPoints[0])["operation"])
}

func TestZeroValuePublisherDoesNotAffectBusinessFlow(t *testing.T) {
	var publisher Publisher
	publisher.Failure(context.Background(), DestinationPush, OperationPushPublish, errors.New("publish failed"))
	publisher.Retry(context.Background(), DestinationPush, OperationPushPublish)
	publisher.Request(context.Background(), OperationPresenceLookup, time.Millisecond, errors.New("request failed"))
}

func TestClassifiersAreBounded(t *testing.T) {
	assert.Equal(t, EventUnknown, NormalizeEventType("not-a-real-event"))
	assert.Equal(t, EventCreated, NormalizeEventType("created"))
	tests := []struct {
		err  error
		want PublishOutcome
	}{
		// Unreachable through Publisher.Failure, which returns before classifying
		// a nil error. Pinned so the function stays total.
		{nil, PublishOtherError},
		{context.DeadlineExceeded, PublishTimeout},
		{nats.ErrNoResponders, PublishNoResponders},
		{nats.ErrDisconnected, PublishDisconnected},
		{nats.ErrReconnectBufExceeded, PublishBufferFull},
		{nats.ErrPermissionViolation, PublishPermission},
		{nats.ErrMaxPayload, PublishPayloadTooLarge},
		{errors.New("dynamic text"), PublishOtherError},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, ClassifyPublishError(tt.err))
	}
}
