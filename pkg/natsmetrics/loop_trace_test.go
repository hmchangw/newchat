package natsmetrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = tp.Shutdown(context.Background())
	})
	return recorder
}

// TestConsume_ProcessingSpanCoversProcess pins the reason this loop opens a
// span at all. Next hands each message over with its receive span already
// ended (o11y docs/guide.md: "trace.SpanFromContext(m.Ctx).SetAttributes(...)
// is a no-op here"), so without a span of the loop's own, every attribute a
// worker sets on the current span — the room and account identity among them —
// is silently dropped, and no span reports how long processing took.
func TestConsume_ProcessingSpanCoversProcess(t *testing.T) {
	recorder := recordSpans(t)

	m, _ := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "MESSAGES-s1", Consumer: "message-worker"})
	c.LoopStarted(context.Background())
	iter := &oneMessageIterator{msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}}
	var wg sync.WaitGroup
	const work = 20 * time.Millisecond

	Consume(context.Background(), iter, c, 1, 5, &wg, nil, func(ctx context.Context, tracked *Message) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("chat.room.id", "room-42"))
		time.Sleep(work)
		assert.NoError(t, tracked.Ack())
	})
	wg.Wait()

	ended := recorder.Ended()
	require.Len(t, ended, 1, "the consume loop must open exactly one span per delivery")

	attrs := make(map[string]string)
	for _, attr := range ended[0].Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	assert.Equal(t, "room-42", attrs["chat.room.id"],
		"an attribute the worker sets on the current span must land on a live span")
	assert.GreaterOrEqual(t, ended[0].EndTime().Sub(ended[0].StartTime()), work,
		"the span must cover processing, not end at handover")
}

// TestConsume_SpanNameIsBoundedByConsumer keeps the span name off the message
// subject, which carries the room id and account and would explode span-name
// cardinality.
func TestConsume_SpanNameIsBoundedByConsumer(t *testing.T) {
	recorder := recordSpans(t)

	m, _ := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "MESSAGES-s1", Consumer: "message-worker"})
	iter := &oneMessageIterator{msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}}
	var wg sync.WaitGroup

	Consume(context.Background(), iter, c, 1, 5, &wg, nil, func(_ context.Context, tracked *Message) {
		assert.NoError(t, tracked.Ack())
	})
	wg.Wait()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "handle MESSAGES-s1/message-worker", ended[0].Name())
}

// TestConsume_MetricsDisabledStillOpensSpan covers the Consumer built from a
// nil Metrics: the trace pillar and the metrics pillar toggle independently,
// so a metrics-off deployment must still produce a named processing span.
func TestConsume_MetricsDisabledStillOpensSpan(t *testing.T) {
	recorder := recordSpans(t)

	var nilMetrics *Metrics
	c := nilMetrics.Consumer(ConsumerConfig{Site: "s1", Stream: "MESSAGES-s1", Consumer: "message-worker"})
	iter := &oneMessageIterator{msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}}
	var wg sync.WaitGroup

	Consume(context.Background(), iter, c, 1, 5, &wg, nil, func(_ context.Context, tracked *Message) {
		assert.NoError(t, tracked.Ack())
	})
	wg.Wait()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "handle MESSAGES-s1/message-worker", ended[0].Name())
}

// TestStartHandling_ToleratesConsumerWithoutName covers the fallbacks: the
// worker loops call StartHandling unconditionally, exactly as they do Track
// and Finish, so a nil or zero-value Consumer must still yield a usable span
// rather than panicking on the delivery path.
func TestStartHandling_ToleratesConsumerWithoutName(t *testing.T) {
	tests := []struct {
		name     string
		consumer *Consumer
	}{
		{"nil consumer", nil},
		{"zero-value consumer", &Consumer{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := recordSpans(t)

			ctx, span := tc.consumer.StartHandling(context.Background())
			require.NotNil(t, ctx)
			span.End()

			ended := recorder.Ended()
			require.Len(t, ended, 1)
			assert.Equal(t, "handle nats delivery", ended[0].Name())
		})
	}
}
