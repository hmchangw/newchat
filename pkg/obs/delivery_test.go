package obs

import (
	"context"
	"testing"

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

// TestDeliveryTracer_SpanIsLiveAfterHandover is the whole point of the helper:
// a pull iterator hands each message over with its receive span already ended,
// so a consume loop that keeps using that ctx writes identity onto a closed
// span. The span this tracer opens must be recording.
func TestDeliveryTracer_SpanIsLiveAfterHandover(t *testing.T) {
	recorder := recordSpans(t)

	// The ctx a pull iterator hands a consume loop: a span that is already done.
	handover := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = handover.Shutdown(context.Background()) })
	handoverCtx, handoverSpan := handover.Tracer("handover").Start(context.Background(), "receive")
	handoverSpan.End()
	require.False(t, trace.SpanFromContext(handoverCtx).IsRecording(),
		"precondition: the handed-over span is closed")

	ctx, span := DeliveryTracer().Start(handoverCtx, "handle MESSAGES-s1/message-worker")
	assert.True(t, span.IsRecording(), "the delivery span must be live")
	trace.SpanFromContext(ctx).SetAttributes(attribute.String(RoomIDKey, "room-42"))
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "handle MESSAGES-s1/message-worker", ended[0].Name())
	assert.Equal(t, trace.SpanKindInternal, ended[0].SpanKind(),
		"a second CONSUMER span per delivery would double-count consumer dashboards")
	attrs := spanAttributes(ended[0])
	assert.Equal(t, "room-42", attrs[RoomIDKey],
		"an attribute set on the current span must survive, unlike on the handed-over one")
}

// TestDeliveryTracer_MaterializesBaggageIdentity covers the path the message
// pipeline actually takes: identity is established on the handed-over ctx as
// baggage, and the span the loop opens must pick it up from the SDK's baggage
// span processor without the loop setting anything itself.
func TestDeliveryTracer_MaterializesBaggageIdentity(t *testing.T) {
	testEnv(t, "delivery-span-svc")
	t.Setenv("O11Y_METRICS_ENABLED", "false")
	t.Setenv("O11Y_USER_BAGGAGE_ENABLED", "true")

	sdk, shutdown, err := Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdk.TracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	ctx := ContextWithIdentity(context.Background(), "alice", "room-42", "site-local")
	_, span := DeliveryTracer().Start(ctx, "handle MESSAGES-site-local/message-worker")
	t.Cleanup(func() { span.End() })

	readable, ok := span.(sdktrace.ReadOnlySpan)
	require.True(t, ok, "span is %T, not a recording SDK span", span)
	attrs := spanAttributes(readable)
	assert.Equal(t, "room-42", attrs[RoomIDKey])
	assert.Equal(t, "site-local", attrs[SiteIDKey])
	assert.Equal(t, "alice", attrs["user.name"])
}

// TestDeliveryTracer_ReadsTheCurrentProvider pins that the tracer is not cached
// across provider installs. A process-global cache would bind these spans to
// whichever provider happened to be installed first — before Init, in the worst
// case — and silently drop every attribute the SDK's processors add.
func TestDeliveryTracer_ReadsTheCurrentProvider(t *testing.T) {
	first := recordSpans(t)
	_, firstSpan := DeliveryTracer().Start(context.Background(), "handle first")
	firstSpan.End()
	require.Len(t, first.Ended(), 1)

	second := recordSpans(t)
	_, secondSpan := DeliveryTracer().Start(context.Background(), "handle second")
	secondSpan.End()

	require.Len(t, second.Ended(), 1, "the tracer must follow the provider installed now")
	assert.Equal(t, "handle second", second.Ended()[0].Name())
	assert.Len(t, first.Ended(), 1, "the earlier provider must not receive the later span")
}

func spanAttributes(span sdktrace.ReadOnlySpan) map[string]string {
	attrs := make(map[string]string)
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	return attrs
}
