//go:build integration

package natsutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestPipelineTraceContinuity is the Phase 2 gate: a message published with an
// active span must carry its trace context through NATS headers across two
// JetStream hops. Each NATS consume hop may start a separate trace per the OTel
// messaging model; continuity is the span link from the consumer span back to
// the upstream producer span, not a shared trace ID.
func TestPipelineTraceContinuity(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	prop := propagation.TraceContext{}

	nc, err := natsutil.Connect(context.Background(), testutil.NATS(t), "", tp, prop)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := nc.JetStream()
	require.NoError(t, err)

	ctx := context.Background()
	const (
		streamName = "TEST_CONTINUITY"
		subjIn     = "test.continuity.in"
		subjOut    = "test.continuity.out"
	)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"test.continuity.>"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })

	hop1 := make(chan oteltrace.SpanContext, 1)
	hop2 := make(chan oteltrace.SpanContext, 1)

	cons1, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "continuity-hop1",
		FilterSubject: subjIn,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)
	cc1, err := cons1.Consume(ctx, func(hctx context.Context, msg jetstream.Msg) {
		hop1 <- oteltrace.SpanContextFromContext(hctx)
		_, _ = js.PublishMsg(hctx, natsutil.NewMsg(hctx, subjOut, msg.Data()))
		_ = msg.Ack()
	})
	require.NoError(t, err)
	t.Cleanup(cc1.Stop)

	cons2, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "continuity-hop2",
		FilterSubject: subjOut,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)
	cc2, err := cons2.Consume(ctx, func(hctx context.Context, msg jetstream.Msg) {
		hop2 <- oteltrace.SpanContextFromContext(hctx)
		_ = msg.Ack()
	})
	require.NoError(t, err)
	t.Cleanup(cc2.Stop)

	tracer := tp.Tracer("continuity-test")
	pctx, pspan := tracer.Start(ctx, "produce")
	wantTID := pspan.SpanContext().TraceID()
	require.True(t, wantTID.IsValid(), "producer span must have a valid trace ID")
	_, err = js.PublishMsg(pctx, natsutil.NewMsg(pctx, subjIn, []byte(`{"hello":"world"}`)))
	require.NoError(t, err)
	pspan.End()

	got1 := receiveSpanContext(t, hop1, "hop1 (gatekeeper) consumer")
	require.True(t, got1.IsValid(), "hop1 must have a valid active consumer span context")

	got2 := receiveSpanContext(t, hop2, "hop2 (worker/broadcast) consumer")
	require.True(t, got2.IsValid(), "hop2 must have a valid active consumer span context")

	spans := recorder.Ended()
	requireLinkedToTrace(t, spans, wantTID, "first NATS consume hop must link to the original producer trace")
	requireLinkedToTrace(t, spans, got1.TraceID(), "second NATS consume hop must link to the first hop's producer trace")
}

func receiveSpanContext(t *testing.T, ch <-chan oteltrace.SpanContext, who string) oteltrace.SpanContext {
	t.Helper()
	select {
	case sc := <-ch:
		require.Truef(t, sc.IsValid(), "%s observed an invalid span context; context did not propagate", who)
		return sc
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", who)
		return oteltrace.SpanContext{}
	}
}

func requireLinkedToTrace(t *testing.T, spans []sdktrace.ReadOnlySpan, traceID oteltrace.TraceID, msg string) {
	t.Helper()
	for _, span := range spans {
		for _, link := range span.Links() {
			if link.SpanContext.TraceID() == traceID {
				return
			}
		}
	}
	t.Fatalf("%s: no ended span linked to trace ID %s", msg, traceID.String())
}
