package natsrouter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/hmchangw/chat/pkg/obs"
)

// recordSpans installs a recording tracer provider as the OTel global — the
// provider obs.Init installs in production, and the one the router reads. The
// caller must already have run initIdentityTelemetry, which restores the
// previous global on cleanup.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	return recorder
}

// spanWithAttribute waits for a recorded span carrying key=value. The router
// ends its span after replying, so the span can still be in flight when
// nc.Request returns.
func spanWithAttribute(t *testing.T, recorder *tracetest.SpanRecorder, key, value string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	require.Eventually(t, func() bool {
		for _, span := range recorder.Ended() {
			for _, attr := range span.Attributes() {
				if string(attr.Key) == key && attr.Value.AsString() == value {
					found = span
					return true
				}
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "no recorded span carries %s=%s", key, value)
	return found
}

// TestRouter_HandlerSpanCarriesIdentity is the regression test for identity
// attributes silently vanishing from traces. The router dispatches each
// delivery to its own goroutine and returns, so otel-nats has already ended
// the consumer span it handed us by the time any middleware runs — a
// SetAttributes on it is a no-op (o11y docs/guide.md documents the same
// hazard for the JetStream handover path). The router must therefore own a
// span that actually covers the handler.
func TestRouter_HandlerSpanCarriesIdentity(t *testing.T) {
	initIdentityTelemetry(t)
	recorder := recordSpans(t)

	nc := startTestNATS(t)
	r := New(nc, "handler-span-test")
	const work = 20 * time.Millisecond
	Register(r, "chat.user.{account}.request.room.{roomID}.site-1.identity",
		func(_ *Context, _ testReq) (*testResp, error) {
			time.Sleep(work)
			return &testResp{Greeting: "ok"}, nil
		})

	data, err := json.Marshal(testReq{})
	require.NoError(t, err)
	_, err = nc.Request(context.Background(),
		"chat.user.weather_bot.request.room.room-42.site-1.identity", data, 2*time.Second)
	require.NoError(t, err)

	span := spanWithAttribute(t, recorder, obs.RoomIDKey, "room-42")

	attrs := make(map[string]string)
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	assert.Equal(t, "weather.bot", attrs["user.name"], "the subject's account belongs on the same span")
	assert.Equal(t, "room-42", attrs[obs.RoomIDKey])

	// The whole point: a span that ends before the handler runs would carry no
	// identity and would report a duration unrelated to the request.
	assert.GreaterOrEqual(t, span.EndTime().Sub(span.StartTime()), work,
		"the identity-bearing span must cover the handler, not end before it runs")
}
