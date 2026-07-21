package obs

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// dropExporter is a no-op SpanExporter: it accepts and discards batches so the
// benchmark pays the BatchSpanProcessor queue/serialize cost of a *sampled* span
// without a network collector (mirrors "export path is non-blocking").
type dropExporter struct{}

func (dropExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }
func (dropExporter) Shutdown(context.Context) error                             { return nil }

// mapCarrier is a tiny TextMapCarrier for the W3C extract/inject legs of a hop.
type mapCarrier map[string]string

func (c mapCarrier) Get(k string) string { return c[k] }
func (c mapCarrier) Set(k, v string)     { c[k] = v }
func (c mapCarrier) Keys() []string {
	ks := make([]string, 0, len(c))
	for k := range c {
		ks = append(ks, k)
	}
	return ks
}

// benchProvider builds a tracer for the given sampler, or a noop tracer when
// sampler is nil (the "tracing off" baseline).
func benchProvider(sampler sdktrace.Sampler) (trace.Tracer, func()) {
	if sampler == nil {
		return noop.NewTracerProvider().Tracer("bench"), func() {}
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithBatcher(dropExporter{}),
	)
	return tp.Tracer("bench"), func() { _ = tp.Shutdown(context.Background()) }
}

// benchHop runs one NATS-style hop: extract traceparent from an inbound carrier,
// start a detached CONSUMER span (the o11y/nats "deliver" model), stamp the
// standard messaging attributes, inject traceparent into an outbound carrier,
// end. This is the per-message unit whose cost the sampler controls.
func benchHop(b *testing.B, sampler sdktrace.Sampler) {
	b.Helper()
	tracer, shutdown := benchProvider(sampler)
	defer shutdown()
	prop := propagation.TraceContext{}

	// A realistic inbound carrier: an upstream producer's traceparent.
	inbound := mapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Extract the upstream span context and LINK to it, then start a DETACHED
		// root (WithNewRoot) — the o11y/nats consumer model. Because each hop is a
		// new root, a TraceIDRatioBased sampler decides per-hop on the new trace ID
		// (that is what makes the ratio actually reduce hot-path cost here).
		upstream := trace.SpanContextFromContext(prop.Extract(context.Background(), inbound))
		ctx, span := tracer.Start(context.Background(), "process chat.msg.canonical.created",
			trace.WithNewRoot(),
			trace.WithLinks(trace.Link{SpanContext: upstream}),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", "nats"),
				attribute.String("messaging.operation.name", "process"),
				attribute.String("messaging.destination.name", "chat.msg.canonical.site-a.created"),
			),
		)
		out := mapCarrier{}
		prop.Inject(ctx, out)
		span.End()
	}
}

// BenchmarkSpanHop_Off is the baseline: NATS tracing disabled (noop tracer).
func BenchmarkSpanHop_Off(b *testing.B) { benchHop(b, nil) }

// BenchmarkSpanHop_On_100pct is worst case: every hop sampled + queued to export.
func BenchmarkSpanHop_On_100pct(b *testing.B) { benchHop(b, sdktrace.AlwaysSample()) }

// BenchmarkSpanHop_Sampler_10pct / _1pct: parent-based ratio samplers — the knob
// the design recommends for production. Most root hops fall to a non-recording
// span (cheap); the ratio governs how much of the 100% cost is actually paid.
func BenchmarkSpanHop_Sampler_10pct(b *testing.B) {
	benchHop(b, sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.10)))
}

func BenchmarkSpanHop_Sampler_1pct(b *testing.B) {
	benchHop(b, sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.01)))
}
