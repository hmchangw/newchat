package obs

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// DeliveryScope is the instrumentation scope shared by every span that covers
// the processing of one NATS delivery, so they are queryable as one group
// regardless of which loop opened them.
const DeliveryScope = "github.com/hmchangw/chat/pkg/obs/delivery"

// DeliveryTracer returns the tracer a NATS consume loop opens its per-delivery
// span with. Resolve it ONCE, where the loop is set up (after Init, which
// installs the provider it reads) — not per message, which takes the
// provider's lock on the hot path — and keep it for the life of the loop.
//
// Why the loop needs a span of its own: it cannot use the one it is handed.
// otel-nats ends each pull delivery's receive span at handover, before the
// loop body runs, and natsrouter dispatches each request to its own goroutine,
// so by the time any handler code runs the span in ctx is already closed.
// Attributes set on a closed span are dropped — o11y docs/guide.md documents
// the no-op and prescribes exactly this span — which silently loses the
// identity ContextWithIdentity materializes, and leaves nothing measuring how
// long processing took.
//
// Start the span SpanKindInternal (the default): it covers in-process work,
// and a second CONSUMER span per delivery would double-count every consumer
// dashboard. Keep the span name bounded — derive it from the consumer or the
// subscription pattern, never from a delivered subject, which carries room ids
// and accounts.
//
// Identity already in ctx as baggage lands on the span automatically via the
// SDK's baggage span processor; identity established after the span starts
// reaches it through the ordinary SetAttributes path, which works precisely
// because this span is still live.
func DeliveryTracer() trace.Tracer {
	return otel.Tracer(DeliveryScope)
}
