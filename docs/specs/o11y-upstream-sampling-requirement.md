# Upstream requirement — consistent sampling across NATS links

**For:** maintainers of `github.com/flywindy/o11y` and its dependency
`github.com/Marz32onE/instrumentation-go/otel-nats` (the `otelnats` /
`oteljetstream` packages).

**From:** the `flywindy/chat` platform, which uses `o11y` v0.8.0 across ~16 Go
services communicating over NATS/JetStream.

**One-line ask:** provide a way for a message consumer's span to **inherit the
upstream producer's sampling decision** (the W3C `traceparent` sampled flag),
so that a logical flow spanning many NATS hops is sampled **as a unit** instead
of each hop deciding independently.

---

## 1. Background: the trace model

`otelnats` deliberately models each NATS hop as a **detached root + link**, not
parent-child, per the OTel messaging semantic conventions:

- a **producer** starts a `send <subject>` span and injects W3C `traceparent`
  into the message headers;
- a **consumer** extracts that context and starts a `<subject> deliver` /
  `process <subject>` span as a **new root** (fresh trace ID) carrying a **link**
  back to the producer's span context.

This gives clean, per-service traces navigable by links in Tempo. We are **not**
asking to change this model — separate traces per hop is desirable.

## 2. The problem: sampling is decided independently per hop

Head sampling (`ParentBased(TraceIDRatioBased(r))`, the standard production
sampler) decides on a span's **own trace ID**. Because each consumer span is a
**new root with a new trace ID**, every hop rolls the ratio **independently**:

- A single logical flow (browser → gatekeeper → message-worker / broadcast /
  notification ≈ 5 traces) is **not** sampled as a unit. At ratio `r`, each of
  its ~5 traces is kept with probability `r`, so at 10% a whole flow rarely
  survives — you get **fragments**.
- Collector-side **tail sampling does not fix this**: the OTel `tail_sampling`
  processor groups by **trace ID**, but these are *different* trace IDs joined
  only by span **links**; there is no standard policy that keeps a
  link-connected constellation together.

The origin's sampled flag **is** present (extracted into the origin
`SpanContext`, and attached as a link), but it is **not used** to drive the
consumer span's sampling decision.

### Current code (otel-nats v0.2.11, `otelnats/conn.go`)

```go
// ConsumerContextWithDeliver: starts the consumer-side "deliver" span.
func (c *Conn) ConsumerContextWithDeliver(ctx context.Context, subject string, origin trace.SpanContext) context.Context {
    ...
    // Empty parent → the deliver span is a NEW ROOT → sampled independently by the ratio.
    detachedCtx := trace.ContextWithSpanContext(ctx, trace.SpanContext{})
    _, deliverSpan := c.deliverTracer.Start(detachedCtx,
        subject+" deliver",
        trace.WithSpanKind(trace.SpanKindProducer),
        trace.WithAttributes(...),
        trace.WithLinks(trace.Link{SpanContext: origin}), // origin.IsSampled() is available but unused for sampling
    )
    deliverSpan.End()
    return trace.ContextWithRemoteSpanContext(detachedCtx, deliverSpan.SpanContext())
}
```

`origin.TraceFlags().IsSampled()` is known here but does not influence whether
`deliverSpan` is sampled.

## 3. Desired behavior

A logical flow should be sampled **consistently**: if the true entry point (the
browser, or the first backend hop) decides to sample, **every** downstream NATS
hop in that flow should also be sampled; if it decides to drop, all should drop.
Each hop keeps its **own trace ID** (the detached-root model is preserved) — only
the sampled **decision** propagates, carried by the existing `traceparent`
sampled flag.

Concretely: `deliverSpan`'s sampled bit should be derived from
`origin.TraceFlags().IsSampled()`, not from an independent ratio roll on the new
trace ID.

## 4. Proposed implementations (any one is acceptable)

1. **A "linked-parent" sampler** (preferred, opt-in): a `sdktrace.Sampler` that,
   for a root span carrying a link, returns `RecordAndSample` iff any link's
   `SpanContext.IsSampled()` is true (else defers to a wrapped sampler for true
   roots with no sampled link). Ship it from `o11y` and let `otelnats` create the
   `deliver` span through a tracer configured with it — or expose it as an o11y
   option (`WithLinkConsistentSampling()`), so `flywindy/chat` opts in via
   `pkg/obs`.

2. **Seed the root's `TraceState`/flags from the origin**: when starting the
   `deliver` span, copy the origin's sampled flag onto the new root's decision
   (e.g. via a custom `SpanContext` with the same `TraceFlags`, or a sampler that
   reads it). Keep the new `TraceID`.

3. **Config passthrough**: at minimum, expose whether the deliver span should
   "inherit sampled from origin link" as an `otelnats`/`o11y` option, defaulting
   off (current behavior) so it is non-breaking.

## 5. Acceptance criteria

- With the feature enabled and a ratio sampler (e.g. `parentbased_traceidratio`,
  arg `0.1`): a multi-hop flow whose entry span was **sampled** produces a
  **complete** constellation (every hop's trace present and linked); a flow whose
  entry was **dropped** produces **no** spans at any hop.
- The detached-root model is unchanged: each hop still has a **distinct trace
  ID**, joined by links.
- Backward compatible: default behavior (no opt-in) is identical to today
  (independent per-hop sampling).
- Works for JetStream `Consume`/`Fetch` and core-NATS subscribe consumer paths.

## 6. Consumer-side note

`flywindy/chat` already reads `OTEL_TRACES_SAMPLER[_ARG]` in `pkg/obs` and maps
them to o11y sampler options, so once this feature exists we would enable it via
one option/env and set the ratio in deploy — no per-service change. Until then we
run 100% in pre-production (see `docs/specs/o11y-performance-and-sampling.md`).
