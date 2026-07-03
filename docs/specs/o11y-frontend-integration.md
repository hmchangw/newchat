# o11y — Frontend Integration Guide

How a chat **web client** wires into the platform's distributed tracing so its
NATS/HTTP calls join the same trace constellation as the backend services.

> **Audience.** The team building the production frontend. The current
> `chat-frontend` is a **test/reference implementation** — the code it ships in
> `src/lib/telemetry.ts` + call-sites is exactly the shape described here; treat
> those files as the canonical example to copy or adapt.
>
> **Read first:** `docs/specs/o11y-trace-design.md` §0 (the propagation model —
> why traces are link-based, not one shared trace ID). This guide is the
> *how-to*; that doc is the *what-it-should-look-like*.

---

## 0. TL;DR — do you need a frontend "SDK"? No.

**Do not build a parallel custom SDK like the backend `flywindy/o11y`.** The
official OpenTelemetry JS packages *are* the SDK. All you write is a thin
(~130-line) adapter that teaches OTel about the **one transport it doesn't know
out of the box: NATS-over-WebSocket** (`nats.ws`). Everything else — provider,
exporter, W3C context propagation, span lifecycle — comes from the official
packages.

| | Backend `flywindy/o11y` | Frontend |
|---|---|---|
| Signals | traces + metrics + logs (+ slog correlation) | **traces only** |
| Drivers wrapped | mongo/cassandra/redis/minio/ES/nats | none (NATS-WS + fetch) |
| Reuse scope | 16 services → worth an SDK | one app |
| Off-the-shelf | Go ecosystem fragmented → must package | **`@opentelemetry/sdk-trace-web` already packages it** |

The adapter's whole job: on **send** start a `CLIENT` span + inject `traceparent`
into NATS headers; on **receive** start a detached `CONSUMER` span with a **link**
+ extract `traceparent` from NATS headers. These two moves mirror the backend
`otelnats` producer/consumer model, so the browser legs stitch into the same
constellation via span links.

---

## 1. Dependencies

```jsonc
// package.json — versions the reference impl ships (track your app's OTel line)
"@opentelemetry/api":                     "^1.9.1",   // stable
"@opentelemetry/sdk-trace-web":           "^2.8.0",   // stable core
"@opentelemetry/sdk-trace-base":          "^2.8.0",
"@opentelemetry/resources":               "^2.8.0",
"@opentelemetry/exporter-trace-otlp-http":"^0.219.0"  // 0.x = OTel "experimental" line, normal
```

`@opentelemetry/api` must be a **single** version across the app (peer of every
other OTel package) — dedupe it if your bundler warns.

---

## 2. Configuration (runtime env)

The reference impl resolves config in priority order **`window.__APP_CONFIG__`
(prod, nginx `envsubst`) → `import.meta.env.VITE_*` (vite dev) → literal
default** (`src/lib/runtimeConfig.js`):

| Config key | `VITE_` dev var | Default | Meaning |
|---|---|---|---|
| `OTEL_ENABLED` | `VITE_OTEL_ENABLED` | `true` | master on/off; when false, all spans no-op |
| `OTEL_EXPORTER_OTLP_TRACES_URL` | `VITE_OTEL_EXPORTER_OTLP_TRACES_URL` | `http://localhost:4318/v1/traces` | OTLP/HTTP traces endpoint (collector) |
| `OTEL_SERVICE_NAME` | `VITE_OTEL_SERVICE_NAME` | `chat-frontend` | `service.name` resource attr |

Point `OTEL_EXPORTER_OTLP_TRACES_URL` at the collector reachable **from the
browser** (CORS matters — see §6). In prod that is usually a collector behind
the same ingress, not `localhost`.

---

## 3. Initialize once, at the entry point

Call `initTelemetry()` **before** the app renders — outside React, in the entry
module (`src/main.jsx`), so it runs once and is not subject to `StrictMode`
double-invoke:

```js
// main.jsx
import { initTelemetry } from './lib/telemetry'
initTelemetry()                 // before createRoot(...).render(...)
```

The init itself (`src/lib/telemetry.ts`):

```ts
export function initTelemetry(): void {
  if (initialized || !OTEL_ENABLED || typeof window === 'undefined') return
  const exporter = new OTLPTraceExporter({ url: OTEL_EXPORTER_OTLP_TRACES_URL })
  const provider = new WebTracerProvider({
    resource: resourceFromAttributes({
      'service.name': OTEL_SERVICE_NAME,
      'service.namespace': 'chat',
      'service.version': '0.0.1',            // TODO: inject at build time (follow-up F3)
      'deployment.environment': 'local',     // TODO: from env (follow-up F3)
    }),
    spanProcessors: [
      new BatchSpanProcessor(exporter, { scheduledDelayMillis: 1000, exportTimeoutMillis: 5000 }),
    ],
  })
  provider.register()   // installs global tracer + W3C TraceContext propagator
  initialized = true
}
```

`provider.register()` installs the **global W3C `TraceContext` propagator** —
that is what makes `propagation.inject`/`extract` below speak `traceparent`. You
do not set a propagator manually.

---

## 4. The four primitives (copy these)

All live in `src/lib/telemetry.ts`. A production frontend can lift this file
almost verbatim.

### 4.1 Inject — producer side

```ts
export function injectTraceHeaders(headers: { set(k: string, v: string): void }) {
  propagation.inject(context.active(), headers, { set: (c, k, v) => c.set(k, v) })
  return headers
}
```

**Must be called while the producing span is active** (i.e. *inside* the
`withSpan` callback), because it injects `context.active()`.

### 4.2 `withSpan` — CLIENT span for publish / request / fetch

```ts
withSpan(name, attributes, fn, kind = SpanKind.CLIENT)
// starts an active span, runs fn (sync or Promise), records exceptions, ends the span
```

### 4.3 `withLinkedSpan` — detached CONSUMER span for received messages

```ts
export function withLinkedSpan(name, attributes, headers, fn, kind = SpanKind.CONSUMER) {
  const extracted = headers ? propagation.extract(ROOT_CONTEXT, headers, headerGetter) : ROOT_CONTEXT
  const linked = trace.getSpanContext(extracted)
  const links = linked && trace.isSpanContextValid(linked) ? [{ context: linked }] : []
  return tracer.startActiveSpan(name, { kind, attributes, links }, ROOT_CONTEXT, (span) => runInSpan(span, fn))
}
```

Rooted at `ROOT_CONTEXT` (**detached — a new trace**) with a **link** back to
the upstream producer. This is the deliberate mirror of the backend's detached
`deliver` span; it is *not* a bug that the receive side is a separate trace.

### 4.4 `natsSpanName` — naming convention

```ts
export const natsSpanName = (op, subject) => `nats ${op} ${subject}`
```

Span name = **`nats <operation> <subject>`** so a Tempo trace list is legible at
a glance (`nats publish chat.user.alice.room.r1.site-a.msg.send`) instead of a
wall of `nats.request`. The subject is *also* on the attribute (§5) — filter by
either.

---

## 5. Wiring rules (where to wrap)

Wrap at the **transport boundary**, once — not in every component. In the
reference impl that boundary is `NatsContext` (publish/subscribe) and
`api/_transport/asyncJob.ts` (request / two-phase async). Components call
`api/<operation>/…` and get tracing for free.

| Transport call | Wrap with | Span name | `SpanKind` |
|---|---|---|---|
| `nc.publish(subject, …)` | `withSpan` + `injectTraceHeaders` | `nats publish <subject>` | CLIENT |
| `nc.request(subject, …)` (sync) | `withSpan` + `injectTraceHeaders` | `nats request <subject>` | CLIENT |
| two-phase async request | `withSpan` + `injectTraceHeaders` | `nats request_async_result <subject>` | CLIENT |
| `nc.subscribe(subject)` delivery | `withLinkedSpan` (extract `msg.headers`) | `nats receive <msg.subject>` | CONSUMER |
| async-result delivery (`response.{reqId}`) | `withLinkedSpan` (extract `msg.headers`) | `nats receive <subject>` | CONSUMER |
| outbound `fetch` (auth/portal/upload) | `withSpan` + `injectTraceHeaders` | `http.client` | CLIENT |

### Attribute conventions (OTel messaging semconv)

```ts
{
  'messaging.system': 'nats',
  'messaging.operation.name': 'publish' | 'request' | 'receive',
  'messaging.destination.name': subject,           // the concrete subject
  'messaging.subscription.name': subscribedSubject, // receive only (the pattern subscribed to)
  'chat.request_id': requestId,                     // request / async only
}
```

### Producer pattern (publish)

```js
withSpan(natsSpanName('publish', subject), {
  'messaging.system': 'nats',
  'messaging.operation.name': 'publish',
  'messaging.destination.name': subject,
}, () => {
  const h = buildHeaders() ?? natsHeaders()
  injectTraceHeaders(h)                 // inject WHILE span is active
  nc.publish(subject, encode(data), { headers: h })
})
```

### Consumer pattern (subscribe delivery)

```js
for await (const msg of sub) {
  const msgSubject = msg.subject || subscribedSubject
  withLinkedSpan(natsSpanName('receive', msgSubject), {
    'messaging.system': 'nats',
    'messaging.operation.name': 'receive',
    'messaging.destination.name': msgSubject,
    'messaging.subscription.name': subscribedSubject,
  }, msg.headers, () => {
    // dispatch/parse here — this runs inside the receive span
  }, SpanKind.CONSUMER)
}
```

The receive span wraps only the **synchronous dispatch** of the message. It
does not cover downstream React re-renders — that is intentional; the span means
"message received & handed off", matching the design.

---

## 6. What the backend already guarantees (so it "just works")

You do not have to configure these — but know they exist:

1. **CORS allows trace headers.** Backend gin CORS (`pkg/ginutil`) permits
   `traceparent`, `tracestate`, `baggage` on cross-origin HTTP, and OPTIONS
   preflight is handled *before* the server-span middleware (no preflight noise
   in Tempo). Your OTLP collector endpoint must also allow the browser origin.
2. **NATS tracing gate is on.** Backend `pkg/natsutil.Connect` force-enables
   `OTEL_INSTRUMENTATION_GO_TRACING_ENABLED` + `OTEL_NATS_TRACING_ENABLED`, so
   backend producers inject `traceparent` into NATS headers and consumers emit
   `deliver`/`process` spans with links. Without this the browser's injected
   context would be dropped at the first backend hop.
3. **Propagation model.**
   - browser → backend over **NATS** = **span link** (new trace each hop).
   - browser → backend over **HTTP** (`auth`/`portal`/`upload` via `o11y/gin`)
     = **parent-child** (genuinely one trace).
   - Go backend request/reply paths using `o11y/nats.Conn.Request` +
     `Conn.Respond` also get a requester-side reply receive span. Browser NATS
     request spans still capture the RTT locally; their backend correlation is
     link-based on the request leg.

---

## 7. Verify

With the local o11y stack up (Tempo + Grafana), open Grafana Explore → Tempo and
run (full checklist in `docs/specs/o11y-local-trace-verification.md`):

```traceql
{ resource.service.name = "chat-frontend" }
{ name =~ "nats publish .*" }
{ name =~ "nats receive .*" }
{ span.messaging.destination.name =~ "chat.*" }
```

Then confirm the scenario shapes in `docs/specs/o11y-trace-design.md` §1–§7: a
browser `nats publish/request` span at the head, each backend `process` span
carrying a **link** back to it, and (for delivery) a browser `nats receive` span
linked to the broadcast-worker producer.

Sanity: `{ name = "OPTIONS" }` should return **no** auth/portal/upload preflight
traces (CORS runs before tracing).

---

## 8. Gotchas

- **Init outside render.** Call `initTelemetry()` in the entry module before
  `createRoot`, guarded by an `initialized` flag — never inside a component/effect
  (StrictMode + re-mounts would double-register).
- **Inject inside the active span.** `injectTraceHeaders` reads `context.active()`;
  call it within the `withSpan` callback, after the span starts.
- **Detached receive is correct.** `withLinkedSpan` uses `ROOT_CONTEXT` on
  purpose — a linked new trace, not a child. Do not "fix" it into a child.
- **`headerGetter.keys()` returns `[]`.** Fine for W3C `TraceContext` (extract
  only calls `get`). Only matters if you later add baggage/other propagators.
- **`OTEL_ENABLED=false` is a hard off switch.** Everything must degrade to a
  no-op (the reference `withSpan`/`withLinkedSpan` still run `fn`, just without a
  real provider) — never let a disabled SDK break a publish/subscribe.
- **Batch export delay.** Spans flush on a 1s batch timer; a trace may take ~1–2s
  to appear in Tempo. For a hard page-unload flush, see follow-up F4.

---

## 9. Follow-ups (optional — none block NATS tracing)

Tracked here for the production frontend; **all use official OTel packages, none
require a custom SDK.**

| ID | Item | Why / when |
|---|---|---|
| **F1** | Auto HTTP instrumentation — `@opentelemetry/instrumentation-fetch` / `-xml-http-request` | replaces the manual `http.client` span; automatic fetch/XHR spans for auth/portal/upload. Manual is lighter; adopt if HTTP surface grows. |
| **F2** | RUM / page observability — `instrumentation-document-load`, `-user-interaction` (or `auto-instrumentations-web`) | page-load + click spans. A product decision (real-user monitoring), unrelated to NATS tracing. |
| **F3** | Real resource attrs — inject `service.version` (git SHA) + `deployment.environment` at build/runtime instead of the hardcoded `0.0.1`/`local` | correct release/env attribution in Tempo/Grafana. Small, worth doing before prod. |
| **F4** | Unload flush — `provider.forceFlush()` on `visibilitychange`/`pagehide` | avoid losing the last ~1s of spans when a user navigates away mid-flow. |
| **F5** | Error capture — record exceptions on a span from `window.onerror` / `unhandledrejection` | closes the gap where the React `ErrorBoundary` can't catch async/handler errors (see `chat-frontend/CLAUDE.md`). |
| **F6** | Sampling policy — a `Sampler` / parent-based ratio instead of the default always-on | control span volume/cost at scale. Config, not new code. |
| **F7** | Backend request/reply limitation (context) — a bare `context.Background()` caller still needs an ambient caller span if you want send + reply receive in the same trace | SDK limitation to document/avoid at call sites; frontend NATS request spans already capture RTT. |

---

## 10. Reference files (test frontend)

| Concern | File |
|---|---|
| Adapter (init + 4 primitives) | `chat-frontend/src/lib/telemetry.ts` |
| Config resolution | `chat-frontend/src/lib/runtimeConfig.js` |
| Init call site | `chat-frontend/src/main.jsx` |
| Publish / subscribe wiring | `chat-frontend/src/context/NatsContext/NatsContext.jsx` |
| Request / two-phase async wiring | `chat-frontend/src/api/_transport/asyncJob.ts` |
| Tests (span-name + linked-receive assertions) | `…/NatsContext.test.jsx`, `…/asyncJob.test.js` |

See also: `docs/specs/o11y-trace-design.md` (expected traces),
`docs/specs/o11y-local-trace-verification.md` (verification),
`docs/specs/o11y-followups.md` (backend follow-ups).
