# o11y — Frontend Client Library: Build-vs-Buy Evaluation

Should the hand-wired frontend tracing in `chat-frontend` become a reusable
library, and does an off-the-shelf one already exist — for **JS/TS (web)** and
for **Swift (iOS app)**?

> **Verdict up front.** For the NATS leg: **nothing off-the-shelf exists, in
> either language.** We must build it. For everything else (provider, exporters,
> W3C propagation, HTTP, RUM): official OTel packages already cover it and we
> should not write a line of it.
>
> The right deliverable is **not an SDK** — it is a ~200-line *traced NATS
> connection wrapper* per language, plus **one shared spec** that keeps the two
> implementations from drifting.
>
> Read first: `o11y-frontend-integration.md` (the current how-to),
> `o11y-trace-design.md` §0 (why traces are link-based).

---

## 1. Where we are today

`chat-frontend/src/lib/telemetry.ts` (141 lines) exposes four loose primitives —
`initTelemetry`, `withSpan`, `withLinkedSpan`, `injectTraceHeaders`,
`natsSpanName` — and every transport call site assembles them by hand.

| Call site | Lines touching OTel |
|---|---|
| `src/context/NatsContext/NatsContext.jsx` | publish (`withSpan` + inject) + subscribe (`withLinkedSpan`) |
| `src/api/_transport/asyncJob.ts` | request, `request_async_result`, async-result receive |

**The handcraft, precisely.** For a single traced publish the author must get
four independent things right:

1. call `withSpan` with the `nats <op> <subject>` name (via `natsSpanName`),
2. hand-build the messaging semconv attribute bag
   (`messaging.system` / `.operation.name` / `.destination.name` /
   `.subscription.name`),
3. call `injectTraceHeaders` **inside** the callback — outside it silently
   injects the wrong (or no) context, and nothing fails loudly,
4. remember to wrap the subscribe iterator too, or the receive leg vanishes.

That is four chances to be silently wrong, already duplicated across two files,
and a third copy is needed for every new transport helper. Step 3 in particular
is a correctness trap that no test at the call site will catch — the span still
exists, it just isn't linked to anything.

**Also true, and worth stating:** the existing code is *good*. It is small,
correct, and tested. The problem is not quality — it is that the knowledge lives
in call sites instead of behind an interface, so it cannot be reused and cannot
be ported to Swift without being re-derived.

---

## 2. "Is frontend o11y only traces?"

Today, yes — by explicit choice (`o11y-frontend-integration.md` §0). But that is
our scope decision, not a platform limit. OTel JS in the browser supports all
three signals:

| Signal | Browser package | Status | Verdict for us |
|---|---|---|---|
| **Traces** | `@opentelemetry/sdk-trace-web` 2.10.x | **stable** | in use — keep |
| **Logs** | `@opentelemetry/sdk-logs` + `exporter-logs-otlp-http` 0.221.x | experimental (0.x line) | **the real gap.** Frontend errors/warnings → Loki, already trace-correlated. Highest-value addition. |
| **Metrics** | `@opentelemetry/sdk-metrics` + `exporter-metrics-otlp-http` | stable SDK, browser use is awkward | **skip.** Per-browser-session metrics have no meaningful aggregation window and unbounded cardinality; emit these as spans/logs and aggregate server-side or in the collector. |
| **RUM** (page-load, user-interaction, web-vitals) | `instrumentation-document-load`, `-user-interaction`, `auto-instrumentations-web` | stable-ish | product decision, orthogonal to NATS tracing (already tracked as follow-up **F2** in the integration doc) |

So the honest answer: **traces-only is a deliberate current scope; logs are the
one signal genuinely worth adding**, and the library should be shaped so adding
a `LoggerProvider` later is a config flag, not a redesign.

Same picture on Swift: `opentelemetry-swift-core` declares **tracing, baggage,
logs and metrics all stable** — so the Swift side is not the constraint.

---

## 3. Off-the-shelf survey

### 3.1 NATS instrumentation — nothing exists

| Ecosystem | Finding |
|---|---|
| OTel JS contrib | [`opentelemetry-js-contrib#753`](https://github.com/open-telemetry/opentelemetry-js-contrib/issues/753) — "Instrumentation for nats", opened **2021-11-23**, labelled `up-for-grabs`, **zero linked PRs after ~4.5 years**. |
| npm registry | `@opentelemetry/instrumentation-nats`, `opentelemetry-instrumentation-nats`, `opentelemetry-nats`, `nats-opentelemetry`, `otel-nats`, `@nats-io/opentelemetry` → **all 404**. Keyword searches for "nats tracing" return only the NATS clients themselves and unrelated vendor tracers. |
| `nats.js` / `nats.ws` upstream | No interceptor, middleware, or hook surface. Tracing has to wrap the connection object from outside. |
| Swift | No OTel NATS package of any kind. |
| Go (for reference) | Even our backend does **not** use an upstream package — `otelnats`/`oteljetstream` are vendored inside `flywindy/o11y`. There is no cross-language reference implementation to port. |

**Conclusion: build. This is the only genuinely custom piece, in both
languages.** It is also small — the whole contract is "inject `traceparent` into
NATS headers on send, extract + link on receive."

### 3.2 Everything else — buy (i.e. use the official packages)

| Concern | JS/Web | Swift/iOS |
|---|---|---|
| API + SDK | `@opentelemetry/api` 1.9.1, `sdk-trace-web` 2.10.x — **stable** | `opentelemetry-swift-core` 2.4.x (API/SDK) — **tracing stable** |
| OTLP export | `exporter-trace-otlp-http` 0.221.x — mature, in production use | `opentelemetry-swift` OTLP exporters. ⚠️ **gRPC is production-ready; OTLP/HTTP is still marked experimental** — but HTTP is the practical mobile choice (mobile networks throttle/block non-standard ports). Accept and pin the version. |
| W3C propagation | `provider.register()` installs it — nothing to write | `W3CTraceContextPropagator` in `OpenTelemetryApi/Trace/Propagation/` — `inject(spanContext:carrier:setter:)` / `extract(carrier:getter:)` over a `[String: String]` carrier |
| Detached + linked consumer span | `startActiveSpan(..., {links}, ROOT_CONTEXT, …)` | `SpanBuilder.setNoParent()` + `.addLink(spanContext:)` — **verified present in `SpanBuilderBase`** |
| HTTP client spans | `instrumentation-fetch` / `-xml-http-request` (follow-up F1) | `URLSessionInstrumentation` (ships in `opentelemetry-swift`) |
| RUM / crash / session | optional: **Grafana Faro Web SDK** — OTel bridge, exports OTLP/HTTP to any collector (not Grafana-Cloud-locked) | optional: **Embrace Apple SDK** (OTel-native, standard `SpanExporter`/`LogRecordExporter`) or **`grafana/faro-otel-swift-exporter`** |

The two Swift APIs our design depends on (`setNoParent` + `addLink`, and the
W3C propagator's `[String:String]` carrier) were checked against upstream source
— they exist, and they map **1:1** onto the JS adapter we already ship. The
design ports cleanly.

### 3.3 The Swift NATS client — the actual project risk

| | Status |
|---|---|
| `nats-io/nats.swift` (official) | **v0.4.0, pre-1.0.** Core NATS + auth + TLS + lame-duck; **headers supported** (`NatsHeaderMap`) — which is all our tracing needs. JetStream management + pull consumers landed in 0.4.0. |
| WebSocket transport | **Not supported.** A native iOS app can use raw TCP, so this only bites if corporate networks force WS — unlike the browser, which has no choice. **Confirm this against the target deployment before committing.** |
| Alternatives | `aus-der-Technik/SwiftyNats`, `hjuraev/nats-swift` — community, thinner support |

The pre-1.0 API surface, not the tracing, is what will move the Swift estimate.

---

## 4. Recommendation

### 4.1 Shape: a traced *connection*, not a bag of primitives

The ask — *"init 之後就自動把 NATS trace 串好"* — is satisfied by moving the
wrapping from the call site into a decorator over the NATS connection:

```ts
// today — every call site repeats this
withSpan(natsSpanName('publish', subject), { /* 3 attrs */ }, () => {
  const h = buildHeaders() ?? natsHeaders()
  injectTraceHeaders(h)              // ← must be inside; nothing enforces it
  nc.publish(subject, encode(data), { headers: h })
})

// proposed — two lines at startup, then nothing
initTelemetry(config)
const nc = traceNats(await connect(opts))
nc.publish(subject, payload)         // traced. headers injected. no OTel import.
```

`traceNats` returns an object satisfying the same `NatsConnection` interface, so
**existing call sites need zero changes** and no file outside the library
imports `@opentelemetry/*`. `subscribe()` returns a wrapped async-iterable that
opens the detached, linked `CONSUMER` span per delivered message.

Swift mirror, same contract:

```swift
try ChatObservability.start(config)
let nc = TracedNatsClient(wrapping: client)
```

### 4.2 Deliverables

1. **`@chat/o11y-web`** (npm, TS) — `initTelemetry`, `traceNats`, `traceFetch`,
   `shutdown`. The escape hatches (`withSpan`, `withLinkedSpan`) stay exported
   for anything the wrapper can't reach.
2. **`ChatObservability`** (SwiftPM) — same five entry points.
3. **One spec doc** defining span names, `SpanKind`, the attribute set, and the
   detached-link rule — the *only* thing the two implementations actually share.

### 4.3 Be honest about what "a library" buys here

TypeScript and Swift cannot share code. This is **two implementations of one
spec**, not one library — so the value is narrower than it first sounds:

- ✅ Call sites stop touching OTel. The inject-inside-the-span trap disappears
  structurally rather than by review discipline.
- ✅ Semconv attributes and span names live in one place per language, so web
  and iOS traces stay queryable by the same TraceQL.
- ✅ Cross-cutting policy (sampling, flush-on-background, kill switch) becomes
  configuration instead of scattered code.
- ❌ It does **not** halve the Swift work. The Swift side is ~all net-new
  regardless; the spec saves design time, not implementation time.
- ⚠️ Reuse count for the **NATS** part is exactly two (web + iOS).
  `admin-frontend` is HTTP-only — its `package.json` dependencies are literally
  `react` + `react-dom`, with no OTel at all — so it would consume only
  `initTelemetry` + `traceFetch`.

Two consumers is a thin margin for a library. It clears the bar here because the
second consumer is in a *different language*, which makes a written spec
necessary anyway — and once the spec exists, the library is the cheap part.

### 4.4 Effort

| | Estimate | Drivers |
|---|---|---|
| `@chat/o11y-web` | **2–3 days** | Adapter logic already exists and is tested. Work is the wrapper, packaging, and migrating two call sites. |
| Spec doc | **0.5 day** | Mostly extraction from `o11y-frontend-integration.md` §4–§5. |
| `ChatObservability` (Swift) | **1–1.5 weeks** | All net-new: SPM setup, OTel-Swift wiring, propagator glue, connection wrapper, tests — plus absorbing `nats.swift` pre-1.0 churn. |

### 4.5 Fold in while building (don't retrofit later)

- **Flush on teardown** — `forceFlush()` on `visibilitychange`/`pagehide` (web,
  follow-up F4) and on `scenePhase == .background` (iOS). Mobile background
  transitions lose spans far more aggressively than a browser tab does.
- **Sampling** — parent-based ratio, *but note the trap*: because NATS hops are
  link-based, head sampling fragments a flow into partially-sampled
  constellations, and collector tail sampling **cannot** repair it (different
  trace IDs, joined only by links). See
  `o11y-upstream-sampling-requirement.md`. Keep the frontend always-on until
  that upstream fix lands.
- **`OTEL_ENABLED=false` stays a hard off switch** — the wrapper must return the
  *raw* connection unwrapped, so a disabled SDK is zero overhead and cannot break
  publish/subscribe.
- **`chat.request_id` as a span attribute** on both sides, mirroring backend
  follow-up F6, so the trace ↔ request-id pivot works in both directions.
- **Error capture** — `window.onerror` / `unhandledrejection` (web, F5);
  `NSSetUncaughtExceptionHandler` or a RUM SDK (iOS).

---

## 5. Decision summary

| Question | Answer |
|---|---|
| Off-the-shelf NATS tracing for JS? | **No.** OTel JS contrib issue open 4.5 years, `up-for-grabs`, no PR; no npm package exists. |
| Off-the-shelf NATS tracing for Swift? | **No.** Nothing at all. |
| Off-the-shelf OTel base for Swift? | **Yes** — `opentelemetry-swift` 2.4.x, tracing stable. OTLP/HTTP exporter still experimental; pin it. |
| Do we need a backend-style custom SDK? | **No.** Wrap the one transport OTel doesn't know (NATS); take everything else from official packages. |
| Frontend signals? | Traces today. **Logs are the gap worth closing**; metrics are not worth it in a browser; RUM is a separate product call. |
| Biggest risk? | Not the tracing — it's `nats-io/nats.swift` being **pre-1.0 with no WebSocket transport**. Validate the iOS transport story before committing to the Swift package. |
