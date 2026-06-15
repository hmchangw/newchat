# Per-Request NATS Debug Header

**Date:** 2026-06-12
**Status:** Approved — ready for plan
**Approach:** A leveled `X-Debug` header (`flow`/`debug`/`trace`) piggybacks on the existing `X-Request-ID` propagation plumbing; a context-aware slog handler emits verbose per-edge logs at/above the requested level for flagged requests, gated by a per-instance rate cap. The wire **token** is the only stable contract — the internal model can grow additively (more rungs, or a comma-list of facets) without breaking clients.

## Problem

NATS makes "why didn't my message take effect?" hard to answer because the transport fails silently:

- A core publish to a subject with no subscriber **succeeds with no error**.
- A publish to a JetStream subject that no stream's filter captures is **silently dropped**.
- Core publishes return **no ack**; JetStream `PubAck`s are not always checked on fire-and-forget paths.
- A reply to `chat.user.{account}.response.{requestId}` whose listener already went away is **published into the void**.

External client teams integrating against the NATS API hit these cases and have no server-side breadcrumb to follow. Steady-state logging is deliberately sparse (one line per message per service) to control volume, so the detail needed to diagnose a single failing message is not normally emitted.

This design adds an **opt-in, per-request** way to turn on verbose end-to-end tracing for *specific* messages, without raising the logging floor for production traffic.

## Goals

- A client can flag a single request and get a complete, server-side, structured trace of that request across every service it touches, joinable by `request_id`.
- Zero added log volume for un-flagged traffic.
- Safe to leave enabled in production — a misbehaving client that flags all its traffic cannot flood the logs.
- Uniform: the same mechanism works for sync request/reply handlers and for hand-rolled JetStream consumers.

## Non-Goals

- Streaming debug logs back to the client (client teams retrieve traces from the server log aggregator by `request_id`).
- A global, cross-replica rate cap (per-instance capping is sufficient; no shared Valkey state).
- Logging message bodies/content. Debug mode logs **metadata only** (see §4).
- Instrumenting every edge of every service in the first cut (see §5 rollout).

## Decisions

| Decision | Choice |
|----------|--------|
| Wire format | `X-Debug: flow\|debug\|trace` NATS header, rides alongside `X-Request-ID` (`1`/`true`/`on` alias `debug`; empty/`0`/`false`/unknown → off) |
| Verbosity ladder | `flow` = cross-service path + timing breadcrumbs (`LevelFlow = -2`); `debug` = + in-service decision branches (`DEBUG = -4`); `trace` = + per-item / per-recipient lines (`LevelTrace = -8`). Cumulative: `off < flow < debug < trace` |
| Timing | Folded **into** `flow`, not a separate token — timestamped in/out breadcrumbs (inter-hop latency by diff) + a `stream_wait_ms` field (JetStream queue lag). Cross-site deltas are clock-skew-approximate |
| Contract | Only the **token spelling + meaning** is permanent. Internal type is swappable; single-value→comma-list and new tokens are non-breaking supersets |
| Propagation | Reuse `pkg/natsutil` `HeaderForContext` / `NewMsg` — debug intent re-stamped onto every outbound message automatically |
| Verbose emission | Context-aware `slog.Handler` lets records at/above the request's threshold through even when the global level is `INFO` |
| Safety guard | Per-instance token-bucket rate cap; decision made **once per message** (all-or-nothing trace) |
| Limiter | `golang.org/x/time/rate` — promote the existing indirect dep (`go.mod` v0.15.0) to direct |
| Cap scope | In-process per replica; no shared state. Cap counts **messages**, not lines (trace weighting deferred) |
| Content | Metadata only — never bodies, tokens, or content |
| First cut | message-gatekeeper, message-worker, broadcast-worker, room-service (full), room-worker (entry + async-reply + per-user fan-out as `trace`) |

## Architecture

Two layers, mirroring how `X-Request-ID` already works:

1. **Propagation (intent)** — lives in `pkg/natsutil`. The requested *level* (`debug` or `trace`) travels on the `X-Debug` header and through `context.Context`, re-emitted onto every outbound message. This is the *intent*, and it flows unchanged across services so each service makes its own independent honor decision.
2. **Emission (honored)** — lives in a new `pkg/logctx`. At each boundary, the service consults its local rate limiter **once** and records the honored *threshold level* in the context. A context-aware slog handler lets records at/above that threshold through.

Keeping intent and honored separate is deliberate: the header propagates the client's requested level unchanged, while each replica independently rate-limits its own verbose output.

```
            X-Debug:1 on inbound msg
                     │
   ┌─────────────────▼──────────────────┐
   │ boundary (natsrouter middleware OR  │
   │ JetStream consumer entry)           │
   │  1. natsutil: parse X-Debug level   │
   │     → ctx carries requested level   │
   │  2. logctx.Admit: limiter.Allow()?  │
   │     → ctx carries honored threshold │
   └─────────────────┬──────────────────┘
                     │ ctx
   ┌─────────────────▼──────────────────┐
   │ handler                            │
   │  slog.Log(ctx, LevelFlow,  …)      │  ← path + timing; honored ≥ flow
   │  slog.DebugContext(ctx, …) edges   │  ← decision branch; honored ≥ debug
   │  slog.Log(ctx, LevelTrace, …)      │  ← per-item; honored = trace
   │  publish via natsutil.NewMsg(ctx)  │  ← re-stamps X-Debug level if requested
   └─────────────────┬──────────────────┘
                     │ X-Debug:<level> propagates downstream
                     ▼
              next service repeats
```

## Section 1: Propagation — `pkg/natsutil`

Add a debug *level* that travels exactly like `X-Request-ID`.

### `pkg/natsutil/debug.go` (new)

```go
// DebugHeader carries the requested verbose-logging level for this request.
const DebugHeader = "X-Debug"

// DebugLevel is the requested verbosity rung. Off is the zero value; the ladder
// is cumulative (each rung includes the ones below it).
type DebugLevel int
const (
    DebugOff   DebugLevel = iota
    DebugFlow             // "flow":  cross-service path + timing breadcrumbs
    DebugBasic           // "debug": + in-service decision branches
    DebugTrace           // "trace": + per-item / per-recipient lines
)

// ParseDebugLevel maps a header value to a rung (trimmed, case-insensitive):
//   "" / "0" / "false" / "off" / unknown → DebugOff
//   "flow"                               → DebugFlow
//   "1" / "true" / "on" / "debug"        → DebugBasic
//   "trace"                              → DebugTrace
// Strict by design: an unrecognized value is OFF, so "X-Debug: 0" never enables.
// Single-token today; accepting a comma-list later is a non-breaking superset
// (a lone token is a one-element list), so this is the sanctioned growth path.
func ParseDebugLevel(v string) DebugLevel

// String renders a rung to its canonical header token ("flow"/"debug"/"trace"/"").
func (l DebugLevel) String() string

// threshold maps a rung to the minimum slog level it admits — the ONLY place the
// ascending-rung → descending-slog inversion lives (off→INFO, flow→LevelFlow,
// debug→LevelDebug, trace→LevelTrace). Unexported in pkg/logctx (it needs the
// custom levels and has no caller outside the package); shown here for the full picture.

type debugKey int
const debugLevelKey debugKey = 0

// WithDebugLevel stores the requested level on ctx (DebugOff is a no-op).
func WithDebugLevel(ctx context.Context, l DebugLevel) context.Context

// DebugLevelFromContext returns the requested level, or DebugOff.
func DebugLevelFromContext(ctx context.Context) DebugLevel
```

### `HeaderForContext` change (`request_id.go`)

`HeaderForContext` currently emits only `X-Request-ID`. Extend it to also emit `X-Debug: <level>` (`DebugLevel.String()`) when `DebugLevelFromContext(ctx) != DebugOff`. Because **every** outbound publish in the instrumented services already builds its message via `natsutil.NewMsg(ctx, …)` (room-service `main.go:162,173`; room-worker `main.go:136`; the worker reply/publish callbacks), this single change propagates the requested level end-to-end with **no per-call-site work**.

`NewMsg` is unchanged — it already delegates to `HeaderForContext`.

## Section 2: Emission — `pkg/logctx` (new package)

A small package owning the rate limiter, the honored threshold, the custom log levels, and the context-aware slog handler. (`logctx` = context-scoped logging; descriptive, not a `utils`/`helpers` bucket.) It imports `pkg/natsutil`; `natsutil` does not import it (no cycle).

### Custom levels + the rung→threshold bridge

```go
// Two custom levels straddle the stdlib ones, giving the cumulative ladder
// off(INFO 0) > flow(-2) > debug(LevelDebug -4) > trace(-8).
const (
    LevelFlow  = slog.Level(-2) // cross-service path + timing breadcrumbs
    LevelTrace = slog.Level(-8) // per-item / per-recipient edges
)

// threshold is the single bridge from the ascending wire rung to the descending
// slog threshold. Nothing else open-codes the inversion.
func threshold(l natsutil.DebugLevel) slog.Level {
    switch l {
    case natsutil.DebugFlow:  return LevelFlow
    case natsutil.DebugBasic: return slog.LevelDebug
    case natsutil.DebugTrace: return LevelTrace
    default:                  return slog.LevelInfo // DebugOff and any unknown
    }
}
```

The base JSON handler's `HandlerOptions.ReplaceAttr` (`RenderLevelNames`) prints `LevelFlow`→`"FLOW"` and `LevelTrace`→`"TRACE"` (slog otherwise renders them `DEBUG-2` / `DEBUG-4`).

### Honored threshold + boundary helper

```go
// Admit decides — once — the verbose-logging threshold honored for this message
// on THIS instance. It:
//   1. parses X-Debug into a natsutil.DebugLevel; if != Off, stores it on ctx
//      (natsutil.WithDebugLevel) so the requested rung propagates downstream;
//   2. if a rung was requested AND the package rate limiter allows, stores the
//      honored slog threshold on ctx (= threshold(rung)).
// The honor decision is made once here so a message's verbose lines are
// all-or-nothing — never a half-emitted trace. Over budget: the requested rung
// still propagates, but nothing below INFO is emitted on this instance.
func Admit(ctx context.Context, headers nats.Header) context.Context

// honoredThreshold returns the admitted minimum level, or LevelInfo if none
// (handler-internal).
func honoredThreshold(ctx context.Context) slog.Level
```

### Context-aware slog handler

```go
// Handler wraps a base slog.Handler. Records at/above INFO pass through normally;
// sub-INFO records (FLOW, DEBUG, TRACE) pass ONLY when ctx was admitted to a
// threshold at or below the record's level.
type Handler struct{ base slog.Handler }

func NewHandler(base slog.Handler) *Handler

func (h *Handler) Enabled(ctx context.Context, lvl slog.Level) bool {
    if lvl >= slog.LevelInfo { return h.base.Enabled(ctx, lvl) }
    return lvl >= honoredThreshold(ctx) // FLOW/DEBUG/TRACE only for admitted requests
}
func (h *Handler) Handle(ctx context.Context, r slog.Record) error { return h.base.Handle(ctx, r) }
// WithAttrs / WithGroup delegate to base, re-wrapping.
```

Each service wraps its existing JSON handler at construction:

```go
base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel, ReplaceAttr: logctx.RenderLevelNames})
slog.SetDefault(slog.New(logctx.NewHandler(base)))
```

This is the only `main.go` logging change. Edges then call `slog.Log(ctx, logctx.LevelFlow, …)` / `slog.DebugContext(ctx, …)` / `slog.Log(ctx, logctx.LevelTrace, …)` idiomatically — no `if debug {…}` branching.

### Rate limiter

Package-level `golang.org/x/time/rate.Limiter` (promote the existing indirect dep `golang.org/x/time` v0.15.0 to direct). Configured once at startup:

```go
type Config struct {
    Rate  float64 `env:"RATE"  envDefault:"50"` // honored debug msgs/sec
    Burst int     `env:"BURST" envDefault:"50"`
}
func Configure(c Config) // sets the package limiter; default (unconfigured) honors nothing
```

Parsed in each service `main.go` with `env.ParseWithOptions(&cfg, env.Options{Prefix: "DEBUG_LOG_"})`, i.e. `DEBUG_LOG_RATE` / `DEBUG_LOG_BURST`. When the bucket is empty, `Admit` still stores the *requested rung* (so the intent keeps propagating downstream) but does not set the *honored threshold* (verbose lines suppressed on this instance; the normal one-line log is unaffected). The cap counts **messages** — a single honored `trace` message may emit many lines; weighting `trace` more heavily (or a separate trace cap) is a deferred tuning knob, not built now.

### Flow breadcrumbs & timing (the `flow` rung)

Timing is **not** a separate token — it has no meaning without the path to attach durations to, so it is folded into `flow`. The `flow` rung emits two breadcrumbs per service — one on receive, one on hand-off/outcome — each at `LevelFlow`. Timing then falls out almost for free:

- **Inter-hop transit + in-service time**: every breadcrumb is slog-timestamped. Diffing consecutive breadcrumbs for a `request_id` in the aggregator yields per-hop transit time; the receive→hand-off pair within a service yields its processing time. **No extra fields.**
- **Stream queue lag** (the one latency timestamp-diffing can't see — time spent waiting in JetStream before a consumer picked the message up): each consumer's receive breadcrumb carries `stream_wait_ms = time.Now().Sub(msg.Metadata().Timestamp)`. One field, high value in a JetStream pipeline.
- **Clock-skew caveat**: intra-site diffs share an NTP domain (often one host) and are trustworthy; **cross-site** diffs can mislead or go negative. Treat cross-site timing as approximate; the rigorous cross-site path is the future `sample` rung (real OTel trace context), not log-timestamp arithmetic.

`debug` and `trace` are strictly additive on top — they keep all `flow` breadcrumbs (including timing) and add decision branches / per-item lines.

## Section 3: Boundary wiring per entry-point type

The two layers attach at exactly the points that already resolve `X-Request-ID`:

- **natsrouter handlers** (room-service: all 20; room-worker: `serverCreateDM`). Extend `pkg/natsrouter`'s `RequestID()` middleware to call `logctx.Admit(ctx, c.Headers())` after `StampRequestID`. Every natsrouter handler then gets debug admission for free — **zero per-handler work**.
- **Hand-rolled JetStream consumers** (message-gatekeeper, message-worker, broadcast-worker, room-worker). Each already extracts the inbound header into context near its consume loop (e.g. room-worker `main.go:271`, message-gatekeeper consumer entry). Add **one line** there: `msgCtx = logctx.Admit(msgCtx, msg.Headers())`.

## Section 4: Content safety (hard constraint)

Debug edge logs carry **metadata only**:

- `request_id`, `subject`, inbound `X-Debug` rung
- payload **byte size** (never the payload)
- flow breadcrumb: `phase` (received/handed-off), outcome, `stream_wait_ms` (`flow`+)
- validation outcome / decision branch taken (`debug`+)
- latency derived from breadcrumb timestamps; recipient/fan-out **counts** (`debug`); per-recipient **identifiers** only at `trace`

Never the message body, tokens, JWTs, or content — per CLAUDE.md logging rules. Debug mode must not become a content-leak backdoor. The spec's review checklist and a semgrep-style scan (existing `log_audit` discipline) guard this.

## Section 5: Rollout / edge instrumentation

Shared plumbing (§1–§3) lands once and is available to all services. Verbose edges are added per service:

**First cut:**

Each edge is logged at the rung matching its volume: path/outcome breadcrumbs at `flow`, in-service decision branches at `debug`, per-item / per-recipient edges at `trace`.

| Service | `flow` breadcrumbs | `debug` edges | `trace` edges |
|---------|--------------------|---------------|---------------|
| message-gatekeeper | receive, publish-to-canonical / reject outcome (+`stream_wait_ms`) | each validation-reject branch | — |
| message-worker | receive, persisted, outbox-publish outcome (+`stream_wait_ms`) | persist (Cassandra/Mongo) decision detail | — |
| broadcast-worker | receive, fan-out outcome (`recipients=N`, +`stream_wait_ms`) | fan-out decision detail | per-recipient delivery line |
| room-service | per-handler entry + outcome | decision branches (~20–30) | — |
| room-worker | consumer entry, async-result reply outcome (+`stream_wait_ms`) | async-result two-phase detail | per-user subscription + per-site outbox fan-out lines |

Two payoffs from the rung split: `flow` answers "how far did it get / where's the latency" at O(services) volume — the cheapest, most-reached-for mode — while per-item edges at `trace` cost nothing unless a client sets `X-Debug: trace` on a single message (and even then are rate-capped), which is what lets the room-worker fan-out and broadcast per-recipient lines ship in the first cut.

**Deferred (adopt helpers incrementally):** notification-worker, search-service/search-sync-worker, inbox-worker, upload-service, user-presence-service, cross-site inbox rich edges. The `X-Debug` level already propagates through these from day one (via §1); they simply don't emit verbose lines yet.

## Section 6: Operational model

Debug output is server-side slog JSON on the normal log stream. A client team hits an issue, supplies the `request_id` (which they already generate and send), and a single filter in the log aggregator reconstructs the full cross-service path. No new transport, dashboard, or client-side change is required.

### Operational migration — `natsrouter.Logging()` INFO → FLOW

As part of this work, the always-on per-request `"nats request"` line emitted by the shared `natsrouter.Logging()` middleware is **demoted from INFO to the on-demand FLOW level**. It now appears only for flagged (`X-Debug`) requests, not on every RPC. This removes per-request log volume from unflagged traffic across **all four natsrouter services** (room-service, room-worker, search-service, history-service).

**Migration risk — requires ops sign-off before merge.** Any existing log-based dashboard or alert that greps `"nats request"` at INFO (e.g. RPC volume or p99-latency panels) will silently go dark. Steady-state per-RPC latency/throughput must instead come from metrics + OTel traces (both already emitted); error visibility is unaffected (`errcode.Classify` still logs at the boundary). Confirm no dashboard/alert depends on the INFO line, and include this change in the release notes.

## Sample output

Real lines from `message-gatekeeper` (first cut), produced by driving the handler through the production JSON + `logctx` handler. `time` is elided to `…`.

**`X-Debug: flow`** — happy path. Two breadcrumbs: hop entry (with the JetStream queue-wait latency that inter-hop timestamp-diffing can't see) and the canonical handoff. Diff the `time`s for in-service latency.

```json
{"time":"…","level":"FLOW","msg":"gatekeeper received","phase":"received","request_id":"01970a4f-…-789f","subject":"chat.user.alice.room.r1.site-A.msg.send","bytes":104,"stream_wait_ms":118}
{"time":"…","level":"FLOW","msg":"gatekeeper published to canonical","phase":"published","request_id":"01970a4f-…-789f","subject":"chat.msg.canonical.site-A.created","bytes":246}
```

**`X-Debug: debug`** — happy path. Cumulative: the `flow` breadcrumbs plus the in-service decision detail between them.

```json
{"time":"…","level":"FLOW","msg":"gatekeeper received","phase":"received",…,"stream_wait_ms":118}
{"time":"…","level":"DEBUG","msg":"gatekeeper subscription resolved","request_id":"…","roles":1}
{"time":"…","level":"DEBUG","msg":"gatekeeper large-room gate","request_id":"…","thread_reply":false,"bypassed":false}
{"time":"…","level":"FLOW","msg":"gatekeeper published to canonical","phase":"published",…}
```

**`X-Debug: debug`** — rejected (empty content). The `flow` `rejected` breadcrumb adds pipeline framing; the always-on `Classify` INFO line (last) already names the branch — so the per-branch detail is *not* duplicated at debug (note `cause` is the errcode **message**, never the body).

```json
{"time":"…","level":"FLOW","msg":"gatekeeper received","phase":"received",…}
{"time":"…","level":"FLOW","msg":"gatekeeper rejected","phase":"rejected","request_id":"…","reason":"bad_request"}
{"time":"…","level":"INFO","msg":"request failed",…,"code":"bad_request","reason":"","cause":"content must not be empty"}
```

**No `X-Debug`** — control: no `FLOW`/`DEBUG` lines at all. Zero added volume for unflagged traffic.

## Future axes (design space — NOT in scope, recorded only)

`flow` revealed that "debug" is not a single dial but a family of orthogonal axes. v1 implements one — **depth** (`off`/`flow`/`debug`/`trace`), with **timing** folded into `flow`. The others are recorded here so the design space is visible; each is an *additive* token (or token modifier), so none requires reopening the v1 contract. Build only on real demand.

| Axis | Question | Candidate token(s) | Notes |
|------|----------|--------------------|-------|
| Depth *(v1)* | how much detail? | `flow` / `debug` / `trace` | the implemented ladder; timing lives inside `flow` |
| Breadth | which subsystem? | `fanout`, `federation`, `auth`, `store`, `gate`, `all` | facets; would arrive as a **comma-list** (`X-Debug: fanout,federation`), the sanctioned single→set growth. Names must be domain words, not package names |
| Destination | where do diagnostics go? | `echo` | return the flow/timing summary in the **reply** envelope (for clients without log-aggregator access); self-limiting, no fan-out volume |
| Side-effects | real or rehearsal? | `dryrun` / `shadow` | validate + resolve recipients without persisting/delivering ("who *would* this reach?"); behavioral, needs hard no-half-commit guard |
| Backend | logs or traces? | `sample` | force-sample into the OTel trace backend; the rigorous cross-site timing path and the clean swap-in target for `flow` |
| Reasoning | why this branch? | `explain` | annotate decisions with the inputs that drove them; a refinement of `debug` |

Rejected on principle: a `tap`/`mirror` token (copy the payload somewhere inspectable) — it collides head-on with the §4 content-safety rule and must never exist.

## Testing (TDD)

Unit:
- `natsutil.ParseDebugLevel`: table-driven over `""`/`0`/`false`/`off`/`flow`/`1`/`true`/`on`/`debug`/`trace`/`garbage`/mixed-case/whitespace → expected rung; unknown → `DebugOff` (the no-footgun guarantee).
- `natsutil`: rung round-trips header→ctx→`NewMsg`/`HeaderForContext` (flow, debug, trace); `DebugOff` ctx → no `X-Debug` emitted.
- `logctx.threshold` (unexported): exhaustive over the four rungs → INFO/LevelFlow/LevelDebug/LevelTrace (locks the inversion in one place).
- `logctx.Handler`: FLOW/DEBUG/TRACE records emitted only when the record level ≥ honored threshold; `flow`-threshold passes FLOW but suppresses DEBUG/TRACE; `debug`-threshold passes FLOW+DEBUG, suppresses TRACE; INFO+ always delegates to base; `WithAttrs`/`WithGroup` preserve wrapping; `RenderLevelNames` prints `"FLOW"`/`"TRACE"`.
- `logctx.Admit` + limiter: honors up to burst then suppresses (table-driven over a fake clock / injected limiter); over-budget still stores requested rung but threshold stays INFO; missing/off header → neither set; each rung → its `threshold` when allowed.
- natsrouter `RequestID()` middleware: admits the rung from `c.Headers()`; downstream ctx carries requested rung + honored threshold.

Integration (`//go:build integration`, `testutil.NATS`):
- Publish a `flow`-flagged message through gatekeeper→canonical→broadcast; assert one receive + one outcome breadcrumb per service for that `request_id`, each carrying `stream_wait_ms`, and that `X-Debug` survives each hop (capture via a test subscriber + a `slog` capture handler); assert **no** decision-branch or per-recipient lines.
- Publish a `debug`-flagged message; assert decision-branch lines additionally appear, still no per-recipient lines.
- Publish a `trace`-flagged message; assert per-recipient/per-user lines additionally appear.
- Un-flagged control message produces only the steady-state one-line logs.

Coverage: ≥80% on `pkg/logctx` and the new `natsutil` functions (target 90%+ for `pkg/`).

## Files

New:
- `pkg/natsutil/debug.go` (+ `debug_test.go`)
- `pkg/logctx/logctx.go`, `pkg/logctx/handler.go`, `pkg/logctx/limiter.go` (+ tests)

Changed:
- `pkg/natsutil/request_id.go` — `HeaderForContext` emits `X-Debug: <level>`
- `pkg/natsrouter/middleware.go` — `RequestID()` calls `logctx.Admit`
- `main.go` of each first-cut service — wrap slog handler with `logctx.NewHandler` (+ `RenderTraceLevel`), parse `DEBUG_LOG_` config, call `logctx.Configure`
- JetStream consumer entry of message-gatekeeper / message-worker / broadcast-worker / room-worker — one `logctx.Admit` line
- handlers of first-cut services — `flow`/`debug`/`trace` edge lines
- `deploy/docker-compose.yml` of first-cut services — set `DEBUG_LOG_RATE` for local dev
- `go.mod` — promote `golang.org/x/time` to a direct require

No client-facing RPC schema change (the `X-Debug` header is an optional transport-level addition, not a request/response field) — `docs/client-api.md` gets a short §6/transport note describing the optional header, its `flow`/`debug`/`trace` values, that it is best-effort and rate-limited, and that responses/behavior are unchanged. Shipped in the same PR as the first cut.

## Resolved decisions (author review, 2026-06-12 / -13)

1. **Limiter:** `golang.org/x/time/rate`, promoting the existing indirect dep to direct.
2. **Header parsing → a rung, not a bool:** strict enum `off`/`flow`/`debug`/`trace` (`1`/`true`/`on` alias `debug`; unknown → off, no `X-Debug:0`-means-on footgun). Cumulative ladder.
3. **`flow` rung:** cheap O(services) path+outcome breadcrumbs — the primary "how far did it get / where's the latency" mode — sitting below `debug`. Earns its own rung because its edge set (cross-service hand-offs) is distinct.
4. **Timing folded into `flow`, not a separate token:** timestamped in/out breadcrumbs + `stream_wait_ms`; cross-site is skew-approximate. Timing has no meaning without a path, so it is not an independent axis.
5. **Contract = token spelling + meaning only.** Internal type is swappable; single-value→comma-list and new tokens are non-breaking supersets. This is the documented answer to "is it ordered or a set?": ship the ordered ladder now, grow to a facet comma-list later if a second high-volume facet appears — without breaking clients.
6. **Future axes** (breadth/destination/side-effects/backend/reasoning) recorded as design space only; not in scope.
7. **Docs:** documented in `docs/client-api.md` (§6/transport) in the same PR as the first cut, framed as best-effort/diagnostic.
