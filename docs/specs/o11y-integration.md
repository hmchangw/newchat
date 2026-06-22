# Spec: Integrate the `flywindy/o11y` Observability SDK

> **Status:** PLANNING (decisions locked). This document is the design/rollout
> plan for adopting [`github.com/flywindy/o11y`](https://github.com/flywindy/o11y)
> as the single observability entry point across the chat platform. No code has
> landed yet. Branch: `feat/integrate-o11y-sdk`.
>
> **Scope premise:** the whole platform is still pre-production (in testing, no
> live users), so there is **no migration-risk or user-impact constraint**. The
> goal is to have a *complete* o11y stack in place before the first production
> release. This permits a full, all-services replacement rather than a
> risk-gated incremental rollout.
>
> **Locked decisions** (see §4): **D1** thin `pkg/obs` wrapper · **D2** fully
> replace Marz32onE with `o11y/nats` (no transitional period) · **D3** OTLP/HTTP
> · **D4** convert all services in one sweep (no pilot gating).

---

## 0. Context

`o11y` is a Go 1.25 observability SDK that unifies three pillars behind one
constructor (`o11y.Init`) with **zero global side effects**:

- **Tracing** — OpenTelemetry SDK over OTLP/HTTP.
- **Logging** — `log/slog` with **automatic `traceId`/`spanId` correlation**, dual
  output (OTLP/HTTP → Loki + JSON stdout).
- **Metrics** — Prometheus pull (`:2112` default) or OTLP push, plus Go runtime metrics.
- **Profiling** — optional Pyroscope (doubly gated, opt-in).

It ships per-domain instrumentation sub-packages — `gin`, `http`, `resty`,
`mongo`, `redis`, `cassandra`, `elasticsearch`, `nats`, `minio` — which map
almost 1:1 onto this platform's stack (see CLAUDE.md §1). That alignment is why
this integration is mostly *substitution* rather than *new code*.

---

## 1. Goal

Replace the platform's hand-rolled, partial observability wiring with a single,
consistent, trace-correlated stack driven by `o11y`, **without changing service
behaviour**, via a layered all-services rollout (D4).

Concretely, after this work:

1. Every service initializes observability through one internal wrapper
   (`pkg/obs`) instead of `pkg/otelutil` + a hand-built `slog` handler.
2. Logs carry `traceId`/`spanId` automatically and stay correlated with the
   existing request-ID propagation (`pkg/logctx`, `pkg/natsrouter`).
3. Datastore and transport clients (Mongo, Cassandra, Valkey, MinIO,
   Elasticsearch, NATS, Gin, Resty) are instrumented through `o11y`'s wrappers,
   wired in the **shared `pkg/* connect helpers`** so per-service churn is minimal.
4. `pkg/otelutil`, the manual `slog.SetDefault` blocks, and the third-party
   `Marz32onE/instrumentation-go/otel-nats` dependency are removed.

### Non-goals

- No new business metrics/dashboards beyond what the SDK emits out of the box
  (tracked as follow-up — see §11).
- No collector/Loki/Tempo/Pyroscope infrastructure provisioning — that is an
  ops/IaC concern; this plan only states the endpoints/env the services expect.
- No change to NATS subject naming, stream topology, or domain logic.

---

## 2. Current State (what we are replacing)

| Concern | Today | Footprint |
|---------|-------|-----------|
| Tracing | `otelutil.InitTracer` — OTLP/**gRPC** (`otlptracegrpc`), global `TracerProvider`, no-op without endpoint env | 14 service `main.go` |
| Metrics | `otelutil.InitMeter` — Prometheus exporter, global `MeterProvider` | 14 service `main.go` |
| Logging | Per-service `slog.SetDefault(slog.NewJSONHandler(os.Stdout, nil))`, **no trace correlation** | 13 services |
| NATS instrumentation | `Marz32onE/instrumentation-go/otel-nats` (`oteljetstream`, `otelnats`) | 24 files incl. shared `pkg/natsutil`, `pkg/natsrouter` |
| Request-ID logging | `pkg/logctx` + `pkg/natsrouter` middleware (UUIDv7 `X-Request-ID`) | shared pkg |
| Datastore tracing | None beyond NATS — Mongo/Cassandra/Valkey/MinIO/ES connect uncorrelated | `pkg/mongoutil`, `pkg/cassutil`, `pkg/valkeyutil`, `pkg/minioutil`, `pkg/searchengine` |
| Health endpoint | `pkg/health` (`Register`/`Serve`) mounts **only** `/healthz` + `/readyz` — no `promhttp`, no `/metrics` | all services |
| Metrics endpoint | **Fragmented:** most services expose no `/metrics` at all (incl. `history-service`, which has a global `MeterProvider` but never serves it); only `search-service` and `oplog-connector` build their own `promhttp` `metricsMux` on `METRICS_ADDR` | 2 services only |

Existing OTel deps are pinned at **v1.43.0** (`go.opentelemetry.io/otel`,
`.../sdk`, `.../exporters/...`, `otelhttp` contrib `v0.60.0`).

**Conversion target is 16 services, not 14.** The `otelutil` footprint above is
14, but `auth-service` and `portal-service` use `slog.SetDefault` without
`otelutil` (no tracing today) and still need `obs.Init`. All 16 deployable
services are converted (see §6 Phase 3 enumeration); the 14/13 counts describe
the *current* footprint of the things being removed, not the target.

### Key implication

`otelhttp` and any other libraries that read the **global** OTel provider keep
working as long as `pkg/obs.Init` installs the SDK's providers globally
(`otel.SetTracerProvider(obs.TracerProvider())` +
`otel.SetTextMapPropagator(obs.Propagator)`). The Marz32onE NATS layer is removed
outright (D2) rather than kept alive on the global provider, so this install is
needed only for `otelhttp`/`o11y/gin` and any future global consumers — not as a
transition crutch.

---

## 3. Target State

```
                 ┌─────────────────────────────────────────┐
   main.go  ───► │  pkg/obs.Init(ctx)  (thin wrapper)        │
                 │   • parse O11Y_* + service identity (env) │
                 │   • o11y.Init(...)                        │
                 │   • install global OTel providers         │
                 │   • return *o11y.SDK + shutdown fn        │
                 └─────────────────────────────────────────┘
                        │ obs.Logger (slog, trace-correlated)
                        │ obs.Tracer(name) / obs.Meter(name)
                        ▼
   pkg/mongoutil ─┐
   pkg/cassutil  ─┤  Connect(...) helpers take a minimal provider
   pkg/valkeyutil─┤  interface (variadic option) and build instrumented
   pkg/minioutil ─┤  clients via o11y/{mongo,cassandra,redis,minio,
   pkg/searchengine┘ elasticsearch}.
   pkg/natsutil  ─┐  Connect / router use o11y/nats (Phase 2).
   pkg/natsrouter─┘
   ginutil       ───► gin.Middleware(obs.Tracer(...)) (HTTP svcs)
```

`pkg/obs` is a **descriptive** package name (CLAUDE.md forbids `utils`/`common`)
that owns only the wiring boilerplate. It does not wrap the SDK's types — callers
hold the real `*o11y.SDK` so the full API stays available.

The concrete `*o11y.SDK` lives only in `pkg/obs` and each `main.go`. Shared
`pkg/*` connect helpers accept a **minimal provider interface** (the
`TracerProvider`/`MeterProvider`/`*slog.Logger` they actually use), per CLAUDE.md
§3 "accept interfaces, return structs" — they never import the concrete SDK,
which keeps them unit-testable with a fake provider.

---

## 4. Decision Points (LOCKED)

All four are decided; recorded here with rationale.

**D1 — Wrapper vs. direct `o11y.Init`. → Thin `pkg/obs` wrapper.**
Centralizes env parsing (via `caarlos0/env`, per CLAUDE.md), the four required
identity options, default endpoints, and the global-provider install + shutdown
ordering, so the 16 services do not copy that boilerplate. Cost: one small
shared package.

**D2 — NATS instrumentation. → Fully replace Marz32onE with `o11y/nats`.**
No transitional period. `pkg/natsutil` + `pkg/natsrouter` migrate to `o11y/nats`
(+ JetStream). Largest blast radius (24 files, shared infra), but acceptable:
pre-prod, no user impact. Still gated by an end-to-end trace continuity
integration test (§9).

> **Outcome note (post-implementation):** "remove Marz32onE" means the chat repo
> no longer imports `Marz32onE/instrumentation-go/otel-nats` **directly** — `grep`
> over the tree is clean. It is NOT removed from the module graph: `o11y/nats`
> wraps Marz's `otelnats`/`oteljetstream` internally (o11y has no native NATS
> implementation in v0.7.x), so it remains a legitimate **indirect** dependency
> in `go.mod`. o11y also gates NATS tracing behind
> `OTEL_INSTRUMENTATION_GO_TRACING_ENABLED` + `OTEL_NATS_TRACING_ENABLED` (both
> default off, no programmatic override), so `pkg/natsutil.Connect` sets them on
> unless an operator opted out.

**D3 — OTLP transport. → OTLP/HTTP (`:4318`).**
`o11y` exports over HTTP; we currently use gRPC (`:4317`). Env/collector change,
not code. Update `deploy/*` env to the HTTP collector endpoint; confirm the
target collector exposes an OTLP/HTTP receiver.

**D4 — Rollout shape. → Convert all services in one sweep (no pilot).**
Because the platform is pre-production, there is no need to prove the wrapper on
a single canary first. All 16 services migrate together, still ordered by layer
(wrapper → shared helpers → services → NATS → cleanup) for reviewable build
order, not for risk gating.

---

## 5. Design: `pkg/obs`

Env var names follow **OTel standard conventions** where they exist (decision in
§8), so operators and tooling recognize them and there is no collision with the
existing `METRICS_ADDR` listeners in `search-service`/`oplog-connector`.

```go
// Package obs wires the platform's observability stack on top of flywindy/o11y.
package obs

type Config struct {
    ServiceName    string            `env:"OTEL_SERVICE_NAME,required"`
    ServiceVersion string            `env:"SERVICE_VERSION"   envDefault:"dev"`
    Environment    string            `env:"DEPLOY_ENV"        envDefault:"development"`
    Namespace      string            `env:"SERVICE_NAMESPACE" envDefault:"chat"`
    OTLPEndpoint   string            `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"http://localhost:4318"`
    OTLPHeaders    map[string]string `env:"OTEL_EXPORTER_OTLP_HEADERS"`
    PrometheusHost string            `env:"OTEL_EXPORTER_PROMETHEUS_HOST" envDefault:""`
    PrometheusPort string            `env:"OTEL_EXPORTER_PROMETHEUS_PORT" envDefault:"2112"`
    // Head sampling is read directly by the SDK from the standard
    // OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG — not duplicated here.
    // Pillar toggles fall through to the SDK's O11Y_*_ENABLED env vars.
}

// Init parses Config from env, starts the SDK, installs the SDK's providers as
// the OTel globals (so library instrumentation that reads the global provider —
// e.g. otelhttp / o11y/gin — stays correlated), and returns the SDK plus a
// shutdown func to defer.
func Init(ctx context.Context) (*o11y.SDK, func(context.Context) error, error)
```

Notes:
- `Init` calls `slog.SetDefault(obs.Logger)` so existing `slog.Info(...)` calls
  keep working and gain trace correlation for free.
- Request-ID correlation is preserved: `pkg/logctx`/`pkg/natsrouter` already add
  `request_id` as a slog attribute; `o11y`'s handler adds `traceId`/`spanId`.
  Verify both appear together in one log line in Phase 1 (package acceptance).
- **Metrics endpoint reconciliation:** `pkg/health` only serves `/healthz` +
  `/readyz` (never `/metrics`), so there is no collision with it. The SDK owns
  `/metrics` on `OTEL_EXPORTER_PROMETHEUS_HOST:PORT` (default `:2112`). The two
  services that already run their own `promhttp` `metricsMux` on `METRICS_ADDR`
  (`search-service`, `oplog-connector`) retire that listener in Phase 3 in favour
  of the SDK's — `METRICS_ADDR` stays their own var until then and is removed when
  they migrate.

---

## 6. Rollout (build order, not risk gating)

All services convert in this branch. Phases are a sane *build order* — each
should still land lint + unit + integration green and follow TDD (CLAUDE.md §4),
and may be split into one PR per phase for reviewability, but there is no
"prove-on-pilot-then-expand" gate (D4).

Each phase below carries an **integration-test checklist** describing what you
should observe once the full stack is run locally. These assume the o11y monitor
backend (OTel Collector, Tempo, Loki, Prometheus, Grafana, Pyroscope) is
reachable from the chat services and that the services' `OTEL_EXPORTER_OTLP_ENDPOINT`
/ metrics scraping are wired to it — wiring the network between the compose stack and the
monitor backend is an operator step, out of scope for this plan.

### Phase 0 — Dependency & baseline
- `go get github.com/flywindy/o11y@<pinned>`; run `make tidy`/`go mod tidy`.
- **Reconcile OTel versions** (§7). Resolve any minor bump from o11y's pins;
  `make build` all services + `make test` to confirm no breakage.
- No behavioural change yet. Gate: full build + `make sast` clean.

  **Integration-test checklist (expect: nothing changes):**
  - [ ] `make build` succeeds for all 16 services.
  - [ ] `make test` and `make sast` are green.
  - [ ] Standing up the stack behaves exactly as before; **Grafana shows no new
        data** (no service emits via o11y yet) — this is the intended outcome.

### Phase 1 — `pkg/obs` wrapper
- TDD `pkg/obs`: env parse, defaults, toggle fallthrough, global-provider
  install, `slog.SetDefault(obs.Logger)`, shutdown ordering.
- Acceptance for the package: log lines carry `traceId` + `spanId` + `request_id`
  together when a span is active and `pkg/logctx`/`natsrouter` set the request ID.

  **Integration-test checklist (expect: correlated stdout logs; Grafana still empty):**
  - [ ] `pkg/obs` unit tests pass at ≥80% coverage (CLAUDE.md §4).
  - [ ] In a manual smoke (run one service wired to `obs.Init`, or the package's
        own test), a **stdout JSON log line emitted inside an active span** shows
        `traceId`, `spanId`, `service.name`, **and** the existing `request_id`
        together in one record.
  - [ ] Toggle precedence works: setting `O11Y_TRACE_ENABLED=false` drops trace
        fields; `O11Y_LOG_ENABLED=false` falls back to stdout-only.
  - [ ] **Grafana still shows no live-service data** — no service is converted
        yet; visibility starts in Phase 2/3.

### Phase 2 — Instrument shared `pkg/* connect helpers` (incl. NATS)
This is where most coverage is bought with the least per-service change. Each
helper gains a **variadic functional option** carrying a minimal provider
interface (not a required `*o11y.SDK` param), so call sites migrate incrementally
and the helper degrades to a no-op tracer when observability is disabled.
- `pkg/mongoutil.Connect` → `o11y/mongo`
- `pkg/cassutil.Connect` → `o11y/cassandra.RegisterObservers`
- `pkg/valkeyutil` → `o11y/redis`
- `pkg/minioutil` → `o11y/minio`
- `pkg/searchengine` → `o11y/elasticsearch`
- `pkg/natsutil.Connect` + `pkg/natsrouter` → `o11y/nats` (+ JetStream),
  **removing** `Marz32onE/instrumentation-go/otel-nats` (D2).
- Integration tests (testcontainers) assert spans/metrics are emitted, plus the
  end-to-end trace continuity test (§9) for the NATS pipeline.

  **Integration-test checklist (expect: first telemetry in Grafana; cross-service traces):**
  - [ ] Per-helper integration test records a span for a representative op
        (Mongo find, Cassandra select, Valkey get, MinIO put, ES search) via an
        in-memory recorder.
  - [ ] **NATS pipeline continuity test passes:** publishing a message yields a
        **single `traceId`** propagated through NATS headers across
        `message-gatekeeper → message-worker → broadcast-worker`.
  - [ ] In **Tempo/Grafana**, that message send appears as one trace with child
        spans for each NATS publish/consume and each datastore call.
  - [ ] In **Loki**, clicking a span jumps to its correlated logs via
        `traceId`/`spanId`, and those logs still carry `request_id`.
  - [ ] `curl <service>:2112/metrics` (default `OTEL_EXPORTER_PROMETHEUS_PORT`)
        returns NATS/datastore client metrics and Go runtime metrics, all
        carrying the constant labels `service_name`, `service_namespace`,
        `service_version`, `deployment_environment_name`.
  - [ ] Confirm **no** Marz32onE spans remain (old instrumentation fully gone).

### Phase 3 — Convert all 16 services
- Every `main.go`: `otelutil.Init*` → `obs.Init`; delete the manual
  `slog.SetDefault` block; pass the SDK into the now-instrumented `pkg/*` helpers.
  `auth-service` and `portal-service` (no `otelutil` today) gain `obs.Init` for
  the first time. `search-service` and `oplog-connector` retire their own
  `promhttp` `metricsMux`/`METRICS_ADDR` listener in favour of the SDK's
  `/metrics`.
- HTTP services additionally adopt `o11y/gin` middleware and `o11y/resty`.
- Suggested edit order (mechanical, not gated): workers (`message`, `broadcast`,
  `notification`, `room`, `inbox`, `search-sync`, `gatekeeper`, oplog-connector)
  → req/reply (`room`, `user`, `history`, `presence`, `search`) → HTTP (`auth`,
  `portal`, `upload`). That enumeration is the full set of **16**.

  **Integration-test checklist (expect: full coverage; complete end-to-end traces):**
  - [ ] All **16 `service.name`s appear** in Tempo/Grafana's service map after
        exercising the stack.
  - [ ] **HTTP services** (`auth`, `portal`, `upload`) emit server spans
        (`o11y/gin`) and outbound client spans (`o11y/resty`); a login →
        first message flow is traceable end-to-end.
  - [ ] Each service exposes `/metrics` on `:2112` (per-container) and is
        scrapeable by Prometheus.
  - [ ] No service logs to stdout via the old hand-built handler — grep confirms
        every `slog.SetDefault(NewJSONHandler(...))` block is removed.
  - [ ] Reconnect / history / search / presence flows each show a coherent trace
        with correlated logs (spot-check one req/reply service per category).

### Phase 4 — Cleanup & opt-in extras
- Delete `pkg/otelutil` (no remaining callers) and confirm
  `Marz32onE/...` is gone from `go.mod`.
- Update `deploy/docker-compose.yml` + `azure-pipelines.yml` + k8s env across
  all services (§8) and `docker-local/` compose.
- Confirm runtime metrics; optionally enable Pyroscope profiling
  (`O11Y_PROFILING_ENABLED`) on one service as a trial.
- Update `docs/architecture.md` observability section + `CLAUDE.md` §1 table.

  **Integration-test checklist (expect: clean tree; optional profiles):**
  - [ ] `grep -rn "otelutil\|Marz32onE"` over the repo returns nothing in
        non-test code; `pkg/otelutil` is deleted and `go.mod` is tidy.
  - [ ] Full stack still builds and behaves identically after the deletions.
  - [ ] If Pyroscope is enabled on the trial service: **flame graphs appear in
        Pyroscope (`:4040`)** and its root spans carry `pyroscope.profile.id`,
        letting you jump from a Tempo trace to the CPU profile.
  - [ ] `docs/architecture.md` and `CLAUDE.md` §1 reflect the o11y-based stack.

---

## 7. Dependency & Version Reconciliation — **hard Phase 0 gate**

`o11y` pins its own OTel module versions and removes our only NATS-tracing
fallback (D2 deletes Marz32onE with no transitional period). Compatibility must
therefore be **proven in Phase 0**, before any service flips — not discovered
late. Phase 0 does not pass until all of the following hold:
- After `go get`, diff the `go.opentelemetry.io/*` versions; if o11y requires a
  newer minor, bump ours to match (single source of truth) and re-run `make test`.
- `o11y` adds transitive deps (Pyroscope client, OTLP/HTTP exporter, possibly
  `otelgin`/`otelmongo`/`gocql` observer libs). Per CLAUDE.md "ask before adding
  deps" — these arrive transitively via the one approved `o11y` dependency; list
  them in the Phase 0 PR description for review.
- Confirm `o11y` supports our pinned driver majors: `mongo-driver/v2`,
  `gocql v1.7`, `go-redis/v9` (cluster mode), `minio-go/v7`,
  `go-elasticsearch/v8`, `nats.go v1.50` + `jetstream`. Any mismatch is a
  Phase 0 blocker.
- **Verify `o11y/nats` JetStream trace-header propagation** matches what
  Marz32onE produced (the same W3C `traceparent` carried on NATS headers across
  publish→stream→consume). Since Marz32onE is removed outright, any difference in
  header injection/extraction would silently break cross-service traces — prove
  it with a minimal publish/consume span test in Phase 0, ahead of the full
  pipeline continuity test in Phase 2.

---

## 8. Config / Env Changes

**Naming decision: OTel-standard-first.** Use the OpenTelemetry standard env var
names wherever one exists, so operators/tooling recognize them and we avoid
colliding with the existing `METRICS_ADDR` listeners. Discrete vars are used only
for resource attributes that have no single-field OTel standard.

New env vars (defaults chosen so local dev "just works"):

| Var | Default | Notes |
|-----|---------|-------|
| `OTEL_SERVICE_NAME` | — (required) | per service; OTel standard |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTel standard; **HTTP** (was gRPC `:4317`) |
| `OTEL_EXPORTER_OTLP_HEADERS` | — | OTel standard; optional auth/routing |
| `OTEL_EXPORTER_PROMETHEUS_HOST` | `""` (all interfaces) | OTel standard; SDK `/metrics` bind host |
| `OTEL_EXPORTER_PROMETHEUS_PORT` | `2112` | OTel standard; SDK `/metrics` port |
| `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` | per SDK | OTel standard; head sampling, read by the SDK directly |
| `SERVICE_VERSION` | `dev` | resource attr (no OTel single-field std); from CI build tag |
| `DEPLOY_ENV` | `development` | resource attr; `production`/`staging`/… |
| `SERVICE_NAMESPACE` | `chat` | resource attr |
| `O11Y_TRACE_ENABLED` / `_METRICS_` / `_LOG_` / `_PROFILING_` | per SDK | SDK-owned runtime toggles |

Removed: the old gRPC OTLP endpoint env consumed by `otelutil`, and — as each of
the two services migrates (Phase 3) — their own `METRICS_ADDR` listener var.
Update every service's `deploy/docker-compose.yml`, `deploy/azure-pipelines.yml`,
and k8s manifests. `docker-local/` compose should set
`OTEL_EXPORTER_OTLP_ENDPOINT` to the local collector (or leave tracing off via
`O11Y_TRACE_ENABLED=false` when no collector is running).

---

## 9. Testing Strategy (TDD)

- **`pkg/obs` unit tests:** env parsing (defaults, required, invalid), toggle
  precedence (code > env > default), shutdown idempotency/timeout. Use the SDK's
  `o11ytest` helpers where applicable.
- **Shared-helper integration tests** (`//go:build integration`, testcontainers
  from `pkg/testutil`): after instrumenting each `pkg/*` connect helper, assert a
  span is recorded for a representative operation (e.g., a Mongo find, a Cassandra
  select) via an in-memory span recorder.
- **Pipeline trace-continuity test:** Phase 2 gate (the phase that swaps NATS to
  `o11y/nats`) — publish a message and assert a single trace ID propagates
  gatekeeper → worker → broadcast through NATS headers, before any service
  flips. Backed by the Phase 0 minimal header-propagation check (§7).
- Coverage floor 80% on `pkg/obs` (CLAUDE.md §4). No real collectors in unit
  tests; assert against in-memory exporters/recorders only.

---

## 10. Risks & Rollback

| Risk | Mitigation |
|------|-----------|
| OTel version skew breaks build | Resolved in Phase 0 before any service change; isolated PR |
| Trace context not propagated across NATS after the `o11y/nats` swap | Phase 2 ships a dedicated end-to-end continuity integration test on the message pipeline before any service flips |
| Log line loses `request_id` when switching to SDK handler | Phase 1 package acceptance explicitly checks `request_id` + `traceId` coexist |
| Prometheus port/exposition reconciliation | No collision with `pkg/health` (it serves only `/healthz` + `/readyz`); SDK owns `/metrics` on `:2112`. The two services with their own `metricsMux`/`METRICS_ADDR` (`search-service`, `oplog-connector`) retire it in Phase 3 (§5) |
| OTLP/HTTP collector not available in an env | `O11Y_*_ENABLED=false` toggles degrade gracefully to stdout JSON |

Rollback: phases are split into independent PRs in build order; reverting a PR
restores the prior working state. `pkg/otelutil` and the Marz32onE dependency are
only deleted once their replacements are in place (Phase 2/4), so any single
revert leaves the tree buildable. No production traffic is affected — the
platform is pre-production for the duration of this work.

---

## 11. Follow-ups (out of scope here)

- Custom domain metrics (messages/sec, fanout size, cross-site lag) using
  `obs.Meter(...)`.
- Span enrichment with domain attributes (`room.id`, `site.id`) via
  `obsctx`/attribute helpers.
- Pyroscope profiling rollout beyond the trial service.
- Dashboards/alerts (Grafana) — ops/IaC.
