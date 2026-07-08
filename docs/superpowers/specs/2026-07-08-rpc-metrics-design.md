# RPC latency & error-rate metrics — design

**Date:** 2026-07-08
**Status:** Approved (brainstorming) — pending implementation plan
**Branch:** `claude/rpc-metrics-latency-errors-w8z4ba`

## Goal

Emit uniform **request latency** and **error rate** metrics for every
synchronous RPC handler across all services, so a single set of Grafana
panels and alert rules works for every service via a `service` label.

## Scope

**In scope — the synchronous request/reply surface:**

- All `natsrouter`-based NATS request/reply services (the six that construct a
  `natsrouter` Router and register routes): `room-service`, `room-worker`,
  `user-service`, `user-presence-service`, `history-service`, `search-service`.
- All Gin HTTP services: `auth-service`, `portal-service`, `media-service`,
  `upload-service`.

**Out of scope (non-goals):**

- JetStream event consumers (`message-worker`, `notification-worker`,
  `message-gatekeeper`, `broadcast-worker`, `inbox-worker`). These are async,
  redelivered,
  reply-less event processors — RPC latency/error-rate semantics do not fit.
  They warrant separate consumer-lag / processing-failure metrics in a future
  effort.
- No new client-facing request/response schema or event struct — this is a
  purely server-side observability change, so `docs/client-api.md` is not
  touched.

## Rationale for key choices

- **Centralized in the two chokepoints, not per-handler.** `natsrouter` and
  `ginutil` are the single middleware seams that cover the entire in-scope
  surface with near-zero per-handler code. This matches the existing
  middleware idiom (`RequestID`/`Recovery`/`Logging`/`AccessLog` are all
  installed via `Use`) and keeps metrics an opt-in concern separate from the
  routing/handler core.
- **Unified metric names, `service` distinguished by label.** The entire point
  of "add to all services" is one dashboard/alert that works everywhere. A
  bespoke per-service name (as `search-service` has today) fragments that.
- **`route` label is always the pattern/template, never a live subject/URL.**
  This is the load-bearing cardinality guard. `natsrouter` matches on patterns
  (`chat.user.*.request.room.get`), and Gin exposes the registered route
  template (`c.FullPath()`) — neither carries live room/user IDs.
- **`status` label is the errcode `Code`, uniformly across both transports.**
  Both NATS and HTTP services already classify via `errcode`; reusing the Code
  gives a single status taxonomy. A pinned 9-value allowlist bounds cardinality.
- **Raw Prometheus `promauto`, not the OTel meter.** `search-service` and
  `pkg/atrest` already prove this exact pattern; every service already serves
  the default Prometheus registry on `/metrics`. OTel stays for tracing.

## Architecture

### 1. `pkg/rpcmetrics` (new) — shared vocabulary

One small package owns the collectors and the label taxonomy so **both**
transports emit identical series. Registered with the default Prometheus
registry via `promauto` (already served on `/metrics` in every service).

```go
package rpcmetrics

// rpc_server_requests_total{service, route, status}       CounterVec
// rpc_server_request_duration_seconds{service, route}     HistogramVec, DefBuckets

// Observe records one completed RPC: latency + terminal status.
func Observe(service, route, status string, d time.Duration)

// StatusLabel maps a handler's returned error onto the `status` label.
// nil -> "ok"; a non-empty *errcode.Error Code in the chain, if in the
// pinned allowlist -> that Code; everything else -> "internal".
func StatusLabel(err error) string
```

- `StatusLabel` is lifted verbatim from `search-service/metrics.go`
  (`statusLabel` + `allowedStatusLabels`): the 9-value pinned allowlist is
  `ok` plus the 8 canonical `errcode` Codes (`bad_request`, `unauthenticated`,
  `forbidden`, `not_found`, `conflict`, `too_many_requests`, `unavailable`,
  `internal`). It is a **pure, non-logging** Code extractor (`errors.As`) — it
  never double-logs against `errcode.Classify`.
- Metric names use the `rpc_server_` prefix (`server` distinguishes from any
  future client-side RPC metrics).
- Histogram uses `prometheus.DefBuckets`.
- Package name is descriptive per the "no `utils`/`common`/`helpers`" rule.

### 2. NATS side — `pkg/natsrouter`

Three tiny enabling changes, then one installable middleware.

- **`Context` gains `route`.** Set at `acquireContext` from the matched route's
  original pattern (`rt.pattern`, threaded through `addRoute`). Exposed as
  `c.Route()`. This is the low-cardinality template, never a live subject.
- **`Context` gains a settable terminal `status`.** The typed `Register` /
  `RegisterNoBody` / `RegisterVoid` wrappers already have the handler's `err`
  in scope: they call `c.setStatus("ok")` on success and
  `c.setStatus(rpcmetrics.StatusLabel(err))` on error, **before** `replyErr`.
  The reply/logging path (`errnats.Reply` -> `errcode.Classify`) is unchanged;
  `StatusLabel` does not log, so there is no double-log.
- **`natsrouter.Metrics(service string) HandlerFunc`.** Wraps `c.Next()`, then
  records `rpcmetrics.Observe(service, c.Route(), <terminal status>, elapsed)`.
  A request that never set a status (e.g. an aborted/early-return path)
  defaults to `"ok"` unless set otherwise; document this default. Installed
  once per service:

  ```go
  r.Use(natsrouter.RequestID(), natsrouter.Recovery(),
        natsrouter.Metrics(cfg.ServiceName), natsrouter.Logging())
  ```

### 3. HTTP side — `pkg/ginutil`

- **`errhttp.Write` stores the classified Code** on the gin context
  (`c.Set(...)` the `e.Code`). Free — `Write` already calls `errcode.Classify`.
- **`ginutil.Metrics(service string) gin.HandlerFunc`.** Times `c.Next()`;
  reads `c.FullPath()` (the registered route **template**, not the live URL)
  for `route`; reads the errcode Code stored by `errhttp.Write` for `status`,
  falling back to an HTTP-status class when none was written
  (`2xx` -> `ok`, `4xx` -> `bad_request`, `5xx` -> `internal`). Skips
  `route == "/healthz"` (and empty `FullPath`, i.e. unmatched 404s) to avoid
  noise. Installed alongside `RequestID`/`CORS`/`AccessLog`.

### 4. Per-service wiring

- Every in-scope service already serves the default Prometheus registry on
  `/metrics`. Router services add the `r.Use(natsrouter.Metrics(...))` line;
  HTTP services add `Use(ginutil.Metrics(...))` and, where not already present,
  bind `otelutil.MetricsServer()` on `METRICS_ADDR`.
- The `service` value comes from each service's existing service-name config
  (the same string passed to `otelutil.InitTracer`/`InitMeter`).

### 5. `search-service` migration

- Delete the bespoke request-path machinery from `search-service/metrics.go`:
  `metricRequestsTotal`, `metricRequestDuration`, `observeRequest`, `durFor`,
  `statusLabel`, `allowedStatusLabels`, the per-`kind` duration handles, and
  the per-handler `defer observeRequest(...)()` calls. It inherits the shared
  middleware instead.
- **Keep** the genuinely service-specific `search_service_es_duration_seconds`
  (Elasticsearch `_search` call latency) and its `observeES` helper.
- Note: the shared metric labels by `route` (pattern) rather than the old
  `kind` label; dashboards keyed on `search_service_requests_total{kind=...}`
  must move to `rpc_server_requests_total{service="search-service",route=...}`.

## Data flow

```
request ──▶ [Metrics mw: start timer] ──▶ RequestID ──▶ ... ──▶ handler
                                                                   │
                                            (Register wrapper sets terminal
                                             status: "ok" | StatusLabel(err))
                                                                   │
request ◀── [Metrics mw: Observe(service, route, status, elapsed)] ◀── reply
```

HTTP mirrors this: gin `Metrics` middleware times the chain; `errhttp.Write`
stamps the Code; middleware reads `FullPath` + Code (or HTTP-class fallback).

## Error handling

- `StatusLabel`/`Observe` never panic on unknown input: an unknown Code
  collapses to `internal` (bounded cardinality); an unknown `route` is only
  ever a pattern/`FullPath` value, so it is inherently bounded by the route
  table.
- Metrics recording is best-effort and must never affect the reply: the
  middleware records after `c.Next()` and swallows nothing meaningful (a
  `promauto` `Inc`/`Observe` cannot error).

## Testing (TDD — Red/Green/Refactor per CLAUDE.md)

- **`pkg/rpcmetrics`** (target 90%+, core `pkg/`):
  - `StatusLabel`: nil -> `ok`; each canonical Code -> itself; non-canonical /
    foreign Code -> `internal`; wrapped error resolved via `errors.As`;
    non-errcode error -> `internal`.
  - `Observe`: correct series/labels and a duration observation, verified with
    `prometheus/testutil`.
- **`pkg/natsrouter`**: middleware test — a handled request increments
  `rpc_server_requests_total` with the expected `route`/`status` and observes a
  duration; both success and error paths; uses the existing router test harness
  (`NewContext`, `main_test.go`).
- **`pkg/ginutil`**: `httptest` middleware test — `route` = `FullPath`,
  errcode-Code status, HTTP-class fallback when no Code, and the `/healthz`
  skip.
- All new `pkg/` code meets the 80% floor; `-race` via the Makefile.

## Docs

- No `docs/client-api.md` change (server-side only).
- `pkg/rpcmetrics/doc.go` documents the metric names, label set, the
  cardinality rule (`route` = pattern / `FullPath`, never live IDs), and the
  `status` allowlist.

## Rollout / sequence (for the implementation plan)

1. `pkg/rpcmetrics` (collectors + `StatusLabel` + `Observe`) with tests.
2. `natsrouter`: `Context.route`/`status` plumbing + `Metrics` middleware + tests.
3. `errhttp.Write` Code stamping + `ginutil.Metrics` middleware + tests.
4. Wire `r.Use(...)` / `Use(...)` into each in-scope service; ensure
   `MetricsServer()` is bound.
5. Migrate `search-service` off its bespoke request metrics (keep ES metric).
6. `make lint`, `make test`, `make sast`; verify `/metrics` exposes the series.
