# o11y Integration — Follow-ups

Deferred items from the `flywindy/o11y` integration (branch
`feat/integrate-o11y-sdk`). These are intentionally out of scope of the
integration PR and tracked here.

## F1 — Migrate `search-service` app metrics to OTel meter (DONE — see Status)

Before this PR, `search-service` exported three **app** metrics via
`client_golang`/`promauto` on the default registry, served by
`promhttp.Handler()` on `:9090` (`METRICS_ADDR`):

| Metric | Type | labels | buckets |
|---|---|---|---|
| `search_service_requests_total` | CounterVec | `kind`, `status` | — |
| `search_service_request_duration_seconds` | HistogramVec | `kind` | `prometheus.DefBuckets` |
| `search_service_es_duration_seconds` | Histogram | — | `prometheus.DefBuckets` |

The o11y SDK's Prometheus endpoint (`:2112`, otelprom + its own registry) did
**not** serve these, so retiring `:9090` required a real migration (not a
deletion) — unlike `oplog-connector`/`oplog-transformer` whose metrics were
already `otel.Meter`. The table below weighed keep-as-is (A) vs migrate (B); (B)
was chosen and implemented (see Status).

### Options

| Aspect | (A) Keep as-is `:9090` promauto | (B) Migrate to OTel meter, unified on `:2112` |
|---|---|---|
| Code effort | None | Medium: rewrite `metrics.go`, `observeRequest/observeES` and thread `ctx` through call sites, set buckets via a View, retire `:9090` |
| Metric visibility | app on `:9090`, SDK/runtime on `:2112` (two targets) | all on `:2112` (single target) |
| Grafana panels / alerts | No impact | High risk: names/labels/buckets change and must be realigned one by one, or panels/alerts break |
| Metric-name fidelity | Unchanged | otelprom double-suffixes counter `_total` / histogram `_seconds`; instruments must drop the suffix to restore the original names |
| Histogram buckets | `DefBuckets` as-is | OTel default buckets differ → must be set explicitly via a View, or `histogram_quantile` is all wrong |
| Extra labels | None | Adds `otel_scope_name`/`otel_scope_version` (+ `target_info`) |
| Prometheus scrape | Unchanged | target `:9090`→`:2112`; panel `job`/`instance` change accordingly |
| Cross-repo coordination | Not needed | Needed: dashboards live in the o11y/k8s monitor stack + owner sign-off |
| Endpoint count | 2 | 1 |
| Consistency with other services | Inconsistent | Consistent |
| Risk | Low | Medium-high (metric breakage, blank dashboards, false/missed alerts) |
| Reversibility | N/A | Involves dashboards; rollback is not clean |
| Matches original spec intent | Partial | Full |
| Best timing | Default | Do it opportunistically when the search-service Grafana is being reworked anyway |

### Decision

**Chosen: (B)** — `search-service` exposes a **single metrics port** (the SDK's
`:2112`), consistent with every other service.

### Status: code done ✅ — dashboard realignment pending (ops)

Done in this PR (`search-service/metrics.go`, `handler.go`, `main.go`):
- Three app metrics re-emitted through the SDK meter (`sdk.Meter("search-service")`),
  so otelprom serves them on `:2112` alongside runtime/SDK metrics. Instrument
  names omit the suffix (`search_service_requests`, `…_request_duration`,
  `…_es_duration`); otelprom re-adds `_total` (counter) and `_seconds` (from the
  `s` unit) to reproduce the original series names.
- Histogram boundaries pinned to `prometheus.DefBuckets` via the
  `WithExplicitBucketBoundaries` advisory (OTel's default buckets differ).
- `ctx` threaded through `observeRequest`/`observeES`; TDD via an in-memory
  manual-reader test (`metrics_test.go`).
- The bespoke `:9090`/promauto `/metrics` listener is gone; that port now serves
  **health only** — `METRICS_ADDR` → `HEALTH_ADDR` (`SEARCH_HEALTH_ADDR`).

**Remaining (ops / cross-repo, not in this repo):** realign the `search-service`
Grafana panels/alerts to scrape `:2112` instead of `:9090` (`job`/`instance`
change), and account for otelprom's extra constant labels
(`otel_scope_name`/`otel_scope_version` + the resource labels). Name-based
PromQL keeps working; only exact-label matchers need review. Coordinate with the
o11y/k8s monitor-stack owner.

---

## Deferred from the PR-1 branch review

Recorded here so nothing is lost; none blocks the integration PR.

### F2 — Trace sampling for production (deploy + collector)
No sampler is set, so every message is 100% sampled (~10–20 spans/message). Set
`OTEL_TRACES_SAMPLER=parentbased_traceidratio` + `OTEL_TRACES_SAMPLER_ARG` in
deploy before production scale (`pkg/obs` already forwards them — no code).
**Tail sampling at the collector** is the real fix to keep *whole* flows at low
ratios (each NATS hop is a detached root, so head `traceidratio` samples hops
independently). Full design + benchmark: `docs/specs/o11y-performance-and-sampling.md`.

### F3 — `OTEL_SERVICE_NAME` rollout note
Now defaults to `unknown-service` (no longer a crash-loop), but production
manifests **must** set it per service — a `unknown-service` entry in the service
map is the signal one was missed. Call out in the deploy runbook / PR body.

### F4 — Dashboard the BatchSpanProcessor dropped-span counter
Export drops on queue overflow are silent (traces thin out while the system
looks healthy). Add the dropped-span counter to the monitor stack — the only
signal of under-provisioned collector/sampler.

### F5 — Instrument `pkg/oidc`'s HTTP client
`pkg/oidc.Validator` uses a bare `net/http.Client` (JWKS/token fetches), so those
outbound calls produce no client spans — also a pre-existing "use Resty" gap.
Switch to `restyutil` (whose `otelhttp` transport picks up the global provider).

### F6 — `request_id` as a span attribute
`request_id` is on log lines but not attached to any span, so the trace→request-id
pivot is one-directional. Add a one-line span attribute in the `natsrouter` and
`ginutil` RequestID middlewares to close the loop.

### F7 — Replace the `os.Setenv` NATS-tracing gate with an upstream option
`pkg/natsutil.enableNATSTracing` pokes process-global env (`os.Setenv`) inside
`Connect` because the SDK exposes no programmatic switch. Upstream a
`WithTracingEnabled` option to `flywindy/o11y` and drop the env mutation.

### F8 — Minor cleanups
- `minioutil.WithObservability` has no production caller yet (only `testutil`) —
  give it one or note it's future-only.
- Redundant `func(ctx) error { return obsShutdown(ctx) }` closures where
  `obsShutdown` already has that signature (several services) — method-value
  style is cleaner; consistency across services matters more, so do it fleet-wide
  or not at all.
- Gin service-name string literals (`"auth-service"`, …) duplicate
  `OTEL_SERVICE_NAME`; read from the obs config so span scope and resource
  attribute can't drift.
