# o11y Integration — Follow-ups

Deferred items from the `flywindy/o11y` integration (branch
`feat/integrate-o11y-sdk`). These are intentionally out of scope of the
integration PR and tracked here.

## F1 — Migrate `search-service` app metrics to OTel meter (or keep as-is)

`search-service` exports three **app** metrics via `client_golang`/`promauto`
on the default registry, served by `promhttp.Handler()` on `:9090`
(`METRICS_ADDR`):

| Metric | Type | labels | buckets |
|---|---|---|---|
| `search_service_requests_total` | CounterVec | `kind`, `status` | — |
| `search_service_request_duration_seconds` | HistogramVec | `kind` | `prometheus.DefBuckets` |
| `search_service_es_duration_seconds` | Histogram | — | `prometheus.DefBuckets` |

The o11y SDK's Prometheus endpoint (`:2112`, otelprom + its own registry) does
**not** serve these, so retiring `:9090` without migrating would drop them.
Unlike `oplog-connector`/`oplog-transformer` (whose metrics are already
`otel.Meter`), this is a real migration, not a deletion.

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

**Default: (A)** — the two endpoints serve *different* metrics (app vs
SDK/runtime), so they are complementary, not duplicate; the only cost is one
extra endpoint + inconsistency with other services. (B) buys single-endpoint
consistency at the price of dashboard breakage + cross-repo coordination — do
it only when the `search-service` Grafana is being reworked anyway.
