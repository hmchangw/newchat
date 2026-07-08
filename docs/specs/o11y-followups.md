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

**Chosen: (B)** — `search-service` should expose a **single metrics port**
(the SDK's `:2112`), consistent with every other service. Retire the bespoke
`:9090`/promauto endpoint and re-emit the three app metrics through the OTel
meter.

**Implementation (pending):**
1. Rewrite `metrics.go` onto `otel.Meter("search-service")`:
   `search_service_requests_total` (Int64Counter), `…request_duration_seconds`
   and `…es_duration_seconds` (Float64Histogram); thread `ctx` through
   `observeRequest`/`observeES` and their call sites.
2. Pin histogram boundaries with an explicit meter **View** (OTel's default
   buckets differ from `prometheus.DefBuckets`, or `histogram_quantile` breaks).
3. Account for otelprom's naming: it double-suffixes `_total`/`_seconds` and adds
   `otel_scope_*`/`target_info` labels — set instrument names so the emitted
   series match, or update the dashboard/alert queries to the new names.
4. Drop the `:9090` listener + `METRICS_ADDR`; TDD the new meter path.
5. **Cross-repo:** realign the `search-service` Grafana panels/alerts to the
   `:2112` target (`job`/`instance` change) — coordinate with the o11y/k8s
   monitor-stack owner. This is the risky part flagged in the table above.
