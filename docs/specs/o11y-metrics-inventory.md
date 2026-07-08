# o11y — Metrics Inventory

What Prometheus metrics each service exposes today (on the SDK's `:2112`), what
**domain** metrics are still missing at the app level, and which metrics can only
come from **dedicated exporters** (infra, not the app SDK). Companion to
`o11y-trace-design.md` (traces) — this is the metrics view.

> Status: inventory / design. Live values to be confirmed against a running
> stack (`docker-local/compose.o11y.yaml` → Prometheus `:9090`).

---

## 0. Three layers of metrics

| Layer | Source | Who owns it |
|---|---|---|
| **A. SDK-auto** | o11y instrumentation on gin/mongo/redis/cassandra + Go runtime | this repo (already wired) |
| **B. App domain** | explicit `sdk.Meter()` instruments (business counters) | this repo — mostly **missing** on hot-path workers |
| **C. Infra** | dedicated Prometheus exporters for NATS/Mongo/Cassandra/Valkey/ES **servers** | ops / IaC — **out of app scope** |

Everything in A + B is served on each service's SDK Prometheus endpoint
(`OTEL_EXPORTER_PROMETHEUS_PORT`, default `:2112`). Layer C lives on separate
exporter targets.

---

## 1. Layer A — what the SDK emits automatically

Enabled wherever the matching `WithObservability` / middleware is wired.

| Instrumentation | Key metrics (otelprom names may add `_total`/unit suffix + `otel_scope_*` labels) | Emitted by |
|---|---|---|
| **HTTP server** (`o11y/gin`) | `http.server.request.duration` (histogram, by route/method/status) | auth, portal, upload |
| **MongoDB** (`mongoutil`) | `db.client.operation.duration`; pool: `db.client.connection.count` / `.wait_time` / `.use_time` / `.create_time` | every service with Mongo |
| **Valkey/Redis** (`valkeyutil`) | `db.client.operation.duration`; connection-pool usage/wait/use/create metrics | gatekeeper, broadcast, notification, room-*, search-*, user-* |
| **Cassandra** (`cassutil`) | `db.client.operation.duration`, `db.client.connection.create_time` | message-worker, history-service, room-* |
| **Go runtime** (`WithRuntimeMetrics`, **on by default**) | goroutines, GC pauses/count, heap/alloc, memory, GOMAXPROCS | **all** services |

**Two notable auto-gaps (spans only, NO metrics):**
- **NATS/JetStream client** (`otelnats`) — emits *spans*, but **no client
  metrics** (no per-subject publish/consume counters or latency histograms from
  the SDK). For a message platform this is a real gap; today the only NATS
  numbers come from tracing.
- **Elasticsearch client** (`searchengine`) — emits *spans* (ES `_search`/`_bulk`
  latency is visible in traces) but **no metrics** instrument.

---

## 2. Layer B — per-service view (auto + app), and what's missing

`R` = Go runtime (always). Auto DB/HTTP columns show what's instrumented. The
last column is the **domain** metrics that would need an explicit `sdk.Meter()`
and are **not present today**.

| Service | HTTP | Mongo | Valkey | Cassandra | NATS | ES | App metrics today | Missing domain metrics (suggested) |
|---|:--:|:--:|:--:|:--:|:--:|:--:|---|---|
| auth-service | ✅ | — | — | — | — | — | — | login success/fail, JWKS refresh (see F5) |
| portal-service | ✅ | ✅ | — | — | — | — | — | account-lookup outcomes |
| upload-service | ✅ | ✅ | — | — | — | — | — | upload count/bytes, MinIO put/get outcomes |
| message-gatekeeper | — | ✅ | ✅ | — | spans | — | — | **validated / rejected counter** (by reason), canonical published |
| message-worker | — | ✅ | — | ✅ | spans | — | — | rows written, thread-sub upserts |
| broadcast-worker | — | ✅ | ✅ | — | spans | — | — | **fan-out size** histogram, deliveries, E2E-key hits |
| notification-worker | — | ✅ | ✅ | — | spans | — | — | notifications/pushes sent, suppressed |
| search-sync-worker | — | — | — | — | spans (Fetch) | spans | — | bulk actions/flush, index vs delete, ES failures |
| search-service | — | ✅ | ✅ | — | spans | spans | **`search_service_requests_total`, `…request_duration_seconds`, `…es_duration_seconds`** | (well covered) |
| room-service | — | ✅ | ✅ | ✅ | spans | — | — | room create/join/leave outcomes |
| room-worker | — | ✅ | ✅ | ✅ | spans | — | — | member-add results, roomkey distributions, vault ops |
| inbox-worker | — | ✅ | — | — | spans | — | — | cross-site events applied/dropped by type |
| user-service | — | ✅ | ✅ | — | spans | — | — | subscription/room RPC outcomes |
| user-presence-service | — | ✅ | ✅? | — | spans | — | — | presence queries, cache hit rate |
| history-service | — | ✅ | — | ✅ | spans | — | — | history reads, bucket-walk depth |
| data-migration/oplog-* | — | ✅ | — | — | spans | — | **rich counters** (`oplog_*_events_processed_total`, `_naks_total`, `_terms_total`, `_skipped_total`, `_exhausted_total`, …) | (good exemplar — copy this pattern) |

**Observation:** the **data-migration services already model domain metrics well**
(processed / nak / term / skip / exhausted counters) — that is the pattern the
**hot-path workers are missing**. The review deferred these by design; F-items
below track them.

---

## 3. Layer C — metrics that need dedicated exporters (infra, out of app scope)

App SDK metrics describe the service's *own* client operations. **Server-side /
broker health** must come from purpose-built exporters — the app cannot emit
these:

| System | Needs | Key metrics it provides (that the app cannot) |
|---|---|---|
| **NATS / JetStream** | NATS server monitoring (`:8222`) + `prometheus-nats-exporter`, or NATS Prometheus endpoint | **stream/consumer lag**, pending/ack-pending, redelivered, dropped, consumer num_ack_pending, stream bytes/msgs, slow consumers |
| **MongoDB** | `mongodb_exporter` | server ops/s, replication lag, connections, cache, locks, oplog window |
| **Cassandra** | JMX → `jmx_exporter` (or Cassandra metrics) | read/write latency percentiles, pending compactions, hinted handoff, tombstones, GC |
| **Valkey/Redis** | `redis_exporter` | memory, evictions, keyspace hits/misses, connected clients, cluster slot health |
| **Elasticsearch** | `elasticsearch_exporter` | cluster status (green/yellow/red), shard/relocation counts, JVM heap, indexing/search rate, queue rejections |

Of these, **NATS/JetStream consumer lag** is the single highest-value infra
metric for this platform (it's how you see a worker falling behind) — and it is
**not** obtainable from the app SDK. Prioritize the NATS exporter.

The local stack (`docker-local/compose.o11y.yaml`) scrapes app `:2112` today; add
these exporters there (and to prod IaC) to cover Layer C.

---

## 4. Gaps & recommendations (ordered)

1. **NATS/JetStream exporter (Layer C).** Highest value — consumer lag is
   invisible today. Deploy `prometheus-nats-exporter` (or scrape NATS `:8222`),
   add to `docker-local/compose.o11y.yaml` + prod. *Infra, not app code.*
2. **Hot-path domain counters (Layer B).** Add `sdk.Meter()` instruments to
   gatekeeper (validated/rejected by reason), broadcast (fan-out size),
   notification (sent/suppressed), search-sync (bulk outcomes). Copy the
   data-migration counter pattern. *App code — one small `metrics.go` per service.*
3. **Confirm/So-what on NATS & ES client metrics (Layer A gap).** otelnats/
   searchengine emit spans but no metrics; decide whether app-side NATS/ES
   latency histograms are worth adding (or rely on traces + the exporters).
4. **DB/Redis/Cassandra/ES server exporters (Layer C).** Standard exporters for
   server health; lower urgency than NATS. *Infra.*
5. **Histogram buckets.** SDK HTTP/DB histograms use `DefaultLatencyBuckets`
   (`WithHistogramBuckets` can override); confirm they match dashboard needs.

Tracked as follow-ups in `docs/specs/o11y-followups.md`.

See also: `o11y-trace-design.md` (traces), `o11y-performance-and-sampling.md`
(cost of the above), `o11y-local-trace-verification.md` (how to view them).
