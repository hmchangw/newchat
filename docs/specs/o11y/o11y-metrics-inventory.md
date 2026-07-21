# o11y — Metrics Inventory

What Prometheus metrics each service exposes today (on the SDK's `:2112`), what
**domain** metrics are still missing at the app level, and which metrics can only
come from **dedicated exporters** (infra, not the app SDK). Companion to
`o11y-trace-design.md` (traces) — this is the metrics view.

> Status: inventory / design. Live values were verified locally on 2026-07-12
> against the Docker Compose o11y stack (`docker-local/compose.o11y.yaml` ->
> Prometheus `:9090`).

### Local verification update (2026-07-12)

After rebasing onto the unified gateway/admin/botplatform upstream change, all
19 Go services in `compose.services.yaml` were rebuilt without cache and
scraped successfully:

| Query / check | Result |
|---|---|
| active targets with `job="chat-services"` | `19` |
| healthy targets | `19/19 up` |
| `count by (service_name) (go_goroutine_count)` | one series for each of the 19 services |
| new upstream services | `admin-service`, `botplatform-service`, and `media-service` expose SDK metrics on `:2112` |
| infrastructure filtering | Traefik is excluded from this SDK scrape job; it no longer creates a false-down `:2112` target |

The complete target set is: `admin-service`, `auth-service`,
`botplatform-service`, `broadcast-worker`, `history-service`, `inbox-worker`,
`media-service`, `message-gatekeeper`, `message-worker`,
`notification-worker`, `outbox-worker`, `portal-service`, `room-service`,
`room-worker`, `search-service`, `search-sync-worker`, `upload-service`,
`user-presence-service`, and `user-service`.

### Local verification update (2026-07-11)

After rebuilding the full local stack and driving browser send, room-create,
member-add, DM, edit/delete, and search traffic:

| Query / check | Result |
|---|---|
| `sum(up{job="chat-services"})` | `16` |
| `count(count by(service_name)(go_goroutine_count))` | `16` |
| `search_service_requests_total` | `subscriptions,status=ok: 2`; `messages,status=ok: 1` |
| `search_service_request_duration_seconds_count` | present for the same three requests |
| `search_service_es_duration_seconds_count` | `3` |
| cache metric series after workload | hits and misses present across gatekeeper, message-worker, broadcast, notification, room-worker, and history-service |

The search series were generated from the real frontend after correcting its
subjects to `chat.user.{account}.request.search.{siteId}.rooms/messages`; the
previous no-site subjects returned NATS 503 and could not exercise the service.

One instrumentation nuance was also confirmed: message-worker emits Cassandra
connection-create metrics and Cassandra spans, but its batch writes did not
produce `db_client_operation_duration_seconds_count` in this run. History's
query/update path did produce Cassandra operation-duration series. Treat
Cassandra batch-operation metrics as a remaining SDK/instrumentation gap; do
not infer that message-worker is idle from the missing operation histogram.

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

`R` = Go runtime (always). Auto DB/HTTP columns show what's instrumented. "App
metrics today" means explicit repo-owned OTel instruments visible on the SDK
`:2112` endpoint. The last column is the **business/domain** coverage still
missing beyond shared cache/key counters.

| Service | HTTP | Mongo | Valkey | Cassandra | NATS | ES | App metrics today | Missing domain metrics (suggested) |
|---|:--:|:--:|:--:|:--:|:--:|:--:|---|---|
| admin-service | ✅ | ✅ | — | — | — | — | — | admin operations and audit outcomes |
| auth-service | ✅ | — | — | — | — | — | — | login success/fail, JWKS refresh (see F5) |
| botplatform-service | ✅ | ✅ | — | — | — | — | — | login/session/password-change outcomes |
| portal-service | ✅ | ✅ | — | — | — | — | — | account-lookup outcomes |
| upload-service | ✅ | ✅ | — | — | — | — | — | upload count/bytes, MinIO put/get outcomes |
| media-service | ✅ | ✅ | — | — | spans | — | — | avatar/emoji upload count/bytes, MinIO put/get outcomes |
| message-gatekeeper | — | ✅ | ✅ | — | spans | — | shared `cache_*_total` | **validated / rejected counter** (by reason), canonical published |
| message-worker | — | ✅ | — | ✅ | spans | — | shared `cache_*_total` | rows written, thread-sub upserts |
| broadcast-worker | — | ✅ | ✅ | — | spans | — | shared `cache_*_total` | **fan-out size** histogram, deliveries, E2E-key hits |
| notification-worker | — | ✅ | ✅ | — | spans | — | shared `cache_*_total` | notifications/pushes sent, suppressed |
| outbox-worker | — | — | — | — | spans | — | — | forwarded/dropped/retried events by destination and type |
| search-sync-worker | — | — | — | — | spans (Fetch) | spans | — | bulk actions/flush, index vs delete, ES failures |
| search-service | — | ✅ | ✅ | — | spans | spans | **`search_service_requests_total`, `search_service_request_duration_seconds`, `search_service_es_duration_seconds`** | (well covered after request traffic) |
| room-service | — | ✅ | ✅ | ✅ | spans | — | — | room create/join/leave outcomes |
| room-worker | — | ✅ | ✅ | ✅ | spans | — | shared `cache_*_total`, `room_key_*_total` | member-add results, roomkey distributions, vault ops |
| inbox-worker | — | ✅ | — | — | spans | — | — | cross-site events applied/dropped by type |
| user-service | — | ✅ | ✅ | — | spans | — | — | subscription/room RPC outcomes |
| user-presence-service | — | ✅ | ✅? | — | spans | — | — | presence queries, cache hit rate |
| history-service | — | ✅ | — | ✅ | spans | — | shared `cache_*_total` | history reads, bucket-walk depth |
| data-migration/oplog-* | — | ✅ | — | — | spans | — | **rich counters** (`oplog_*_events_processed_total`, `_naks_total`, `_terms_total`, `_skipped_total`, `_exhausted_total`, …) | (good exemplar — copy this pattern) |

**Observation:** shared cache/room-key counters are already present on some
hot-path services, but they do not answer the core product questions (accepted
vs rejected messages, fan-out size, delivered/suppressed notifications, ES bulk
outcomes). The **data-migration services already model domain metrics well**
(processed / nak / term / skip / exhausted counters) — that is the pattern the
hot-path workers should copy. The review deferred these by design; F-items below
track them.

Live verification from the local stack:

```promql
count by (service) (up{job="chat-services"})
count by (service_name) (go_goroutine_count)
count by (service_name, db_system_name) (db_client_operation_duration_seconds_count)
sum by (service_name, cache, tier) (cache_hits_total)
sum by (service_name, cache, tier) (cache_misses_total)
```

### Local verification result (2026-07-08)

Environment: Docker Compose local stack with `compose.deps.yaml`,
`compose.o11y.yaml`, and `compose.services.yaml`.

Configuration checks:

```bash
docker compose -f docker-local/compose.o11y.yaml config --quiet
docker compose -f docker-local/compose.services.yaml config --quiet
docker run --rm -v "$PWD/docker-local/o11y/otel-collector.yaml:/config.yaml:ro" otel/opentelemetry-collector:0.115.1 validate --config=/config.yaml
```

Prometheus scrape status:

| Job | Result |
|---|---|
| `chat-services` | 15 active targets up |
| `otel-collector` | 1 active target up |

The 2026-07-08 live run predated the upstream `outbox-worker`, admin, and
botplatform changes and did not include `media-service` in
`docker-local/compose.services.yaml`. The 2026-07-12 result above supersedes
that prediction: the aggregate stack now scrapes 19 Go-service targets.

Active `chat-services` targets scraped `:2112/metrics` for:

`auth-service`, `broadcast-worker`, `history-service`, `inbox-worker`,
`message-gatekeeper`, `message-worker`, `notification-worker`,
`portal-service`, `room-service`, `room-worker`, `search-service`,
`search-sync-worker`, `upload-service`, `user-presence-service`,
`user-service`.

Direct endpoint checks confirmed that SDK metrics are on `:2112`; old app
ports such as `:9090/metrics` are not the metrics endpoint for these services.
The local Prometheus config rewrites Docker service targets to `:2112` and drops
stale/orphan services such as an old `vault` container.

PromQL evidence:

| Query | Result |
|---|---|
| `count by (service) (up{job="chat-services"})` | one live target for each of the 15 services above |
| `count by (service_name) (go_goroutine_count)` | runtime metrics present for all 15 services |
| `count by (service_name, db_system_name) (db_client_operation_duration_seconds_count)` | Mongo, Valkey/Redis, and Cassandra client metrics present on the expected services |
| `sum by (service_name, cache, tier) (cache_hits_total)` | cache hit counters present for `message-worker`, `history-service`, `broadcast-worker`, and `message-gatekeeper` |
| `sum by (service_name, cache, tier) (cache_misses_total)` | cache miss counters present for `message-worker`, `notification-worker`, `history-service`, `room-worker`, `broadcast-worker`, and `message-gatekeeper` |
| `cache_errors_total` | empty in this run; expected when no cache errors occurred |
| `search_service_*` | empty in this Prometheus window because no search request traffic was generated after Prometheus was recreated |
| NATS/JetStream client metric queries | empty; expected, because NATS currently emits spans but no SDK client metrics |

Sample cache counter values observed in this run:

| Metric | Observed series |
|---|---|
| `cache_hits_total` | `message-worker/user/l1=3`, `history-service/history_sub/l1=2`, `broadcast-worker/roommeta/l2=1`, `broadcast-worker/user/l1=3`, `message-gatekeeper/roommeta/l2=2`, `message-gatekeeper/user/l1=4` |
| `cache_misses_total` | `message-worker/user/l1=1`, `notification-worker/roommeta/l1=1`, `notification-worker/roommeta/l2=1`, `notification-worker/roomsub/l2=4`, `history-service/history_sub/l1=1`, `room-worker/roommeta/l1=1`, `room-worker/user/l1=4`, `broadcast-worker/roommeta/l1=4`, `broadcast-worker/roommeta/l2=2`, `broadcast-worker/user/l1=1`, `message-gatekeeper/gatekeeper_sub/l1=6`, `message-gatekeeper/roommeta/l1=3`, `message-gatekeeper/roommeta/l2=1`, `message-gatekeeper/user/l1=2` |

Conclusion: Layer A runtime and client DB metrics are wired and scrapeable from
the local stack, and the shared Layer B cache counters are visible where
implemented. The remaining gaps are domain counters on hot-path services and
Layer C infra exporters, especially NATS/JetStream lag.

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

Tracked as follow-ups in `docs/specs/o11y/o11y-followups.md`.

See also: `o11y-trace-design.md` (traces), `o11y-performance-and-sampling.md`
(cost of the above), `o11y-local-trace-verification.md` (how to view them).
