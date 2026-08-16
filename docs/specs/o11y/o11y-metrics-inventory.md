# o11y — Metrics Inventory

What Prometheus metrics each service exposes today (on the SDK's `:2112`), what
**domain** metrics are still missing at the app level, and which metrics can only
come from **dedicated exporters** (infra, not the app SDK). Companion to
`o11y-trace-design.md` (traces) — this is the metrics view.

> Status: inventory / design. Live values were verified locally on 2026-07-12
> against the Docker Compose o11y stack (`docker-local/compose.o11y.yaml` ->
> Prometheus `:9090`). The instrument list was re-derived from source on
> 2026-08-14 — every OTel and client_golang instrument in the repository is
> accounted for in §2 through §2.3 — but that pass was static, not a rescrape.

> Storage note (2026-08-11): the authoritative, code-reverified MongoDB and
> Cassandra metric set, direct-client coverage, exporter gaps, and shared
> dashboard contract are maintained in
> [`storage-dependency-metrics.md`](storage-dependency-metrics.md). Older storage
> service lists and shorthand metric names below are historical snapshots.

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
| **MongoDB** (`mongoutil`) | `db.client.operation.duration`; pool count, idle-min, max, pending-requests, timeouts, and create-time | direct clients passing `mongoutil.WithObservability`; see the storage contract for gaps |
| **Valkey/Redis** (`valkeyutil`) | `db.client.operation.duration`; connection-pool usage/wait/use/create metrics | gatekeeper, broadcast, notification, room-*, search-*, user-* |
| **Cassandra** (`cassutil`) | `db.client.operation.duration`, `cassandra.query.attempts`, `db.client.connection.create_time`, `cassandra.connection.attempts` | message-worker, bot-message-worker, history-service ordinary queries; raw batches remain a gap |
| **Go runtime** (`WithRuntimeMetrics`, **on by default**) | goroutines, GC pauses/count, heap/alloc, memory, GOMAXPROCS | **all** services |

**Two notable auto-gaps (spans only, NO metrics):**
- **NATS/JetStream client** (`otelnats`) — emits *spans*, but **no client
  metrics** from the SDK itself. This is still true of the instrumentation
  layer; the gap is now covered at the application layer instead, by the shared
  `chat.nats.*` families in §2.1. Do not expect SDK-auto NATS series.
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
| message-gatekeeper | — | ✅ | ✅ | — | spans + `chat_nats_*` | — | shared `cache_*_total`, **`message_gatekeeper_messages_total`** | — |
| message-worker | — | ✅ | — | ✅ | spans + `chat_nats_*` | — | shared `cache_*_total`, **`message_worker_persistence_total`** | thread-sub upserts |
| broadcast-worker | — | ✅ | ✅ | — | spans + `chat_nats_*` | — | shared `cache_*_total`, **`broadcast_worker_fanout_recipients`**, **`broadcast_worker_recipient_deliveries_total`** | E2E-key hits |
| notification-worker | — | ✅ | ✅ | — | spans + `chat_nats_*` | — | shared `cache_*_total`, **`notification_worker_outcomes_total`** | — |
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

### 2.1 Shared application NATS metrics (2026-08-16)

The SDK emits no NATS client metrics (§1), so these are owned by this repo.
The shared consumer, request, and publisher helpers are adopted by
`message-gatekeeper`, `message-worker`, `broadcast-worker`,
`notification-worker`, `history-service`, and `room-service`. Connection
lifecycle metrics are opt-in through `natsutil.ConnectWithMetrics`. Names are
OTel instrument names; Prometheus renders them with `_` separators and adds
counter / unit suffixes. An instrument whose name already ends in `_total`
keeps exactly one — the exporter strips the existing token before appending —
so `nats_slow_consumer_events_total` exports under that name, not a doubled one.

Consumer and publisher families share a base of `service_name` + `site`; the
"labels" column lists what each adds on top. Every adopter reads `service_name`
from `OTEL_SERVICE_NAME` so the label matches the OTel resource, and the bot and
Teams deployments carry distinct identities while the package instrumentation
scopes stay stable.

| Instrument | Type | Owner | Labels beyond the base |
|---|---|---|---|
| `chat.nats.consumer.loop.up` | up-down counter | `pkg/natsmetrics` | stream, consumer |
| `chat.nats.consumer.messages` | counter | `pkg/natsmetrics` | stream, consumer, event_type, outcome |
| `chat.nats.consumer.redeliveries` | counter | `pkg/natsmetrics` | stream, consumer, event_type |
| `chat.nats.consumer.processing.duration` | histogram (s) | `pkg/natsmetrics` | stream, consumer, event_type, outcome |
| `chat.nats.terminal.failures` | counter | `pkg/natsmetrics` | stream, consumer, event_type, reason |
| `chat.nats.publish.attempts` | counter | `pkg/natsmetrics` | destination_kind, operation, outcome |
| `chat.nats.publish.retries` | counter | `pkg/natsmetrics` | destination_kind, operation |
| `chat.nats.requests` | counter | `pkg/natsmetrics` | operation, outcome |
| `chat.nats.request.duration` | histogram (s) | `pkg/natsmetrics` | operation, outcome |
| `chat.nats.request.handled` | counter | `pkg/natsmetrics` / `pkg/natsrouter` | operation, result |
| `chat.nats.request.handler.duration` | histogram (s) | `pkg/natsmetrics` / `pkg/natsrouter` | operation, result |
| `chat.nats.client.connected` | up-down counter | `pkg/natsutil` | none — one series per process; value is the live connection count |
| `chat.nats.client.connection.events` | counter | `pkg/natsutil` | event |
| `nats_slow_consumer_events_total` | counter | `pkg/natsutil` | subject, queue |

The two `chat.nats.client.*` families are the exception: they carry no
`service_name` or `site` at all, because they are emitted from the opt-in
connection helper, which sits below the layer that knows the site. They are
scoped by the OTel resource instead, so join them through `target_info` rather
than expecting inline labels. `nats_slow_consumer_events_total` is scoped the
same way.

All subject- and error-derived dimensions are closed enums. Inbound request
`result` is one of `success`, `bad_request`, `unauthenticated`, `forbidden`,
`not_found`, `conflict`, `too_many_requests`, `unavailable`, or `internal`.
Room and history operations are coarse bounded categories — `room_read`,
`room_mutation`, `member_read`, `member_mutation`, `history_read`,
`history_mutation`, `room_publish`, `member_publish`, `outbox_publish`. Subject
families that do not map normalize to `unknown` rather than minting a label.
Raw subjects, room IDs, account IDs, site IDs parsed out of subject tokens, and
error strings are never labels.

`service_name` and `site` are the exception: they are operator-supplied
deployment identity, not closed enums, so deployment configuration is what
constrains their cardinality to the real service and site inventory.

Reconnect-buffer overflow is **not** a connection event: nats.go returns
`ErrReconnectBufExceeded` synchronously from `publish()` and never routes it
through `ErrorHandler`, so it is counted as
`chat_nats_publish_attempts_total{outcome="buffer_full"}`. The full semantics,
label enums, and the alerts these drive are specified in the NATS failure
metrics contract under `docs/load-testing/failure-testing/`.

### 2.2 Services not previously inventoried (2026-08-14)

The Layer B table above predates these services. Verified from source: none of
them own application metric instruments, so they expose Layer A only (Go
runtime, plus whichever clients they wire). Their dependency columns are not
filled in here because they were not re-verified against a running stack.

`bot-message-handler`, `bot-room-service`, `client-update-service`,
`hr-sync-worker`, `push-notification-service`, `tcard-service`,
`teams-chat-member-sync`, `teams-chat-sync`, `teams-hr-sync`,
`teams-room-creation`, `teams-room-inspector`, `teams-room-verify`,
`teams-user-sync`, `translation-service`.

### 2.3 Metrics registered outside the OTel meter (2026-08-14)

Seven application metrics are registered through `prometheus/client_golang`
(`promauto`) rather than the OTel meter. They are therefore **not** on the SDK
`:2112` endpoint that the rest of this document describes, and they carry none
of the resource attributes (`service_name`, `site`) the OTel families are joined
by — so they cannot be correlated with anything else here without knowing which
endpoint exposes them.

| Metric | Owner | Labels |
|---|---|---|
| `bot_msg_worker_permanent_error_total` | `bot-message-worker` | — |
| `atrest_dek_cache_hits_total` | `pkg/atrest` | — |
| `atrest_dek_cache_misses_total` | `pkg/atrest` | — |
| `atrest_dek_creations_total` | `pkg/atrest` | — |
| `atrest_kek_wrap_total` | `pkg/atrest` | result |
| `atrest_kek_unwrap_total` | `pkg/atrest` | result |
| `atrest_kek_renewal_failures_total` | `pkg/atrest` | — |

`atrest_kek_renewal_failures_total` is documented in its own Help text as a hard
alert (a sustained non-zero rate means the service cannot obtain a Vault token
and encryption will fail once the current one expires). An alert on a series
that is not on the standard endpoint is a trap worth closing: these belong on
the shared meter, which is tracked as gap 6.

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
| NATS/JetStream client metric queries | empty; expected at the time, because NATS emitted spans but no SDK client metrics. Superseded by §2.1: the repository now emits application-side `chat.nats.*` families. |

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
2. **Hot-path domain counters (Layer B).** *Mostly done (2026-08-14).*
   Gatekeeper, message-worker, broadcast, and notification now own domain
   counters (§2 table). Still open: **search-sync bulk outcomes**, and
   broadcast's E2E-key hits.
3. **ES client metrics (Layer A gap).** The NATS half of this item is closed —
   see §2.1 for the application-side families that replaced it. `searchengine`
   still emits spans but no metrics; decide whether app-side ES latency
   histograms are worth adding or whether traces plus the exporter suffice.
4. **DB/Redis/Cassandra/ES server exporters (Layer C).** Standard exporters for
   server health; lower urgency than NATS. *Infra.*
5. **Histogram buckets.** SDK HTTP/DB histograms use `DefaultLatencyBuckets`
   (`WithHistogramBuckets` can override); confirm they match dashboard needs.
6. **Move the seven `promauto` metrics onto the OTel meter** (`pkg/atrest` and
   `bot-message-worker`, §2.3). They are off the `:2112` endpoint and outside
   the shared label scheme, and one of them (`atrest_kek_renewal_failures_total`)
   is meant to be a hard alert.

Tracked as follow-ups in `docs/specs/o11y/o11y-followups.md`.

See also: `o11y-trace-design.md` (traces), `o11y-performance-and-sampling.md`
(cost of the above), `o11y-local-trace-verification.md` (how to view them).
