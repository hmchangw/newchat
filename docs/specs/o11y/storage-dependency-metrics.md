# Storage Dependency Metrics

> Verified against the code on 2026-08-21. This is the repository's dictionary
> of MongoDB and Cassandra client metrics: what each family is called, what
> labels it carries, and what it proves. Panels and alerts are built from it,
> but are not defined here.
>
> MongoDB server metrics come from the managed service's own dashboard and are
> deliberately not inventoried here.

## 1. Status Vocabulary

| Status | Meaning |
|---|---|
| Existing | Emitted by current code and scrapeable when the service is deployed with the repository's SDK metrics endpoint |
| Partial | Emitted only by some direct clients, or lacks enough labels/coverage for a reliable dashboard |
| Missing | Not emitted or not scraped in the current repository; required before the related failure campaign can produce a conclusive result |

Prometheus converts OpenTelemetry dots to underscores and appends unit/type suffixes. For example, `db.client.operation.duration` is normally exported as the `db_client_operation_duration_seconds_*` histogram family. Verify the actual collector/exporter output in the target environment before installing alerts.

## 2. Existing MongoDB Client Metrics

`pkg/mongoutil.Connect` instruments clients only when the caller passes `mongoutil.WithObservability`. The o11y integration currently emits the following complete MongoDB client metric set:

| OTel instrument / Prometheus family | Type | Allowed labels | What it proves | Status |
|---|---|---|---|---|
| `db.client.operation.duration` / `db_client_operation_duration_seconds_*` | Histogram | `service_name`, `db_system_name`, `db_operation_name`, `network_peer_address`, `network_peer_port`, `error_type` | Command throughput, latency, and observed errors by service/operation/server | Existing for instrumented clients |
| `db.client.connection.count` / `db_client_connection_count` | Up/down counter | `service_name`, `db_system_name`, pool name, connection state (`used`/`idle`), server address/port | Pool occupancy and connection loss/recovery | Existing for instrumented clients |
| `db.client.connection.idle.min` | Up/down counter | `service_name`, `db_system_name`, pool name, server address/port | Configured warm idle floor | Existing for instrumented clients |
| `db.client.connection.max` | Up/down counter | Same as above | Configured maximum; zero means the driver/default was not exposed as a finite configured value | Existing for instrumented clients |
| `db.client.connection.pending_requests` | Up/down counter | Same as above | Checkout queue pressure | Existing for instrumented clients |
| `db.client.connection.timeouts` | Counter | Same as above | Pool checkout timeouts | Existing for instrumented clients |
| `db.client.connection.create_time` | Histogram | Same as above | Connection establishment latency | Existing for instrumented clients |

The SDK deliberately drops `db.client.connection.idle.max`, `.use_time`, and `.wait_time`. Dashboard queries must not depend on those families.

Mongo operation metrics intentionally omit database and collection labels to control cardinality. Traces are required when an operator must distinguish collections or individual calls.

### 2.1 MongoDB instrumentation coverage

| Coverage | Direct MongoDB processes |
|---|---|
| Instrumented | admin-service; bot-message-handler; bot-message-worker; botplatform-service; bot-room-service; broadcast-worker; history-service; hr-sync-worker; inbox-worker; media-service; message-gatekeeper; message-worker; notification-worker; portal-service; room-service; room-worker; search-service; search-sync-worker; tcard-service; teams-room-inspector; upload-service; user-presence-service; user-service; data-migration oplog connector/direct-transfer/transformers |
| Missing instrumentation | teams-chat-member-sync; teams-chat-sync; teams-hr-sync; teams-room-creation; teams-room-verify; teams-user-sync; loadgen's own MongoDB clients |

Every service that carries ordinary chat traffic is instrumented. The uninstrumented processes are the Teams synchronisation lane, the migration lane and loadgen's own clients; none of them serve user traffic. A healthy metric from another service is never evidence that an uninstrumented client recovered.

## 3. Existing Cassandra Client Metrics

`pkg/cassutil.Connect` instruments sessions only when the caller passes `cassutil.WithObservability`.

| OTel instrument / Prometheus family | Type | Allowed labels | What it proves | Status |
|---|---|---|---|---|
| `db.client.operation.duration` / `db_client_operation_duration_seconds_*` | Histogram | `service_name`, `db_system_name`, `db_operation_name`, `db_namespace`, server address/port, `error_type` | Per-attempt/page query latency, rate, errors, and coordinator changes | Existing for ordinary queries |
| `cassandra.query.attempts` / `cassandra_query_attempts_total` | Counter | Same bounded query labels | One observation per driver attempt and per result page | Existing for ordinary queries |
| `db.client.connection.create_time` / `db_client_connection_create_time_seconds_*` | Histogram | `service_name`, `db_system_name`, pool name, server address/port | Successful connection establishment latency | Existing |
| `cassandra.connection.attempts` / `cassandra_connection_attempts_total` | Counter | Same connection labels plus `error_type` | Connection attempts and failures by node | Existing |

Direct Cassandra session coverage is:

| Coverage | Processes |
|---|---|
| Instrumented | message-worker; bot-message-worker; history-service |
| Missing instrumentation | data-migration/es-index-migrator; loadgen direct-Cassandra seed/teardown and legacy direct modes |

Batch call sites need o11y's `ExecuteBatch` seam to produce spans/metrics; a direct `session.ExecuteBatch` is a blind spot even inside an otherwise instrumented service. Current coverage:

| Batch call site | Seam | Status |
|---|---|---|
| `message-worker` message and thread-message writes (4 sites, plaintext and encrypted) | `o11ycassandra.ExecuteBatch` | Instrumented |
| `history-service` reactions (4 sites) and pin/unpin (2 sites) | `session.ExecuteBatch` | Missing |
| `bot-message-worker` bot message fan-out (4 sites) | `session.ExecuteBatch` | Missing |

The message-worker sites cover the first core-message campaign's write path. The remaining sites must move to the seam before a campaign claims Cassandra coverage for reactions, pin/unpin, or the bot lane.

## 4. Existing Storage-Relevant Application and Loadgen Metrics

These metrics do not replace database telemetry; they connect dependency behavior to a product outcome.

| Metric family | Producer | Use during a storage fault | Status |
|---|---|---|---|
| `loadgen_soak_operations_total{action,outcome,phase}` | loadgen soak | Offered and completed Cassandra-path operations | Existing |
| `loadgen_soak_retries_total{action,phase}` | loadgen soak | Harness retries; must be kept separate from application/driver retries. `phase` is `warmup` or `measured` | Existing |
| `loadgen_soak_errors_total{action,class,phase}` | loadgen soak | Failure class by business action. `phase` is `warmup` or `measured` | Existing |
| `loadgen_soak_error_reasons_total{action,class,reason,phase}` | loadgen soak | Service-supplied errcode reason beside the class, which alone cannot separate answers that need different responses (`not_subscribed` vs `outside_access_window`) | Existing |
| `loadgen_soak_rpc_latency_seconds_*{action}` | loadgen soak | End-to-end latency through message/history paths | Existing |
| `loadgen_soak_verifications_total{action,class,field}` | loadgen soak | Sampled Cassandra read-back correctness. `field` names what disagreed on a mismatch | Existing |
| `loadgen_soak_mutation_target_missing_total` | loadgen soak | Persisted target still absent after the dedicated wait/retry policy | Existing |
| `loadgen_soak_lane_saturation_total{lane}` | loadgen soak | Invalid-run detector: a lane dropped work because its own in-flight budget filled | Existing |
| `loadgen_soak_global_saturation_total{lane}` | loadgen soak | Invalid-run detector: a lane dropped work because the shared in-flight budget filled | Existing |
| `loadgen_failure_operations_total{scenario,lane,result}` | loadgen soak | Terminal `good`/`bad`/`unverified`/`not_sent`/`missing_after_deadline` results for durable operation lanes | Existing for Cassandra user-message sends |
| `loadgen_failure_observations_total{scenario,lane,observer,result}` | loadgen soak | Separates admission failures from Cassandra history loss or mismatch | Existing for admission and Cassandra history |
| `loadgen_failure_inflight{scenario,lane}` | loadgen soak | Unresolved-operation backlog and deadline pressure | Existing for Cassandra user-message sends |
| `loadgen_failure_recipient_expectations` | loadgen soak | Recipient expectations retained in memory, collected at scrape time so a stalled expiry sweep cannot freeze it. A healthy run tracks `loadgen_failure_inflight`; climbing past it means the map is outliving the operations again | Existing |
| `loadgen_failure_reconcile_claims_total{outcome}` | loadgen soak | Splits reconcile claims into `advanced` (an observer resolved), `retried` (the history poll found nothing yet), `idle` (nothing was due, so the lane has slack) and `failed`. The startup capacity rule can only model one claim per observer; `retried` is the cost it cannot derive, and `rate(loadgen_failure_reconcile_claims_total{outcome="idle"}[5m]) == 0` is the lane saturating | Existing |
| `loadgen_failure_recovered_operations_total` | loadgen soak | Operations restored from the PVC-backed WAL after loadgen restart | Existing |
| `loadgen_failure_invalidations_total{reason}` | loadgen soak | Ledger capacity or WAL failures that invalidate evidence | Existing |
| `loadgen_failure_journal_bytes` | loadgen soak | Persistent evidence footprint and compaction health | Existing |
| `loadgen_failure_wal_append_duration_seconds`, `loadgen_failure_wal_appends_total{result}` | loadgen soak | Caller-visible WAL delay and bounded append result; pre-publish intents include the durability barrier | Existing |
| `loadgen_failure_wal_flush_duration_seconds{result}`, `loadgen_failure_wal_flush_batch_size{result}` | loadgen soak | Direct grouped fsync latency and records committed per barrier | Existing |
| `loadgen_failure_evidence_flush_duration_seconds{claim,result}`, `loadgen_failure_evidence_records_total{kind}` | loadgen soak | Terminal recipient-sidecar barrier latency, failures, and bounded evidence kinds | Existing |
| `loadgen_failure_untracked_total{reason}` | loadgen soak | Sends the ledger could not account for, so degraded observation is visible instead of silent | Existing |
| `loadgen_failure_dropped_total` | loadgen soak | Recovered operations discarded because the WAL exceeded the configured capacity | Existing |
| `loadgen_nats_connected{pool}`, `loadgen_nats_connection_events_total{pool,event}`, `loadgen_nats_outage_duration_seconds_*{pool}` | loadgen soak | Separates generator connection loss from service/storage impact | Existing for soak; other loadgen pools remain a gap |
| `loadgen_published_total`, `loadgen_publish_errors_total`, E1/E2 histograms | loadgen message modes | Admission and visible-delivery impact on Mongo/Cassandra-backed paths | Existing; terminal ledger is currently limited to soak message sends |
| `loadgen_member_*` | loadgen member modes | Mongo-backed room/member operation impact | Existing but no final state ledger |
| `loadgen_botroom_*` | loadgen botroom mode | Bot path traffic and latency | Existing, but it does not exercise the real bot-message-worker persistence lane |
| `oplog_events_published_total`, `oplog_publish_errors_total`, `oplog_events_skipped_total`, `oplog_events_degraded_total`, `oplog_replication_lag_ms` | oplog-connector | Mongo change-stream progress and downstream publish health | Existing |
| `atrest_dek_cache_hits_total`, `atrest_dek_cache_misses_total`, `atrest_dek_creations_total`, `atrest_kek_wrap_total`, `atrest_kek_unwrap_total`, `atrest_kek_renewal_failures_total` | at-rest encryption package | Separates storage failure from key-cache/Vault behavior on encrypted message paths | Existing where at-rest encryption is wired |
| `cache_hits_total`, `cache_misses_total`, `cache_errors_total` | service caches | Explains whether Mongo load/impact was hidden or amplified by cache behavior | Existing in selected services |
| `go_*`, `process_*`, container CPU/memory/network | SDK/runtime, cAdvisor | Detects client or loadgen exhaustion and recovery surge | Existing where targets are scraped |

The Cassandra soak message-send lane now proves a terminal admission and
history result for every operation successfully admitted to its ledger and
retains unresolved evidence on a dedicated PVC. Sends that cannot be admitted
or observed are counted as untracked, and over-capacity WAL replay is counted
as dropped; either condition invalidates the affected observation interval.
Other loadgen lanes remain aggregate or sampled. A campaign that relies on one
of those lanes remains inconclusive if aggregate success looks healthy but
individual operations can disappear.

## 5. Missing Metrics and Telemetry

### 5.1 Gaps that change what a result can prove

| Missing signal | Required implementation | Why it is required |
|---|---|---|
| Cassandra server metrics | JMX exporter or agent on every node | Client latency cannot identify dropped messages, pending compactions, hints, tombstones, coordinator saturation, GC, disk, or repair progress |
| Storage readiness | No service probes MongoDB or Cassandra; only `natsutil.HealthCheck` is registered | A storage-broken pod stays `Ready`. Deliberate for now — a probe would pull every pod from its Service at once during a full outage — so client metrics have to carry the signal |
| Mongo instrumentation gaps | Instrument every direct service/process listed in Section 2.1 | Otherwise the service-by-service dashboard is incomplete |
| Cassandra batch telemetry | Route production batches through the o11y batch seam or add equivalent batch duration/error metrics | The highest-risk denormalized writes are currently absent from Cassandra operation metrics |
| Retry/exhaustion metrics | Count application retry, driver attempt, JetStream redelivery, terminal failure, and permanent drop separately | Logs alone cannot prove retry safety or enumerate exhausted work |
| Operation outcome ledger expansion | The ledger settles message sends and the five room/member lanes. Thread and user writes, the real bot chain and federation are not covered | Aggregate success can look healthy while individual operations disappear |

### 5.2 Cassandra server signals to normalize

| Canonical signal | Dimensions | Dashboard/alert use |
|---|---|---|
| `chat_storage_member_up{dependency="cassandra"}` | cluster, dc, rack, node | Node reachability/topology |
| `chat_cassandra_client_request_total{operation,outcome}` | cluster, dc, node | Read/write failure rate |
| `chat_cassandra_client_request_latency_seconds_*{operation}` | cluster, dc, node | Server-side read/write latency |
| `chat_cassandra_timeouts_total{operation}` | cluster, dc, node | Timeout classification |
| `chat_cassandra_unavailable_total{operation}` | cluster, dc, node | Consistency/quorum failure |
| `chat_cassandra_dropped_messages_total{type}` | cluster, dc, node, type | Internal overload/loss signal |
| `chat_cassandra_pending_compactions` | cluster, dc, node | Compaction backlog and recovery |
| `chat_cassandra_compaction_bytes_total` | cluster, dc, node | Compaction work rate |
| `chat_cassandra_hints{state}` | cluster, dc, node, state | Hinted handoff backlog/delivery |
| `chat_cassandra_read_repair_total{outcome}` | cluster, dc, node | Replica convergence evidence |
| `chat_cassandra_tombstone_scanned_*` | cluster, dc, node | Delete/read amplification |
| `chat_cassandra_threadpool_tasks{pool,state}` | cluster, dc, node, pool, state | Native transport/mutation/read saturation |
| `chat_cassandra_gc_pause_seconds_*` | cluster, dc, node | JVM stall attribution |
| `chat_cassandra_sstable_count{table}` | cluster, dc, node, keyspace, table | TWCS/read amplification |
| `chat_storage_disk_bytes{dependency="cassandra",state}` | cluster, dc, node, state | Capacity and recovery headroom |
| `chat_cassandra_repair_progress{state}` | cluster, dc, node, state | Operator repair completion |

Table/keyspace labels are acceptable on server metrics because their values are bounded by deployed schema. Never label application hot-path metrics with room ID, message ID, account, trace ID, or arbitrary error text.

## 6. Core PromQL Patterns

The following queries use the SDK's current Prometheus naming. Label spelling must be verified against the deployed collector.

```promql
# Operation rate by storage and service
sum by (db_system_name, service_name, db_operation_name) (
  rate(db_client_operation_duration_seconds_count[5m])
)

# Operation error ratio (error_type exists only on failures)
sum by (db_system_name, service_name) (
  rate(db_client_operation_duration_seconds_count{error_type!=""}[5m])
)
/
sum by (db_system_name, service_name) (
  rate(db_client_operation_duration_seconds_count[5m])
)

# p99 client latency
histogram_quantile(0.99,
  sum by (le, db_system_name, service_name, db_operation_name) (
    rate(db_client_operation_duration_seconds_bucket[5m])
  )
)

# Mongo pool checkout pressure
sum by (service_name, db_client_connection_pool_name) (
  db_client_connection_pending_requests
)

# Mongo pool timeout rate
sum by (service_name, db_client_connection_pool_name) (
  rate(db_client_connection_timeouts_total[5m])
)

# Cassandra observed attempts by service/coordinator
sum by (service_name, server_address, error_type) (
  rate(cassandra_query_attempts_total[5m])
)

# Loadgen soak terminal mix
sum by (action, outcome, phase) (
  rate(loadgen_soak_operations_total[5m])
)

# Read-back failures
sum by (action, class) (
  increase(loadgen_soak_verifications_total{class!="ok"}[$__range])
)
```

In PromQL, `error_type!=""` excludes series where the label is absent as well as series where it is empty. Keep the denominator unfiltered so it includes successful and failed operations.

## 7. Code Evidence

- Mongo client setup and startup ping: `pkg/mongoutil/mongo.go`, `tuning.go`, and `readpref.go`.
- Cassandra consistency, timeout, host policy, and pool setup: `pkg/cassutil/cass.go`.
- SDK metric definitions and label filters: cached `github.com/flywindy/o11y v0.9.1` Mongo and Cassandra integrations, selected by `go.mod`.
- Current loadgen collectors: `tools/loadgen/metrics.go`.
- Mongo change-stream outcome metrics: `data-migration/oplog-connector/metrics.go`.
- At-rest metrics: `pkg/atrest/metrics.go`.
- Current readiness wiring: service `main.go` files and `pkg/natsutil/health.go`; no MongoDB/Cassandra probe is registered.
