# NATS / JetStream Failure Metrics Contract

Status: implementation contract. Metrics marked **Existing** are present in the
repository today. Metrics marked **Required** must be implemented and scraped
before the associated campaign is conclusive.

## 1. Purpose and Ownership

This document defines the stable metrics required to distinguish:

- load-generator failure from system-under-test failure;
- broker/JetStream failover from application retry failure;
- a growing durable backlog from a dead consumer loop;
- transient redelivery from terminal exhaustion or silent drop;
- an accepted message that persisted from one that reached its recipients.

Metric definitions and labels are an observability contract and belong with
o11y specifications. Campaign thresholds, time windows, and verdicts belong in
the failure-test runbook. Exact missing operation IDs remain in the loadgen
evidence bundle, not in metrics.

Owners:

| Signal layer | Owner |
|---|---|
| NATS server, RAFT, stream, and consumer state | Platform/IaC via NATS monitoring and exporter |
| Application publish, consume, retry, and loop state | Owning Go service |
| Offered traffic, client observations, and operation outcomes | loadgen |
| Canonical recording rules and production dashboard | o11y/platform |
| Fault annotations and campaign verdict queries | failure-test operator/tooling |

## 2. Naming, Labels, and Cardinality

OTel instruments use dotted names; Prometheus families below show the expected
normalized export. Project-owned metrics use a `chat_` prefix except existing
loadgen families, which retain `loadgen_` for compatibility.

One further exception, and it takes precedence: **an instrument that implements
an OpenTelemetry semantic convention uses the convention's name, labels, unit
and bucket boundaries verbatim, prefix included or omitted as the convention
says.** The request/reply families are the case in point — they are
`rpc.client.call.duration` and `rpc.server.call.duration`, not `chat.nats.*`.
Renaming a standard metric into a house prefix costs exactly what the standard
buys: a generic RPC dashboard, a collector processor, or a backend's built-in
RPC view stops recognising it. Project-specific labels (`site`) may still be
added; the convention permits extra attributes.

Allowed bounded labels:

- `service_name`;
- `site`;
- `stream` and `consumer`, sourced from deployed configuration;
- `lane`, `operation`, `event_type`, and `destination_kind`, from code-owned
  enums;
- `rpc.system.name`, `rpc.method`, and `error.type` on instruments that
  implement the RPC semantic conventions, all three from code-owned enums;
- `outcome`, `reason`, and `event`, from the vocabularies in this document;
- `server`, `cluster`, and `role`, from infrastructure discovery.

Forbidden hot-path labels:

- run, request, trace, message, room, account, user, recipient, inbox subject,
  arbitrary destination subject, pod UID, raw error text, or stack trace;
- concrete request/reply inboxes;
- dynamically generated consumer names outside the declared durable registry.

Use traces, logs, and retained report sidecars for individual identifiers. A
bounded `run_info` metric may describe scenario/environment/traffic profile,
but it must not carry `run_id`.

## 3. Outcome Vocabularies

### Consumer outcomes

`ack`, `nak`, `term`, `left_pending`, and `handler_cancelled`.

- `ack` includes poison messages deliberately acknowledged after a permanent
  classification; distinguish those with `reason="permanent"` on the terminal
  metric, not a new Ack value.
- `nak` means the application requested redelivery.
- `term` means the application explicitly terminated delivery.
- `left_pending` means processing ended without Ack/Nak/Term while the consumer
  loop was still live, so the delivery relies on AckWait redelivery.
- `handler_cancelled` means the delivery ended without Ack/Nak/Term while the
  consumer loop was already down or the handler context was cancelled — shutdown
  took the work away before it settled. Implementations must pick between this
  and `left_pending` from the loop state, not record `left_pending` for both;
  during a long outage the two have opposite meanings.

### Publish outcomes

`timeout`, `no_responders`, `disconnected`, `buffer_full`, `permission`,
`payload_too_large`, and `other_error`.

There is deliberately no `success` value: a publish that succeeded is not
recorded at all (§7.1). The same vocabulary supplies `error.type` on
`rpc.client.call.duration`, where a successful call likewise carries no value —
the convention makes the label conditional on failure, so absence is what marks
success.

### Terminal reasons

`max_deliver`, `permanent`, `publish_exhausted`, `consumer_deleted`,
`stream_unavailable`, `invalid_payload`, and `internal`.

Do not derive these values from arbitrary error strings. Classify using typed
errors, JetStream metadata, and advisory type.

An explicit `Term`/`TermWithReason` is a deliberate application drop and records
`reason="permanent"`. Reserve `internal` for unexpected faults such as a
recovered handler panic; recording a deliberate drop as `internal` fires
`NATSTerminalDelivery` as though the service malfunctioned. A free-text Term
reason goes to JetStream, never to a label. A handler that already classified
the drop keeps its own reason — the first classification per delivery wins.

## 4. Existing Metrics

| Metric | Producer | Status | Limitation |
|---|---|---|---|
| `loadgen_consumer_pending{stream,durable}` | loadgen `ConsumerSampler` | Existing | Only configured samplers; lookup failure is log-only |
| `loadgen_consumer_ack_pending{stream,durable}` | loadgen | Existing | No ack-floor or last-active context |
| `loadgen_consumer_redelivered{stream,durable}` | loadgen | Existing | Gauge snapshot, not terminal evidence |
| `loadgen_nats_connected{pool}` | soak loadgen | Existing | Only soak pool is currently instrumented |
| `loadgen_nats_connection_events_total{pool,event}` | soak loadgen | Existing | Events: connected/disconnected/reconnected/closed/async_error |
| `loadgen_nats_outage_duration_seconds_*{pool}` | soak loadgen | Existing | Only completed reconnect outages are observed |
| `loadgen_failure_operations_total{scenario,lane,result}` | soak loadgen | Existing | Cassandra user-message lane only |
| `loadgen_failure_observations_total{scenario,lane,observer,result}` | soak loadgen | Existing | Admission and Cassandra history only |
| NATS exporter JSZ families | `prometheus-nats-exporter` | Existing in local loadgen/observability compose | Staging/production deployment is external and must be proven by preflight |
| OTel NATS spans | instrumented services | Existing | Spans do not replace counters, gauges, or terminal advisories |
| `nats_slow_consumer_events_total{subject,queue}` | `pkg/natsutil` | Existing | Per-episode count; exact drops are in the log fields |
| `chat_nats_client_connected` / `chat_nats_client_connection_events_total{event}` | `pkg/natsutil` | Existing | Every service connecting through the shared helper; scoped by resource, not by inline `service_name` |
| Section 7 shared application families | `pkg/natsmetrics` | Existing for message-gatekeeper, message-worker, broadcast-worker, notification-worker, history-service, room-service, and room-worker | Adoption depth differs: the first four and room-worker instrument the consumer path; history-service and room-service are publisher-side only |
| Section 8 domain families | owning service | Existing for the four first-campaign services | See Section 8 for the channel fan-out caveat |

## 5. Canonical Infrastructure Metrics

Exporter metric names vary by exporter/version. Platform recording rules must
normalize them into the following project-owned series and document the source
expression beside each rule.

### Server and cluster

| Canonical series | Type | Labels | Required use |
|---|---|---|---|
| `chat_nats_server_up` | gauge | `cluster,site,server` | Member reachability and preflight |
| `chat_nats_connections` | gauge | `cluster,site,server` | Reconnect surge and client loss |
| `chat_nats_routes` | gauge | `cluster,site,server` | Intra-cluster route health |
| `chat_nats_gateways` | gauge | `cluster,site,server,direction` | Cross-site link health |
| `chat_nats_slow_consumers_total` | counter | `cluster,site,server` | Server-side client backpressure |
| `chat_nats_in_bytes_total` / `chat_nats_out_bytes_total` | counter | `cluster,site,server` | Traffic continuity and recovery surge |
| `chat_nats_memory_bytes` | gauge | `cluster,site,server` | Capacity/saturation |
| `chat_nats_storage_bytes` | gauge | `cluster,site,server,state` | Used/free storage and full-disk risk |
| `chat_nats_raft_elections_total` | counter | `cluster,site,group_kind` | Election count/timeline |
| `chat_nats_raft_quorum_up` | gauge | `cluster,site,group_kind,group` | Metadata/stream/consumer quorum |

`group` values must be bounded to configured stream and durable names. If the
exporter exposes ephemeral RAFT group IDs, map them to configured resources or
drop the label.

### Stream

| Canonical series | Type | Labels |
|---|---|---|
| `chat_jetstream_stream_up` | gauge | `cluster,site,stream` |
| `chat_jetstream_stream_leader_up` | gauge | `cluster,site,stream,server` |
| `chat_jetstream_stream_replicas_current` | gauge | `cluster,site,stream` |
| `chat_jetstream_stream_messages` | gauge | `cluster,site,stream` |
| `chat_jetstream_stream_bytes` | gauge | `cluster,site,stream` |
| `chat_jetstream_stream_publish_total` | counter | `cluster,site,stream,outcome` |

Leader series must evaluate to exactly one current leader per available stream.
Recording rules must deduplicate replica-exported copies before summing.

### Consumer

| Canonical series | Type | Labels |
|---|---|---|
| `chat_jetstream_consumer_up` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_leader_up` | gauge | `cluster,site,stream,consumer,server` |
| `chat_jetstream_consumer_pending` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_ack_pending` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_redelivered` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_ack_floor_stream_sequence` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_ack_floor_consumer_sequence` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_oldest_pending_age_seconds` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_last_active_age_seconds` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_max_deliver` | gauge | `cluster,site,stream,consumer` |
| `chat_jetstream_consumer_ack_wait_seconds` | gauge | `cluster,site,stream,consumer` |

`oldest_pending_age_seconds` is required even if it must initially be computed
by a small bounded collector against monitoring APIs. Pending count alone
cannot distinguish a fresh burst from a permanently parked message.

## 6. Advisory Metrics

JetStream advisories are **Core NATS messages, not a replayable stream**. A
direct subscriber that is connected to the node being faulted loses every
advisory emitted while it reconnects, with no replay - in F02, precisely the
leader-change and terminal-delivery advisories the campaign is trying to
capture. A plain subscription therefore cannot support any claim that terminal
or max-delivery evidence is complete.

Provision a stream capturing the required `$JS.EVENT.ADVISORY.>` subjects
**before** the campaign starts, consume it with a monitored durable using
explicit acknowledgements, and persist the raw advisory JSON to the run evidence
bundle from that durable. Emit only bounded aggregates as metrics. A campaign
whose advisory capture was a direct subscription may report advisory counts, but
must not claim advisory completeness.

| Metric | Type | Labels |
|---|---|---|
| `chat_jetstream_advisories_total` | counter | `cluster,site,advisory_type,stream,consumer` |
| `chat_jetstream_terminal_deliveries_total` | counter | `cluster,site,stream,consumer,reason` |
| `chat_jetstream_resource_events_total` | counter | `cluster,site,resource,event` |

Required advisory types include max-delivery exceeded, consumer created or
deleted, stream created or deleted, delivery terminated, and quorum/leader
events available in the deployed NATS version. Unknown advisory types are
counted as `other`; raw type names are not labels.

## 7. Shared Application Metrics

Implement these through one shared package or identical small service-local
wrappers so meanings do not drift.

| OTel instrument / Prometheus family | Type | Labels | Semantics |
|---|---|---|---|
| `chat.nats.consumer.loop.up` / `chat_nats_consumer_loop_up`<br><sub>`chat.nats.consumer.loop.up`</sub> | gauge | `service_name,site,stream,consumer` | 1 only while a live iterator/loop can receive messages |
| `chat.nats.consumer.messages` / `chat_nats_consumer_messages_total`<br><sub>`chat.nats.consumer.messages`</sub> | counter | `service_name,site,stream,consumer,event_type,outcome` | One terminal application disposition per delivery attempt |
| `chat.nats.consumer.processing.duration` / `chat_nats_consumer_processing_duration_seconds_*` | histogram | `service_name,site,stream,consumer,event_type,outcome` | Handler start through disposition attempt |
| `chat.nats.publish.failures` / `chat_nats_publish_failures_total` | counter | `service_name,site,destination_kind,operation,outcome` | One Core or JetStream publish that failed. Successes are not recorded — see below |
| `chat.nats.terminal.failures` / `chat_nats_terminal_failures_total`<br><sub>`chat.nats.terminal.failures`</sub> | counter | `service_name,site,stream,consumer,event_type,reason` | Work that will receive no further application attempt |
| `rpc.client.call.duration` / `rpc_client_call_duration_seconds_*` | histogram | `service_name,site,rpc.system.name,rpc.method,error.type` | One outbound request/reply call. `error.type` absent on success |
| `rpc.server.call.duration` / `rpc_server_call_duration_seconds_*` | histogram | `service_name,site,rpc.system.name,rpc.method,error.type` | One inbound request/reply handler call. `error.type` absent on success |

Three families this table used to require were deleted rather than implemented,
and the reasoning is the standard to hold new ones to:

- `chat.nats.publish.retries` — no path in this repo loops around its own
  publish, so it could never leave zero.
- `chat.nats.consumer.redeliveries` — the broker already publishes
  `jetstream_consumer_num_redelivered`, and which event type keeps failing is
  carried by `chat_nats_consumer_messages_total{event_type,outcome="nak"}`, the
  nak being what causes the redelivery.
- the publish **success** half of `chat.nats.publish.attempts` — for a JetStream
  destination `jetstream_stream_total_messages` is the acceptance count any
  failure ratio needs, and for a Core NATS destination a nil error only means
  the message entered the client's write or reconnect buffer, so a "success"
  there never meant delivery. What remains is `chat.nats.publish.failures`.

Similarly, the counters that used to shadow the two RPC histograms
(`chat.nats.requests`, `chat.nats.request.handled`) are gone: a histogram
publishes its own `_count`, which is the same number on one fewer series.

Rules:

- `messages_total` counts delivery attempts; business operations are counted by
  loadgen ledger metrics and must not be inferred by summing redeliveries.
- Update `consumer_loop_up` to zero before returning from a terminal `Next`,
  iterator, or consumer lookup error. Set it to one only after iterator creation
  succeeds.
- A failed metric update never changes Ack/Nak/Term behavior.
- `destination_kind` is a code enum such as `canonical`, `recipient_event`,
  `notification`, `push`, `outbox`, or `inbox`; never use a concrete subject.
- `event_type` is derived from the subject's last token, not from the payload.
  Every consumer of a stream must derive it identically or the label cannot join
  across services, and a payload parse on the dispatch path serializes the
  consumer. Unrecognized tails use `unknown`.
- A handler that swallows a recipient publish failure must still emit a publish
  failure and a terminal failure when the canonical message will be Acked.
- Business rejections are not terminal failures. A message the service validated
  and refused (not subscribed, restricted room, oversize content) is an ordinary
  client error that received an error reply; it belongs in the domain counter,
  not in `terminal_failures_total`. Counting it there makes the loss signal
  non-zero at baseline and hides real loss.
- Both latency histograms must declare explicit second-scale bucket boundaries.
  The o11y SDK registers bucket views only for its own instrument names, so an
  undeclared histogram inherits the OpenTelemetry default boundaries (0 to
  10000) and every sub-second duration lands in the first bucket. Use the SDK's
  shared latency boundaries so percentiles stay comparable with `http.server.*`.
- There is no publish-retry family. It was declared for "a future retry loop"
  and never acquired one — no service loops around its own publish, and
  JetStream's internal PubAck retries and the consumer Nak path are not
  application retries. A family whose expected value is permanently empty is a
  series, a hot-path attribute set and a reviewer's attention spent on nothing;
  add it when the retry loop exists, not before.

### 7.1 Core NATS publish success is not delivery

This is why the publish family counts failures only. A publish "success" means
different things per destination, and the difference decides how F07a/F07b are
read:

- **JetStream destinations** (`canonical`, `outbox`, `inbox`, `push`) wait for a
  PubAck. Success is broker-confirmed — and already counted, by the broker, as
  `jetstream_stream_total_messages`.
- **Core NATS destinations** (`recipient_event`, `client_response`) return nil as
  soon as the message enters the client's write or reconnect buffer. nats.go
  buffers publishes across a disconnect, so a complete broker outage would
  produce a rising "success" count until the reconnect buffer overflows.

A Core NATS success was therefore never proof a recipient was reached, which is
what made it not worth a series. Read `chat_nats_publish_failures_total`
alongside the client connection metrics below, treat any interval overlapping
`disconnected`, or any `buffer_full` publish outcome, as unproven for recipient
delivery, and take the denominator from the broker (JetStream) or from the
loadgen recipient observer (Core NATS), which is the authoritative recipient
evidence either way.

### 7.2 Client connection state

Every service that opens a broker connection must emit these. Without them a
broker outage and a wedged consumer loop are indistinguishable: the consumer
loop gauge can stay at one while the client is detached, and Core NATS publishes
keep succeeding into the buffer.

| OTel instrument / Prometheus family | Type | Labels | Semantics |
|---|---|---|---|
| `chat.nats.client.connected` / `chat_nats_client_connected`<br><sub>`chat.nats.client.connected`</sub> | gauge | resource only | 1 while the process holds a live broker connection |
| `chat.nats.client.connection.events` / `chat_nats_client_connection_events_total`<br><sub>`chat.nats.client.connection.events`</sub> | counter | `event` | Lifecycle transitions: `connected`, `disconnected`, `reconnected`, `closed`, `async_error` |

Reconnect-buffer overflow is deliberately absent from this family. nats.go
returns `ErrReconnectBufExceeded` synchronously from `publish()` and never routes
it through `ErrorHandler`, so a connection-level event value for it could never
be recorded. The overflow is counted at the publish boundary instead, as
`chat_nats_publish_failures_total{outcome="buffer_full"}` (Section 7), which is
what `NATSReconnectBufferFull` in Section 11 fires on. Any future client-side
loss signal must be taken from a publish return value, not from an async
callback.

These two carry no `service_name` or `site` label. They are emitted from the
shared connection helper, which is below the layer that knows the site, and the
values are already on the OpenTelemetry resource that every service installs —
the same scoping `nats_slow_consumer_events_total` already uses. Recording rules
must join through the resource (`target_info`) rather than expect the labels
inline.

The gauge is idempotent per transition; the events counter is a raw transition
count, matching `loadgen_nats_connection_events_total` in Section 9.

## 8. First-Campaign Service Matrix

The first core-message campaign targets the following minimum instrumentation.

| Service | Consume path | Publish/side effect | Required additions |
|---|---|---|---|
| `message-gatekeeper` | `MESSAGES` durable, default AckWait 30s / MaxDeliver 5 | canonical JetStream PubAck | loop gauge; delivery outcome; canonical publish attempt; Nak/redelivery; terminal exhaustion evidence |
| `message-worker` | user canonical durable, default AckWait 30s / MaxDeliver 5 | Cassandra writes and downstream events | loop gauge; delivery outcome/redelivery; terminal failure; Cassandra batch metrics from the storage contract |
| `broadcast-worker` | user canonical durable, default AckWait 30s / MaxDeliver 5 | channel/DM recipient Core NATS publishes | loop gauge; delivery outcome; fanout-size histogram; recipient publish success/failure; partial-fanout terminal failure |
| `notification-worker` | user canonical durable, default AckWait 30s / MaxDeliver 5 | notification and push events | loop gauge; delivery outcome; sent/suppressed/publish-failed counters |

Recommended domain additions:

| Metric | Type | Labels | Owner |
|---|---|---|---|
| `message_gatekeeper_messages_total` | counter | `result,reason` | message-gatekeeper |
| `message_worker_persistence_total` | counter | `message_kind,result` | message-worker |
| `broadcast_worker_fanout_recipients` | histogram | `room_kind,event_type` | broadcast-worker |
| `broadcast_worker_recipient_deliveries_total` | counter | `room_kind,event_type,result` | broadcast-worker |
| `notification_worker_outcomes_total` | counter | `kind,result` | notification-worker |

These domain counters explain user impact. They supplement, but do not replace,
the shared NATS metrics or the per-operation recipient observer.

`broadcast_worker_fanout_recipients` and
`broadcast_worker_recipient_deliveries_total` are not a ratio for channel rooms.
A channel event is delivered by a single publish to the room stream, so the
histogram records the intended audience while the counter records one attempt;
dividing them looks like large-scale loss on the dominant room type. They are
directly comparable only for DM, bot-DM, and thread fan-out, where the service
publishes once per recipient. Channel per-recipient delivery evidence comes from
the loadgen recipient observer and from nothing else — see Section 7.1 for why
the Core NATS publish outcome cannot stand in for it.

broadcast-worker's reaction author-notification rides the `recipient_event`
destination rather than `notification`: it is a Core NATS publish on the
recipient lane. The `notification` and `inbox` destination values are declared
for future producers and are expected to be absent in the first campaign.

## 9. Loadgen Metrics Required for All Pools

Generalize the existing soak callback implementation:

| Metric | Required labels | Required change |
|---|---|---|
| `loadgen_nats_connected` | `pool` | Instrument daily, soak, members, presence, recipient observer, and every enabled pool |
| `loadgen_nats_connection_events_total` | `pool,event` | Leave as lifecycle-only; buffer overflow is a publish return value, not a connection event |
| `loadgen_nats_outage_duration_seconds_*` | `pool` | Observe completed outages and expose current outage age separately |
| `loadgen_nats_current_outage_seconds` | `pool` | Required gauge for an outage still in progress |
| `loadgen_failure_observer_up` | `observer` | Required observer health |
| `loadgen_failure_observer_events_total` | `observer,result` | Required normalized observer results |
| `loadgen_failure_observer_queue_depth` | `observer` | Required observer saturation evidence |
| `loadgen_consumer_sample_errors_total` | `stream,durable,reason` | Required; lookup/info failures cannot remain log-only |

The loadgen sampler must also export the canonical consumer fields in Section
5 or query the platform recording rules. A missing configured durable is an
error and invalidates the affected campaign; it must not be silently skipped.

## 10. Recording and Verdict Queries

Examples use canonical series; exact windows come from the runbook.

```promql
# Exactly one stream leader.
sum by (site, stream) (chat_jetstream_stream_leader_up) != 1

# A configured consumer is absent.
min_over_time(chat_jetstream_consumer_up[1m]) < 1

# Consumer loop died while the durable still exists.
chat_jetstream_consumer_up == 1
and on (site, stream, consumer)
chat_nats_consumer_loop_up == 0

# Backlog and oldest age are both above baseline.
chat_jetstream_consumer_pending > 0
and on (site, stream, consumer)
chat_jetstream_consumer_oldest_pending_age_seconds > 30

# Terminal application failures.
sum by (service_name, stream, consumer, reason) (
  increase(chat_nats_terminal_failures_total[$__range])
)

# Loadgen connection-impact signal. A transient zero during the declared NATS
# fault is expected evidence; permanent closure, failed recovery, observer loss,
# or inability to offer the declared traffic is what invalidates the interval.
min_over_time(loadgen_nats_connected[1m]) == 0
```

Do not use `pending == 0` as the only recovery condition. Ack floor must resume,
oldest pending age must return to baseline, consumer loop must be up, and final
business reconciliation must complete.

## 11. Required Alerts

| Alert | Minimum condition | Campaign meaning |
|---|---|---|
| `NATSStreamUnavailable` | configured stream up != 1 | Fault/recovery boundary or unexpected platform failure |
| `NATSConsumerMissing` | configured consumer up != 1 | Hard availability failure |
| `NATSConsumerLoopStopped` | durable exists but application loop gauge is 0 | Alive-but-not-consuming failure |
| `NATSConsumerOldestPendingHigh` | age exceeds approved objective | Stuck/slow recovery |
| `NATSTerminalDelivery` | terminal advisory or application terminal counter increases | Enumerate possible loss |
| `NATSReconnectBufferFull` | `chat_nats_publish_failures_total{outcome="buffer_full"}` or the loadgen equivalent increases | Potential client-side loss/inconclusive loadgen interval |
| `NATSClientDisconnected` | `chat_nats_client_connected == 0` outside a declared fault window | Service lost its broker connection; Core NATS publish success is unproven for the interval |
| `LoadgenObserverDown` | required observer up is 0 | `INCONCLUSIVE` |

Threshold values are owned by the campaign/SLO documents, not hardcoded into
the metric implementation.

## 12. Verification Requirements

Every new application metric requires unit tests for:

- happy-path disposition;
- transient error and Nak;
- permanent poison-message path;
- redelivery metadata;
- Ack/Nak/Term failure;
- iterator creation and terminal `Next` failure;
- graceful shutdown;
- partial recipient fanout;
- bounded label values.

Integration verification must:

1. create the target stream and durable through test infrastructure;
2. process one successful message;
3. force one transient failure and redelivery;
4. force one permanent failure;
5. terminate and recreate the consumer loop;
6. scrape metrics and assert exact counter deltas and loop gauge state;
7. confirm operation, message, room, account, and inbox IDs are absent from
   metric labels.

Status: steps 1-4, 6, and 7 are covered by broadcast-worker's embedded-JetStream
test, which drives the production consume composition rather than a parallel
copy of the loop. Step 5 (terminate and recreate the consumer loop mid-run) is
still outstanding and must be added before a formal campaign. The test must
exercise the real loop: a test that drives a copy proves nothing about the code
that ships.

The metrics contract is ready for the core-message campaign only when every
series required by the runbook has a fresh baseline sample and a deliberate
known failure changes the expected series and verdict.

## 13. Application instrument registry

Every OTel instrument this repo declares appears here. A guard test
(`pkg/obs/instrument_registry_test.go`) scans the tree and fails when a new
instrument is not listed, which makes "is this metric necessary?" a gate rather
than a question re-litigated in review.

The **Read by** column is the load-bearing one. A metric nobody reads still
costs a permanent series in the backend, an attribute set on a hot path, and
reviewer time on every PR that touches it. Two instruments were deleted for
exactly that: `chat.nats.publish.retries` never had a producer, and
`chat.nats.consumer.redeliveries` duplicated `jetstream_consumer_num_redelivered`
while `chat.nats.consumer.messages{event_type,outcome="nak"}` already carried
the event-type breakdown. Both would have been stopped here, because neither
could have been given a reader.

**Appears** distinguishes an instrument visible from process start from one
that only materialises once something happens — a counter does not exist in
`/metrics` until its first increment, so an absent family is not evidence of a
broken deployment.

Each row shows the **exported Prometheus name** you query, with the **OTel
instrument name** you grep for in source underneath where the two differ.

### 13.1 Shared NATS families (`pkg/natsmetrics`)

| Exported name | Type | Emitted by | Appears | Platform alternative | Read by |
|---|---|---|---|---|---|
| `chat_nats_consumer_loop_up` | gauge | the 5 JetStream consumers | at startup | none — the broker sees the durable exist, not whether the client loop is alive | failure campaign F11; **the alert this family exists for** |
| `chat_nats_consumer_messages_total` | counter | the 5 JetStream consumers | on first delivery | partial: ack throughput via ack-floor rate; naks and terms have no broker equivalent | campaign; carries the event-type breakdown a redelivery question needs |
| `chat_nats_consumer_processing_duration_seconds`<br><sub>`chat.nats.consumer.processing.duration`</sub> | histogram | the 5 JetStream consumers | on first delivery | none — the broker does not know handler time | campaign (AckWait headroom) |
| `chat_nats_terminal_failures_total` | counter | the 5 JetStream consumers | on first terminal loss | none | campaign; work permanently lost |
| `chat_nats_publish_failures_total`<br><sub>`chat.nats.publish.failures`</sub> | counter | all 7 NATS services | on first failure | none — the broker has no record of a publish that never arrived | campaign |
| `rpc_client_call_duration_seconds`<br><sub>`rpc.client.call.duration`</sub> | histogram | room-service, history-service, message-gatekeeper, broadcast-worker, notification-worker | on first outbound request | none — Core NATS request/reply is invisible to the broker | cross-site health; its `_count` is the call count |
| `rpc_server_call_duration_seconds`<br><sub>`rpc.server.call.duration`</sub> | histogram | every `natsrouter` service | on first inbound request | none | **SLO-4 / SLO-5** (`sli-slo.md` roadmap P1) |

These two are the only families here that do not carry the `chat_` prefix, and
the exception is deliberate: they implement the OpenTelemetry RPC semantic
conventions, so they carry the convention's instrument names
(`rpc.client.call.duration` / `rpc.server.call.duration`), its `rpc.system.name`
and `rpc.method` labels, its `error.type` label, and its bucket boundaries. A
standard name is what makes a generic RPC dashboard, a collector processor or a
backend's RPC view read them without a translation table — the whole point of
using one. They keep our `site` label, which the convention permits.

`error.type` is conditional on failure, so a successful call carries no error
label at all — `sum without (error_type) (...) - sum(...{error_type!=""})` is
not needed; query the unlabelled series directly. Each family replaced a
histogram **and** a counter (`chat.nats.requests`, `chat.nats.request.handled`):
a histogram already publishes `_count`, so the counters were the same numbers on
a second series built from a second attribute set.

The five JetStream consumers are `message-gatekeeper`, `message-worker`,
`broadcast-worker`, `notification-worker`, and `room-worker`. `room-service` and
`history-service` have no consumer loop, so the consumer families are correctly
absent there.

### 13.2 Connection families (`pkg/natsutil`)

| Exported name | Type | Emitted by | Appears | Platform alternative | Read by |
|---|---|---|---|---|---|
| `chat_nats_client_connected` | gauge | all 7 NATS services | at startup | none at per-process granularity | **SLO-1b connection-risk backstop** (roadmap P4) |
| `chat_nats_client_connection_events_total` | counter | all 7 NATS services | at startup (initial connect) | none | SLO-1b backstop |
| `nats_slow_consumer_events_total` | counter | any service using the shared helper | on first episode | none | campaign |

These three carry no `site` label: they are emitted below the layer that knows
the site, so join them through `target_info`.

### 13.3 Domain families

| Exported name | Type | Emitted by | Appears | Platform alternative | Read by |
|---|---|---|---|---|---|
| `message_gatekeeper_messages_total` | counter | message-gatekeeper | on first message | none | campaign; **SLO-1a upstream denominator** (roadmap P2) |
| `message_worker_persistence_total` | counter | message-worker | on first persist | none | campaign; SLO-1a |
| `broadcast_worker_fanout_recipients` | histogram | broadcast-worker | on first fan-out | none | campaign; SLO-1b |
| `broadcast_worker_recipient_deliveries_total` | counter | broadcast-worker | on first fan-out | none | campaign; SLO-1b |
| `notification_worker_outcomes_total` | counter | notification-worker | on first notification | none | campaign; SLO-6 |
| `room_key_fanout_errors_total` | counter | room-worker | on first failure | none | room-key delivery health. Carries no label: the room id lives on the log line beside it |
| `room_key_store_errors_total` | counter | room-worker | on first failure | Mongo driver metrics cover I/O broadly, not this operation | room-key store health |
| `room_key_absent_errors_total` | counter | room-worker | on first occurrence | none | distinguishes "no key" from "store broken" |
| `cache_hits_total` / `cache_misses_total` / `cache_errors_total` | counter | 4 workers + history-service via `pkg/cachemetrics` | on first access | none | **already on the cache-hit-rates dashboard** |
| `search_service_requests_total` | counter | search-service | on first search | none | SLO-7 |
| `search_service_request_duration_seconds` | histogram | search-service | on first search | none | **SLO-8** (needs the status label, roadmap P4) |
| `search_service_es_duration_seconds` | histogram | search-service | on first search | ES `_nodes/stats` is cluster-wide, not per-query | SLO-8 attribution |
| `search_sync_worker_bulk_flush_duration_seconds` | histogram | search-sync-worker | on first flush | none — the o11y ES integration is trace-only (ADR 0020 §6) | ES backpressure attribution |
| `search_sync_worker_bulk_flush_actions` | histogram | search-sync-worker | on first flush | none | batch-fill attribution |
| `search_sync_worker_bulk_item_failures_total` | counter | search-sync-worker | on first item failure | none — 429 visibility exists nowhere else | ES rejection attribution |
| `search_sync_worker_messages_total` | counter | search-sync-worker | on first message | partial: `jetstream_consumer_num_redelivered` counts redeliveries but not their cause | redelivery-source attribution |
| `search_sync_worker_parent_resolve_duration_seconds` | histogram | search-sync-worker | on first thread reply | none | consumer-loop drag attribution |

### 13.4 Adding an instrument

1. Name what reads it — a dashboard panel, an alert rule, or an SLO row. If the
   answer is "nothing yet", that is the finding; add the metric with the
   consumer, or not at all.
2. Check the platform first. `jetstream_*` from the NATS exporter, Mongo driver
   metrics, cAdvisor and ES node stats already answer many questions, and a
   broker-side series needs no application hot path.
3. Every label value comes from a closed enum, and the attribute sets are built
   once and looked up. Two semgrep rules in `.semgrep/metrics.yml` enforce both:
   no per-call `attribute.NewSet`, and no room, account, user, message or
   subject identifier as a label. Identity belongs on the log line and the span.
4. Build the sets on first use, not across the cross product. A closed label
   space can still be wide — `destination_kind` x `operation` x `outcome` is
   1,680 combinations, of which a running service records about fifteen —
   so `pkg/natsmetrics` caches per combination through `optTable` and keeps the
   warm lookup lock-free and allocation-free. Precomputing all of it cost 8,094
   allocations per `Publisher`, retained for the life of the process.
4. Add the row here. The guard test fails otherwise.
