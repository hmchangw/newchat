# NATS / JetStream Metrics — Reference & Ops Guide

What each metric means, what to do when it goes high, and which metrics move for
a given operational event. Companion to the dashboards in `grafana/dashboards/`.

Two exposure paths feed Prometheus:

- **JetStream state** — `prometheus-nats-exporter -jsz=all` reads the NATS monitoring
  endpoint (`:8222`) and re-exposes `jetstream_*` series. No service code involved.
- **Application metrics** — each service exposes OpenTelemetry/Prometheus on `:9090` (the
  observability SDK's exporter). These are the `chat_nats_*`, `cache_*`, `room_key_*`, and
  `search_service_*` series.

> `prometheus-nats-exporter` (jsz) has **no per-subject** message counts — the axes are
> per-stream (`stream_name`) and per-consumer (`consumer_name`) only.

---

## 1. Gauge vs counter — read this first

Two shapes of metric behave oppositely; confusing them is the most common misread.

- **Gauge (point-in-time state).** Reflects *now*. Rises during an incident, **returns to 0
  once resolved**. All `jetstream_*` series are gauges — e.g. `jetstream_consumer_num_redelivered`
  spikes while messages are looping and drops back to 0 the moment they clear. A gauge at 0
  means "nothing stuck right now," **not** "nothing ever went wrong."
- **Counter (cumulative, `_total` suffix).** Monotonic — only ever counts up, resetting to 0
  only on process restart. Never read raw; wrap in `rate()` / `increase()[window]`. All
  `chat_nats_*_total` series are counters — e.g. `increase(chat_nats_consumer_redeliveries_total[1h])`
  answers "how many redeliveries happened in the last hour."

A redelivery gauge falling to 0 does **not** prove the messages were processed — they may
have been dropped after MaxDeliver. To tell healed from dropped, pair the gauge with the
`chat_nats_terminal_failures_total{reason="max_deliver"}` counter (see §4).

---

## 2. JetStream metrics (exporter)

| metric | scope | definition | when HIGH / growing → do |
|---|---|---|---|
| `jetstream_stream_total_messages` | stream | messages retained | Expected within retention. Unbounded growth on a work-queue stream = a consumer not draining. |
| `jetstream_stream_total_bytes` | stream | bytes retained | Approaching a byte limit → discard policy kicks in. Check limits vs ingest. |
| `jetstream_stream_last_seq` | stream | highest stored seq | `rate()` = **publish rate**. A cliff to zero on a busy stream = producers stopped. |
| `jetstream_consumer_num_pending` | consumer | **undelivered backlog** (not yet handed to the consumer) | Sustained rise = consumer behind. Scale workers (`MAX_WORKERS`), check handler latency, confirm the loop is alive (`chat_nats_consumer_loop_up`). |
| `jetstream_consumer_num_ack_pending` | consumer | **delivered-but-unacked** (in-flight); capped by `MaxAckPending` | Pinned at the cap = handlers wedged, or a `MaxAckPending=1` FIFO lane parked on a down dependency. Look at the handler, not the producer. |
| `jetstream_consumer_num_redelivered` | consumer | messages **currently** in a redelivered state (delivery count > 1) | Any sustained non-zero = a failing/naking handler. Cross-check the `max_deliver` terminal counter — if that's also rising, messages are exhausting retries and being dropped. |
| `jetstream_consumer_num_waiting` | consumer | parked pull requests | High with `num_pending>0` = pull starvation (workers idle while backlog sits) — usually a stuck/slow handler holding the ack-pending budget. |
| `jetstream_consumer_delivered_stream_seq` | consumer | last delivered stream seq | Use with the next row. |
| `jetstream_consumer_ack_floor_stream_seq` | consumer | seq below which all acked | `rate()` = **ack rate**. `delivered − ack_floor` widening = **live lag** — the clearest "falling behind" number. |

## 3. Application metrics (`:9090`)

**`pkg/natsmetrics`** — consumer/publisher instrumentation. Consumer labels: `service_name` (resource-derived, added to every series via `WithResourceAsConstantLabels`), `site`, `stream`, `consumer`. `service_name` disambiguates two services that happen to share a durable name.

| metric | type | key labels | meaning / use |
|---|---|---|---|
| `chat_nats_consumer_loop_up` | gauge | (base) | 1 = loop alive, 0 = **consumer loop failed** (graceful stop/error). A hard process crash *removes* the series (it can't emit 0), so a stale `1` can linger on a `lastNotNull` panel — detect a crashed process with `absent(chat_nats_consumer_loop_up)` or the Prometheus scrape `up==0`, not this gauge alone. |
| `chat_nats_consumer_messages_total` | counter | `event_type`, `outcome` | Terminal disposition per delivery. `outcome` ∈ ack / nak / term / left_pending / handler_cancelled. |
| `chat_nats_consumer_redeliveries_total` | counter | `event_type` | Cumulative redelivery count (companion to the `num_redelivered` gauge). |
| `chat_nats_consumer_processing_duration_seconds` | histogram | `event_type`, `outcome` | Handler latency. Rising p99 precedes a `num_pending` climb. |
| `chat_nats_terminal_failures_total` | counter | `event_type`, `reason` | Work that gets no further attempt. `reason="max_deliver"` = a message exhausted retries and was dropped — alert on any non-zero rate. Other reasons: permanent, publish_exhausted, consumer_deleted, stream_unavailable, invalid_payload, plus a catch-all. |

Publisher / request-reply (label `site`): `chat_nats_publish_attempts_total{destination_kind,operation,outcome}`,
`chat_nats_publish_retries_total`, `chat_nats_requests_total{operation,outcome}`,
`chat_nats_request_duration_seconds`, `chat_nats_request_handled_total{operation,result}`,
`chat_nats_request_handler_duration_seconds`. A rising publish `outcome="no_responders"` = a
downstream service is down.

**`pkg/cachemetrics`** — `cache_hits_total` / `cache_misses_total` / `cache_errors_total`, labels
`cache`, `tier`. A collapsing hit ratio or rising `cache_errors_total` = a cache tier degraded → backing-store load rises.

**`pkg/roomkeymetrics`** — `room_key_fanout_errors_total{roomId}`, `room_key_store_errors_total{op}`,
`room_key_absent_errors_total`. Non-zero = clients may fail to decrypt a room.

**`search-service`** — `search_service_requests_total{kind,status}`,
`search_service_request_duration_seconds{kind}`, `search_service_es_duration_seconds`. Rising
`es_duration_seconds` isolates slowness to Elasticsearch vs the handler.

---

## 4. Scenario → which metrics move

Read the dashboard backwards: match the shape you see to the cause.

| What happened | Metrics that move | How to read it |
|---|---|---|
| Consumer can't keep up with ingest | `num_pending` ↑ (sustained), `processing_duration` p99 ↑ | Backlog growing faster than drain. Scale workers / speed up the handler. |
| A handler is wedged (stuck on a dependency) | `num_ack_pending` → pinned at `MaxAckPending`, `num_pending` ↑, ack rate (`rate(ack_floor)`) → 0 | Delivered work not acking. The producer is fine; the consumer isn't. |
| A dependency has a brief blip, handler naks + retries | `num_redelivered` ↑ then → 0, `chat_nats_consumer_redeliveries_total` rate ↑, then recovers | Self-heal in progress — **but only if `chat_nats_terminal_failures_total{reason="max_deliver"}` stayed flat.** The gauge returns to 0 whether messages were acked *or* dropped after MaxDeliver, so confirm no `max_deliver` uptick before calling it cleared (next row). |
| A dependency outage lasts longer than the retry budget | `num_redelivered` ↑ then falls to 0 **while** `chat_nats_terminal_failures_total{reason="max_deliver"}` ↑ | **Not healed — dropped.** Messages exhausted MaxDeliver and were discarded (no DLQ). The most important row: the pending/redelivered gauges *look* recovered. |
| Consumer process crashed / loop died | `chat_nats_consumer_loop_up` → 0, `num_pending` ↑ (nothing consuming) | Page-worthy. Nothing is being processed for that durable. |
| A cross-site peer is unreachable | that destination's `num_ack_pending` ↑ (concurrent lane) and `num_pending` ↑ (FIFO ordered lane, stuck behind one in-flight message) | Isolate by `consumer_name` (`…-{dest}`). Self-clears when the peer recovers; healthy peers' lanes stay flat. |
| A stream is filling toward its limit | `stream_total_messages` / `stream_total_bytes` ↑ toward the configured limit | Discard policy will start dropping. Check retention vs ingest. |
| A downstream RPC target is down | `chat_nats_requests_total{outcome="no_responders"}` ↑, `chat_nats_publish_attempts_total{outcome="no_responders"}` ↑ | The caller is healthy; the callee is absent. |
| Cache tier (Valkey) degraded | `cache_errors_total` ↑, hit ratio ↓, backing-store latency ↑ | Load shifts to Mongo/Cassandra; expect secondary latency rises. |

---

## Dashboards

- **NATS — JetStream Overview** / **JetStream — Pending & Backlog** — the `jetstream_*` gauges.
- **NATS — Application Health & Terminal Failures** — the `chat_nats_*` app series above,
  including the `max_deliver` silent-drop counter and `consumer_loop_up` liveness.
- **Containers — CPU & Memory**, **Cache hit rates**.

Run with `make obs-up` (see `README.md`); Grafana at `http://localhost:3002`.
