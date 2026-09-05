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

- **Gauge (point-in-time state).** Reflects *now*; may rise or fall and is **not** necessarily
  monotonic or expected to return to 0 (e.g. `jetstream_stream_last_seq`, the ack-floor seq, and
  retained-message/byte gauges all sit at healthy nonzero baselines). All `jetstream_*` series are
  gauges. The **return-to-0** behavior applies specifically to the *backlog/redelivery* gauges —
  e.g. `jetstream_consumer_num_redelivered` spikes while messages loop and drops back to 0 the
  moment they clear; there, 0 means "nothing stuck right now," **not** "nothing ever went wrong."
- **Counter (cumulative, `_total` suffix).** Monotonic — only ever counts up, resetting to 0
  only on process restart. Never read raw; wrap in `rate()` / `increase()[window]`. All
  `chat_nats_*_total` series are counters — e.g.
  `increase(chat_nats_consumer_messages_total{outcome="nak"}[1h])` answers "how many deliveries
  were nak'd in the last hour."

A redelivery gauge falling to 0 does **not** prove the messages were processed — they may
have been abandoned after MaxDeliver (the consumer stops delivering them; they stay in the
stream, unprocessed). To tell healed from abandoned, pair the gauge with the
`chat_nats_terminal_failures_total{reason="max_deliver"}` counter (see §4).

> ⚠️ The app counter is **best-effort, not authoritative**: it's incremented by the
> consumer when it observes the final attempt, so if the process **crashes on that last
> delivery before recording it**, JetStream still exhausts MaxDeliver and stops — but the
> increment is lost, and no later attempt repairs it. The authoritative signal is the
> **broker's MaxDeliver advisory** (`$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>`); capture
> and alert on that for a guarantee. Treat the app counter as a fast secondary diagnostic.

---

## 2. JetStream metrics (exporter)

| metric | scope | definition | when HIGH / growing → do |
|---|---|---|---|
| `jetstream_stream_total_messages` | stream | messages retained | Expected within retention. Unbounded growth on a work-queue stream = a consumer not draining. |
| `jetstream_stream_total_bytes` | stream | bytes retained | Approaching a byte limit → discard policy kicks in. Check limits vs ingest. |
| `jetstream_stream_last_seq` | stream | highest stored seq | `rate()` = **publish rate**. A cliff to zero on a busy stream = producers stopped. |
| `jetstream_consumer_num_pending` | consumer | **undelivered backlog** (not yet handed to the consumer) | Sustained rise = consumer behind. Scale workers (`MAX_WORKERS`), check handler latency, confirm the loop is alive (`chat_nats_consumer_loop_up`). |
| `jetstream_consumer_num_ack_pending` | consumer | **delivered-but-unacked** (in-flight); capped by `MaxAckPending` | Pinned at the cap = handlers wedged, or a `MaxAckPending=1` FIFO lane parked on a down dependency. Look at the handler, not the producer. |
| `jetstream_consumer_num_redelivered` | consumer | messages **currently** in a redelivered state (delivery count > 1) | Any sustained non-zero = repeated delivery attempts — a handler sending NAKs, **or** `AckWait` expiring on a slow/hung handler, **or** a consumer crash-looping on the same message. Cross-check the `max_deliver` terminal counter — if that's also rising, messages are exhausting retries and the consumer **stops delivering** them (they stay in the stream until retention; an advisory fires — recoverable, but nothing has processed them). |
| `jetstream_consumer_num_waiting` | consumer | parked pull requests | High with `num_pending>0` = pull starvation (workers idle while backlog sits) — usually a stuck/slow handler holding the ack-pending budget. |
| `jetstream_consumer_delivered_stream_seq` | consumer | last delivered stream seq | Use with the next row. |
| `jetstream_consumer_ack_floor_stream_seq` | consumer | highest contiguous acked **stream** seq | With concurrent/out-of-order processing `rate(ack_floor)` measures only **contiguous floor movement** — one early unacked delivery makes it stall then spike, so it is **not** ack throughput. `delivered − ack_floor` is a **stream-sequence span, NOT the in-flight message count** — for a subject-filtered consumer it counts unrelated messages between matches. Use **`num_ack_pending`** for outstanding work, **`num_pending`** for undelivered backlog, and `rate(chat_nats_consumer_messages_total{outcome="ack"}[$__rate_interval])` for ack throughput. |

## 3. Application metrics (`:9090`)

**`pkg/natsmetrics`** — consumer/publisher instrumentation. Consumer labels: `service_name` (resource-derived, added to every series via `WithResourceAsConstantLabels`), `site`, `stream`, `consumer`. `service_name` disambiguates two services that happen to share a durable name.

| metric | type | key labels | meaning / use |
|---|---|---|---|
| `chat_nats_consumer_loop_up` | gauge | (base) | 1 = loop alive, 0 = **consumer loop failed** (graceful stop/error). A hard process crash *removes* the series (it can't emit 0), so a stale `1` can linger on a `lastNotNull` panel. Two distinct detectors: **`up == 0`** catches the whole scrape target (process/pod) going down; a **single** crashed durable while its process still runs needs a **label-scoped** `absent(chat_nats_consumer_loop_up{service_name="…",site="…",stream="…",consumer="…"})` per expected durable — bare `absent(chat_nats_consumer_loop_up)` fires only when *every* consumer's series is gone, so it misses one durable when siblings remain. |
| `chat_nats_consumer_messages_total` | counter | `event_type`, `outcome` | Terminal disposition per delivery. `outcome` ∈ ack / nak / term / left_pending / handler_cancelled. |
| `chat_nats_consumer_processing_duration_seconds` | histogram | `event_type`, `outcome` | Handler latency. Rising p99 precedes a `num_pending` climb. |
| `chat_nats_terminal_failures_total` | counter | `event_type`, `reason` | Work that gets no further attempt. `reason="max_deliver"` = a message exhausted retries; JetStream **stops delivering it to this consumer** (it stays in the stream until retention, and an advisory fires — recoverable, but no handler processed it). Alert on any non-zero rate. Other reasons: permanent, publish_exhausted, consumer_deleted, stream_unavailable, invalid_payload, plus a catch-all. |

There is deliberately no application redelivery counter. `jetstream_consumer_num_redelivered`
is **not** its replacement — that gauge is current state (§2), so a redelivery that completes
between two scrapes leaves nothing behind. This is an accepted blind spot, documented in
[`docs/specs/o11y/nats-metrics-contract.md`](../../docs/specs/o11y/nats-metrics-contract.md) §4:
a worker that finishes a side effect and then exceeds `AckWait` or crashes before acking shows
only an ack once the retry succeeds, and the duplicate processing is invisible.

Publisher / request-reply (label `site`):

| metric | type | key labels | meaning / use |
|---|---|---|---|
| `chat_nats_publish_failures_total` | counter | `destination_kind`, `operation`, `outcome` | Publishes that **failed**, by bounded cause. Successes are not counted, so read it as a rate against itself over time, never as a ratio — there is no acceptance denominator (`jetstream_stream_total_messages` is stream depth, not accepted publishes). A rising `outcome="no_responders"` = a downstream service is down; `outcome="buffer_full"` = client-side loss, and the alert for it is `NATSReconnectBufferFull`. |
| `rpc_client_call_duration_seconds` | histogram | `rpc_method`, `error_type` | Outbound request/reply. The histogram's `_count` **is** the call count — there is no paired counter. Per OTel RPC semconv `error_type` is absent on success, so a successful call is `{error_type=""}`, and total calls are the bare `_count`. |
| `rpc_server_call_duration_seconds` | histogram | `rpc_method`, `error_type` | Inbound handler latency at the `natsrouter` boundary, same shape. SLO-4/5 read this; see [`docs/load-testing/common/sli-slo.md`](../../docs/load-testing/common/sli-slo.md) §3 for the good/valid expressions — the denominator is **not** the bare `_count`, because the 4xx classes are excluded from valid events. |

Both RPC families carry `rpc_system_name="nats"`. `rpc_method` is a bounded operation, never the
subject: it is declared at route registration (`natsrouter.Register`/`RegisterNoBody`/
`RegisterOptionalBody` take a required `natsmetrics.RPCMethod` argument, and `.semgrep/rpcmethod.yml`
is what forces that argument to be a `Method*` constant — the compiler alone accepts a bare string
literal), and `RegisterVoid`
routes (fire-and-forget, no reply) carry no method and record no `rpc_server_call_duration_seconds`
sample at all — they are absent from this family, not present with an empty label.
`rpc_method="_OTHER"` is **not** a fallback bucket for an unmapped subject; it only appears when
registration itself was wrong (an undeclared method reaching `addRPCRoute`, which the semgrep rule
and each service's `routes_test.go` golden test are meant to catch first). It should be zero in
steady state — alert on any non-zero rate.

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
| A handler is wedged (stuck on a dependency) | `num_ack_pending` → pinned at `MaxAckPending`, `num_pending` ↑ | Delivered work not acking. The producer is fine; the consumer isn't. (Use `num_ack_pending`, not the `ack_floor` seq delta — the latter is a sequence span, not the in-flight count.) |
| A dependency has a brief blip, handler naks + retries | `num_redelivered` ↑ then → 0, `chat_nats_consumer_messages_total{outcome="nak"}` rate ↑, then recovers | Self-heal in progress — **but only if `chat_nats_terminal_failures_total{reason="max_deliver"}` stayed flat.** The gauge returns to 0 whether messages were acked *or* abandoned after MaxDeliver. And since the app `max_deliver` counter is best-effort (a final-attempt crash loses the increment, §1), don't call it cleared from a flat counter alone — confirm the broker MaxDeliver advisory / broker state shows nothing exhausted (next row). The nak counter covers only the *nak* half of this shape: a redelivery caused by `AckWait` expiry, cancellation or a crash never naks, so `num_redelivered` can move while the counter stays flat. |
| A dependency outage lasts longer than the retry budget | `num_redelivered` ↑ then falls to 0 **while** `chat_nats_terminal_failures_total{reason="max_deliver"}` ↑ | **Not healed — abandoned.** Messages exhausted MaxDeliver; the consumer stops delivering them and no handler processed them. They remain in the stream until retention (recoverable via replay/advisory), but nothing auto-routes them (no DLQ). The most important row: the pending/redelivered gauges *look* recovered. |
| Consumer loop exited / process crashed | **Graceful** loop exit → `chat_nats_consumer_loop_up` → 0; a **hard crash** removes the series (it can't emit 0 — the last value goes stale, then disappears), `num_pending` ↑ | Page-worthy. Page on the gauge `== 0` **and** on `up == 0` (whole target down); for one durable dying while its process survives, use a label-scoped `absent(chat_nats_consumer_loop_up{…consumer=…})` per expected durable — a crash won't show as 0. Nothing is being processed for that durable. |
| A cross-site peer is unreachable | that destination's `num_ack_pending` ↑ (concurrent lane) and `num_pending` ↑ (FIFO ordered lane, stuck behind one in-flight message) | Isolate by `consumer_name` (`…-{dest}`). Self-clears when the peer recovers; healthy peers' lanes stay flat. |
| A stream is filling toward its limit | `stream_total_messages` / `stream_total_bytes` ↑ toward the configured limit | Discard policy will start dropping. Check retention vs ingest. |
| A downstream RPC target is down | `rpc_client_call_duration_seconds_count{error_type="no_responders"}` ↑, `chat_nats_publish_failures_total{outcome="no_responders"}` ↑ | The caller is healthy; the callee is absent. |
| Cache tier (Valkey) degraded | `cache_errors_total` ↑, hit ratio ↓, backing-store latency ↑ | Load shifts to Mongo/Cassandra; expect secondary latency rises. |

---

## Dashboards

- **NATS — JetStream Overview** / **JetStream — Pending & Backlog** — the `jetstream_*` gauges.
- **NATS — Application Health & Terminal Failures** — the `chat_nats_*` app series above,
  including the `max_deliver` retry-exhaustion counter, `consumer_loop_up` liveness, and a
  `up == 0` scrape-health panel (the crash signal the loop gauge can't show). Its one
  non-app panel is the `jetstream_consumer_num_redelivered` gauge, kept here rather than on
  the JetStream boards because it is only conclusive read against the `max_deliver` counter
  beside it.
- **Containers — CPU & Memory**, **Cache hit rates**.

Run with `make obs-up` (see `README.md`); Grafana at `http://localhost:3002`.
