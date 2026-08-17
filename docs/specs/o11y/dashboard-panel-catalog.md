# Dashboard Panel Catalog

> Companion to the [Dashboard and Alert Guide](dashboard-and-alert-guide.md).
> That document owns the model, the prerequisites, and the reading traps
> referenced here as "trap 7.x". This one specifies the panels.

Every panel carries: source status, applicable situations, PromQL, expected
reading, and what no-data means. Source status values are defined in the guide
§3. Verified against `main` at `d4d270e`. Everything the application emits is
Available; the only Proposed panels are the JetStream backlog row, which is
blocked on a deployment rather than on code (guide §9).

Metric names below are the expected Prometheus rendering of the OTel instrument
names. **Verify label spelling and unit suffixes against the deployed
collector before building** (guide §12).

Query conventions: `[5m]` on operations dashboards, `[2m]` on D4 evidence
panels, never `[1m]` at a 30s scrape (trap 7.5). `$site` and `$environment` are
applied to every query that has those labels available; they are omitted from
the snippets below for readability except where the label's presence or absence
is itself the point.

---

## D1 · Chat Overview

One screen, no scrolling. The only question is whether the pipeline is moving,
and if not, where it stops.

### Row 1 — Evidence

Placed first for the same reason it is first on D4: a healthy-looking funnel
read from an incomplete window is not a result.

#### D1-1.1 · Expected consumers present

- **Source:** Available (`chat_nats_consumer_loop_up`, #272)
- **Applies to:** operations, soak, failure

```promql
# One expression per expected (stream, consumer) pair. A maintained inventory
# is the point of this panel — see guide §2 item 5.
absent(chat_nats_consumer_loop_up{
  site="$site", stream="MESSAGES-$site", consumer="message-gatekeeper"})
absent(chat_nats_consumer_loop_up{
  site="$site", stream="MESSAGES-CANONICAL-$site", consumer="message-worker"})
absent(chat_nats_consumer_loop_up{
  site="$site", stream="MESSAGES-CANONICAL-$site", consumer="broadcast-worker"})
absent(chat_nats_consumer_loop_up{
  site="$site", stream="MESSAGES-CANONICAL-$site", consumer="notification-worker"})
```

- **Expected reading:** every expression returns nothing. Any expression
  returning `1` means a consumer that should exist has no series at all — the
  durable was deleted, the service was never deployed, or scraping is broken.
  This is the one panel where a result is the alarm.
- **No data:** the healthy state. This inverted polarity is why the panel needs
  an explicit title saying so.
- **Traps:** 7.1. This panel exists precisely because absence produces silence
  everywhere else.

#### D1-1.2 · Scrape health

- **Source:** Available (Prometheus `up`)
- **Applies to:** all

```promql
count by (job) (up == 0)
```

- **Expected reading:** empty. A non-zero count means some targets are down and
  every absence claim on this dashboard is unreliable for that period.
- **No data:** healthy.

### Row 2 — J1 send funnel

#### D1-2.1 · Pipeline funnel

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
# 1. Accepted by the gatekeeper
sum(rate(message_gatekeeper_messages_total{result="accepted"}[5m]))

# 2. Published to MESSAGES-CANONICAL with a PubAck
sum(rate(chat_nats_publish_attempts_total{
  site="$site", destination_kind="canonical", outcome="success"}[5m]))

# 3a. Persisted to Cassandra
sum(rate(message_worker_persistence_total{result="success"}[5m]))

# 3b. Acked by broadcast-worker
sum(rate(chat_nats_consumer_messages_total{
  site="$site", stream="MESSAGES-CANONICAL-$site",
  consumer="broadcast-worker", outcome="ack"}[5m]))

# 3c. Notifications emitted
sum(rate(notification_worker_outcomes_total{result="sent"}[5m]))
```

- **Expected reading:** steps 1 and 2 track each other closely. Steps 3a, 3b,
  and 3c each consume the same canonical stream, so all three should sit near
  step 2 — they are parallel branches, not a serial chain, and none of them
  should be compared to each other. A drop between 1 and 2 is a publish
  problem; a drop from 2 to any of 3a/3b/3c isolates the failure to that one
  worker.
- **No data:** on any single step, that worker is not reporting — which is not
  the same as reporting zero. Check D1-1.1 before concluding the stage is idle.
- **Traps:** 7.7 (step 3a counts attempts, so a redelivery burst can push it
  above step 2 — expected, not a defect), 7.13 (steps 1, 3a, and 3c have no
  inline `site` label while step 2 does; in a multi-site deployment this panel
  compares one site against all sites until the join is added).

#### D1-2.2 · Gatekeeper rejections by reason

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
sum by (reason) (rate(message_gatekeeper_messages_total{result="rejected"}[5m]))
```

- **Expected reading:** a non-zero steady floor is normal — `not_subscribed`
  and `room_restricted` are legitimate client outcomes. The signal is a change
  in shape. `canonical_publish` or `dependency` appearing means an
  infrastructure problem being surfaced as a rejection, not a client error.
- **No data:** the gatekeeper is not reporting, or nothing has been rejected.
  Cross-check against `result="accepted"` being non-zero to distinguish.

### Row 3 — Health strip

Six stat tiles. This row is the visual form of the alert set in
[Alerting Baseline](alerting-baseline.md) §3, deliberately one-to-one with it
so the two cannot drift.

#### D1-3.1 · Consumer loops down

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
count(chat_nats_consumer_loop_up == 0)
```

- **Expected reading:** 0. Any other value means a process is alive and not
  consuming.
- **No data:** no consumer is reporting at all — worse than a non-zero count,
  and caught by D1-1.1.
- **Traps:** 7.11 — there is no supervisor, so a loop that drops does not come
  back on its own. A non-zero value here is immediately actionable and will not
  resolve itself.

#### D1-3.2 · Terminal failures

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
sum(rate(chat_nats_terminal_failures_total[10m]))
```

- **Expected reading:** 0 at baseline, by construction. The NATS metrics
  contract §7 excludes business rejections from this family specifically so
  that it stays at zero when healthy. Any increase is work that will receive no
  further application attempt.
- **No data:** no consumer is reporting.

#### D1-3.3 · Reconnect buffer overflow

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
sum(increase(chat_nats_publish_attempts_total{outcome="buffer_full"}[5m]))
```

- **Expected reading:** 0. Any increase means publishes were dropped
  client-side. It also means recipient delivery is unproven for that interval,
  because Core NATS publish success is buffered (guide §2 item 6).
- **No data:** healthy or not reporting; check D1-1.2.

#### D1-3.4 · Services disconnected from the broker

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
count(
  chat_nats_client_connected == 0
    * on (job, instance) group_left (service_name)
      target_info
)
```

- **Expected reading:** 0.
- **No data:** healthy, or the `target_info` join is not resolving — verify the
  join keys in the target environment before trusting this tile.
- **Traps:** 7.12. Without the join this tile can tell you a connection was
  lost but not by which service.

#### D1-3.5 · Slow consumer events

- **Source:** Available
- **Applies to:** operations, soak, failure

```promql
sum(increase(nats_slow_consumer_events_total[5m]))
```

- **Expected reading:** 0. Non-zero means the broker is discarding messages for
  one of our subscriptions.
- **No data:** healthy or not scraped.

#### D1-3.6 · At-rest key renewal failures

- **Source:** Available, **but see trap 7.14**
- **Applies to:** operations

```promql
sum(rate(atrest_kek_renewal_failures_total[15m]))
```

- **Expected reading:** 0. Sustained non-zero means the service cannot obtain a
  Vault token and encryption will fail once the current one expires — a slow
  fuse, not an immediate outage.
- **No data:** **most likely means the endpoint is not scraped at all.** These
  counters are registered through `promauto`, not the OTel meter, so they are
  not on `:2112`. Confirm the exposing endpoint is a Prometheus target before
  reading this tile as healthy.

### Row 4 — Dependencies at a glance

#### D1-4.1 · Storage client error ratio

- **Source:** Available
- **Applies to:** operations, soak, failure

```promql
sum by (db_system_name) (
  rate(db_client_operation_duration_seconds_count{error_type!=""}[5m])
)
/
sum by (db_system_name) (
  rate(db_client_operation_duration_seconds_count[5m])
)
```

- **Expected reading:** near zero. A step change points at D3.
- **No data:** no instrumented client is issuing operations.
- **Traps:** 7.2 (a MongoDB election can pass through with this ratio
  unchanged — this tile is not sufficient to clear MongoDB), 7.4 (do not filter
  the denominator).

#### D1-4.2 · MongoDB checkout pressure

- **Source:** Available
- **Applies to:** operations, soak, failure

```promql
sum(db_client_connection_pending_requests{db_system_name="mongodb"})
```

- **Expected reading:** near zero. This is the tile that moves during a primary
  election, precisely because D1-4.1 does not (trap 7.2).
- **No data:** no instrumented MongoDB client.

---

## D2 · Message Pipeline

Opened from D1. Repeat rows 1–4 per service for the seven adopters
(`message-gatekeeper`, `message-worker`, `broadcast-worker`,
`notification-worker`, plus `history-service`, `room-service`, `room-worker`
after #286). Use `$service_name` as a repeat variable.

**`history-service` has no JetStream consumer loop.** It adopts only the
connection, request/reply, and publish families, so rows 1 and 2 are
structurally empty for it. That is a property of the service, not a broken
panel, and its row must say so.

### Row 1 — Consume path

#### D2-1.1 · Loop state

- **Source:** Available (#272, extended to seven services by #286)
- **Applies to:** operations, soak, failure

```promql
chat_nats_consumer_loop_up{service_name="$service_name", site="$site"}
```

- **Expected reading:** the gauge sits at 1 per (stream, consumer), and **it
  does not recover** — once it drops it stays down until the process restarts,
  so any dip is either a restart boundary or a dead loop. There is no
  self-healing to wait out and no third state to interpret.
- **No data:** the service is not deployed or not scraped — except for
  `history-service`, where it is correct and permanent.
- **Traps:** 7.11.

#### D2-1.2 · Delivery dispositions

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
sum by (stream, consumer, event_type, outcome) (
  rate(chat_nats_consumer_messages_total{
    service_name="$service_name", site="$site"}[5m])
)
```

- **Expected reading:** dominated by `outcome="ack"`. A rising `nak` share is
  transient failure with retry; `term` is a permanent poison drop;
  `left_pending` means the handler returned without a disposition, which will
  become a redelivery after AckWait. `handler_cancelled` clusters at shutdown.
- **No data:** the consumer is not receiving. Distinguish "no traffic" from "not
  consuming" using D2-1.1.
- **Note:** this counts delivery attempts, not business operations. Do not
  infer message counts by summing it (NATS metrics contract §7).

#### D2-1.3 · Redelivery ratio

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
sum by (stream, consumer) (
  rate(chat_nats_consumer_redeliveries_total{
    service_name="$service_name", site="$site"}[5m])
)
/
sum by (stream, consumer) (
  rate(chat_nats_consumer_messages_total{
    service_name="$service_name", site="$site"}[5m])
)
```

- **Expected reading:** near zero at baseline. A rise means work is being
  retried; sustained high values approaching the `MaxDeliver` cap predict
  terminal exhaustion, which will appear in D2-1.5.
- **No data:** no deliveries at all in the window.

#### D2-1.4 · Processing duration

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
histogram_quantile(0.99,
  sum by (le, stream, consumer, event_type) (
    rate(chat_nats_consumer_processing_duration_seconds_bucket{
      service_name="$service_name", site="$site", outcome="ack"}[5m])
  )
)
```

- **Expected reading:** diagnostic only. `sli-slo.md` §0.1 forbids raw
  percentiles as SLO targets, so this panel informs but never scores. Filtering
  to `outcome="ack"` keeps failed-fast errors from flattering the number.
- **No data:** no successful processing in the window.

#### D2-1.5 · Terminal failures by reason

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
sum by (stream, consumer, event_type, reason) (
  rate(chat_nats_terminal_failures_total{
    service_name="$service_name", site="$site"}[5m])
)
```

- **Expected reading:** flat zero. `max_deliver` means redelivery hit the cap
  and stopped — the message stays in the stream and a max-delivery advisory
  fires, so this is enumerable loss, not deleted data. `permanent` is a
  deliberate poison drop. `publish_exhausted` means a downstream publish failed
  while the input was acked. `stream_unavailable` and `consumer_deleted` are
  infrastructure.
- **No data:** healthy, provided D1-1.1 confirms the consumer exists.

#### D2-1.6 · Broker connection state

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
chat_nats_client_connected
  * on (job, instance) group_left (service_name)
    target_info{service_name="$service_name"}

sum by (service_name, event) (
  increase(
    chat_nats_client_connection_events_total
      * on (job, instance) group_left (service_name) target_info
  [5m])
)
```

- **Expected reading:** the gauge holds at the process's live connection count.
  A `disconnected` event followed promptly by `reconnected` is a recovered
  blip; `closed` is terminal for that connection. Read this **before** drawing
  any conclusion from D2-2.1: a Core NATS publish that succeeded while this
  gauge read 0 went into a buffer, not to a subscriber.
- **No data:** either the service does not use `natsutil.ConnectWithMetrics`
  (it is opt-in), or the `target_info` join is not resolving. These are not
  distinguishable from this panel — verify the join in the target environment
  first.
- **Traps:** 7.12 — this family carries no `service_name` or `site` label of
  its own, so the join is mandatory, not cosmetic.

#### D2-1.7 · Slow consumer events

- **Source:** Available
- **Applies to:** operations, soak, failure

```promql
sum by (subject, queue) (increase(nats_slow_consumer_events_total[5m]))
```

- **Expected reading:** flat zero. Non-zero means the broker discarded messages
  destined for one of this process's subscriptions — loss that no consumer-side
  counter will ever show, because the message never arrived.
- **No data:** healthy, or the endpoint is not scraped.
- **Traps:** 7.12 — same resource-only scoping as D2-1.6.

### Row 2 — Publish path

#### D2-2.1 · Publish attempts by outcome

- **Source:** Available (#272), extended to seven services by #286
- **Applies to:** operations, soak, failure

```promql
sum by (destination_kind, operation, outcome) (
  rate(chat_nats_publish_attempts_total{
    service_name="$service_name", site="$site"}[5m])
)
```

- **Expected reading:** overwhelmingly `success`. `no_responders` on a
  request/reply operation means the callee is gone. `disconnected` and
  `buffer_full` are client-side loss. `timeout` on a JetStream destination
  means no PubAck arrived.
- **No data:** the service published nothing in the window.
- **Traps:** guide §2 item 6. `success` means broker-confirmed only for
  `canonical`, `outbox`, `inbox`, and `push`. For `recipient_event` and
  `client_response` it means "entered the client buffer" and is **not**
  evidence a recipient was reached. Read this panel next to D2-4.1 and never
  treat a Core NATS success as delivery.

#### D2-2.2 · Publish retries

- **Source:** Available, **expected to be empty**
- **Applies to:** operations, soak, failure

```promql
sum by (destination_kind, operation) (
  rate(chat_nats_publish_retries_total{
    service_name="$service_name", site="$site"}[5m])
)
```

- **Expected reading:** no series. No service currently loops around its own
  publish; JetStream's internal PubAck retries and the consumer Nak path are
  not application retries. The family is declared so a future retry loop has a
  defined home.
- **No data:** the expected state. Do not treat this empty panel as a freshness
  failure.

### Row 3 — Domain outcomes

One panel per service; only the four workers have domain metrics today.

#### D2-3.1 · message-gatekeeper business outcomes

- **Source:** Available (#272) · **Applies to:** operations, soak, failure

```promql
sum by (result, reason) (rate(message_gatekeeper_messages_total[5m]))
```

- **Expected reading:** `accepted` dominates. `retry` and `failed` are
  infrastructure trouble; `rejected` split by reason separates client error
  from dependency failure.
- **No data:** no messages submitted, or the service is not reporting.
- **Traps:** 7.13 — no inline `site` label.

#### D2-3.2 · message-worker persistence

- **Source:** Available (#272) · **Applies to:** operations, soak, failure

```promql
sum by (message_kind, result) (rate(message_worker_persistence_total[5m]))
```

- **Expected reading:** `result="error"` should be flat zero.
- **Traps:** 7.7 — per attempt, not per message.

#### D2-3.3 · broadcast-worker fan-out

- **Source:** Available (#272) · **Applies to:** operations, soak, failure

```promql
# Intended audience
histogram_quantile(0.99,
  sum by (le, room_kind, event_type) (
    rate(broadcast_worker_fanout_recipients_bucket[5m])
  )
)

# Actual publish attempts
sum by (room_kind, event_type, result) (
  rate(broadcast_worker_recipient_deliveries_total[5m])
)
```

- **Expected reading:** `result="failed"` flat zero. The two queries answer
  different questions and belong on the same panel only to keep the warning
  next to them.
- **Traps:** 7.8 — **do not divide these.** For `room_kind="channel"` the
  ratio is meaningless because one publish serves the whole room. Only `dm`,
  `bot_dm`, and `thread` are comparable. Per-recipient channel evidence comes
  from the loadgen recipient observer and nowhere else.

#### D2-3.4 · notification-worker outcomes

- **Source:** Available (#272) · **Applies to:** operations, soak, failure

```promql
sum by (kind, result) (rate(notification_worker_outcomes_total[5m]))
```

- **Expected reading:** `suppressed` is a large and legitimate share —
  presence-based suppression is the design. `publish_failed` and `failed`
  should be flat zero.
- **Note:** `kind` has only two values — `push` and `unknown`. #286 removed the
  unused `notification` value, so a query or legend expecting three kinds is
  written against a vocabulary that no longer exists.
- **No data:** no canonical messages reached the worker.

### Row 4 — Request/reply

#### D2-4.1 · Inbound handler results

- **Source:** Available (#286)
- **Applies to:** operations, soak, failure

```promql
# Budget-burning error ratio. The result enum maps exactly onto the
# error-budget eligibility table in sli-slo.md §0.1: 4xx-equivalent results are
# removed from valid events entirely rather than counted as good.
sum by (service_name, operation) (
  rate(chat_nats_request_handled_total{
    result=~"internal|unavailable|too_many_requests"}[5m])
)
/
sum by (service_name, operation) (
  rate(chat_nats_request_handled_total{
    result=~"success|internal|unavailable|too_many_requests"}[5m])
)
```

- **Expected reading:** near zero. This is the only service-side view of the
  read and mutation lanes. Because the denominator excludes client errors
  entirely, a spike here is always ours.
- **No data:** no inbound requests reached a metrics-enabled router. For
  `history-service` under any soak profile that would itself be the finding;
  for a service in the Emitters gap below it is expected and permanent.
- **Traps:** 7.9 — `operation="history_read"` merges the entire read lane and
  `history_mutation` merges reactions with edit/delete/pin/unpin at roughly
  20:1, so a mutation-only anomaly is invisible here. Also 7.15 — pin
  `service_name` with a regex, not a literal, or the Teams deployments drop out
  silently.
- **Emitters:** only routers built with `natsrouter.WithMetrics` —
  `history-service`, `room-service`, and `room-worker`. `user-service`,
  `search-service`, `media-service`, `bot-message-handler`, and
  `bot-room-service` build a router without it and emit nothing, so their
  absence here is a coverage gap, not a healthy service (guide §8.2).

#### D2-4.2 · Inbound handler latency

- **Source:** Available (#286)
- **Applies to:** operations, soak, failure

```promql
# Event-ratio form: successful requests completed within the bound, over
# successful requests. Requires an explicit bucket boundary at the bound.
sum by (service_name, operation) (
  rate(chat_nats_request_handler_duration_seconds_bucket{
    result="success", le="0.5"}[5m])
)
/
sum by (service_name, operation) (
  rate(chat_nats_request_handler_duration_seconds_count{result="success"}[5m])
)
```

- **Expected reading:** the share completing within the bound. Written as a
  ratio rather than a percentile so it is comparable with the SLO definitions,
  which are all event ratios.
- **No data:** no successful requests, or — the likelier cause — **the `le`
  boundary does not exist.** The SDK's shared latency boundaries must include
  the bound being tested; a missing boundary yields an empty numerator against
  a healthy denominator, which reads as 0% success rather than as an error.
  Verify the boundary set before trusting this panel.
- **Traps:** 7.9 — SLO-4 (500 ms) and SLO-5 (300 ms) share `history_read`, so
  this panel cannot evaluate either one on its own. Use one bound per panel copy
  and label it as indicative until the operation label is refined.

#### D2-4.3 · Outbound request results and latency

- **Source:** Available (#272)
- **Applies to:** operations, soak, failure

```promql
sum by (service_name, operation, outcome) (
  rate(chat_nats_requests_total{site="$site"}[5m])
)

histogram_quantile(0.99,
  sum by (le, service_name, operation) (
    rate(chat_nats_request_duration_seconds_bucket{
      site="$site", outcome="success"}[5m])
  )
)
```

- **Expected reading:** the caller's view of a callee. Before #286 this was the
  only signal about `history-service` health, seen through
  `operation="history_get_message"` from `message-gatekeeper`. Still useful as a
  cross-check, but it is the reply-parent lookup path, not `LoadHistory` — do
  not read it as representative of the read lane. D2-4.1 is now the primary.
- **No data:** no outbound requests were made.
- **Traps:** 7.16 — this family is transport-shaped. A remote "not a room
  member" is deliberately **not** counted as a failure, so `outcome` here says
  whether the exchange worked, not whether the caller got what it wanted.

### Row 5 — JetStream backlog

Built on the platform team's exported series through the recording rules
recommended in guide §9. This is the row that guide §1.1 carves out of the
dependency boundary: our streams and durables, hosted on their server.

#### D2-5.1 · Consumer backlog

- **Source:** Proposed — recording rules over exporter series (guide §9.3).
  Blocked only on the exporter being scraped; no application change needed.
- **Applies to:** operations, soak, failure

```promql
chat_jetstream_consumer_pending{site="$site", stream=~"$stream"}
chat_jetstream_consumer_ack_pending{site="$site", stream=~"$stream"}
chat_jetstream_consumer_redelivered{site="$site", stream=~"$stream"}
```

- **Expected reading:** pending returns to baseline between bursts. A rising
  floor is production outpacing consumption. Ack-pending is the in-flight
  window: it should sit well below the consumer's `MaxAckPending`, and pinned
  at the limit means workers are the bottleneck rather than the broker.
- **No data:** the exporter is not scraped, or the recording rules are not
  installed. It does **not** mean the backlog is zero.
- **Traps:** 7.6 (without leader deduplication these are multiplied by the
  replication factor — guide §9.4 question 3). Guide §2 item 4 — the same number
  means opposite things on different consumers, so thresholds are per consumer.

#### D2-5.2 · Ack floor stalled

- **Source:** Proposed — recording rule over
  `jetstream_consumer_ack_floor_stream_seq`, which **is** in the exporter
  vocabulary (guide §9.1)
- **Applies to:** operations, soak, failure

```promql
chat_jetstream_consumer_ack_floor_stalled{site="$site"} == 1
```

- **Expected reading:** empty. A stuck floor with non-zero pending is the
  substitute for the missing oldest-pending-age gauge. It is the **only** signal
  that works on `outbox-worker`'s ordered lanes, where `MaxAckPending=1` makes
  ack-pending thresholds structurally incapable of firing (guide §2 item 3).
- **No data:** healthy, or the rule is not installed. Not distinguishable from
  this panel alone, which is why guide §12 requires every panel to have produced
  data at least once.
- **Traps:** the condition is **necessary but not sufficient**. A consumer that
  keeps acknowledging too slowly to catch up never freezes its floor, so this
  reads clean while the backlog grows. Read it beside D2-5.1 and D2-5.3.
- **Note:** a parked forward to a down peer is by design — `MaxDeliver=-1`,
  never Ack. This panel says the floor is stuck, **not for how long**. During a
  loadgen run `loadgen_consumer_ack_floor_stall_seconds` answers the duration
  question; outside a run nothing does.

#### D2-5.3 · Publish versus ack throughput

- **Source:** Proposed — recording rules over the two monotonic cursors
  (guide §9.3)
- **Applies to:** operations, soak, failure

```promql
# Publish rate into the stream
chat_jetstream_stream_publish_rate{site="$site", stream=~"$stream"}

# Ack rate out of each consumer
chat_jetstream_consumer_ack_rate{site="$site", stream=~"$stream"}

# The gap that D2-5.2 cannot see
chat_jetstream_stream_publish_rate{site="$site", stream=~"$stream"}
  - on (site, stream) group_right ()
    chat_jetstream_consumer_ack_rate{site="$site", stream=~"$stream"}
```

- **Expected reading:** the two rates track each other. **A sustained positive
  gap is the failure mode D2-5.2 is blind to** — the consumer is still
  acknowledging, so its floor advances and no stall fires, but it acknowledges
  more slowly than production and the backlog grows without bound. This panel is
  what makes the stall signal safe to rely on.
- **No data:** the exporter is not scraped. Note both inputs are derived from
  monotonic sequence gauges via `rate()`, so a consumer reset or stream purge
  produces a counter-reset artifact rather than a real rate.

#### D2-5.4 · Stream retention

- **Source:** Proposed — recording rules over `jetstream_stream_total_messages`
  and `_total_bytes` (guide §9.1)
- **Applies to:** operations, soak, failure

```promql
chat_jetstream_stream_messages{site="$site", stream=~"$stream"}
chat_jetstream_stream_bytes{site="$site", stream=~"$stream"}
```

- **Expected reading:** bounded by the stream's retention policy. Monotonic
  growth against a limits-based policy predicts discard; against an
  interest-based policy it means a consumer is not acknowledging, which should
  already be visible in D2-5.1.
- **No data:** the exporter is not scraped.

### Row 6 — Runtime

#### D2-6.1 · Process and container health

- **Source:** Available
- **Applies to:** all

```promql
go_goroutine_count{service_name="$service_name"}
process_resident_memory_bytes{service_name="$service_name"}
rate(container_cpu_usage_seconds_total[5m])
kube_pod_container_status_restarts_total
```

- **Expected reading:** goroutine count flat. A monotonic climb is a leak,
  which matters most on soak. Restarts explain a `loop_up` series vanishing
  rather than reading zero (trap 7.1).
- **No data:** cAdvisor or kube-state-metrics is not scraped.

---

## D3 · Dependencies — Client View

Opened by both operations and testing. Answers one question: is this us or
them? Everything here is the client's experience; server-side health is the
platform teams' (guide §1.1).

### Row 1 — Client operations

#### D3-1.1 · Operation rate

- **Source:** Available · **Applies to:** all

```promql
sum by (db_system_name, service_name, db_operation_name) (
  rate(db_client_operation_duration_seconds_count[5m])
)
```

- **Expected reading:** tracks traffic. A drop with steady upstream traffic
  means work is not reaching storage.
- **No data:** the client is uninstrumented, not that it is idle. Coverage is
  listed in `storage-dependency-metrics.md` §3.1 and §4.

#### D3-1.2 · Operation error ratio

- **Source:** Available · **Applies to:** all

```promql
sum by (db_system_name, service_name) (
  rate(db_client_operation_duration_seconds_count{error_type!=""}[5m])
)
/
sum by (db_system_name, service_name) (
  rate(db_client_operation_duration_seconds_count[5m])
)
```

- **Traps:** 7.4, and 7.2 — **this panel cannot clear MongoDB.** A retried
  write after a step-down records no `error_type`.

#### D3-1.3 · Client latency

- **Source:** Available · **Applies to:** all

```promql
histogram_quantile(0.99,
  sum by (le, db_system_name, service_name, db_operation_name) (
    rate(db_client_operation_duration_seconds_bucket[5m])
  )
)
```

- **Expected reading:** during a MongoDB election this rises while D3-1.2 stays
  flat. That combination *is* the election signature from the client side.
- **No data:** no operations in the window.

### Row 2 — MongoDB pool

#### D3-2.1 · Pool occupancy, pressure, and timeouts

- **Source:** Available · **Applies to:** all

```promql
sum by (service_name, db_client_connection_pool_name, state) (
  db_client_connection_count{db_system_name="mongodb"}
)

sum by (service_name, db_client_connection_pool_name) (
  db_client_connection_pending_requests{db_system_name="mongodb"}
)

sum by (service_name, db_client_connection_pool_name) (
  rate(db_client_connection_timeouts_total{db_system_name="mongodb"}[5m])
)
```

- **Expected reading:** pending near zero, timeouts flat zero. **These are the
  authoritative MongoDB impact signals**, not the error ratio (trap 7.2).
- **No data:** no instrumented MongoDB client on the selected service.
- **Note:** the SDK deliberately drops `idle.max`, `use_time`, and `wait_time`.
  Do not build panels expecting them.

### Row 3 — Cassandra client

#### D3-3.1 · Query attempts and connection health

- **Source:** Available · **Applies to:** all

```promql
sum by (service_name, server_address, error_type) (
  rate(cassandra_query_attempts_total[5m])
)

sum by (service_name, server_address, error_type) (
  rate(cassandra_connection_attempts_total[5m])
)

histogram_quantile(0.99,
  sum by (le, service_name) (
    rate(db_client_connection_create_time_seconds_bucket{
      db_system_name="cassandra"}[5m])
  )
)
```

- **Expected reading:** attempts spread across coordinators. Concentration onto
  fewer `server_address` values means nodes left the pool. One attempt is
  recorded per driver attempt **and per result page**, so this is not a query
  count.
- **No data:** only three processes have instrumented sessions —
  `message-worker`, `bot-message-worker`, `history-service`.
- **Traps:** 7.10 — **this row does not cover the reaction and pin/unpin write
  paths.** Ten bare `ExecuteBatch` call sites emit nothing. At the default soak
  reaction rate of 100/s, that is the second-busiest lane writing to Cassandra
  with no client telemetry whatsoever. A reaction-path storage problem appears
  only as latency on D2-4.1's `history_mutation` series.

### Row 4 — Cache

#### D3-4.1 · Cache effectiveness

- **Source:** Available · **Applies to:** all

```promql
sum by (service_name, cache, tier) (rate(cache_hits_total[5m]))
/
(
  sum by (service_name, cache, tier) (rate(cache_hits_total[5m]))
  +
  sum by (service_name, cache, tier) (rate(cache_misses_total[5m]))
)

sum by (service_name, cache, tier) (rate(cache_errors_total[5m]))
```

- **Expected reading:** explains whether MongoDB load was absorbed or
  amplified. A hit-rate collapse preceding a MongoDB latency rise is the cause,
  not a symptom.
- **No data:** the selected service has no shared cache counters.

### Row 5 — Platform team links

Text panels only. No empty panels (guide §1.1).

| Panel | Content |
|---|---|
| D3-5.1 NATS | Link to the NATS team's dashboard. What to look for: server up per cluster member, route and gateway health, RAFT quorum for metadata and stream groups, memory and storage headroom, server-side slow consumers. |
| D3-5.2 MongoDB | Link to the MongoDB team's dashboard. What to look for: primary count equal to one per replica set, election timeline, replication lag, oplog window, server connections, WiredTiger cache pressure, flow control. |
| D3-5.3 Cassandra | Link to the Cassandra team's dashboard. What to look for: node up per DC and rack, dropped messages, pending compactions, hinted handoff backlog, thread pool saturation, GC pauses, disk headroom. |

If a read-only datasource for any of these is granted later, these panels can
become real. That is an improvement, not a prerequisite.

---

## D4 · Load Test Run

Serves soak and failure tests from one dashboard. The two ask the same three
questions; the only structural difference is whether an externally supplied
fault annotation exists.

Row order is fixed: **Evidence first.** The Impact and Correctness rows produce
absence claims, and an absence claim read from an invalid window is not a
result. Placing Evidence at the top makes reading it first unavoidable.

Variables per the evidence contract: `lookback` (2m), `step` (1m), and
`healthy_points` (5) are visible, and the selected window auto-extends far
enough to evaluate recovery.

### Row 1 — Evidence

#### D4-1.1 · Dispatch validity

- **Source:** Available (#271)
- **Applies to:** soak, failure

```promql
# Pacing validity (evidence contract). Holds the identity checked in D4-1.2.
sum by (lane) (increase(loadgen_soak_dispatched_total[2m]))
/
(sum by (lane) (loadgen_soak_configured_rate) * 120)

# Traffic validity (#295). Required in addition, not instead.
sum by (lane) (
  increase(loadgen_soak_lane_attempts_total{outcome="sent"}[2m])
)
/
(sum by (lane) (loadgen_soak_configured_rate) * 120)
```

- **Expected reading:** at or above 0.95 for every enabled lane. The exact
  0.95 boundary is valid. Below it, the run did not offer the declared traffic
  and every absence claim for that lane is `INCONCLUSIVE`.
- **Both queries are needed, and they answer different questions.**
  `loadgen_soak_dispatched_total` counts **scheduler slots**, and a lane
  consumes one even when it finds no usable target — so a lane idling on an
  exhausted pool reads as fully loaded. The first query cannot be replaced
  because it holds the pacing identity in D4-1.2; the second cannot be omitted
  because it is the only one that expresses offered load. The threshold on the
  attempts gate is **90%**, and it makes the window inconclusive for that lane
  only. The two non-`sent` outcomes name the two ways a slot passes
  without a request: `no_target` for an exhausted pool, `refused` for a
  mutation the ledger declined.
- **No data:** `INCONCLUSIVE`, never a pass. Do not add `or vector(0)`.
- **Note:** `loadgen_soak_intended_total` is an observed pacing counter and is
  not a substitute for this stable target calculation.

#### D4-1.2 · Dispatch identity check

- **Source:** Available (#271)
- **Applies to:** soak, failure

```promql
increase(loadgen_soak_intended_total[2m])
- (
    increase(loadgen_soak_dispatched_total[2m])
  + increase(loadgen_soak_scheduler_underrun_total[2m])
  + increase(loadgen_soak_lane_saturation_total[2m])
  + increase(loadgen_soak_global_saturation_total[2m])
  )
```

- **Expected reading:** exactly 0. A non-zero residual is an unattributed
  dispatch shortfall, which makes the evidence inconclusive. Attribution
  differs by cause: scheduler underrun, process down, and scrape gaps make
  evidence inconclusive outright; lane or global saturation invalidates absence
  claims while positive impact remains reportable.
- **No data:** inconclusive.
- **Note:** `not_sent` is a dispatched publish lifecycle outcome and is **not**
  subtracted from dispatch.

#### D4-1.3 · Observer validity

- **Source:** Available (#271)
- **Applies to:** soak, failure

```promql
# Numerator and denominator must both be displayed, not just the verdict.
# The contract pins every selector; scenario is fixed to message_soak.
increase(loadgen_failure_observations_total{
  scenario="message_soak", lane="$lane", observer="$observer",
  result="unverified"}[2m])

increase(loadgen_failure_observer_eligible_total{
  scenario="message_soak", lane="$lane", observer="$observer"}[2m])

# Invalid when unverified exceeds max(3, ceil(0.001 * eligible))
increase(loadgen_failure_observations_total{result="unverified"}[2m])
>
clamp_min(
  ceil(0.001 * increase(loadgen_failure_observer_eligible_total[2m])),
  3
)
```

- **Expected reading:** unverified stays under the limit. An observer blind
  interval overlapping an operation's observation window invalidates the
  absence claim for that operation. Startup-down time, disconnects, queue
  overflow, truncated health history, and stale health all count as blind.
- **No data:** inconclusive. A disabled observer is not eligible and cannot
  make the interval unverified.
- **Traps:** `loadgen_failure_observer_configured` **does not cover every
  observer.** It is set for `admission`, `cassandra_history`, and
  `recipient_broadcast` only. `room_state` — which is `Required: true` for every
  member, room, read-receipt and create operation — has **no series in that
  gauge at all**, and neither does `search_index`. For `room_state`, use
  `loadgen_failure_observer_up` (D4-1.4), which does cover it: that answers
  live-versus-down, but enabled-versus-disabled is not answerable from metrics.
  A `configured` query returning nothing for `room_state` is the expected
  state, not a broken observer.

#### D4-1.4 · Observer liveness

- **Source:** Available (#271)
- **Applies to:** soak, failure

```promql
loadgen_failure_observer_up
loadgen_failure_observer_queue_depth
rate(loadgen_failure_observer_events_total[2m])
```

- **Expected reading:** up at 1, queue depth bounded. A growing queue precedes
  overflow, which becomes blind evidence.
- **Coverage:** `loadgen_failure_observer_up` is emitted for
  `recipient_broadcast` and `room_state`. The `admission` and
  `cassandra_history` observers are inline in the send path and have no
  liveness gauge — their health shows up as `unverified` results in D4-1.3
  rather than as a down gauge.
- **No data:** the observer is not reporting, which is itself a blind interval.

#### D4-1.5 · Ledger integrity

- **Source:** Available (#271)
- **Applies to:** soak, failure

```promql
sum by (reason) (increase(loadgen_failure_invalidations_total[$__range]))
sum by (reason) (increase(loadgen_failure_untracked_total[$__range]))
increase(loadgen_failure_dropped_total[$__range])
loadgen_failure_journal_bytes

# WAL and evidence-sidecar health (#271)
histogram_quantile(0.95, rate(loadgen_failure_wal_flush_duration_seconds_bucket[2m]))
histogram_quantile(0.95, rate(loadgen_failure_wal_append_duration_seconds_bucket[2m]))
rate(loadgen_failure_wal_flush_batch_size_sum[2m])
increase(loadgen_failure_evidence_records_total[$__range])
```

- **Expected reading:** invalidations, untracked, and dropped all zero.
  Untracked sends are operations the ledger could not account for; dropped are
  recovered operations discarded because the WAL exceeded capacity. Either
  condition invalidates the affected interval.

  The WAL histograms are a different question — whether the generator is keeping
  up with its own durability. Flush latency measures the actual grouped fsync;
  append latency is caller-observed and includes the durability wait for
  pre-publish intents. **Correlate WAL flush p95 and batch size against D4-1.1
  and D4-2.2**: a generator stalling on its own fsync produces dispatch shortfall
  that has nothing to do with the system under test.
- **No data:** inconclusive, not clean. One exception: a missing
  ordinary-delivery evidence sidecar is expected, because only terminal
  anomalies and authoritative missing sets are persisted. A sidecar *flush
  error*, by contrast, makes the dependent recipient interval inconclusive.

#### D4-1.6 · Generator connection state

- **Source:** Available
- **Applies to:** soak, failure

```promql
loadgen_nats_connected{pool="$pool"}
increase(loadgen_nats_connection_events_total[$__range])
loadgen_nats_current_outage_seconds
```

- **Expected reading:** during a declared NATS fault a transient zero is
  **expected evidence**, not a failure. What invalidates the interval is
  permanent closure, failed recovery, observer loss, or inability to offer the
  declared traffic.
- **No data:** the generator is not reporting.
- **Note:** place this next to D4-4.4 so generator disconnection and service
  disconnection are never confused.

#### D4-1.7 · Run metadata and scrape continuity

- **Source:** Available
- **Applies to:** soak, failure

```promql
loadgen_run_info
increase(loadgen_consumer_sample_errors_total[$__range])
count by (job) (up == 0)
```

- **Expected reading:** run identity resolves to exactly one run; sample errors
  and down targets zero. A scrape gap anywhere in the window degrades every
  absence claim overlapping it.
- **No data:** the run is not identifiable, which makes the whole dashboard
  unattributable.

### Row 2 — Impact

#### D4-2.1 · Lane latency

- **Source:** Available
- **Applies to:** soak, failure

```promql
histogram_quantile(0.99,
  sum by (le, action) (rate(loadgen_soak_rpc_latency_seconds_bucket[2m]))
)
```

- **Expected reading:** with a fault annotation overlaid, a step at injection
  that returns to baseline is `TRANSIENT_RECOVERED`; a step that persists to
  the end of the window is `UNRECOVERED`. Recovery requires five consecutive
  healthy evaluation points and at least six minutes of post-remediation data —
  any failing point resets the streak.
- **No data:** inconclusive. If the window ends before the minimum
  post-remediation duration, recovery is **unconfirmed**, not recovered.
- **Note:** this is the only place where the soak lanes are separable by action
  (`action` here is the loadgen label, not the coarse service-side `operation`
  of trap 7.9). SLO-4 and SLO-5 can be evaluated from here even though D2-4.2
  cannot separate them.

#### D4-2.2 · Lane errors and saturation

- **Source:** Available
- **Applies to:** soak, failure

```promql
sum by (action, class) (rate(loadgen_soak_errors_total[2m]))
sum by (action) (rate(loadgen_soak_retries_total[2m]))

# Saturation is split by scope. #271 removed the combined
# loadgen_soak_saturation_total; a query still using it returns nothing.
sum by (lane) (rate(loadgen_soak_lane_saturation_total[2m]))
sum (rate(loadgen_soak_global_saturation_total[2m]))
```

- **Expected reading:** saturation is an **invalid-run detector**, not an
  impact signal — it means loadgen dropped work because its own in-flight budget
  filled. Lane saturation invalidates absence claims for that lane only; global
  saturation invalidates the whole window. Harness retries must stay separate
  from application and driver retries.
- **No data:** inconclusive.

#### D4-2.3 · Unresolved backlog

- **Source:** Available
- **Applies to:** soak, failure

```promql
loadgen_failure_inflight{scenario="$scenario", lane="$lane"}
increase(loadgen_failure_recovered_operations_total[$__range])
```

- **Expected reading:** in-flight returns to baseline after the fault window. A
  plateau means operations are approaching their deadlines and will resolve as
  `missing_after_deadline`.
- **No data:** inconclusive.

#### D4-2.4 · Consumer backlog during the run

- **Source:** Available (#271, extended to nine durables by #295)
- **Applies to:** soak, failure

```promql
loadgen_consumer_up{stream="$stream", durable="$durable"}
loadgen_consumer_pending{stream="$stream", durable="$durable"}
loadgen_consumer_ack_pending{stream="$stream", durable="$durable"}
loadgen_consumer_redelivered{stream="$stream", durable="$durable"}

# Seconds since the ack floor last advanced while work remains pending.
loadgen_consumer_ack_floor_stall_seconds{stream="$stream", durable="$durable"}
```

- **Expected reading:** pending drains after the fault window. A plateau with
  the loop gauge at 1 is a consumer that is running but not keeping up.
- **No data:** nine durables are sampled — `message-gatekeeper` on MESSAGES;
  `message-worker`, `broadcast-worker`, `notification-worker` and
  `message-sync` on MESSAGES-CANONICAL; `room-worker` and
  `notification-worker-room-event-invalidate` on ROOMS; `spotlight-sync` and
  `user-room-sync` on INBOX. No data means the sampler could not reach the consumer; check
  `loadgen_consumer_sample_errors_total{reason}` and `loadgen_consumer_up`
  before reading it as an idle consumer. A consumer that is simply not deployed
  sets `loadgen_consumer_up` to 0 and counts a bounded sample error — it never
  invalidates the ledger.
- **Traps:** 7.18 — five of those nine durables have no `chat_nats_consumer_loop_up`
  at all, so this run-scoped sampler is the *only* backlog evidence for them
  until the platform exporter lands.
- **Note:** the stall gauge is emitted **only** while `NumPending > 0` and
  `ConsumerInfo.AckFloor.Last` is available, so its absence is not a statement
  that the floor is healthy. It also does not replace oldest-pending age: a
  consumer that keeps acknowledging too slowly to catch up never freezes its
  floor, so the backlog grows while this reads zero. Pair it with
  `loadgen_consumer_pending`.
- **Note:** this is the run-scoped view. D2-5.2 is the daily-operations
  equivalent and is still blocked on the platform exporter (guide §8.1).

### Row 3 — Correctness

#### D4-3.1 · Terminal result mix

- **Source:** Available (#271)
- **Applies to:** soak, failure

```promql
sum by (lane, result) (increase(loadgen_failure_operations_total[$__range]))
```

- **Expected reading:** the five terminal results are `good`, `bad`,
  `unverified`, `not_sent`, and `missing_after_deadline`. `bad` and
  authoritative missing are **positive correctness evidence** and survive an
  incomplete window — a retained content mismatch stays
  `CONFIRMED_VIOLATION` even during an unrelated scrape gap. `unverified`
  invalidates absence claims but proves nothing by itself.
- **No data:** `INCONCLUSIVE`. An empty panel is never `CLEAN`.

#### D4-3.2 · Observation results and reasons

- **Source:** Available (#271)
- **Applies to:** soak, failure

```promql
sum by (observer, result) (
  increase(loadgen_failure_observations_total[$__range])
)
sum by (reason) (
  increase(loadgen_failure_observation_reasons_total[$__range])
)
```

- **Expected reading:** the bounded reasons separate admission failure from
  history loss from recipient anomalies: `admission_rejected`,
  `history_content_mismatch`, `history_missing`, `recipient_duplicate`,
  `recipient_unexpected`, `recipient_identity_mismatch`, `recipient_missing`,
  and `publish_local_error`. Duplicates, unexpected recipients, and identity
  mismatches are positive evidence of a correctness violation, not noise.
- **No data:** inconclusive.
- **Note:** observation results omit `not_sent`; it exists only as an operation
  result.

#### D4-3.3 · Read-back verification

- **Source:** Available
- **Applies to:** soak, failure

```promql
sum by (action, class) (
  increase(loadgen_soak_verifications_total{class!="ok"}[$__range])
)
increase(loadgen_soak_mutation_target_missing_total[$__range])
```

- **Expected reading:** zero. `mutation_target_missing` means a persisted
  target was still absent after the dedicated wait and retry policy — that is
  loss, not slowness.
- **No data:** the verify lane at 1/s is sparse; confirm it dispatched at all
  via D4-1.1 before reading an empty panel as clean.

#### D4-3.4 · Service-side enumerable loss

- **Source:** Available (#272)
- **Applies to:** soak, failure

```promql
sum by (service_name, stream, consumer, reason) (
  increase(chat_nats_terminal_failures_total[$__range])
)
```

- **Expected reading:** the production-side counterpart to the ledger. Where
  the ledger says an operation was lost, this says which consumer gave up on it
  and why. Correlating the two is what turns "something disappeared" into a
  located failure.
- **No data:** healthy, provided D1-1.1 confirms the consumers exist.

### Row 4 — Signals expected to fire

Until alerting is enabled, this row is the dashboard evidence that would
otherwise have been an alert. Each panel corresponds one-to-one with a rule in
[Alerting Baseline](alerting-baseline.md) §3, drawn as a time series with the
fault annotation overlaid rather than as a stat tile (guide §4.3).

| Panel | Query | Expected during a fault window |
|---|---|---|
| D4-4.1 Consumer loop | `chat_nats_consumer_loop_up` | Drops to 0 and **stays** at 0 until the pod restarts — there is no supervisor on `main` (trap 7.11). **A vanishing series is a killed pod, a zero is a live process not consuming** (trap 7.1) — these are different findings. |
| D4-4.2 Terminal failures | `sum by (reason) (rate(chat_nats_terminal_failures_total[2m]))` | Non-zero is expected and is the enumerable-loss evidence. `reason` identifies whether it was `max_deliver` exhaustion or a permanent drop. |
| D4-4.3 Buffer full | `sum(increase(chat_nats_publish_attempts_total{outcome="buffer_full"}[2m]))` | Non-zero during a broker outage. **Any interval overlapping this has unproven recipient delivery** — mark the affected absence claims inconclusive. |
| D4-4.4 Client connected | `chat_nats_client_connected * on (job, instance) group_left (service_name) target_info` | Drops to 0 for affected services. Read beside D4-1.6 to separate service disconnection from generator disconnection. |
| D4-4.5 Slow consumers | `sum(increase(nats_slow_consumer_events_total[2m]))` | May rise during a reconnect surge. |

**Do not silence these signals during a test.** They are the expected result.
Routing, not suppression, is the correct handling — see
[Alerting Baseline](alerting-baseline.md) §6.

---

## Appendix A — Metric source inventory

Everything the four dashboards draw on. Prometheus names are the expected
rendering of the OTel instrument names: `.` becomes `_`, counters gain
`_total`, histograms gain a unit suffix plus `_bucket` / `_sum` / `_count`.
**Verify against the deployed collector** (guide §12).

Label values listed here are closed enums enforced in code. A value outside the
enum is a bug, not a new series — the code normalizes unrecognized inputs to
`unknown` rather than emitting them.

### A.1 Shared NATS application metrics — `pkg/natsmetrics`

`service_name` and `site` are **inline attributes set in code**
(`pkg/natsmetrics/metrics.go`, the `base` slice) on every family in this table.

| Prometheus family | Type | Labels beyond base | Emitters | Status |
|---|---|---|---|---|
| `chat_nats_consumer_loop_up` | gauge | `stream`, `consumer` | 6 of 7 adopters | Available |
| `chat_nats_consumer_messages_total` | counter | `stream`, `consumer`, `event_type`, `outcome` | 6 of 7 | Available |
| `chat_nats_consumer_redeliveries_total` | counter | `stream`, `consumer`, `event_type` | 6 of 7 | Available |
| `chat_nats_consumer_processing_duration_seconds_*` | histogram | `stream`, `consumer`, `event_type`, `outcome` | 6 of 7 | Available |
| `chat_nats_terminal_failures_total` | counter | `stream`, `consumer`, `event_type`, `reason` | 6 of 7 | Available |
| `chat_nats_publish_attempts_total` | counter | `destination_kind`, `operation`, `outcome` | 7 | Available |
| `chat_nats_publish_retries_total` | counter | `destination_kind`, `operation` | none | Available, **no producer** |
| `chat_nats_requests_total` | counter | `operation`, `outcome` | 7 (outbound) | Available |
| `chat_nats_request_duration_seconds_*` | histogram | `operation`, `outcome` | 7 (outbound) | Available |
| `chat_nats_request_handled_total` | counter | `operation`, `result` | `history-service`, `room-service`, `room-worker` (inbound) | Available (#286) |
| `chat_nats_request_handler_duration_seconds_*` | histogram | `operation`, `result` | same | Available (#286) |

Adopters are the seven services in the NATS failure-test scope:
`message-gatekeeper`, `message-worker`, `broadcast-worker`,
`notification-worker` (#272), plus `history-service`, `room-service`, and
`room-worker` (#286).

**`history-service` has no JetStream consumer loop** and therefore emits no
consumer family at all — only the connection, request/reply, and publish
families. Every other adopter runs exactly one instrumented consumer loop per
process; the Teams and bot variants are separate deployments of the same binary
with their own `service_name` (trap 7.15), not second consumers inside one
process.

Inbound request metrics come only from a `natsrouter` built with `WithMetrics`,
which is `history-service`, `room-service`, and `room-worker`. `user-service`,
`search-service`, `media-service`, `bot-message-handler`, and `bot-room-service`
build a router without it and emit nothing.

**Closed label enums:**

| Label | Values |
|---|---|
| `outcome` (consumer) | `ack` `nak` `term` `left_pending` `handler_cancelled` |
| `outcome` (publish) | `success` `timeout` `no_responders` `disconnected` `buffer_full` `permission` `payload_too_large` `other_error` |
| `reason` (terminal) | `max_deliver` `permanent` `publish_exhausted` `consumer_deleted` `stream_unavailable` `invalid_payload` `internal` |
| `result` (request) | `success` `bad_request` `unauthenticated` `forbidden` `not_found` `conflict` `too_many_requests` `unavailable` `internal` |
| `event_type` | `created` `updated` `deleted` `pinned` `unpinned` `reacted` `thread_reply_added` `send` `teams_batch` `room_create` `member_add` `member_remove` `room_rename` `member_muted` `unknown` |
| `destination_kind` | `canonical` `recipient_event` `notification` `push` `outbox` `inbox` `client_response` `user_sync` `room_canonical` `room_event` `member_event` `unknown` |
| `operation` | `canonical_publish` `client_response` `recipient_publish` `notification_publish` `push_publish` `history_get_message` `presence_lookup` `thread_tcount` `teams_user_upsert` `history_read` `history_mutation` `room_read` `room_mutation` `member_read` `member_mutation` `teams_room` `room_publish` `member_publish` `outbox_publish` `unknown` |

The request `result` enum maps one-to-one onto the error-budget eligibility
table in `sli-slo.md` §0.1: `internal`, `unavailable`, and
`too_many_requests` burn budget; the rest are legitimate client outcomes and
are removed from valid events entirely. Do not invent a second classification.

`operation` is coarse by design — see trap 7.9 for the two collisions that
matter.

### A.2 Connection metrics — `pkg/natsutil`

Opt-in through `natsutil.ConnectWithMetrics`.

| Prometheus family | Type | Labels |
|---|---|---|
| `chat_nats_client_connected` | gauge | **none inline** |
| `chat_nats_client_connection_events_total` | counter | `event`: `connected` `disconnected` `reconnected` `closed` `async_error` |
| `nats_slow_consumer_events_total` | counter | `subject`, `queue` |

These carry **no `service_name` and no `site`** — they are emitted below the
layer that knows the site and are scoped by the OpenTelemetry resource. Join
`target_info` (trap 7.12). Reconnect-buffer overflow is deliberately absent
from the events family; it is counted at the publish boundary as
`chat_nats_publish_attempts_total{outcome="buffer_full"}`.

### A.3 Domain metrics — four workers

No inline `site` label (trap 7.13).

| Prometheus family | Type | Labels and enums |
|---|---|---|
| `message_gatekeeper_messages_total` | counter | `result`: `accepted` `rejected` `retry` `failed` · `reason`: `none` `invalid_subject` `invalid_payload` `not_subscribed` `room_restricted` `canonical_publish` `dependency` `unknown` |
| `message_worker_persistence_total` | counter | `message_kind`: `user` `system` `thread_reply` `teams_migration` `unknown` · `result`: `success` `error` |
| `broadcast_worker_fanout_recipients_*` | histogram | `room_kind`: `channel` `dm` `bot_dm` `thread` `unknown` · `event_type` |
| `broadcast_worker_recipient_deliveries_total` | counter | `room_kind` · `event_type` · `result`: `success` `failed` |
| `notification_worker_outcomes_total` | counter | `kind`: `push` `unknown` (#286 removed the unused `notification` value) · `result`: `sent` `suppressed` `publish_failed` `failed` |

No other service owns domain metrics. `room-service`, `room-worker`,
`inbox-worker`, `outbox-worker`, and `search-sync-worker` are tracked as gaps
in guide §8.2.

### A.4 Dependency client metrics — o11y SDK

| Prometheus family | Type | Labels |
|---|---|---|
| `db_client_operation_duration_seconds_*` | histogram | `service_name`, `db_system_name`, `db_operation_name`, `db_namespace` (Cassandra), `network_peer_address`, `network_peer_port`, `error_type` |
| `db_client_connection_count` | gauge | + pool name, state (`used` / `idle`) |
| `db_client_connection_pending_requests` | gauge | + pool name, server address/port |
| `db_client_connection_timeouts_total` | counter | same |
| `db_client_connection_create_time_seconds_*` | histogram | same |
| `db_client_connection_idle_min` / `_max` | gauge | same |
| `cassandra_query_attempts_total` | counter | bounded query labels |
| `cassandra_connection_attempts_total` | counter | connection labels + `error_type` |

`error_type` exists **only on failures** (trap 7.4). The SDK deliberately drops
`idle.max`, `use_time`, and `wait_time` — do not build panels expecting them.
Mongo operation metrics omit database and collection labels by design; use
traces to distinguish collections.

Instrumented Cassandra sessions: `message-worker`, `bot-message-worker`,
`history-service` only. Mongo coverage is broad; the exceptions are listed in
`storage-dependency-metrics.md` §3.1.

### A.5 Other application metrics

| Family | Labels | Notes |
|---|---|---|
| `search_service_requests_total`, `search_service_request_duration_seconds_*`, `search_service_es_duration_seconds_*` | per service | No soak lane exercises search; flat on D4 by design |
| `cache_hits_total`, `cache_misses_total`, `cache_errors_total` | `service_name`, `cache`, `tier` | Present on selected services |
| `room_key_absent_errors_total`, `room_key_fanout_errors_total`, `room_key_store_errors_total` | — | `room-worker` via `pkg/roomkeysender` |
| `oplog_events_published_total`, `oplog_publish_errors_total`, `oplog_events_skipped_total`, `oplog_events_degraded_total`, `oplog_replication_lag_ms` | — | `data-migration/oplog-connector` |
| `oplog_transformer_*`, `oplog_collections_transformer_*`, `oplog_direct_transfer_*` — each with `_events_processed_total`, `_events_skipped_total`, `_naks_total`, `_terms_total`, `_exhausted_total`, `_writes_total` and family-specific extras | — | `data-migration` transformers. The richest domain-metric set in the repo and the pattern `o11y-metrics-inventory.md` §2 tells the hot-path workers to copy. |
| `atrest_dek_cache_hits_total`, `atrest_dek_cache_misses_total`, `atrest_dek_creations_total`, `atrest_kek_wrap_total{result}`, `atrest_kek_unwrap_total{result}`, `atrest_kek_renewal_failures_total` | mostly none | **`promauto`, not the OTel meter — not on `:2112`, no resource attributes** (trap 7.14) |
| `bot_msg_worker_permanent_error_total` | none | Same `promauto` caveat |
| `go_*`, `process_*`, cAdvisor, kube-state-metrics | — | Runtime |

### A.6 Loadgen metrics — D4 only

Every family below is **Available on `main` since #271**. They exist only while a
run is in progress, which is the reason D4 is a separate dashboard (guide §4.2).

**Dispatch and pacing.** `loadgen_soak_configured_rate{lane}`,
`loadgen_soak_intended_total`, `loadgen_soak_dispatched_total`,
`loadgen_soak_scheduler_underrun_total`, `loadgen_soak_lane_saturation_total`,
`loadgen_soak_global_saturation_total`.

> #271 **removed** the combined `loadgen_soak_saturation_total` and split it into
> the lane and global counters above. A query still referencing the old name
> returns nothing, which reads as "no saturation" — the exact false-clean this
> document exists to prevent.

**Observer validity.** `loadgen_failure_observer_configured{observer}`,
`loadgen_failure_observer_eligible_total{scenario,lane,observer}`,
`loadgen_failure_observer_up`, `loadgen_failure_observer_events_total`,
`loadgen_failure_observer_queue_depth`.

**Ledger integrity.** `loadgen_failure_invalidations_total{reason}`,
`loadgen_failure_untracked_total{reason}`, `loadgen_failure_dropped_total`,
`loadgen_failure_journal_bytes`, `loadgen_failure_wal_appends_total`,
`loadgen_failure_wal_append_duration_seconds_*`,
`loadgen_failure_wal_flush_duration_seconds_*`,
`loadgen_failure_wal_flush_batch_size_*`,
`loadgen_failure_evidence_records_total`,
`loadgen_failure_evidence_flush_duration_seconds_*`.

**Generator connection and run identity.** `loadgen_nats_connected{pool}`,
`loadgen_nats_connection_events_total{pool,event}`,
`loadgen_nats_outage_duration_seconds_*`, `loadgen_nats_current_outage_seconds`,
`loadgen_run_info`.

**Impact and correctness.**
`loadgen_failure_operations_total{scenario,lane,result}`,
`loadgen_failure_observations_total{scenario,lane,observer,result}`,
`loadgen_failure_observation_reasons_total`,
`loadgen_failure_not_sent_total`, `loadgen_failure_inflight{scenario,lane}`,
`loadgen_failure_recovered_operations_total`,
`loadgen_soak_operations_total{action,outcome,phase}`,
`loadgen_soak_errors_total{action,class}`,
`loadgen_soak_retries_total{action}`,
`loadgen_soak_verifications_total{action,class}`,
`loadgen_soak_mutation_target_missing_total`,
`loadgen_soak_rpc_latency_seconds_*{action}`.

**Consumer sampling.** All labelled `stream` and `durable`:
`loadgen_consumer_up`, `loadgen_consumer_pending`,
`loadgen_consumer_ack_pending`, `loadgen_consumer_redelivered`,
`loadgen_consumer_delivered_sequence`,
`loadgen_consumer_ack_floor_sequence`,
`loadgen_consumer_ack_floor_stream_sequence`,
`loadgen_consumer_ack_floor_stall_seconds`,
`loadgen_consumer_last_active_timestamp_seconds`,
`loadgen_consumer_max_deliver`, `loadgen_consumer_ack_wait_seconds`, and
`loadgen_consumer_sample_errors_total{stream,durable,reason}`.

Sampled durables are all four hot-path consumers: `message-gatekeeper` on
MESSAGES, plus `message-worker`, `broadcast-worker`, and `notification-worker`
on MESSAGES-CANONICAL.

`loadgen_consumer_ack_floor_stall_seconds` is emitted **only** while
`NumPending > 0` and `ConsumerInfo.AckFloor.Last` is available, so its absence
is not a healthy-floor claim. It also does not replace oldest-pending age: a
consumer advancing too slowly to catch up never freezes its floor.

**Terminal result enums.** Operations are `good`, `bad`, `unverified`,
`not_sent`, `missing_after_deadline`. Observations use the same set **minus
`not_sent`**. Bounded reasons: `admission_rejected`,
`history_content_mismatch`, `history_missing`, `recipient_duplicate`,
`recipient_unexpected`, `recipient_identity_mismatch`, `recipient_missing`,
`publish_local_error`.

**Added by #295.** Eleven families, all bounded; room IDs, accounts,
and message IDs stay WAL and log content, never Prometheus labels.

| Family | Meaning |
|---|---|
| `loadgen_soak_lane_attempts_total{lane,outcome}` | Lane slots by outcome: `sent`, `no_target`, `refused`. **The traffic-validity gate** — see D4-1.1. |
| `loadgen_soak_room_candidates{state}` | Member candidates by lifecycle state |
| `loadgen_soak_room_quarantine_probes_total{result}` | Parked-pair re-probe outcomes |
| `loadgen_soak_room_pool_exhausted_total{reason}` | Mutations skipped for lack of a usable target |
| `loadgen_soak_room_pool_degraded` | Reversible candidate-pool degradation flag |
| `loadgen_soak_room_create_budget_remaining` | Rooms the create lane may still add |
| `loadgen_soak_room_state_source_total{source,result}` | Room-state observer outcomes per source |
| `loadgen_soak_presence_signals_total{signal}` | Presence signals published, by kind |
| `loadgen_soak_presence_checks_total{result}` | Batch-query comparison outcomes |
| `loadgen_soak_presence_connections{status}` | Connections the lane currently claims online |
| `loadgen_failure_abandoned_journals` | Retained journals from earlier ledger epochs |

The existing `loadgen_failure_*` and `loadgen_soak_*` pacing families cover the
new lanes through their existing `lane` and `observer` labels — no new terminal
result values, and `scenario` stays `message_soak`.

**Pool degradation is reversible; ledger invalidation is not.** Quarantine
overflow is reported through `loadgen_soak_room_pool_degraded` and
`loadgen_soak_room_pool_exhausted_total`, deliberately **not** through
`loadgen_failure_invalidations_total`. Invalidation is a one-way,
process-lifetime latch: using it for a recoverable degradation would make every
later fault window in a multi-day run inconclusive. Read a degraded pool as
"this lane's offered load dropped" — which D4-1.1's attempts gate already
measures — not as "the evidence is void".

**Cardinality contract.** `scenario` is fixed to **`message_soak`** — #271
renamed it from `cassandra_soak`, so a dashboard variable or hardcoded selector
carrying the old value silently matches nothing. Lanes, observers, results,
reasons, NATS pools and events, and consumer identifiers are bounded
configuration values. Forbidden as labels anywhere on a hot path: run,
operation, message, room, account, user, recipient, subject, inbox, raw error or
advisory text, and pod UID.

### A.7 Platform exporter series

Consumed through the recording rules in guide §9.3, never referenced directly
by a panel. The exporter vocabulary itself is verified in guide §9.1 against
`natsio/prometheus-nats-exporter:0.16.0` with `-jsz=all`, which is what
`tools/observability/` runs and what the repo's own local dashboards query.

| Exporter series (source) | Normalized to | Feeds |
|---|---|---|
| `jetstream_consumer_num_pending` | `chat_jetstream_consumer_pending` | D2-5.1, D2-5.2, alert 9 |
| `jetstream_consumer_num_ack_pending` | `chat_jetstream_consumer_ack_pending` | D2-5.1 |
| `jetstream_consumer_num_redelivered` | `chat_jetstream_consumer_redelivered` | D2-5.1 |
| `jetstream_consumer_num_waiting` | `chat_jetstream_consumer_waiting` | (declared; no panel yet) |
| `jetstream_consumer_ack_floor_stream_seq` | `chat_jetstream_consumer_ack_floor`, `chat_jetstream_consumer_ack_rate` | D2-5.2, D2-5.3, alert 9 |
| `jetstream_consumer_delivered_stream_seq` | `chat_jetstream_consumer_delivered` | (declared; no panel yet) |
| `jetstream_stream_last_seq` | `chat_jetstream_stream_publish_rate` | D2-5.3 |
| `jetstream_stream_total_messages` | `chat_jetstream_stream_messages` | D2-5.4 |
| `jetstream_stream_total_bytes` | `chat_jetstream_stream_bytes` | D2-5.4 |

Source labels are `stream_name` and `consumer_name`; the rules rename them to
`stream` and `consumer` so they join the application families.

**Everything in this table is Proposed, and blocked on one thing only: the
exporter being scraped in staging and production.** No application change is
involved. Guide §9.4 lists what to establish before adopting the rules.

Absent from the exporter vocabulary, confirmed rather than assumed:

- `chat_jetstream_consumer_oldest_pending_age_seconds` — **Missing.** jsz does
  not expose it. The ack-floor stall rule is a partial substitute; a consumer
  that keeps acknowledging too slowly to catch up never freezes its floor, so
  D2-5.3's publish-versus-ack gap is the complementary signal.
- `chat_jetstream_consumer_last_active_age_seconds` — **Missing.** Listed in the
  NATS failure metrics contract §5; no jsz source. Nothing here depends on it.
- Per-subject message counts — **Missing** and not expected. jsz breaks down by
  stream and consumer only. Since durables are named after the consuming
  service, per-consumer is the axis that matters.

During a loadgen run the `loadgen_consumer_*` family (A.6) covers the same
ground from the client side, including an ack-floor stall **duration** the
exporter cannot provide. Outside a run there is no substitute.
