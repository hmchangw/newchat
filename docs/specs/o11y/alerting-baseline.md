# Alerting Baseline

> Companion to the [Dashboard and Alert Guide](dashboard-and-alert-guide.md)
> and the [Dashboard Panel Catalog](dashboard-panel-catalog.md). Trap
> references ("trap 7.x") point at the guide.
>
> Verified against `main` at `d2fdb33`. **Nine rules, eight deployable
> today** — the eighth needs the NATS exporter scraped in staging and
> production, which is a deployment task, not an instrumentation one. The
> consumer-recovery rule that earlier revisions carried has been removed:
> #283 is not landing, and guide §3.2 explains why no coverage is lost. #318
> added the search-sync domain metrics that rule 9 watches.

---

## 1. Scope

This document owns alerts built on **metrics this repository emits**. It
deliberately does not restate:

- **Kubernetes platform alerts** — pod down, CPU saturation, OOM kill, restart
  loops, node pressure. These exist already and are assumed.
- **Dependency infrastructure alerts** — the NATS, MongoDB, and Cassandra teams
  each supply recommended alerts for their own systems, which are adopted
  as-is.
- **SLO targets and burn-rate policy** — owned by `../../load-testing/common/sli-slo.md`
  §7. Section 5 here describes how this baseline hands over to it.

The gap this fills is narrow and specific: guide §2 lists seven questions that
neither the Kubernetes alerts nor the platform teams' alerts can answer,
because answering them requires knowing what this application expects of
itself.

---

## 2. Two phases

Starting with a small alert set and growing it is standard practice, and the
reason is not workload. **A threshold set before a baseline exists is a guess**,
and a guessed threshold that fires wrongly gets the whole alert set muted. This
project has already committed to the observational period: `sli-slo.md` §0.2
states that all targets are achievable-first starting values, to be run
observationally for four to six weeks, with **no paging alerts before then**.

| | **Phase 0** — now through calibration | **Phase 1** — after calibration |
|---|---|---|
| Admission rule | The healthy value is **structurally 0 or 1**, derivable from the metric's own semantics | Ratios and durations whose healthy range must be measured |
| Basis | Metric semantics | Four to six weeks of observed distribution |
| Action | Mostly ticket; three exceptions page (Section 3) | Multi-window burn-rate paging per `sli-slo.md` §7 |
| Size | 9 rules, 8 deployable | Grows with the SLO set |

The Phase 0 admission rule does most of the work. Applied to the metrics this
project owns — and to the platform-exporter series the recording rules in guide
§9.3 derive from — it yields exactly nine rules, none of which needs a
baseline, because each metric's own contract says what healthy means. Eight can
be written today; the ninth is blocked on a deployment, not on measurement.

Phase 0 rules are not provisional. They remain in place through Phase 1; the
burn-rate rules are added beside them, not instead of them.

---

## 3. Phase 0 rule set

Nine rules. Severity `critical` means it pages; `warning` means it opens a
ticket or posts to a channel.

| # | Rule | `for` | Severity | Pages | Deployable |
|---|---|---|---|---|---|
| 1 | `ChatConsumerMissing` | 10m | critical | yes | yes |
| 2 | `ChatNATSConsumerLoopStopped` | 2m | critical | yes | yes |
| 3 | `ChatNATSTerminalFailures` | 10m | warning | no | yes |
| 4 | `ChatNATSReconnectBufferFull` | 0m | critical | yes | yes |
| 5 | `ChatNATSClientDisconnected` | 5m | warning | no | yes |
| 6 | `ChatNATSSlowConsumer` | 5m | warning | no | yes |
| 7 | `AtRestKEKRenewalFailing` | 30m | critical | yes | yes |
| 8 | `ChatJetStreamAckFloorStalled` | 15m | warning | no | **not yet — exporter** |
| 9 | `ChatSearchSyncPoisonDrop` | 10m | warning | no | yes |

Three page, six do not. The three that page share a property: by the time a
human notices without being told, data has already been lost or a total outage
is imminent.

**Eight of the nine can be written today.** Rule 8 is blocked on the NATS
exporter being scraped in staging and production. It needs no application
change at all, and it closes a gap nothing else covers: `sli-slo.md` §7 names
stalled JetStream backlog as the outage backstop for every asynchronous SLO,
and today that backstop does not exist outside a loadgen run.

### 3.1 `ChatConsumerMissing`

```promql
# One rule instance per expected (stream, consumer). The maintained inventory
# is the rule itself.
absent(chat_nats_consumer_loop_up{
  site="<site>", stream="MESSAGES-CANONICAL-<site>", consumer="message-worker"})
```

- **`for`:** 10m · **Severity:** critical · **Pages:** yes
- **Threshold rationale:** no baseline needed. The consumer is either deployed
  or it is not. 10m absorbs a rolling deploy in which every replica is
  simultaneously between old and new pods.
- **Why this is ours:** a deleted durable, an undeployed service, or a broken
  scrape all produce *no series*. An infrastructure dashboard shows nothing and
  an infrastructure alert stays silent, because nothing external knows this
  consumer was supposed to exist. This is the operational form of the evidence
  contract's rule that a missing series is unknown, never zero.
- **Complements:** rule 2. A dead process makes the series vanish, so rule 2
  cannot fire; this rule catches it. A live process that stopped consuming
  keeps the series at 0, so this rule cannot fire; rule 2 catches it. **Neither
  covers the other**, and deploying only one leaves a real failure mode
  unmonitored.
- **False positives:** a genuine decommission. Removing the consumer from the
  rule inventory is the intended workflow — the inventory is meant to be
  edited deliberately.
- **Panel:** D1-1.1

### 3.2 `ChatNATSConsumerLoopStopped`

```promql
chat_nats_consumer_loop_up == 0
```

- **`for`:** 2m · **Severity:** critical · **Pages:** yes
- **Threshold rationale:** `pkg/natsmetrics` sets this to 1 only after iterator
  creation succeeds and to 0 before returning from a terminal `Next`, iterator,
  or consumer-lookup error. A running process should read 1. No baseline
  applies to a structurally binary gauge.
- **Why `for: 2m`:** there is no supervisor. `natsmetrics.Consume` calls
  `LoopFailed` on a terminal `Next` error and returns, so the loop is gone until
  the process restarts (trap 7.11) — there is no recovery window to wait out,
  and a longer `for` only delays the response.
- **Why this is ours:** from the broker's side the consumer looks healthy — the
  durable exists, it has a leader, pending climbs slowly and would take a long
  time to cross any generic threshold. Nothing outside this repository knows
  this gauge exists.
- **False positives:** graceful shutdown sets it to 0 momentarily, but the
  process then exits and the series disappears, so the `for` window is not
  satisfied. Rolling deploys are covered by the same reasoning.
- **Blind spot:** `history-service` has no JetStream consumer loop and never
  emits this series. Do not write a rule instance for it.
- **Panel:** D1-3.1, D2-1.1, D4-4.1

### 3.3 `ChatNATSTerminalFailures`

```promql
sum by (service_name, site, stream, consumer, reason) (
  rate(chat_nats_terminal_failures_total[10m])
) > 0
```

- **`for`:** 10m · **Severity:** warning · **Pages:** no
- **Threshold rationale:** the NATS metrics contract §7 **excludes business
  rejections from this family specifically so that it reads zero at baseline**.
  A message the service validated and refused is an ordinary client error and
  belongs in the domain counter. That design decision is what makes `> 0` a
  legitimate threshold with no baseline.
- **Why it does not page in Phase 0:** the family is new (#272) and has not yet
  been observed in production for a sustained period. If the observed baseline
  turns out to be genuinely zero over the calibration window, promoting this to
  a page in Phase 1 is the expected outcome. If it is not zero, the correct
  response is to find out which `reason` is polluting it and fix the
  classification — not to raise the threshold.
- **Reading:** `max_deliver` means redelivery hit the cap and stopped. The
  message is **not deleted** — it stays in the stream and a max-delivery
  advisory fires. Treat it as enumerable work to recover, not as data loss.
- **Panel:** D1-3.2, D2-1.5, D4-4.2

### 3.4 `ChatNATSReconnectBufferFull`

```promql
sum by (service_name, site, destination_kind, operation) (
  increase(chat_nats_publish_attempts_total{outcome="buffer_full"}[5m])
) > 0
```

- **`for`:** 0m · **Severity:** critical · **Pages:** yes
- **Threshold rationale:** there is no acceptable non-zero rate. Overflow means
  publishes were discarded client-side. `for: 0m` because a single occurrence
  is already loss; waiting only delays the response.
- **Why this is ours, and why it matters twice:** nats.go returns
  `ErrReconnectBufExceeded` synchronously from `publish()` and never routes it
  through `ErrorHandler`, so it can only be counted at the publish boundary. It
  is both a data-loss alert and an **evidence-invalidation** signal: any
  interval overlapping it has unproven recipient delivery, because Core NATS
  publish success only means the payload entered a buffer (guide §2 item 6).
- **Panel:** D1-3.3, D2-2.1, D4-4.3

### 3.5 `ChatNATSClientDisconnected`

```promql
chat_nats_client_connected == 0
```

- **`for`:** 5m · **Severity:** warning · **Pages:** no
- **Threshold rationale:** a connected process reads 1. Reconnection is usually
  automatic, so a short disconnect is not actionable; 5m distinguishes a blip
  from a service that cannot reach the broker at all.
- **Attribution:** `service_name` is on the series already, as a
  resource-derived constant label, so the alert names the affected service with
  no join. Earlier revisions required a `target_info` join here; that was wrong
  on both counts — unnecessary for `service_name`, and unable to supply `site`,
  which this family does not have at all (trap 7.12).
- **Blind spot:** with no `site` label, a multi-site Prometheus cannot tell you
  *which* site lost the connection from this rule alone.
- **Panel:** D1-3.4, D2-1.6, D4-4.4

### 3.6 `ChatNATSSlowConsumer`

```promql
sum by (subject, queue) (increase(nats_slow_consumer_events_total[5m])) > 0
```

- **`for`:** 5m · **Severity:** warning · **Pages:** no
- **Threshold rationale:** healthy is zero; the event means the broker is
  discarding messages destined for one of our subscriptions.
- **Note:** same scoping as rule 5 — `service_name` is present, `site` is
  not.
- **Panel:** D1-3.5, D2-1.7, D4-4.5

### 3.7 `AtRestKEKRenewalFailing`

```promql
sum(rate(atrest_kek_renewal_failures_total[15m])) > 0
```

- **`for`:** 30m · **Severity:** critical · **Pages:** yes
- **Threshold rationale:** healthy is zero. The metric's own Help text
  documents it as a hard alert: a sustained non-zero rate means the service
  cannot obtain a Vault token, and encryption fails once the current token
  expires. `for: 30m` filters transient Vault unavailability while leaving
  ample margin before expiry.
- **Why it pages despite not being user-visible yet:** it is a slow fuse. When
  it does become visible, it is a total failure of the encrypted message path,
  and the remediation requires a human with Vault access.
- **Blocking prerequisite:** trap 7.14 — these counters are registered through
  `promauto`, **not** the OTel meter, so they are absent from the SDK `:2112`
  endpoint and carry none of the resource attributes. **Confirm Prometheus
  scrapes the exposing endpoint before enabling this rule.** An alert on an
  unscraped series is strictly worse than no alert: it is a rule that can never
  fire while creating the impression of coverage.
- **Panel:** D1-3.6

### 3.8 `ChatJetStreamAckFloorStalled`

```promql
chat_jetstream_consumer_ack_floor_stalled == 1
```

- **`for`:** 15m · **Severity:** warning · **Pages:** no
- **Deployable:** **not yet.** The recording rule is fully specified in guide
  §9.3 and its exporter input (`jetstream_consumer_ack_floor_stream_seq`) is
  verified to exist. What is missing is the exporter being scraped in staging
  and production — a deployment task, not an instrumentation one.
- **Threshold rationale:** the rule already encodes the condition, so the alert
  expression is binary and needs no baseline: pending work exists and the
  acknowledgment floor has not advanced in the lookback. `for: 15m` outlasts a
  deploy-window pause; a genuinely parked head-of-line lasts far longer.
- **What only this catches:** the `outbox-worker` ordered lanes. They run with
  `MaxAckPending=1`, so a generic ack-pending threshold is **structurally
  incapable** of firing on them no matter how long a peer has been parked
  (guide §2 item 3). This is the only rule that sees them.
- **Why it does not page:** a parked forward to a down peer is by design
  (`MaxDeliver=-1`, never Ack). Firing means "a lane has been stuck long
  enough to look at", which is a ticket. Promote to a page only after the
  calibration window shows what a normal parked duration looks like.
- **Known blind spot, and its mitigation:** the condition is necessary but not
  sufficient. **A consumer that keeps acknowledging too slowly to catch up
  never freezes its floor**, so this rule stays silent while the backlog grows
  without bound. Panel D2-5.3 (publish rate minus ack rate) is the
  complementary signal, and a sustained-gap alert on it is a Phase 1 candidate
  because that threshold does need a baseline.
- **Per-consumer thresholds:** guide §2 item 4 — do not generalize the `for`
  window across consumers without checking. A soak-time pause on
  `message-worker` and a parked outbox lane are the same series with opposite
  meanings.
- **Panel:** D2-5.2

### 3.9 `ChatSearchSyncPoisonDrop`

```promql
sum by (collection) (
  rate(search_sync_worker_messages{outcome="acked", reason="poison"}[10m])
) > 0
```

- **`for`:** 10m · **Severity:** warning · **Pages:** no
- **Threshold rationale:** healthy is zero. `poison` is the Ack-and-drop path
  for a payload that could not be decoded or turned into a bulk action — there
  is no legitimate rate of undecodable messages on a working pipeline, so no
  baseline is needed.
- **What only this catches:** search-index loss. Every JetStream-shaped signal
  reads clean through it — pending goes to zero, the ack floor advances, there
  is no Nak and no redelivery — and `search-sync-worker` is not a `chat_nats_*`
  adopter, so it emits no terminal-failure counter either (trap 7.17).
  **Before #318 a log line was the only evidence.**
- **Why it does not page:** the loss is already permanent by the time this
  fires; there is nothing to catch in progress. It is a reindex-and-investigate
  ticket, not a wake-up.
- **Blind spot:** this proves a payload was undecodable, not that an otherwise
  valid message reached the index. There is still no end-to-end check — the
  loadgen search-index observer is refused at startup until soak payloads carry
  a per-message searchable marker (guide §8.2).
- **Panel:** D2-3.5

---

## 4. Deliberately excluded from Phase 0

Excluding these is a decision, not an oversight. Each has a stated reason.

| Candidate | Why not in Phase 0 |
|---|---|
| `message_gatekeeper_messages_total{result="rejected"}` ratio | A non-zero floor is legitimate — `not_subscribed` and `room_restricted` are normal client outcomes. Needs a measured baseline. |
| `notification_worker_outcomes_total{result="suppressed"}` ratio | Presence-based suppression is the design. A high suppressed share is correct behavior. |
| `chat_nats_request_handled_total` error ratio | Available since #286, but the healthy ratio is unknown — it is a ratio, so it needs the calibration window by definition. The strongest Phase 1 candidate: its `result` enum maps exactly onto the error-budget eligibility table in `sli-slo.md` §0.1, so no new classification has to be invented. |
| Any p99 latency threshold | Two independent reasons. No baseline, and `sli-slo.md` §0.1 forbids raw percentiles as targets outright — a percentile has no good/valid ratio, so no error budget or burn rate can be computed from it. Latency alerts must be written as "share completing within a bound", which is a Phase 1 construction. |
| JetStream backlog thresholds | The same series means opposite things on different consumers (guide §2 item 4). 5000 pending on `message-worker` during a soak is routine; the same on an `outbox-worker` FIFO lane means a peer has been down for hours. Requires per-consumer baselines. |
| JetStream backlog *rate* gap (publish minus ack) | Needs a baseline: the healthy gap is not zero, it oscillates around zero. Phase 1 candidate, and the necessary complement to rule 8 — see that rule's blind spot. |
| Fan-out size versus deliveries | Not a ratio for channel rooms (trap 7.8). An alert on it would fire constantly on the dominant room type. |
| SLO burn rate | Phase 1 by definition. Also blocked on the P2 counters in `sli-slo.md` §8, which do not exist yet. |
| Cassandra reaction/pin write path | Cannot be alerted on at all: ten bare `ExecuteBatch` call sites emit no client telemetry (trap 7.10). Listed here so the absence is recorded rather than assumed covered. |

---

## 5. Handing over to Phase 1

Phase 1 is burn-rate alerting on error budgets, per `sli-slo.md` §7: page at
14.4x over 1h (with a 5m short window), page at 6x over 6h (30m short window),
ticket at 1x over 3d. This is well-defined only because every SLO there is an
event ratio.

Two things gate the handover:

1. **The v1 counters are approximate.** `sli-slo.md` §0.1 marks the
   asynchronous SLOs *approximate (lag-enforced)*: the gatekeeper ignores
   `PubAck.Duplicate`, Cassandra's idempotent inserts do not flag the first
   write, `jsretry.Settle` has no exhaustion callback, and the outbox retries
   forever. Under redelivery the counters can double-count or split a
   numerator and denominator across windows. Until the exact outcome ledger
   lands, **the primary enforcement signal is consumer lag and oldest-pending
   age**, not the ratio.
2. **Oldest-pending age does not exist** (guide §8.2). #271 added
   `loadgen_consumer_ack_floor_stall_seconds`, which is the right shape — a
   duration — but it exists only while a loadgen run is in progress, so it
   cannot back a production alert. And neither form closes the gap: a consumer
   that keeps acknowledging too slowly to catch up never freezes its floor, so
   the stall signal stays silent while the backlog grows. A duration-based
   backlog alert needs the platform exporter verified first (guide §8.1).

Consequence: the Phase 0 set above is not a placeholder for burn-rate alerting.
It covers a different failure class — structural failures with binary healthy
values — that burn-rate alerting will never cover, and it stays in place
permanently.

---

## 6. Behavior during load and failure tests

Alerting is not enabled during the first campaigns. Until it is, the evidence
that would have come from an alert comes from D4 Row 4, which draws each Phase 0
signal as a time series with the fault annotation overlaid. That row is
maintained one-to-one with Section 3 (see Section 7) so the two cannot drift.

When alerting is enabled, four of the seven deployable rules — 2, 3, 4, and 5 —
will fire during a NATS failure test. That is the expected result of the test.

### 6.1 Route, do not suppress

**Silencing these rules during a test is wrong**, because a firing alert *is*
the evidence the test is collecting. Silencing an Alertmanager rule stops the
notification but should never be allowed to stop evaluation or recording.

The correct handling is to route them elsewhere for the duration: attach a
campaign label to the run and route matching alerts to the test channel instead
of the on-call rotation. The rules keep evaluating, the firings stay in the
record, and nobody is woken.

Two rules should be routed but watched especially closely during a test rather
than treated as noise:

- **Rule 4 (`ChatNATSReconnectBufferFull`)** marks the overlapping interval as
  having unproven recipient delivery. Its firing changes how the correctness
  panels must be read.
- **Rule 3 (`ChatNATSTerminalFailures`)** enumerates what was lost. It is one
  of the campaign's primary outputs.

### 6.2 Cross-team coordination is required

The platform teams' alerts will fire **at the platform teams** during our
failure tests. This project cannot silence or route those, and discovering the
problem by waking someone else's on-call is not an acceptable outcome.

Before any staging failure campaign:

1. Agree the fault window with the NATS team — and with the MongoDB and
   Cassandra teams once their metrics arrive on the same terms.
2. Agree explicitly **who silences or routes which rule set**. Ours is ours;
   theirs is theirs. Neither side can do the other's.
3. Agree how the window is communicated if it over-runs, since an unexpectedly
   extended fault window is exactly when someone else's alert becomes a real
   incident for them.

This is a scheduling and communication item, not a technical one, which is why
it is easy to forget until the first time it goes wrong.

---

## 7. Rule to panel mapping

Every Phase 0 rule has a panel on D1's health strip and a time series on D4 Row
4. The mapping is deliberate: when alerting is enabled, an on-call responder
who receives a page must find the corresponding panel without searching.

| Rule | D1 tile | D2 detail | D4 time series |
|---|---|---|---|
| 1 `ChatConsumerMissing` | D1-1.1 | — | — |
| 2 `ChatNATSConsumerLoopStopped` | D1-3.1 | D2-1.1 | D4-4.1 |
| 3 `ChatNATSTerminalFailures` | D1-3.2 | D2-1.5 | D4-4.2 |
| 4 `ChatNATSReconnectBufferFull` | D1-3.3 | D2-2.1 | D4-4.3 |
| 5 `ChatNATSClientDisconnected` | D1-3.4 | D2-1.6 | D4-4.4 |
| 6 `ChatNATSSlowConsumer` | D1-3.5 | D2-1.7 | D4-4.5 |
| 7 `AtRestKEKRenewalFailing` | D1-3.6 | — | — |
| 8 `ChatJetStreamAckFloorStalled` | — | D2-5.2 | — |
| 9 `ChatSearchSyncPoisonDrop` | — | D2-3.5 | — |

Rule 1 has no D4 series by design: consumer absence during a fault window is
usually a killed pod, which the Kubernetes alerts already explain.

Rule 8's panel (D2-5.2) must not be built until the exporter is scraped — a
permanently empty panel teaches the same no-data blindness this document is
built to avoid (guide §4.2).

Rule 8 has no D1 tile by design. The health strip answers "is the pipeline
moving right now"; a stalled ack floor is a slower question that belongs on the
drill-down. Rule 8 also has no D4 series: during a declared fault a parked
consumer is the expected outcome, and D4-2.4's run-scoped
`loadgen_consumer_ack_floor_stall_seconds` already measures it in seconds
rather than as a boolean.

---

## 8. Before enabling any rule

1. **Confirm the series is scraped.** Rule 7 is the worked example of why
   (trap 7.14), but the check applies to all of them.
2. **Confirm label spelling** against the deployed collector. In particular
   confirm that `service_name` arrives as a resource constant label on the
   families the rule touches, and remember that `site` exists only on the
   `chat_nats_*` consumer/publisher families (traps 7.12, 7.13).
3. **Confirm the rule covers every deployment of the binary.** Since #286 the
   Teams and bot deployments carry their own `service_name`
   (`teams-message-worker`, `bot-broadcast-worker`, `bot-notification-worker`,
   `teams-room-worker`). A rule pinned to the base name alone silently excludes
   them, and the exclusion looks like a narrower scope rather than a gap
   (trap 7.15). None of the rules above needs a `service_name` selector —
   keep it that way unless there is a reason, and use a regex if there is.
4. **Confirm the rule can fire.** Force the condition in a non-production
   environment and observe the alert. The NATS metrics contract §12 requires a
   deliberate known failure to change the expected series; the same standard
   applies to the rule built on it. An untested rule is an assumption.
5. **Confirm the routing** before the first campaign, per Section 6.
