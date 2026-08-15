# Alerting Baseline

> Companion to the [Dashboard and Alert Guide](dashboard-and-alert-guide.md)
> and the [Dashboard Panel Catalog](dashboard-panel-catalog.md). Trap
> references ("trap 7.x") point at the guide.

---

## 1. Scope

This document owns alerts built on **metrics this repository emits**. It
deliberately does not restate:

- **Kubernetes platform alerts** — pod down, CPU saturation, OOM kill, restart
  loops, node pressure. These exist already and are assumed.
- **Dependency infrastructure alerts** — the NATS, MongoDB, and Cassandra teams
  each supply recommended alerts for their own systems, which are adopted
  as-is.
- **SLO targets and burn-rate policy** — owned by `../../load-testing/system/sli-slo.md`
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
| Size | 8 rules | Grows with the SLO set |

The Phase 0 admission rule does most of the work. Applied to the metrics this
project owns, it yields exactly eight rules, every one of which can be written
today without waiting for any baseline — because the metric's own contract says
what healthy means.

Phase 0 rules are not provisional. They remain in place through Phase 1; the
burn-rate rules are added beside them, not instead of them.

---

## 3. Phase 0 rule set

Eight rules. Severity `critical` means it pages; `warning` means it opens a
ticket or posts to a channel.

| # | Rule | `for` | Severity | Pages |
|---|---|---|---|---|
| 1 | `ChatConsumerMissing` | 10m | critical | yes |
| 2 | `ChatNATSConsumerLoopStopped` | 5m | critical | yes |
| 3 | `ChatNATSConsumerRecoveryFailing` | 15m | warning | no |
| 4 | `ChatNATSTerminalFailures` | 10m | warning | no |
| 5 | `ChatNATSReconnectBufferFull` | 0m | critical | yes |
| 6 | `ChatNATSClientDisconnected` | 5m | warning | no |
| 7 | `ChatNATSSlowConsumer` | 5m | warning | no |
| 8 | `AtRestKEKRenewalFailing` | 30m | critical | yes |

Three page, five do not. The three that page share a property: by the time a
human notices without being told, data has already been lost or a total outage
is imminent.

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

- **`for`:** 5m · **Severity:** critical · **Pages:** yes
- **Threshold rationale:** `pkg/natsmetrics` sets this to 1 only after iterator
  creation succeeds and to 0 before returning from a terminal `Next`, iterator,
  or consumer-lookup error. A running process should read 1. No baseline
  applies to a structurally binary gauge.
- **Why `for: 5m` and not shorter:** with #283, a terminal iterator error
  triggers recreation with capped exponential backoff, so a transient loss
  self-heals in seconds (trap 7.11). A sustained zero now means **recovery is
  repeatedly failing**, which is a much stronger statement than the pre-#283
  meaning. Before #283 merges, 2m is appropriate; after it merges, 5m.
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

### 3.3 `ChatNATSConsumerRecoveryFailing`

```promql
sum by (service_name, site, stream, consumer) (
  rate(chat_nats_consumer_recovery_attempts_total{result="failure"}[10m])
) > 0
```

- **`for`:** 15m · **Severity:** warning · **Pages:** no · **Source:** #283 pending
- **Threshold rationale:** healthy is zero — a stable consumer never needs
  recreating. Non-zero is not immediately user-visible because backoff may still
  succeed, so it opens a ticket rather than paging.
- **What only this catches:** the churn case. A consumer that alternates between
  successful and failed recreation leaves `chat_nats_consumer_loop_up` reading 1
  for most of every evaluation window, so rule 2 never fires, while the consumer
  is in fact unstable. This rule is the only view of it.
- **False positives:** a broker restart produces a burst of failures followed by
  success. `for: 15m` outlasts a normal restart.
- **Panel:** D1-3.2, D2-1.1, D4-4.2

### 3.4 `ChatNATSTerminalFailures`

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
- **Panel:** D1-3.3, D2-1.5, D4-4.3

### 3.5 `ChatNATSReconnectBufferFull`

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
- **Panel:** D1-3.4, D2-2.1, D4-4.4

### 3.6 `ChatNATSClientDisconnected`

```promql
(
  chat_nats_client_connected
    * on (job, instance) group_left (service_name)
      target_info
) == 0
```

- **`for`:** 5m · **Severity:** warning · **Pages:** no
- **Threshold rationale:** a connected process reads 1. Reconnection is usually
  automatic, so a short disconnect is not actionable; 5m distinguishes a blip
  from a service that cannot reach the broker at all.
- **Why the join is mandatory:** this family carries no `service_name` or `site`
  label (trap 7.12). Without joining `target_info`, the alert can say a
  connection was lost but not by which service, which makes it unactionable.
  **Verify the join keys resolve in the target environment before enabling** —
  they depend on collector configuration, and a join that silently returns
  nothing produces a rule that can never fire.
- **Panel:** D1-3.5, D2-1.6, D4-4.5

### 3.7 `ChatNATSSlowConsumer`

```promql
sum by (subject, queue) (increase(nats_slow_consumer_events_total[5m])) > 0
```

- **`for`:** 5m · **Severity:** warning · **Pages:** no
- **Threshold rationale:** healthy is zero; the event means the broker is
  discarding messages destined for one of our subscriptions.
- **Note:** same resource-scoping caveat as rule 6 — join `target_info` to
  attribute it to a service.
- **Panel:** D1-3.6, D2-1.7, D4-4.6

### 3.8 `AtRestKEKRenewalFailing`

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
- **Panel:** D1-3.7

---

## 4. Deliberately excluded from Phase 0

Excluding these is a decision, not an oversight. Each has a stated reason.

| Candidate | Why not in Phase 0 |
|---|---|
| `message_gatekeeper_messages_total{result="rejected"}` ratio | A non-zero floor is legitimate — `not_subscribed` and `room_restricted` are normal client outcomes. Needs a measured baseline. |
| `notification_worker_outcomes_total{result="suppressed"}` ratio | Presence-based suppression is the design. A high suppressed share is correct behavior. |
| `chat_nats_request_handled_total` error ratio | #283 pending, and the healthy ratio is unknown. Phase 1 candidate, and a strong one: its `result` enum maps exactly onto the error-budget eligibility table in `sli-slo.md` §0.1. |
| Any p99 latency threshold | Two independent reasons. No baseline, and `sli-slo.md` §0.1 forbids raw percentiles as targets outright — a percentile has no good/valid ratio, so no error budget or burn rate can be computed from it. Latency alerts must be written as "share completing within a bound", which is a Phase 1 construction. |
| JetStream backlog thresholds | The same series means opposite things on different consumers (guide §2 item 4). 5000 pending on `message-worker` during a soak is routine; the same on an `outbox-worker` FIFO lane means a peer has been down for hours. Requires per-consumer baselines. |
| Ack-floor stall | Structurally a good Phase 0 candidate — the healthy value is binary. Blocked on verifying that `ack_floor` exists in the deployed exporter's output (guide §8.1). Promote as soon as that is confirmed. |
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
2. **Oldest-pending age does not exist** (guide §8.2). The ack-floor stall rule
   is the substitute: it proves the floor is stuck, not for how long. A
   duration-based backlog alert cannot be written until the gauge exists.

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

When alerting is enabled, four of the eight rules — 2, 4, 5, and 6 — will fire
during a NATS failure test. That is the expected result of the test.

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

- **Rule 5 (`ChatNATSReconnectBufferFull`)** marks the overlapping interval as
  having unproven recipient delivery. Its firing changes how the correctness
  panels must be read.
- **Rule 4 (`ChatNATSTerminalFailures`)** enumerates what was lost. It is one
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
| 3 `ChatNATSConsumerRecoveryFailing` | D1-3.2 | D2-1.1 | D4-4.2 |
| 4 `ChatNATSTerminalFailures` | D1-3.3 | D2-1.5 | D4-4.3 |
| 5 `ChatNATSReconnectBufferFull` | D1-3.4 | D2-2.1 | D4-4.4 |
| 6 `ChatNATSClientDisconnected` | D1-3.5 | D2-1.6 | D4-4.5 |
| 7 `ChatNATSSlowConsumer` | D1-3.6 | D2-1.7 | D4-4.6 |
| 8 `AtRestKEKRenewalFailing` | D1-3.7 | — | — |

Rule 1 has no D4 series by design: consumer absence during a fault window is
usually a killed pod, which the Kubernetes alerts already explain.

---

## 8. Before enabling any rule

1. **Confirm the series is scraped.** Rule 8 is the worked example of why
   (trap 7.14), but the check applies to all eight.
2. **Confirm label spelling** against the deployed collector, including whether
   `service_name` and `site` appear inline or only via `target_info`
   (traps 7.12, 7.13).
3. **Confirm the rule can fire.** Force the condition in a non-production
   environment and observe the alert. The NATS metrics contract §12 requires a
   deliberate known failure to change the expected series; the same standard
   applies to the rule built on it. An untested rule is an assumption.
4. **Confirm the routing** before the first campaign, per Section 6.
