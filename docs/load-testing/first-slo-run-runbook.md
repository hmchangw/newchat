# First SLO Run — Runbook

> The first item in the plan: **#337 merged, then a soak run that produces real
> numbers for the SLOs that are measurable.** This is the operator's page — what
> to set, what to add, what to read, and what the result may and may not be
> called.
>
> Why it works and which SLOs it covers:
> [`slo-measurement-map.md`](slo-measurement-map.md) §7.

| | |
|---|---|
| **Produces** | Run-window SLIs for **SLO-1a, 4, 5, 7**, plus the loadgen-vs-production latency gap for 4/5 |
| **loadgen code changes** | **None** |
| **Dashboard changes** | **Yes** — the existing dashboard has 22 panels and not one SLO ratio |
| **Does not produce** | SLO-1b, 2, 3, 6, 8, 9 — see §8 |

---

## 1. Preconditions

| # | Precondition | Owner | Note |
|---|---|---|---|
| P1 | **#337 merged** | app | Delivers `rpc.server.call.duration{rpc.method, error.type}` → SLO-4/5 |
| P2 | **Prometheus scrapes the *services*' o11y SDK endpoint**, not just loadgen's `:9099` | infra | This is the one that gets missed. The Helm chart annotates **loadgen's own** service only (`metrics.serviceAnnotations`, port 9099). Every domain counter this run reads — `message_gatekeeper_messages_total`, `message_worker_persistence_total`, `rpc_server_call_duration_seconds`, `search_service_requests_total` — is emitted through the SDK meter on the services' own endpoint (`:2112` in compose; confirm the port on your service manifests). No service scrape, no SLO numbers |
| P3 | **A dedicated test `SITE_ID`** | infra | Counters are monotonic and shared. `increase()` excludes history, not concurrent traffic |
| P4 | **JetStream consumer metrics reachable** | infra | Backlog is the validity gate in §6, and `sli-slo.md` calls it the *primary* enforcement signal for every async SLO |
| P5 | Cassandra keyspace, `MESSAGE_BUCKET_HOURS` matching every reader/writer, Vault/KMS up for the encryption preflight | infra | The soak's own pre-run gate (`soak/cassandra-soak-plan.md` §6.1) |

---

## 2. loadgen needs no changes — and the defaults are already the plan

This is worth stating explicitly, because it is easy to assume otherwise. The
chart's soak defaults **are** the workload model:

| Value | Default | Workload-model input |
|---|---|---|
| `soak.sendRate` | `100` | I1 — peak sustained send rate |
| `soak.readRate` | `700` | I2 — the 7:1 read:write ratio |
| `soak.threadShare` | `0.10` | I3 |
| `soak.mutationRate` | `5` | I4 |
| `soak.payloadMedian/P95/MaxBytes` | `1024 / 2048 / 10240` | I5 |
| `soak.softDeleteRatio` | `0.001` | I9 |
| `soak.roomCount` / `channelRatio` / `channelMembers` | `10000 / 0.30 / 100` | I11 scaled |
| `soak.maxUsers` / `activeUsers` | `20000 / 2000` | I13 |
| `soak.largeRoomThreshold` | `500` | matches the services' default |

**Do not change any of them for this run.** The point is a number at the declared
baseline. Changing the rate changes what the number means.

The read lane already drives the two RPCs SLO-4/5 are defined on —
`subject.MsgHistory` → `rpc.method="channel_history"`, `subject.MsgThread` →
`thread_open` — and the send lane goes through the real gatekeeper.

---

## 3. The values overlay

Only these differ from the chart defaults. Everything else stays.

```yaml
phase: soak
runId: slo-baseline-YYYYMMDD-01     # new ID per run; owns the seeded topology
siteId: <dedicated test site>

soak:
  runMode: continuous               # a Deployment must be continuous
  warmup: 30s
  environment: staging

ledger:
  enabled: true
  epoch: v2

encryptionPreflight:
  enabled: true                     # the at-rest evidence; never off on staging

recipientObserver:
  enabled: false                    # see below

searchObserver:
  enabled: false                    # refused at startup by design

runtime:
  goMemLimit: 3GiB                  # below resources.limits.memory (4Gi)
```

**Three decisions worth making deliberately rather than inheriting:**

- **`recipientObserver.enabled: false` for this run.** It gives per-recipient
  delivery *presence/absence*, not latency, so it adds nothing to the four SLOs
  being measured — while costing a 32-connection NATS pool and a second reconcile
  step per message against the read-lane budget. More importantly the WAL header
  stores the observer contract: **a restart must use the same setting, and
  changing it requires a new `runId`.** So decide before the run, never mid-run.
- **`ledger.epoch` stays `v2`.** Changing it starts a new evidence journal.
- **`runtime.goMemLimit` must be set.** Go does not read the container memory
  limit, so without it the heap grows to roughly twice the live set and the pod is
  OOM-killed instead of collecting.

Also set, outside the chart, on the **services**: `O11Y_ENABLED=true` with
`OTEL_TRACES_SAMPLER=parentbased_traceidratio` and a **fixed, recorded**
`OTEL_TRACES_SAMPLER_ARG`. An unset sampler is 100% and distorts the very
overhead you are measuring.

---

## 4. Dashboard: add an SLO row

The shipped dashboard (`tools/loadgen/deploy/grafana/dashboards/loadtest.json`)
has **22 panels and no SLO ratio** — it was built around the evidence ledger,
because until #337 there were no counters to build ratios from. Keep every one of
those panels: they are the validity gate in §6. Add one row above them.

Two panels to be aware of:

- **"E2 broadcast latency" will be empty on a soak run.** `E1Latency`/`E2Latency`
  belong to the `run`/`daily`/`max-rps` collector; the soak does not correlate
  publish→broadcast. Not a fault — expect the gap.
- **"Search index evidence" will be empty** — the observer is refused at startup
  by design.

New row, five panels:

| Panel | Query | Read as |
|---|---|---|
| SLO-1a run-window SLI | §5 query 1 | good / valid, against the drafted 99.9% |
| SLO-4 run-window SLI | §5 query 2 | against 95% within 500 ms |
| SLO-5 run-window SLI | §5 query 3 | against 95% within 250 ms (#337 moved the bound) |
| SLO-7 run-window SLI | §5 query 4 | against 99.5% |
| SLO-4/5 measurement gap | §5 query 5 | loadgen-side minus server-side p95 — the size of the blind spot |

---

## 5. The queries

Set `$window` to the hold window explicitly (`[30m]`), not `$__range` — the panel
must not silently include warm-up.

```promql
# 1 — SLO-1a: persisted / canonical published
sum(increase(message_worker_persistence_total{
      site="$site", message_kind=~"user|thread_reply", result="success"}[$window]))
/ sum(increase(message_gatekeeper_messages_total{site="$site", result="accepted"}[$window]))

# 2 — SLO-4: channel load succeeded within 500 ms / eligible
sum(increase(rpc_server_call_duration_seconds_bucket{
      service_name="history-service", rpc_method="channel_history",
      error_type="", le="0.5"}[$window]))
/ sum(increase(rpc_server_call_duration_seconds_count{
      service_name="history-service", rpc_method="channel_history",
      error_type=~"|internal|unavailable|too_many_requests"}[$window]))

# 3 — SLO-5: thread open within 250 ms / eligible   (le="0.25")
#     same shape as 2 with rpc_method="thread_open"

# 4 — SLO-7: search ok / eligible
sum(increase(search_service_requests_total{status="ok"}[$window]))
/ sum(increase(search_service_requests_total{
      status=~"ok|internal|unavailable|too_many_requests"}[$window]))

# 5 — the measurement gap for SLO-4/5
histogram_quantile(0.95, sum by (le) (rate(loadgen_soak_rpc_latency_seconds_bucket{
      action="msg_history"}[$window])))
- histogram_quantile(0.95, sum by (le) (rate(rpc_server_call_duration_seconds_bucket{
      service_name="history-service", rpc_method="channel_history"}[$window])))
```

Three things the denominators encode, all from `sli-slo.md` §0.1:

- **The 4xx classes are excluded from *valid* entirely**, never counted as
  failures — that is why the `error_type` regex lists only the budget-burning
  classes plus the empty (success) alternative, instead of using `_count`.
- **`message_kind=~"user|thread_reply"`** — message-worker also persists `system`
  and `teams_migration` messages that enter MESSAGES-CANONICAL from
  history-service and room-worker, never through the gatekeeper. Without the
  filter the numerator counts messages the denominator never saw.
- **Group by site** in a multi-site Prometheus, or one healthy site hides
  another's total failure.

---

## 6. Read the validity gate *before* the ratios

A ratio taken from an invalid window is worse than no ratio. All of these are
already on the shipped dashboard.

| Check | Panel | Fail means |
|---|---|---|
| Dispatch ratio ≥ 95% of configured rate, per lane | Cassandra soak operation rate | The offered load was not delivered — window inconclusive before anything else is read |
| `loadgen_failure_invalidations_total` did not increase | Reconciliation inflight and invalidations | Sticky for the process lifetime. A `lease_abort` makes the interval INCONCLUSIVE |
| Consumer `num_pending` bounded and **not monotonically growing** | Consumer pending | A latency number taken while a consumer backs up measures a queue, not a service |
| loadgen stayed connected to NATS | NATS connection and lifecycle events | A generator-side disconnect makes the window inconclusive — read this before attributing any error spike |
| Mongo probe fresh, heartbeat not degraded | Loadgen Mongo and heartbeat validity | Control-plane blind interval |
| `unverified` below `max(3, 0.001 × eligible)` per observer | Reconciliation observations | Observer blind — absence claims not trustworthy |

---

## 7. Run protocol and what to record

1. Seed (`phase: seed`), confirm the encryption preflight in the log.
2. Start the soak. **Note the wall-clock time when warm-up ends** — the
   production counters carry no phase label, so the analysis window has to be
   pinned by hand. `loadgen_soak_rpc_latency_seconds` starts recording at that
   boundary, so its first sample is a usable marker.
3. Hold for **at least 30 minutes** at steady state. Longer is better for the
   ratios' sample size; this is not an endurance run.
4. Stop dispatch, then **wait out the settle window** before reading: the
   numerator of an async ratio lands after the sender stops. Denominator over the
   hold window; numerator to the hold window + the max SLO deadline + one scrape
   interval.
5. Record, with every number: `runId`, `siteId`, image digest, sampler ratio, the
   exact window, all six validity checks, and the five ratios.

**Write it up as a run-window SLI, not as the SLO:**

> SLO-1a run-window SLI = 99.94% at 100 msg/s over 30 min, isolated site
> `slo-test-a`, sampler 0.1, backlog flat, no invalidations.

Never *"SLO-1a = 99.94%"*. The SLO is a 28-day window over production traffic;
this is an achievability check at a chosen load.

---

## 8. What this run cannot answer

| | Why | Where it comes from instead |
|---|---|---|
| **SLO-1b, SLO-2** | The enqueue counter and age histogram do not exist | P2a / P2b ([`p2-instrumentation-spec.md`](p2-instrumentation-spec.md)). A short `run --preset=realistic --rate=100` gives SLO-2 a one-sided bound in the meantime |
| **SLO-3** | The soak connects with backend creds and never touches auth-service's HTTP leg | A separate short `max-rps --workload=login` |
| **SLO-6** | The notification counter is per message, not per recipient | P4 |
| **SLO-8** | No `status` on the duration histogram, and the soak has no client-side SLO-8 scoring | P4, or `max-rps --workload=search` for a client-side number |
| **SLO-9** | Single-site driver | A second site |
| **Anything about the ceiling** | This is a fixed-load run | Track 2 — the same ratios re-read at each ramp step |

Two caveats to carry into the write-up:

- **SLO-4's number will be optimistic.** The soak has no historical backfill, so
  `LoadHistory` reads hit only run-generated, shallow, dense data. Production
  walks aged buckets.
- **SLO-4/5 are measured under the soak's full mix** — room and member mutations,
  user reads, presence, search, read receipts all running concurrently. For an
  achievability check that is closer to production than a single-journey test, but
  say so, because it is not the same number a focused `max-rps --workload=history`
  would give.

---

## 9. Sibling documents

- [`slo-measurement-map.md`](slo-measurement-map.md) §7 — why this works, per SLO
- [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md) — what unlocks SLO-1b/2
- [`execution-priority-plan.md`](execution-priority-plan.md) — Track 1.0b
- [`loadgen/dashboard-contract.md`](loadgen/dashboard-contract.md) — the validity rules in §6
- `tools/loadgen/deploy/k8s/README.md` — the chart's own operational runbook
