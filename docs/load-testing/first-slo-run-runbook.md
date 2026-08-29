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
| **Produces** | Run-window SLIs for **SLO-4, 5, 7** (measured), an **approximate indicator** for **SLO-1a** (§5), plus the loadgen-vs-production latency gap for 4/5 |
| **loadgen code changes** | **None** — but the warm-up→measurement boundary needs a run protocol, not a code path; see §7 |
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
| P6 | **Decide how a run is scoped to a site, and verify it before the run** | infra | **There is no `site` label on any metric.** `pkg/obs` defines `SiteIDKey = "chat.site.id"` as a **baggage / span** attribute (`obs.go:37,44,265`) — it reaches traces, not the metric stream — and the Prometheus relabels add `instance` and `service` only. A query filtering `site="…"` returns nothing. Pick one of the two options below and confirm the label (or the isolation) exists **before** the run, not while reading an empty panel |
| P7 | **Workload-model inputs confirmed or the shape explicitly named** | product + infra | G2. See §2 — the defaults are not a neutral baseline |

### P6 — how to scope a run to a site

Two workable options. **Do not derive a metric label from request baggage** — it
is per-request data, and the repo's own semgrep cardinality rule blocks exactly
that class of label.

| Option | How | Cost |
|---|---|---|
| **A — static label at scrape time** | Add `site: <id>` as a static relabel on the service scrape job (one Prometheus, many sites) | One config line per site's job; queries then group `by (site)` as written |
| **B — one Prometheus per site** | Each site's runs land in their own tenant; drop the `site` selector from every query | No label work; needs the tenancy to exist |

Whichever you pick, the §5 queries change accordingly. They are written for
option A; under option B delete the `site` selector and the `by (site)` grouping.

---

## 2. loadgen needs no changes — but the defaults are a shape, not a baseline

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

**Two of those "defaults" are derived, not confirmed — and the derivation is
aggressive.** The chart's own startup log says so: `logSoakAssumptions`
(`soak_config.go:542-558`) emits `"provisional", true` alongside
`i12MessagesPerActiveUserPerDay` and `i12Derived`, because I12 was never
confirmed and the value is computed rather than given.

| Derived quantity | Value at the defaults | Why it matters |
|---|---|---|
| Messages per active user per day | `100 × 86400 ÷ 2000` = **4 320** | An active user sending a message every 20 seconds, all day. That is not a chat user; that is a bot |
| Hottest room's share of sends | **20.8%** (Zipf s=1.2, v=1 over 10 000 rooms) | ≈ **20.8 msg/s into a single room**, sustained |
| Top-10 rooms' share | **51.4%** | Half of all traffic into ten partitions |
| Top-100 rooms' share | **75.1%** | |

So the defaults are a **deliberate stress shape**, not a production baseline —
they already run the hot-partition scenario (`extreme-scenarios.md` X6b) by
accident. A number measured under them is still useful, but it must not be
reported as "the system meets its SLOs at expected load".

**Therefore: G2 moves ahead of this run**, and the presets split three ways.

| Preset | Purpose | What changes |
|---|---|---|
| `realistic` | The number that answers "do we meet the SLOs at expected load" | `activeUsers` raised until messages/user/day is defensible; a flatter room distribution |
| `hot-room` | The X6b hot-partition probe, deliberately | Today's Zipf, stated as the intent |
| `stress` | Ramp fodder for Track 2 | Rate raised, shape held |

Until G2 lands, run the first pass on the current defaults **and label the result
`shape=default-zipf, i12=derived`** so nobody later mistakes it for a baseline.
Do not silently retune the rate — that changes what the number means without
saying so.

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
| SLO-4 run-window SLI | §5 query 2 | against 95% within 500 ms |
| SLO-5 run-window SLI | §5 query 3 | **see the bound warning below** — this checkout's spec and #337's differ |
| SLO-7 run-window SLI | §5 query 4 | against 99.5% |
| SLO-1a approximate indicator | §5 query 1 | **label the panel `approximate · lag-enforced · non-gating`** — it can read over 100% (§5) |
| SLO-4/5 measurement gap | §5 query 5 | loadgen-side minus server-side p95 — the size of the blind spot |

---

## 5. The measurement method and the queries

**Do not compute these from a rolling range on a Grafana panel.** A moving
`increase(...[$window])` cannot express what §7 requires — a denominator over the
send window and an async numerator carried out to the settle boundary — and it
silently slides warm-up traffic in and measured traffic out as the dashboard
refreshes.

**Take counter snapshots at three marked instants and subtract.**

**Synchronous and asynchronous SLIs need different boundaries — and mixing them
is the mistake that inflates a ratio past 100%.**

| Mark | When | Snapshot | Serves |
|---|---|---|---|
| **t0** | Measurement starts — see §7 for how to get a clean one | **All** counters: the baseline every later value is measured from | everything |
| **t1** | Dispatch stopped, **plus** the few seconds for in-flight RPCs to return | `rpc_server_call_duration_seconds` **`_bucket` and `_count` together**, and `search_service_requests_total` (both sides) | SLO-4, 5, 7 |
| **t2** | message-worker's `num_pending + num_ack_pending` back at its pre-run floor, **capped** — see below | `message_worker_persistence_total` **and** `message_gatekeeper_messages_total` | SLO-1a |

**Why SLO-4/5/7 take both sides at `t1`.** Their numerator and denominator come
from the *same* observation: `_bucket{le}` and `_count` on one histogram
increment together when the handler returns. Freezing `_count` at `t1` while
letting `_bucket` run to `t2` counts every RPC that finished in between in the
numerator only — the ratio rises, and can exceed 100%. These are synchronous
SLIs measured inside one process; they have no settle window at all. The only
wait they need is for in-flight requests to return, which is seconds.

**Why SLO-1a's boundary is a drained backlog, not a deadline.** SLO-1a is an
availability ratio — persisted / published — and the spec gives it **no latency
deadline**. Borrowing SLO-4's 500 ms would count as persistence failures every
message still sitting in the JetStream backlog, in `jsretry` backoff, or being
recovered, all of which are messages the system has not failed to persist.

So `t2` is *"message-worker has caught up"*, with a cap:

- **Cap the wait at 15 minutes.** That is past `jsretry.DefaultBackoff`'s
  ~12.6-minute client-side budget against `MaxDeliver=6`, so anything still
  pending after it is not going to be retried into existence.
- **If the backlog has not drained by the cap, SLO-1a is INCONCLUSIVE for this
  window** — and the run has already failed the §6 validity gate, because a
  backlog that does not drain is not a flat backlog. Do not snapshot anyway at an
  arbitrary instant to get a number.

Panels are for watching the run; the recorded number comes from the snapshots.

```promql
# Snapshot form — evaluate each at the marked instant (Prometheus `time=` /
# `@` modifier / an instant query at the recorded timestamp), then subtract.

# 1 — SLO-1a  (BOTH sides at t2 — the backlog-drained mark — minus their t0 value)
sum(message_worker_persistence_total{
      site="$site", message_kind=~"user|thread_reply", result="success"})   # @t2 − @t0
sum(message_gatekeeper_messages_total{site="$site", result="accepted"})      # @t2 − @t0

# 2 — SLO-4: channel load within 500 ms / eligible
sum(rpc_server_call_duration_seconds_bucket{
      site="$site", service_name="history-service",
      rpc_method="channel_history", error_type="", le="0.5"})                # @t1 − @t0
sum(rpc_server_call_duration_seconds_count{
      site="$site", service_name="history-service", rpc_method="channel_history",
      error_type=~"|internal|unavailable|too_many_requests"})                # @t1 − @t0
      # both sides at t1: same histogram, same observation instant

# 3 — SLO-5: same shape, rpc_method="thread_open".
#     le= and the target depend on which spec this checkout carries — see the
#     bound warning below before scoring. Do not hardcode 0.25 against a 300 ms spec.

# 4 — SLO-7: search ok / eligible   (synchronous — both sides @t1 − @t0)
sum(search_service_requests_total{site="$site", status="ok"})
sum(search_service_requests_total{site="$site",
      status=~"ok|internal|unavailable|too_many_requests"})

# 5 — the measurement gap for SLO-4/5 (a rolling quantile is fine here:
#     it is a diagnostic comparison, not a scored ratio)
histogram_quantile(0.95, sum by (le) (rate(loadgen_soak_rpc_latency_seconds_bucket{
      action="msg_history"}[$window])))
- histogram_quantile(0.95, sum by (le) (rate(rpc_server_call_duration_seconds_bucket{
      service_name="history-service", rpc_method="channel_history"}[$window])))
```

Drop the `site` selector under P6 option B.

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

### SLO-5's bound depends on whether #337 merged — check before scoring

`common/sli-slo.md` on **this checkout** says SLO-5 is *99% within 300 ms*. #337
changes it to *95% within 250 ms*, and that change has **not merged**. Score
against whichever spec the checkout actually carries, and note that the two are
not interchangeable:

| If the checkout says | Score | Why |
|---|---|---|
| **95% / 250 ms** (post-#337) | `le="0.25"`, target 0.95 | 250 ms is a real bucket boundary — an exact read |
| **99% / 300 ms** (this branch today) | **Cannot be computed exactly** | 300 ms falls between the `0.25` and `0.5` boundaries. Report both readings as a bracket — `le="0.25"` understates the good share, `le="0.5"` overstates it — and mark SLO-5 **INCONCLUSIVE for a verdict**. The gap between the two is precisely where the tail sits, which is why #337 moved the bound |

**Action:** rebase this branch after #337 merges, or carry the SLO change here
explicitly with its own approval. Do not score 250 ms against a document that
says 300 ms — the same checkout would then produce a verdict its own spec
contradicts.

### SLO-1a is an approximate indicator, not a hard number

Both sides count **attempts**, and they do not fail the same way. The gatekeeper
records `accepted` once, on success. message-worker records `success` once per
delivery attempt, so a redelivery after a partial batch write increments the
numerator twice for one logical message. Under a worker crash, an Ack failure or
a recovery replay the ratio can therefore **exceed 100%**, or split its numerator
and denominator across two windows.

Report it as **`SLO-1a approximate (lag-enforced) indicator`**, paired with the
consumer-lag panel, and read a value near or above 100% as evidence of redelivery
rather than as a good result. A hard gate on SLO-1a needs one of: logical-outcome
dedup (the P7 outcome ledger), max-delivery advisories, or loadgen's own
authoritative read-back of run-owned message IDs — which the soak ledger already
does per message and which is the cheapest of the three to reach for.

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

### There is no drain barrier at the end of warm-up — pick how to get `t0`

The obvious protocol — "let warm-up finish, wait for the backlog to drain, then
start measuring" — **is not executable as the workload is built**. At the warm-up
deadline the soak only flips a boolean: `measured := !w.now().Before(warmupDeadline)`
(`soak_workload.go:405`), evaluated per dispatch. Every lane keeps dispatching at
full rate. Nothing pauses, so nothing drains, and in a continuous Deployment there
is no phase boundary to wait at.

Two ways to get a usable `t0` without changing loadgen:

**Option A — pre-warm, stop, restart with `warmup: 0`** *(recommended)*

1. Run normally until steady state.
2. Scale the Deployment to 0. `continuous` mode stops gracefully on SIGTERM, and
   **a replacement process resumes the same run** — the `runId` owns the topology,
   the PVC owns the ledger. This gap is the drain.
3. Watch the backlog fall to its pre-run floor. Keep the gap **shorter than
   `soak.heartbeatStaleAfter`**, and do not run teardown during it.
4. Scale back to 1 with `soak.warmup: 0`. Mark `t0` at the first dispatch.

One caveat that is not a blocker: the recent-message catalog is in-memory, so
after the restart the mutation, thread and verification lanes idle until new
messages age past `soak.persistGrace`. The **send and read lanes carry SLO-1a/4/5/7
and are unaffected** — but discard the first two minutes if you want the full mix
represented.

**Option B — one continuous run, bound the contamination instead of removing it**

Mark `t0` at the warm-up deadline and *quantify* the bias rather than eliminating
it. The only messages that can pollute the measured numerator are those admitted
before `t0` that persist after it, and that population is bounded by the in-flight
depth at `t0` — read `loadgen_failure_inflight` plus the consumers' `num_pending`
at that instant. If it is **under 0.1% of the measured denominator**, the bias sits
below the SLO's own stated precision; record the number next to the ratio and move
on. If it is larger, use Option A.

Either way the bias runs **upward** — a warm-up message persisting after `t0`
lands in the numerator with no matching denominator — which is the direction that
makes a bad result look acceptable. That is why it has to be bounded, not ignored.

### The sequence

1. Seed (`phase: seed`), confirm the encryption preflight in the log.
2. Reach steady state, then establish `t0` by Option A or B above. Snapshot **all**
   counters at `t0`.
3. Hold for **at least 30 minutes** at steady state. Longer is better for sample
   size; this is not an endurance run.
4. Stop dispatch. Wait a few seconds for in-flight RPCs to return, then **mark
   `t1`** and snapshot the synchronous families (SLO-4/5/7, both sides).
5. Watch message-worker's `num_pending + num_ack_pending` fall to its pre-run
   floor. **Mark `t2`** there and snapshot SLO-1a's two counters. If it has not
   drained within **15 minutes**, stop — SLO-1a is INCONCLUSIVE for this window
   and the validity gate has already failed.
6. Compute every ratio from the snapshot differences (§5).
7. Record, with every number: `runId`, `siteId`, image digest, sampler ratio,
   which `t0` option was used (and the contamination bound if Option B),
   `t0/t1/t2`, the workload shape label from §2, all six validity checks, and
   the ratios.

**Write it up as a run-window SLI, not as the SLO:**

> SLO-4 run-window SLI = 96.2% within 500 ms, at 100 msg/s over 30 min
> (t0 14:32:10 → t1 15:02:10 → t2 15:03:15), isolated site `slo-test-a`,
> shape `default-zipf / i12=derived`, sampler 0.1, backlog flat, no
> invalidations.
> SLO-1a approximate indicator = 99.94% (per-attempt, see §5).

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
