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
| **Produces** | The **achieved distribution** for **SLO-4, 5, 7** and an approximate indicator for **SLO-1a** — the evidence a target gets chosen *from*, not a verdict against one. Plus the loadgen-vs-production latency gap for 4/5 |
| **Stance** | **This run is calibration-only and gates nothing.** `common/sli-slo.md` remains the acceptance contract for any gating run until Track 1.3 amends it — §4a |
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
| P4 | **A backlog observer that outlives the loadgen pod** | infra | **Not optional, and not satisfiable by loadgen.** `loadgen_consumer_*` comes from a `ConsumerSampler` running *inside* the loadgen binary (`consumerlag.go:17`), so `phase: stopped` deletes the Deployment and the backlog signal with it — exactly when §7 tells you to watch the backlog drain. Both `t0-async` and `t2` need one of: the **P3 JetStream exporter deployed**, or an **operator reading `ConsumerInfo` directly** (NATS monitoring endpoint / `nats consumer info`) at those two marks. The service-side counters are unaffected — they are scraped from the services, which stay up |
| P5 | Cassandra keyspace, `MESSAGE_BUCKET_HOURS` matching every reader/writer, Vault/KMS up for the encryption preflight | infra | The soak's own pre-run gate (`soak/cassandra-soak-plan.md` §6.1) |
| P6 | **Decide how a run is scoped to a site, and verify it before the run** | infra | **There is no `site` label on any metric.** `pkg/obs` defines `SiteIDKey = "chat.site.id"` as a **baggage / span** attribute (`obs.go:37,44,265`) — it reaches traces, not the metric stream — and the Prometheus relabels add `instance` and `service` only. A query filtering `site="…"` returns nothing. Pick one of the two options below and confirm the label (or the isolation) exists **before** the run, not while reading an empty panel |
| P7 | **Workload-model inputs confirmed or the shape explicitly named** | product + infra | G2. See §2 — the defaults are not a neutral baseline |
| P8 | **Read the live `MaxDeliver`, `AckWait`, the consumer `BackOff`, and the scrape interval** | infra | They set `t2`'s cap (§5) and how long every mark waits. All are environment overrides. Two cases to resolve *before* the run, not during it: `MaxDeliver` of `0`/`-1` is unlimited, so no finite cap exists and the limit becomes policy; and `BACKOFF_STEPS=0` yields an empty consumer schedule, in which case that path costs one `AckWait` per delivery |

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

New row, five panels. Note what the latency panels show: **a curve across every
candidate bound, not a single ratio against a target.**

| Panel | Query | Shows |
|---|---|---|
| J2 channel-load good-ratio curve | §5 query 2 | good-ratio at **every** bucket boundary — the input to choosing SLO-4's bound and target |
| J2 thread-open good-ratio curve | §5 query 3 | same, for SLO-5 |
| J4 search availability | §5 query 4 | ok / eligible — a ratio, no bound to choose |
| SLO-1a approximate indicator | §5 query 1 | **label the panel `approximate · lag-enforced · non-gating`** — it can read over 100% (§5) |
| Measurement gap, J2 | §5 query 5 | loadgen-side minus server-side p95 — the size of the blind spot |

Draw a **reference line** at whatever `sli-slo.md` currently drafts, so the
distance between draft and achieved is visible. Do not paint the panel red when
the draft is missed — the draft is what this run exists to revise.

---

## 4a. This run is calibration-only — the contract stays where it is

Two things that are easy to conflate, and must not be:

| | |
|---|---|
| **`common/sli-slo.md` is the acceptance contract.** §10 is explicit: a hard gate "fails if it can't meet the **actual SLO predicate and target** from this document" | It stays binding for every **gating** run, unchanged, until Track 1.3 approves an amendment |
| **This run does not gate.** §0.2 asks for achievable-first values, 4–6 weeks of observation, then adjust and seek approval — and nothing has been through that | So 1.0b measures the distribution and **produces no verdict**. It cannot fail, and it cannot ratify |

The programme therefore has an explicit ordering, and this runbook only covers
the first row:

| Track | What it does | Gates? |
|---|---|---|
| **1.0b** (this run) | Calibration. Outputs the achieved distribution across candidate bounds | **No** |
| **1.2** | Observational window on real traffic | No |
| **1.3** | Choose bounds and targets from 1.0b + 1.2, get approval, **and update `common/sli-slo.md`** | — |
| **1.4 / 1.5** | Assertion mode and the re-run, scored against the **approved, updated** criteria | **Yes** |

**SLO-5's divergence is an open item for 1.3, not a licence to ignore either
number.** This checkout says 99% / 300 ms; #337 proposes 95% / 250 ms. Neither is
"the draft you may pick" — the source of truth says 300 ms today, and 1.3 must
either keep it or change it *in the document*. What 1.0b contributes is the
evidence for that decision, plus one hard fact: **300 ms is not computable**, so
whichever way the target goes, the bound has to move to a real boundary. Record
which text the checkout carried so a later reader knows what the reference line
meant.

Report the good-ratio at every bucket boundary, let 1.3 choose, and record why.

Three constraints on that choice, all learned the hard way here:

1. **A bound must land on a real bucket boundary.**
   `o11y.DefaultLatencyBuckets()` is `{.005 .01 .025 .05 .1 .25 .5 1 2.5 5 10}`.
   A bound between two of them cannot be computed at all — 300 ms can only be read
   as 250 ms (understating the good share) or 500 ms (overstating it), and the gap
   between those readings is exactly where the tail sits. **This constraint belongs
   in `sli-slo.md`'s calibration section**, because it rules out most round numbers
   before anyone argues about them.
2. **A load test sets a floor, not a commitment.** It shows what is achievable
   under a chosen shape on chosen hardware. What users actually experience needs
   the observational window (Track 1.2) — and for J2 a caller-visible measurement,
   since the server-side histogram stops timing at `Respond`. Let the load test
   veto a target that is not even reachable; let observation decide where inside
   the reachable range to sit.
3. **Bound and target move together.** A tighter bound with a looser target
   degrades honestly; a bound nobody can compute does not degrade, it reports
   nothing. #337's 300/99% → 250/95% is that trade made once, and the shape of the
   answer is reasonable even if the numbers change again.

**Consequence for #337:** the SLO-5 edit inside it is a draft revision, not an
approved SLO change, and it need not block this run either way. What the run must
record is **which draft the checkout carried**, so a later reader knows what the
reference line meant.

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

**Every mark waits out a full scrape interval.** An instant query at the barrier
can still read the sample taken *before* it. The soak's RPC timeout is 5 s and the
scrape interval is 5 s locally (longer on staging — check yours), so a mark taken
"a few seconds after dispatch stops" misses the slowest, last-finishing RPCs and
biases the J2 distribution **good**; a `t2` taken the instant the backlog hits
floor can read a persistence counter that has not been scraped yet and biases
SLO-1a **bad**.

| Mark | When | Snapshot | Serves |
|---|---|---|---|
| **t0-async** | Baseline for SLO-1a. Taken while dispatch is stopped and the backlog is at floor — **and scraped before dispatch resumes** (§7) | `message_worker_persistence_total`, `message_gatekeeper_messages_total` | SLO-1a |
| **t0-sync** | Baseline for the synchronous families. Taken once the **full lane mix is running** — specifically once the thread-read lane is producing samples again (§7) | `rpc_server_call_duration_seconds` (both series), `search_service_requests_total` | SLO-4, 5, 7 |
| **t1** | Dispatch stopped → wait until in-flight RPCs reach zero, **or** at least one full `soak` RPC timeout → **then wait one full scrape interval** | the same synchronous families | SLO-4, 5, 7 |
| **t2** | message-worker's `num_pending + num_ack_pending` at its pre-run floor **and stable there for at least one scrape interval**, capped — see below | the same two counters as `t0-async` | SLO-1a |

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

So `t2` is *"message-worker has caught up"*, with a cap.

**There is no theoretical safe upper bound here, and three revisions of this
document pretending otherwise is the finding.** Each attempt patched the formula
— max of the two totals, then per-delivery max — and each was still short,
because the retry *delays* are only part of the interval between deliveries. The
handler runs first: `jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, h.processMessage(...))`
(`message-worker/handler.go:89`) evaluates `processMessage` — Mongo reads,
Cassandra writes, a thread-count scan — *before* `Settle` is called, so the
application backoff starts only once that returns. Nothing bounds that execution
time per attempt, so nothing bounds the sum.

**So treat the number below as an operational cap, chosen and recorded, not
derived and guaranteed.** Exceeding it makes SLO-1a `INCONCLUSIVE` **by rule**,
which is the same stance the unlimited-`MaxDeliver` case already takes.

Compute the delay floor, then add margin for execution:

```
app[k]      = jsretry.DefaultBackoff[min(k, len(app)-1)]
consumer[k] = BackOff[min(k, len(BackOff)-1)]   if BackOff is non-empty
            = AckWait                            if BackOff is empty (steps <= 0)

delay_floor = Σ over N in [1 .. MaxDeliver-1] of max(app[N-1], consumer[N-1])
cap         = delay_floor + (MaxDeliver-1) x AckWait + one scrape interval
```

The `(MaxDeliver-1) × AckWait` term is the execution margin: a handler that runs
past `AckWait` has already triggered a server redelivery, so `AckWait` is the
longest execution that can still precede an application-scheduled nak.

Two paths, mixing within one message's life — delivery 1 can be Nak'd on the
application schedule while delivery 2 expires by `AckWait` on the consumer
schedule, which is why the max is taken **per delivery** rather than over the
totals:

| Path | When it applies | Delays from |
|---|---|---|
| **Handler error** (the normal one) | `processMessage` returns a transient error → `jsretry.Settle` → `NakWithDelay` | `jsretry.DefaultBackoff` — compiled into the binary, 1s/5s/30s/2m/10m, last entry repeating |
| **Un-acked / settle failure** | The pod died, hung, or the Nak itself failed, so the message expires by `AckWait` | The **consumer's** `BackOff`, derived as `AckWait × Factor^i` capped at `BackOffMax` (`pkg/stream/consumer.go:22-27,58`) |

These are distinct JetStream mechanisms — an acknowledgement-timeout `BackOff`
and a delayed NAK — so neither dominates the other at every step.

At the shipped defaults (app `1, 5, 30, 120, 600`; consumer `30, 60, 120, 240,
480`; `AckWait` 30 s):

| `MaxDeliver` | delay floor | execution margin | **operational cap** |
|---:|---:|---:|---:|
| 6 | 1 050 s (17.5 min) | 150 s | **≈ 20 min** |
| 10 | 3 450 s (57.5 min) | 270 s | **≈ 62 min** |

`BACKOFF_STEPS=0` makes `backOffSchedule()` return `nil`
(`pkg/stream/consumer.go:75-77`) — that switches the consumer *schedule* off, not
the *path*: redelivery falls back to plain `AckWait`, so `consumer[k] = AckWait`
for every `k`. **`MaxDeliver` of `0` or `-1` is unlimited** (the server normalizes
anything `< -1` to `-1`), so no arithmetic applies at all and the cap is purely a
policy number.

**If the backlog has not drained by the cap, SLO-1a is INCONCLUSIVE for this
window** — and the run has already failed the §6 validity gate, because a backlog
that does not drain is not a flat backlog. Do not snapshot anyway at an arbitrary
instant to get a number.

Panels are for watching the run; the recorded number comes from the snapshots.

```promql
# Snapshot form — evaluate each at the marked instant (Prometheus `time=` /
# `@` modifier / an instant query at the recorded timestamp), then subtract.

# 1 — SLO-1a  (both sides: @t2 − @t0-async)
sum(message_worker_persistence_total{
      site="$site", message_kind=~"user|thread_reply", result="success"})   # @t2 − @t0-async
sum(message_gatekeeper_messages_total{site="$site", result="accepted"})      # @t2 − @t0-async

# 2 — J2 channel load: good-ratio at EVERY bucket boundary.
#     Keep `le` as a series dimension instead of pinning one value — the curve is
#     the deliverable (§4a), and a single le would bake in a draft bound.
sum by (le) (rpc_server_call_duration_seconds_bucket{
      site="$site", service_name="history-service",
      rpc_method="channel_history", error_type=""})                          # @t1 − @t0-sync
sum(rpc_server_call_duration_seconds_count{
      site="$site", service_name="history-service", rpc_method="channel_history",
      error_type=~"|internal|unavailable|too_many_requests"})                # @t1 − @t0-sync
      # both sides at t1: same histogram, same observation instant

# 3 — J2 thread open: same shape, rpc_method="thread_open".

# 4 — SLO-7: search ok / eligible   (synchronous — both sides @t1 − @t0-sync)
sum(search_service_requests_total{site="$site", status="ok"})
sum(search_service_requests_total{site="$site",
      status=~"ok|internal|unavailable|too_many_requests"})

# 5 — the measurement gap for SLO-4/5 (a rolling quantile is fine here:
#     it is a diagnostic comparison, not a scored ratio)
histogram_quantile(0.95, sum by (le) (rate(loadgen_soak_rpc_latency_seconds_bucket{
      action="load_history"}[$window])))    # the action is load_history, not msg_history
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

### There is no drain barrier at the end of warm-up

The obvious protocol — "let warm-up finish, wait for the backlog to drain, then
start measuring" — **is not executable as the workload is built**. At the warm-up
deadline the soak only flips a boolean: `measured := !w.now().Before(warmupDeadline)`
(`soak_workload.go:405`), evaluated per dispatch. Every lane keeps dispatching at
full rate. Nothing pauses, so nothing drains, and in a continuous Deployment there
is no phase boundary to wait at.

**Use the chart's own lifecycle, not `kubectl scale`.** The Deployment template is
rendered only when `.Values.phase == "soak"` and hardcodes `replicas: 1`
(`soak-deployment.yaml:1,12`). Scaling to zero by hand fights the chart, and under
Argo CD self-heal will simply put it back. The sanctioned pause is
`phase: soak → phase: stopped`, then back. If your GitOps controller prunes on
`stopped`, note that the PVC and the Mongo manifest survive — the `runId` owns the
topology and a replacement process resumes the same run.

**The restart empties the recent-message catalog, and that is not cosmetic.**
`TestSoakWorkload_RestartBeginsWithEmptyCatalog` pins it — *"manifest lifecycle
never checkpoints recent messages"* — and `soak_read.go:221-228` **skips
`thread_open` entirely** when the catalog has no eligible entry. So immediately
after the restart there is **no SLO-5 traffic at all**, not merely less of it, and
the mutation, thread and verification lanes are absent from the contention mix.
An earlier revision of this runbook claimed the read lanes were unaffected. They
are not.

That is why the baselines are split:

1. **Reach steady state**, then `phase: stopped`.
2. **Watch the backlog fall to its pre-run floor — using the out-of-band
   observer from P4, not `loadgen_consumer_*`.** The sampler died with the pod in
   step 1. Hold at floor for at least one scrape interval, then take
   **`t0-async`** — the SLO-1a baseline — *while dispatch is still stopped*.
   Taking it at first dispatch races the scrape. Keep the pause shorter than
   `soak.heartbeatStaleAfter`, and do not run teardown during it.
3. **`phase: soak` with `soak.warmup: 0`.** Dispatch resumes immediately.
4. **Wait for the full mix.** Watch

   ```promql
   rate(loadgen_soak_rpc_latency_seconds_count{
         action="get_thread_messages", pod="<the NEW pod>"}[1m])
   ```

   climb back to its expected rate — that is the catalog having refilled past
   `soak.persistGrace`. Three things this query has to get right, each of which an
   earlier revision got wrong:

   - **Query the `_count` series, not the bare metric name.** A Prometheus
     histogram exposes only `_count`, `_sum` and `_bucket`; a selector on the base
     name returns nothing, and `t0-sync` would never be markable.
   - **Pin the new pod, and wait out the window.** A bare `[1m]` immediately after
     the restart still covers the minute *before* it. Depending on whether the
     `instance`/`pod` label changed, you either sum the old and new series
     together or read one series across a counter reset — `rate()` compensates for
     resets rather than isolating processes, so both readings can sit near the
     expected rate while the new catalog is still empty, and `t0-sync` gets marked
     early. Bind the selector to the new pod, **and** wait a full range window past
     the restart so no pre-restart sample remains, **then** require two consecutive
     confirming scrapes.
   - **Take a post-restart baseline** for the same counter, and judge readiness on
     the fresh delta rather than on an absolute rate that could be carrying
     history.

   **"Expected rate" is derived, not configured.** The read lane rolls its action
   from a fixed mix — 75% history, **15% thread**, 10% point lookup
   (`soak_read.go:22-32`) — so the target is `SOAK_READ_RATE × 0.15`, i.e. **105/s**
   at the default 700. Treat it as ready when the rate is within ~10% of that and
   stable over two consecutive scrapes; it is a sampled share, so it will not sit
   exactly on the number.

   Then take **`t0-sync`**, the baseline for SLO-4/5/7.
   Everything between step 3 and here is excluded from the synchronous
   measurement by construction, which is what "discard the first two minutes"
   should have meant.

   Two label facts worth pinning, because an earlier revision got both wrong:
   the action is **`get_thread_messages`** (`soak_rpc.go:24`), not `msg_thread`;
   and `loadgen_soak_lane_attempts_total` is **not** an alternative — it is
   incremented only by the room/member lanes (`soak_roommember.go:843`), never by
   a read lane. The latency histogram works as the signal precisely because a
   skipped `thread_open` records no sample at all (`soak_read.go:221-228` returns
   `skip`, and the collector only observes when `Latency > 0`), so the series
   goes from absent to at-rate exactly when the catalog refills.

### If the restart is impossible, SLO-1a is INCONCLUSIVE — there is no fallback

An earlier revision offered "keep one continuous run and bound the contamination
at 0.1% of the denominator". **That threshold is exactly the error budget of a
99.9% target**, so a bias inside it can move 99.8% to 99.9% on its own. The bound
was also not an upper bound: it used `loadgen_failure_inflight` plus
`num_pending`, omitting `num_ack_pending`, and `loadgen_failure_inflight` counts
operations awaiting *observers*, which is not the broker's redelivery population —
a message whose observer accounting finished but whose Ack failed can still
increment the attempt-based numerator again.

**The interval fallback does not work either, for the same reason one level
deeper.** `num_pending + num_ack_pending` counts **messages**; the numerator
counts **delivery attempts**. A single ack-pending message at `t0` can redeliver
up to `MaxDeliver − NumDelivered` more times and increment `success` on each, so
the message count is not an upper bound on the attempt-count bias. Turning it
into a *ratio* bias would also require normalising by the measurement
denominator, which the earlier text did not do.

A defensible bound would have to sum the **remaining delivery budget per pending
message** and divide by the denominator — which needs per-message delivery counts
nothing exports today. So: **if the restart is impossible, SLO-1a is
`INCONCLUSIVE` for that run.** Report the other three and say why the fourth is
missing. Do not publish a number, an interval, or a caveat-laden estimate.

### The sequence

1. Seed (`phase: seed`), confirm the encryption preflight in the log.
2. Preflight the two run-shaping facts: the **live consumer config**
   (`MaxDeliver`, `AckWait`, backoff) to derive `t2`'s cap, and the **scrape
   interval** every mark below waits on.
3. Establish `t0-async` and `t0-sync` per the restart procedure above.
4. Hold for **at least 30 minutes** of steady state measured from `t0-sync`.
   Longer is better for sample size; this is not an endurance run.
5. Stop dispatch. Wait until in-flight RPCs reach zero (or one full RPC timeout),
   **then one full scrape interval**, then mark **`t1`** and snapshot the
   synchronous families — both sides together.
6. Watch message-worker's `num_pending + num_ack_pending` reach its pre-run floor
   and **stay there for one scrape interval**. Mark **`t2`** and snapshot
   SLO-1a's two counters. If the floor is not reached within the **operational
   cap recorded in preflight** (§5), stop — SLO-1a is INCONCLUSIVE by rule and
   the validity gate has already failed.
7. Compute every ratio from the snapshot differences (§5).
8. Record, with every number: `runId`, `siteId`, image digest, sampler ratio,
   the live `MaxDeliver`/backoff and the derived cap, the scrape interval,
   `t0-async / t0-sync / t1 / t2`, the workload shape label from §2, all six
   validity checks, and the ratios.

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
