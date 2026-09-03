# First SLO Run — Runbook

> The first item in the plan: **#337 merged, then a soak run that produces real
> numbers for the SLOs that are measurable.** This is the operator's page — what
> to set, what to add, what to read, and what the result may and may not be
> called.
>
> Why it works and which SLOs it covers:
> [`measurement-map.md`](measurement-map.md) §7.

| | |
|---|---|
| **Produces** | Six artefacts, listed in **§7a** — achieved good-ratio *curves* for **SLO-4/5**, a ratio for **SLO-7**, an approximate indicator for **SLO-1a**, one-sided bounds for **1b/2**, the loadgen-vs-production latency gap, and the verified measurement apparatus itself. Evidence a target gets chosen *from*, not a verdict against one |
| **How many runs** | **3–6 shakedown runs** (instrument only, nothing reported) then **3 identical measurement runs**, median + spread. Which knobs may move between them, and which may not: **§7b** |
| **Stance** | **This run is calibration-only and gates nothing.** `common/sli-slo.md` remains the acceptance contract for any gating run until Track 1.3 amends it — §4a |
| **loadgen code changes** | **None** — but the warm-up→measurement boundary needs a run protocol, not a code path; see §7 |
| **Dashboard changes** | **Yes** — the existing dashboard has 22 panels and not one SLO ratio |
| **Does not produce** | SLO-1b, 2, 3, 6, 8, 9 — see §8 |

---

## Contents

**Before the run** — §1 preconditions · §1a what a busy site changes ·
§2 the defaults are a shape, not a baseline · §3 the values overlay ·
§4 the dashboard · §4a why this run gates nothing

**Doing the run** — §5 the measurement method and the queries ·
§6 the validity gate · §7 the run protocol · §7b which knobs may move between runs

**After the run** — §7a what it produces · §8 what it cannot answer

**Sequencing** — §9 order of operations and the dry-run ·
§9a **what changes once the P2 metrics land** · §10 siblings

> New here? Read **§9 first** — it says what to do in what order, and everything
> else is detail hung off it.

---

## 1. Preconditions

> **Reading the identifiers.** Three prefixes appear across these documents and
> they are not the same kind of thing:
>
> | Prefix | Means | Lives in |
> |---|---|---|
> | **PRE-n** | A precondition for *this run* — an operator step, checked off before the run starts | this document, §1 |
> | **Gn** | A **gate** — external work (infra, product, app) that blocks one or more items in the programme, with a named owner | [`execution-priority-plan.md`](../execution-priority-plan.md) §"Gate backlog" |
> | **P1 / P2 / P3** | An **instrumentation priority tier** — how urgent a piece of missing telemetry is | [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md), [`measurement-map.md`](measurement-map.md) |
>
> The ones this run actually waits on: **G1** (isolated site — same thing as
> PRE-3), **G2** (workload shape — same thing as PRE-7), **G6** (backlog
> observer — the enabler PRE-4 needs).
>
> **Watch for the collision.** `PRE-n` and `P<n>` are different things and both
> appear in this document: the `P4` in §8 and the `P7` in §5 are
> **instrumentation tiers**, not preconditions. An earlier revision of this file
> renamed some of them by mistake.

| # | Precondition | Owner | Note |
|---|---|---|---|
| ~~PRE-1~~ | ~~#337 merged~~ — **done, 2026-08-30 (`bf0ea62`)** | app | `main` carries `rpc_server_call_duration_seconds{rpc_method, error_type}`, with `channel_history` and `thread_open` as disjoint methods |
| PRE-2 | **Prometheus scrapes the *services*' o11y SDK endpoint**, not just loadgen's `:9099` | infra | This is the one that gets missed. The Helm chart annotates **loadgen's own** service only (`metrics.serviceAnnotations`, port 9099). Every domain counter this run reads — `message_gatekeeper_messages_total`, `message_worker_persistence_total`, `rpc_server_call_duration_seconds`, `search_service_requests_total` — is emitted through the SDK meter on the services' own endpoint (`:2112` in compose; confirm the port on your service manifests). No service scrape, no SLO numbers |
| PRE-3 | **A dedicated test `SITE_ID`** — this is **G1** in the plan's gate backlog, restated here as an operator step | infra | Counters are monotonic and shared, so a snapshot difference excludes *historical* traffic but not *concurrent* traffic. See §1a: on a site that already carries bot and developer traffic this is what decides whether the numbers are the run's |
| PRE-4 | **A backlog observer that outlives the loadgen pod** | infra | **Not optional, and not satisfiable by loadgen.** `loadgen_consumer_*` comes from a `ConsumerSampler` running *inside* the loadgen binary (`consumerlag.go:17`), so `phase: stopped` deletes the Deployment and the backlog signal with it — exactly when §7 tells you to watch the backlog drain. Both `t0-async` and `t2` need one of: the **JetStream exporter (G6) deployed**, or an **operator reading `ConsumerInfo` directly** (NATS monitoring endpoint / `nats consumer info`) at those two marks. The service-side counters are unaffected — they are scraped from the services, which stay up |
| PRE-5 | Cassandra keyspace, `MESSAGE_BUCKET_HOURS` matching every reader/writer, Vault/KMS up for the encryption preflight | infra | The soak's own pre-run gate (`soak/cassandra-soak-plan.md` §6.1) |
| PRE-6 | **Decide how a run is scoped to a site, and verify it before the run** | infra | **There is no `site` label on any metric.** `pkg/obs` defines `SiteIDKey = "chat.site.id"` as a **baggage / span** attribute (`obs.go:37,44,265`) — it reaches traces, not the metric stream — and the Prometheus relabels add `instance` and `service` only. A query filtering `site="…"` returns nothing. Pick one of the two options below and confirm the label (or the isolation) exists **before** the run, not while reading an empty panel |
| PRE-7 | **Workload-model inputs confirmed or the shape explicitly named** | product + infra | G2. See §2 — the defaults are not a neutral baseline |
| PRE-8 | **Read the live `MaxDeliver`, `AckWait`, the consumer `BackOff`, and the scrape interval** | infra | They set `t2`'s cap (§5) and how long every mark waits. **Read them from the live consumer, never from the table in §5** — `message-worker` and `broadcast-worker` both opt into `stream.WithOutageRetryBudget`, which raises `MaxDeliver` well above the repo default (§5). Two cases to resolve *before* the run, not during it: `MaxDeliver` of `0`/`-1` is unlimited, so no finite cap exists and the limit becomes policy; and `CONSUMER_BACKOFF_STEPS=0` yields an empty consumer schedule, in which case that path costs one `AckWait` per delivery |
| PRE-9 | **Query dry-run: every query in §5 returns a non-empty result of the expected shape, against the real Prometheus** | us | **Do this before building the dashboard, not after.** Four review rounds on this document found label names, histogram suffixes, range-window semantics and `rate()`'s reset behaviour wrong — none of which is visible in the Go source, and all of which a single dry-run would have caught. See §9 |

### PRE-6 — how to scope a run to a site

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

## 1a. The site already carries traffic — what that changes

Staging is **not quiet**: it carries continuous real traffic from bots and from
developers. That changes two of the steps below, one for the better and one for
the worse.

**Better — the series already exist, so verification does not need a fixture
run.** The concern in §9 was that otelprom only exports an instrument once a
data point has been recorded for it, so on a quiet site the metric names simply
are not there to check. With bot and developer traffic that no longer holds:
`rpc_server_call_duration_seconds` and the domain counters are already being
recorded. **Confirming #337's metrics, and the whole PRE-9 query dry-run, can be
done today** — against real data, with no loadgen involvement. One caveat
survives: a *label value* still only exists if something exercised it. Check
`rpc_method="thread_open"` and the thread lanes specifically; if organic traffic
never opens a thread, that series is still absent and only loadgen will produce
it.

**Worse — the counters are shared, and the measurement method is a snapshot
difference.** §5 computes every ratio as `@t1 − @t0` on monotonic counters.
That difference contains *everything* the site did in the window, not just the
run. Three consequences:

| | What breaks | What to do |
|---|---|---|
| **Ratios** (SLO-1a, 7) | A ratio is only *diluted*, not biased — unless the background traffic's own good-ratio differs from the run's. You cannot assume it doesn't: bots retry on their own schedules and developers hit half-broken branches | Decompose (below) or isolate |
| **Latency** (SLO-4, 5) | Outright distribution mixing. Developer traffic is bursty, low-volume, and often against cold or unusual rooms; those samples land in the same histogram buckets and there is no label to separate them | Isolation is the only clean answer — a histogram cannot be decomposed after the fact |
| **The `t1` / `t2` marks** (§7) | `t1` says "wait until in-flight RPCs reach zero" and `t2` says "backlog at its **pre-run floor**". With traffic that never stops, in-flight never reaches zero and the floor is not zero | Use `t1`'s stated fallback — one full `soak` RPC timeout — and record that you did. For `t2`, characterise the floor first: sample `num_pending + num_ack_pending` over a background-only window *before* the run, and treat `t2` as "back inside that band, stable for one scrape interval" |

### If G1 cannot be had in time

There is a defensible decomposition, and it is worth knowing its exact limit.
Run the **same §5 queries over a background-only window** of the same length
immediately before the run. That gives the background's own count `n_b` and its
own ratio `r_b`. The run window gives the combined `n_c`, `r_c`. Then

```
r_run = (r_c·n_c − r_b·n_b) / (n_c − n_b)
```

This is valid **only while the run's added load does not change the
background's behaviour** — it assumes the background rate and its success ratio
are stationary across the two windows. That assumption holds for a low-load
calibration run. It does **not** hold for a ramp, a breach run, or anything in
Track 2, where changing the background's behaviour is the entire point. So:

- **Calibration run at declared load** — decomposition acceptable, report
  `n_b / n_c` (the contamination share) beside every number.
- **Any ramp or failure run** — decomposition invalid. **G1 is required**, and
  without it the run is `INCONCLUSIVE`, not "approximately right".
- **Latency (SLO-4, 5)** — decomposition never applies. Report the mixed
  distribution as mixed, or isolate.

This is why PRE-3 / G1 moved from hygiene to load-bearing once the site turned
out to be busy. Isolation is a cheaper answer than the arithmetic above, and it
is the only one that survives Track 2.

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
| J2 thread-open good-ratio curve | §5 query 3 | same, for SLO-5. Reference line at the current draft, **95% within 250 ms** |
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

**SLO-5's bound is settled as a draft; its target is not.** #337 merged on
2026-08-30, so `sli-slo.md` now reads *95% within 250 ms*, carrying its own
`target provisional — no baseline yet` label. That closed the earlier
300 ms / 250 ms divergence **in the source of truth**, by moving the bound onto a
real histogram boundary — 300 ms sat between `0.25` and `0.5` and could not be
computed at all. The **target** still has no evidence behind it, which is exactly
what 1.0b supplies and 1.3 decides. Record the `sli-slo.md` revision the run was
measured against, so a later reader knows what the reference line meant.

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

**`MaxDeliver` is not 6 on the services this run measures.** The repo default is
6 (`pkg/stream/consumer.go:20`), but `message-worker` and `broadcast-worker` both
wrap their settings in `stream.WithOutageRetryBudget`
(`message-worker/main.go:360`, `broadcast-worker/main.go:558`), which raises the
cap until the client-side schedule spans `stream.OutageRetryWindow` = **1 hour**
(`pkg/stream/consumer.go:139,152-160`). `jsretry.DeliveriesFor` resolves that to:

| Service | app schedule | **live `MaxDeliver`** |
|---|---|---:|
| `message-worker` | `jsretry.DefaultBackoff` | **17** |
| `broadcast-worker` | `jsretry.LowLatencyBackoff` | **18** |

At those values (consumer `30, 60, 120, 240, 480`; `AckWait` 30 s):

| Consumer | delay floor | execution margin | **operational cap** |
|---|---:|---:|---:|
| `message-worker` — **the one `t2` waits on** | 7 650 s (127.5 min) | 480 s | **≈ 2 h 16 min** |
| `broadcast-worker` | 8 130 s (135.5 min) | 510 s | ≈ 2 h 25 min |
| *repo default `MaxDeliver=6`, for reference only* | 1 050 s (17.5 min) | 150 s | ≈ 20 min |

**Do not shorten the cap to something more convenient.** A two-hour ceiling looks
unusable next to a 30-minute run, and the temptation is to declare INCONCLUSIVE
at 20 minutes instead. That would call a legitimate outage-budget drain a failed
measurement. The cap is a *ceiling on waiting*, not an expectation: a healthy run
drains in minutes, and a run still draining at 20 minutes has **already failed the
§6 validity gate** on backlog flatness — which is the signal to act on, not the
cap. Record the derived cap for the live `MaxDeliver` you read in PRE-8, and if
you choose a shorter operational limit, record that it is a policy choice and
that overshooting it is not evidence of loss.

`CONSUMER_BACKOFF_STEPS=0` makes `backOffSchedule()` return `nil`
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

Drop the `site` selector under PRE-6 option B.

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
rather than as a good result.

**A hard gate on SLO-1a needs one of exactly two things**, and max-delivery
advisories are **not** one of them: an advisory proves the broker stopped
delivering a message, which says nothing about the ordinary redeliveries that
double-count the success numerator, and nothing about the case where one message
is lost and another is processed twice — the two cancel and the ratio reads
100%.

| Route | What it gives |
|---|---|
| **P7's logical-outcome dedup ledger** | One outcome per logical message; the real fix |
| **loadgen's authoritative read-back** of run-owned message IDs | The soak ledger already does this per message — **the cheapest route**, and the one available now, though it is a client-observed verdict rather than the production boundary |
| *(a per-window redelivery-bias bound)* | Would work in principle, but nothing exports per-message delivery counts, so it is not constructible today |

Advisories add **terminal-delivery evidence and attribution** — which consumer
stopped delivering — on top of whichever of those you use. They do not convert
an attempt-based ratio into a logical one.

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
   observer from PRE-4, not `loadgen_consumer_*`.** The sampler died with the pod in
   step 1. Hold at floor for at least one scrape interval, then take
   **`t0-async`** — the SLO-1a baseline — *while dispatch is still stopped*.
   Taking it at first dispatch races the scrape. Keep the pause shorter than
   `soak.heartbeatStaleAfter`, and do not run teardown during it.
3. **`phase: soak` with `soak.warmup: 0`.** Dispatch resumes immediately.
4. **Wait for the full mix.** The signal is the thread-read lane producing
   samples again, on the **new** process only.

   **First, in PRE-9, find out what identifies a target here.** PRE-6 already
   established that the scrape adds `instance` and `service` and nothing else —
   **there is no `pod` label unless your infra added one.** An earlier revision of
   this document wrote `pod="<the NEW pod>"` anyway, which returns empty forever
   on the default relabel set and reads as "the lane has not recovered". Run
   this first and use whatever it actually returns:

   ```promql
   # PRE-9: what distinguishes one loadgen process from the next?
   count by (instance, service, pod) (
     loadgen_soak_rpc_latency_seconds_count{action="get_thread_messages"})
   ```

   Substitute the label that changes across a restart for `<target>` below.
   If **nothing** changes — a stable `instance` such as a Service DNS name, or a
   pod name recycled by the StatefulSet — you cannot isolate the new process by
   label at all, and the snapshot method in step (b) is the only one that works.

   **(a) Wait out the range window.** A bare `[1m]` taken immediately after the
   restart still covers the minute *before* it. `rate()` compensates for counter
   resets rather than isolating processes, so both the old and the new series can
   sit near the expected rate while the new catalog is still empty, and `t0-sync`
   gets marked early. Wait a full range window past the restart, then:

   ```promql
   rate(loadgen_soak_rpc_latency_seconds_count{
         action="get_thread_messages", <target>}[1m])
   ```

   **(b) Judge on a post-restart delta, not an absolute rate.** This is the part
   the earlier revision named but never made executable, and it is the one that
   works with no distinguishing label at all. Take one snapshot at resume, one
   later, and divide:

   ```promql
   # B0, at `phase: soak` + one scrape interval — instant query, record the value
   sum(loadgen_soak_rpc_latency_seconds_count{action="get_thread_messages"})

   # B1, at least 2 min later — instant query, same expression
   sum(loadgen_soak_rpc_latency_seconds_count{action="get_thread_messages"})

   # ready when (B1 - B0) / (t_B1 - t_B0) is within ~10% of the expected rate
   ```

   A counter reset at the restart makes `B0` small, which is exactly what you
   want: the delta then measures only the new process. Require **two consecutive
   confirming intervals** before marking `t0-sync`.

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

### How to write it up

**Evidence for a target, not a verdict against one:**

> J2 channel load, achieved at 100 msg/s over 30 min (t0-sync 14:32:10 → t1
> 15:02:10 → t2 15:03:15), isolated site `slo-test-a`, shape
> `default-zipf / i12=derived`, sampler 0.1, backlog flat, no invalidations,
> `sli-slo.md` @ `bf0ea62`:
> `≤250 ms 91.4% · ≤500 ms 96.2% · ≤1 s 99.1%`.
> Draft at run time: 95% within 500 ms — met.
> J2 thread open: `≤250 ms 97.8% · ≤500 ms 99.3%`. Draft: 95% within 250 ms
> (target provisional) — met at the drafted bound.
> SLO-1a approximate indicator = 99.94% (per-attempt, §5).

Never *"SLO-1a = 99.94%"*, and never *"SLO-5 passed"* or *"failed"* — the SLO is
a 28-day window over production traffic, and its target is still a provisional
draft this run exists to inform.

---

## 7a. What this run is expected to produce

Not "a number for the SLOs". Six artefacts, of which only the first is a
measurement. **A filled-in worked example of all six — with an invented but
internally consistent set of numbers — is
[`first-run-report.md`](first-run-report.md).** Fill that template in
with blanks *before* the run: a blank you cannot see how to fill is a missing
step in this SOP.

| # | Artefact | Shape | Who uses it next |
|---|---|---|---|
| **1** | **Achieved good-ratio curves for SLO-4 and SLO-5** | Not one number per SLO — **one point per histogram boundary**: `≤250 ms x% · ≤500 ms y% · ≤1 s z%`, for each of `channel_history` and `thread_open`. `le` stays a series dimension on the dashboard for exactly this reason | Track 1.3 picks the target *off the curve*: the boundary where the ratio is comfortably reachable, not the one already drafted |
| **2** | **An achieved ratio for SLO-7** | One ratio (`ok / eligible`), with its denominator | Same |
| **3** | **SLO-1a's approximate per-attempt indicator, or `INCONCLUSIVE`** | One ratio, explicitly labelled per-*attempt* not per-*message*, or the word INCONCLUSIVE and the reason (§7) | Track 1.3, and the argument for building P2a |
| **4** | **One-sided bounds for SLO-1b and SLO-2** | Intervals, from a short `run --preset=realistic --rate=100`. Weak, and known to be weak — SLO-2's follows from E2's interval strictly containing it | The case for P2b, and a floor under the drafted targets |
| **5** | **The loadgen-vs-production latency gap for SLO-4/5** | A delta, measured **once**. loadgen's client-side timing vs `rpc_server_call_duration_seconds` | Sizes the permanent blind spot, so later runs need not re-derive it |
| **6** | **The measurement apparatus itself** | The verified query set (PRE-9), the SLI/SLO dashboard built on it, the background contamination share `n_b/n_c`, and `t2`'s floor band (§1a) | Every subsequent run in Tracks 2 and 3. This is arguably the most durable output of the whole exercise |

**The honest framing of #1–#4:** they are *achievability evidence*. "At this
load, on this box, with this shape, the system did X." They are the input to
choosing a target. They are not a verdict, and §7's write-up template exists to
stop them being read as one.

**And a genuinely useful negative result is on the table.** If a curve comes back
well under the drafted target — say SLO-5 achieves 88% at 250 ms against a drafted
95% — that is not a failed run. It is the run doing its job: the draft was set by
gut feel, and this is the first evidence that the gut was wrong. Either the target
moves or the system does, and now there is a number to argue from.

---

## 7b. Iteration: which knobs may move, and when

Yes, there will be repeated runs — the first few will not produce usable numbers.
That is expected. What matters is that the iteration is over the **instrument**,
never over the **workload** and never over the **system**, because those two
converge on a number rather than measuring one.

### Three kinds of change, three different rules

| | Examples | Rule |
|---|---|---|
| **A — the instrument** | A query returns empty; the `site` selector does not resolve; `t0-sync` was marked before the catalog refilled; the scrape interval is too coarse for a 30 min window; the backlog observer was not actually running; sampler ratio wrong | **Iterate freely, before `t0`.** These runs are shakedown runs and claim nothing. A change *after* `t0` discards the run entirely — do not repair a window in flight |
| **B — the workload** | `sendRate`, `readRate`, `threadShare`, the room mix, the preset, `ENCRYPTION_ENABLED` | **Freeze before the run (G2), and do not touch it to improve a number.** A different shape is a **different measurement**, not a better one. Changing it produces a *new labelled point*, and both points stay in the ledger |
| **C — the system under test** | Consumer `MaxDeliver`/`AckWait`, pool sizes, cache TTLs, replica counts | **Not during calibration.** Tuning the system mid-programme means the numbers describe a configuration that exists nowhere else. If something genuinely needs to change, it lands as a normal change, and every prior run's numbers are marked superseded |

### The failure mode this is guarding against

If the target is chosen *from* the run, and the run is repeated with adjustments
until the number looks acceptable, then the target is the **maximum of N
samples**, not a reachable level. It will be missed in production for reasons
nobody can reconstruct. This is the same error as tuning a threshold on the test
set.

Two rules make it structurally hard:

1. **Every run gets a ledger row — including the discarded ones, with the
   reason.** "Discarded: `t0-sync` marked 40 s early, catalog not refilled" is a
   perfectly good row. A ledger with twelve rows and one reported result is
   *healthy*; a ledger with one row is the suspicious one. The count and the
   reasons are the evidence that nobody went shopping.
2. **The reported number is the median of three runs at the frozen shape, with
   the spread — never the best of three.** And **a margin over the drafted
   target that is smaller than the spread is not "met"** — it means the target
   sits inside the environment's noise. That is a separate, sharper test than
   the 10%-of-value bar below, and both apply. This is execution rule 10 in
   [`execution-priority-plan.md`](../execution-priority-plan.md), applied here. If
   the spread exceeds ~10% of the value, the result is INCONCLUSIVE until the
   variance is explained.

### So the realistic sequence

| Phase | Runs | Duration each | Produces |
|---|---|---|---|
| **Shakedown** | However many it takes — expect **3–6** | 10–30 min | A working instrument. No numbers are reported from these, ever |
| **Freeze** | — | — | The shape is declared and recorded (G2), the queries are verified (PRE-9), the dashboard is built |
| **Measurement** | **3**, identical | ≥30 min steady state each | Artefacts 1–5 above, as median + spread |

Shakedown runs are cheap and short — do not run them for 30 minutes. The point
is to break the instrument early, on purpose.

**One asymmetry worth stating:** discovering a defect in the *instrument* during a
measurement run is normal and costs one run. Discovering that the *workload
shape* was wrong costs all three, because the shape is what the numbers are
labelled with. That is why G2 gates the run rather than being resolved during it.

---

## 8. What this run cannot answer

| | Why | Where it comes from instead |
|---|---|---|
| **SLO-1b, SLO-2** | The enqueue counter and age histogram do not exist | P2a / P2b ([`p2-instrumentation-spec.md`](p2-instrumentation-spec.md)). A short `run --preset=realistic --rate=100` gives SLO-2 a one-sided bound in the meantime |
| **SLO-3** | The soak connects with backend creds and never touches auth-service's HTTP leg | A separate short `max-rps --workload=login` |
| **SLO-6** | The notification counter is per message, not per recipient | **P4** (the instrumentation tier — recipient-granular counters), not PRE-4 |
| **SLO-8** | No `status` on the duration histogram, and the soak has no client-side SLO-8 scoring | **P4** (the search duration `status` label), or `max-rps --workload=search` for a client-side number |
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

## 9. Order of operations, and the dry-run

The natural order — *check the metrics exist → build the dashboard → run
loadgen* — is close to right, given §1a. Two adjustments.

**Verification comes first and needs no loadgen.** On a quiet site it would
not: otelprom emits a series only once a data point has been recorded for it
(the instruments themselves are constructed at service start,
`pkg/natsmetrics/metrics.go:238-247`), so the names would not be there to check.
The site is not quiet, so steps 1–3 below run today against organic traffic.
The one thing organic traffic may not cover is a *label value* nothing
exercises — check `rpc_method="thread_open"` explicitly, and if it is missing,
that is what a short loadgen run is for.

**Missing entirely: G6.** The backlog observer that `t0-async` and `t2` depend
on runs inside the loadgen pod and dies with it (PRE-4). It has infra lead time,
it is not discovered by any of the steps below, and without it the measurement
run cannot mark either boundary. Start it now, in parallel.

**And G1 got heavier.** §1a: on a busy site the isolated `SITE_ID` is what
decides whether the numbers belong to the run. It is on the same critical path
as G6.

### The sequence

| # | Step | Proves | Blocks on |
|---|---|---|---|
| **0** | Start **G6** and **G1** (isolated `SITE_ID`), plus PRE-6 (`site` label decision) and G2 (`realistic` preset) — **all in parallel, all now** | — | infra + product lead time |
| **1** | Confirm the deployed services run a build containing `bf0ea62` | The counters can exist at all | deploy |
| **2** | **Query dry-run** (PRE-9): run every §5 query against the real Prometheus, on organic traffic | The queries are *right* — the class of defect four review rounds kept finding | 1 |
| **2b** | Only if a label value is missing (typically `thread_open`): a 10–15 min loadgen run to produce it, then re-check. Not a measurement | The absent series was absence of traffic, not a wrong name | 2 |
| **3** | **Background baseline**: the same §5 queries over a background-only window, plus `num_pending + num_ack_pending` over the same window | The contamination share `n_b`, and `t2`'s non-zero floor band (§1a) | 2 |
| **4** | Build the SLI/SLO dashboard **from the verified queries** | Panels show data on first load | 2 |
| **5a** | **Shakedown runs** (§7b): short, repeated, instrument-only. Expect 3–6 | A working instrument. Nothing is reported from these | 0, 4 |
| **5b** | **Freeze the shape** (G2) and record it | The measurement runs are all labelled with the same thing | 5a |
| **5c** | **3 identical measurement runs** (§7) | The artefacts in §7a, as median + spread | 5b |

Steps 1–4 are one afternoon and start immediately. **Step 0 is the long pole**,
and it is the only part of this that is not ours to schedule.

### What the dry-run has to check

Per query, not just "it returned something":

| Check | Why it is on this list |
|---|---|
| Non-empty result | The base failure mode — a wrong label value returns empty, which reads as "no data" rather than "wrong query" |
| The `action` values exist as spelled: `load_history`, `get_thread_messages` | Both were wrong in earlier drafts, from memory rather than from source |
| Histogram queries use `_count` / `_bucket` / `_sum`, never the bare name | A bare histogram name selects nothing |
| `rpc_method` has exactly `channel_history` and `thread_open` as separate values | The disjointness SLO-4/5 depend on |
| `error_type` is **absent** on success, and the regex's empty alternative matches those series | The convention makes it conditional; a `{error_type=""}` matcher must actually match |
| The `site` selector resolves (or is deleted, per PRE-6 option B) | There is no `site` label by default |
| `le` values include the bounds you intend to report | `0.25`, `0.5`, `1` must be real boundaries in the deployed build |
| A restart makes the `_count` series behave as expected under `rate()` | The `t0-sync` readiness signal depends on it |

Record the dry-run result — query, returned shape, sample value — next to the
run. A dashboard built on unverified queries is how a green panel comes to mean
nothing.

---

## 9a. After the P2 metrics land: what changes

§9 sequences the run **against today's instruments**. Once
[`p2-implementation-task.md`](p2-implementation-task.md) ships, four things
change and three deliberately do not.

### What becomes possible

| | Before P2 | After P2a | After P2b |
|---|---|---|---|
| **SLO-1b** (channel enqueue) | one-sided bound from a separate short `run` | **A real ratio** — `broadcast_channel_enqueue_total{outcome="ok"}` over `messages_canonical_published_total{broadcast_path="room_subject"}` | unchanged |
| **SLO-2** (enqueue ≤ 1 s) | one-sided bound | unchanged | **A real ratio**, from `broadcast_channel_enqueue_age_seconds` |
| **SLO-1a** | approximate, attempt-based | **still approximate** — see below | still approximate |

**P2 does not make SLO-1a exact, and does not make any of the three a hard
gate.** Every numerator stays consumer-side and attempt-based, so a lost message
and a redelivered one still cancel. Nothing here changes §4a: the run is
calibration evidence, and `common/sli-slo.md` stays the contract.

### The three new preconditions

Add these to §1 for any run that intends to report 1b or 2:

| # | Precondition | Why |
|---|---|---|
| **PRE-10** | The deployed build carries the P2a commit, and **`broadcast_path` has all four values in Prometheus** — including `unknown` | A missing label value is indistinguishable from "no traffic of that shape". Exercise a channel message, a thread reply and a DM in the smoke run |
| **PRE-11** | **`broadcast_path="unknown"` is zero over a quiet window** before the run | §3.3 of the task brief makes any non-zero `unknown` an `INCONCLUSIVE` for SLO-1b/2. Establish the baseline is clean *before* you spend a measurement run |
| **PRE-12** | The **`le` boundaries on `broadcast_channel_enqueue_age_seconds` include `1`** | SLO-2's bound is 1 s. If it is not a real boundary the good-ratio cannot be read at the bound |

### The queries to add to §5

Both are synchronous-shaped in the sense that matters — numerator and denominator
move together — but the **denominator is upstream** (gatekeeper) while the
**numerator is downstream** (broadcast-worker), so they need the async
treatment: take both at `t2`, not `t1`.

```promql
# 5 — SLO-1b   (both sides @t2 - @t0-async)
sum(broadcast_channel_enqueue_total{site="$site", outcome="ok"})
sum(messages_canonical_published_total{site="$site", broadcast_path="room_subject"})

# 6 — SLO-2: good-ratio at every boundary, le kept as a dimension
sum by (le) (broadcast_channel_enqueue_age_seconds_bucket{site="$site"})
sum(broadcast_channel_enqueue_age_seconds_count{site="$site"})

# 7 — the validity gate for both of the above
sum(increase(messages_canonical_published_total{
      site="$site", broadcast_path="unknown"}[$window]))     # must be 0
sum(increase(broadcast_channel_enqueue_age_invalid_total{site="$site"}[$window]))
```

**The last two are gates, not results.** Read them before the ratios, exactly as
§6 says for the existing checks. A non-zero `unknown` makes SLO-1b/2
`INCONCLUSIVE` for the window unless the worst case is shown to pass; a non-zero
`_age_invalid_total` means the SLO-2 measurement itself is broken and the number
is not reportable.

### What does not change

- **The loadgen invocation.** Still no code change and no new flags — P2 adds
  production-side instruments, and loadgen is the traffic source either way.
- **§7's run protocol.** Same marks, same restart procedure, same caps. SLO-1b/2
  ride the `t0-async`/`t2` pair that SLO-1a already uses.
- **§7b's iteration rules.** A new instrument is a change to the *measurement
  apparatus*, so the first run after P2 lands is a **shakedown run**, not a
  measurement run. Do not report numbers from it.

---

## 10. Sibling documents

- [`first-run-report.md`](first-run-report.md) — **the output contract**, as a filled-in worked example
- [`measurement-map.md`](measurement-map.md) §7 — why this works, per SLO
- [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md) — what unlocks SLO-1b/2
- [`execution-priority-plan.md`](../execution-priority-plan.md) — Track 1.0b
- [`loadgen/dashboard-contract.md`](../loadgen/dashboard-contract.md) — the validity rules in §6
- `tools/loadgen/deploy/k8s/README.md` — the chart's own operational runbook
