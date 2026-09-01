# Load Test Execution & Priority Plan

> Scheduling document. It does **not** define new tests or new acceptance
> criteria — it orders the ones the program documents already specify, names
> what blocks each, and says which can start today.
>
> Acceptance criteria: [`common/sli-slo.md`](common/sli-slo.md).
> Scenario inventory: [`performance/end-to-end-plan.md`](performance/end-to-end-plan.md) §5.
> What may be pushed: [`common/environments-and-data-ownership.md`](common/environments-and-data-ownership.md) §3.
> Driver: `tools/loadgen`.

| | |
|---|---|
| **Status** | Draft — for review · **rev 2** (post-soak, post-failure-round-1) |
| **Answers** | In what order do we run the load tests, and what has to be true first |
| **Does NOT answer** | What "pass" means (§ sli-slo), how to ramp (§ capacity-test-plan), how to read a fault result (§ failure/), what the worst-case shapes are (§ extreme-scenarios) |

## 0. Where we are

| Done | Evidence it produced | What it did **not** produce |
|---|---|---|
| Cassandra Run A soak on staging (latest loadgen) | Schema/access patterns under sustained *realistic* load; ledger + observer evidence | No ceiling (non-destructive by decision D1/D8); no pathological shape; reads hit only run-generated, shallow, dense data (soak blind spot 11) |
| Failure round 1 with the NATS / MongoDB / Cassandra teams (latest loadgen) | Per-dependency fault behaviour, recovery classification | No capacity number; no evidence for the silent-drop path X9 unless max-delivery advisories were collected |

**So the remaining work is exactly the three gaps below**, plus the pre-production
set in §3.5:

1. **The SLI/SLO targets are unvalidated guesses.** They were set achievable-first
   (sli-slo §0.2 calls for a 4–6 week calibration that has not started) and no run
   has ever asserted against the production predicates. For five of the nine the
   counters genuinely do not exist yet; for the rest, including **SLO-1a, which is
   computable on `main` today**, nobody has looked. Hop-by-hop coverage:
   [`slo-measurement-map.md`](slo-measurement-map.md).
2. **No ceiling exists for anything.** Both completed programs are
   non-destructive by design. Nothing has been ramped to a breach.
3. **No extreme shape has been exercised.** See
   [`extreme-scenarios.md`](extreme-scenarios.md) — ten code-derived worst cases,
   two of which (X1 unbounded thread scan, X5 login storm) have no loadgen
   coverage at all.

---

## 1. How the order was decided

Rank = **(journey criticality × likelihood the current code has a problem × still-unvalidated) ÷ (cost + unmet preconditions)**.

The last term is what separates this document from
[`end-to-end-plan.md`](performance/end-to-end-plan.md) §5. That inventory ranks
by *value*; several of its Tier-1 items cannot execute today because an
environment, an isolation guarantee, or a service-side metric does not exist
yet. So the ladder below front-loads everything that is **valuable and
unblocked**, and parks high-value blocked work behind an explicit gate.

Three ordering rules fall out of that:

1. **Cheap attribution first.** A `max-rps` ramp that names the
   first-saturating component costs ~30 minutes on a laptop stack and tells you
   which of the expensive staging runs is actually worth its window.
2. **A measurement gap outranks a measurement.** Rev 2's biggest change: with
   the soak and the first failure round done, the binding constraint is no
   longer "what should we run" but "what can we legitimately conclude". Shipping
   the two missing counter sets (G4, G9) makes five of nine SLOs computable and is
   cheaper than any staging run, so it moves ahead of every test.
3. **A blocked item is scheduled as its gate, not as a test.** Elasticsearch
   capacity is not "later" — it is "after a run-scoped index with verified
   teardown exists". The gate is the schedulable unit.

---

## 2. Readiness snapshot (verified against the code, 2026-08-27)

What exists decides what can be *asserted*, not just what can be *driven*.

| Capability | State | Consequence for scheduling |
|---|---|---|
| loadgen `messages`/`thread`/`history`/`thread-read`/`room-read`/`read-receipt`/`login`/`search` ramps | **Exists** | Track 2 needs no new tooling — only a breach run |
| loadgen `soak` (Cassandra Run A + room/member/user/search/presence lanes, durable ledger) | **Exists** | Run A is implementation-ready; gated on environment only |
| loadgen `daily`, `max-room-size`, `members-*`, `presence-*` | **Exists** | Track 2; `max-room-size --rooms-per-size=1` also serves 3.6 |
| P1 — RPC server duration histogram | **Delivered.** #337 merged 2026-08-30 (`bf0ea62`), so `main` carries `rpc_server_call_duration_seconds{rpc_method, error_type}` with `channel_history` / `thread_open` as separate methods | SLO-4/5 become computable on merge — as a **server-side proxy**: the timer stops after `Respond`, so a server→client partition moves the SLI toward green. SLO-5's bound moved 300 ms → 250 ms and its target 99% → 95% to sit on a real bucket boundary |
| P2 — J1 counters | **More present than the roadmap says.** `message_gatekeeper_messages_total{result="accepted"}` is the denominator and `message_worker_persistence_total{message_kind,result}` the SLO-1a numerator — **both on `main` today**. Missing: a `broadcast_path`-scoped denominator at the gatekeeper's publish site, a per-message channel enqueue counter, and the age histogram. #337 does not touch P2. Implementable brief: [`p2-implementation-task.md`](p2-implementation-task.md) | **SLO-1a is computable now** (with a `message_kind` filter). SLO-1b/2 remain unmeasurable, so a J1 run still gates on loadgen L1 E2E correlation for those two — a *different, downstream* boundary. Never report it as "SLO-2 passed". Scope and cost: [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md); full path coverage: [`slo-measurement-map.md`](slo-measurement-map.md) |
| `search_service_requests_total{kind,status}` | **Exists** | SLO-7 scorable (partial-failure only) |
| `search_service_request_duration_seconds` `status` label (P4) | **Still absent after #337** — the PR adds `status` to the *counter*, but `durationOptFor(kind)` keeps the histogram on `kind` alone | SLO-8 client-side only (loadgen `--workload=search` scores it); not enforceable from production recording rules |
| JetStream Prometheus exporter with `is_consumer_leader` filtering (P3) | Local overlay only (`nats-exporter` sidecar) | Backlog is the primary enforcement signal for every async SLO — a staging run without it has no backstop |
| loadgen `search_index` observer | **Refused at startup** by design (soak bodies analyze to one token) | Search index-convergence loss is unobservable; do not claim search E2E coverage |
| Isolated staging `SITE_ID` / Mongo DB / Cassandra keyspace | **Unconfirmed** (environments §7) | Blocks every staging SLO-asserting run — counters are shared and monotonic, so isolation is the only denominator control |
| Cassandra disk-reclaim branch (disposable keyspace **or** TTL + budget) | **Neither exists** (environments §5) | Blocks *repeated* and *pathological* Cassandra runs, not the first Run A |
| ES run-scoped index + ES telemetry contract | **Neither exists** | Blocks ES capacity outright |
| Valkey run-scoped key namespace + expiry + verified cleanup | **Absent** | Blocks Valkey capacity execution (damage lands during the run, not at result time) |
| Cross-site federation traffic in loadgen | **Absent** — single-site | SLO-9 unschedulable this round |
| `PUSH_NOTIFICATION` recipient observer | **Absent** | SLO-6 unschedulable this round |
| `pkg/threadcount` bound on the reply-count scan | **Absent** — full partition scan, `LIMIT`-less, 15 s timeout backstop (`count.go:33-45`) | Item 3.1. The soak plan documents a `Cap(99)`; a test sized from that doc is wrong by the whole thread length |
| Enforced maximum thread length | **Absent** anywhere in the send path | I7's "max 500" is an assumption, so 3.1's partition is genuinely unbounded |
| Deployed TWCS `compaction_window_size` == `MESSAGE_BUCKET_HOURS` (360) | **Unverified** — init DDL says 360, the TWCS migration says 72 | Item 3.5. Until checked, the completed soak's compaction and SSTables-per-read evidence is uninterpretable |

---

## 3. The priority ladder (rev 2)

Four tracks. **Only the *final* capacity verdict waits on Track 1** — exploratory
capacity work starts immediately, in parallel.

The earlier revision had T1 and T2 strictly sequential, on the reasoning that a
ceiling is "the load at which an SLO first breaks". That is right for the number
you put in a go/no-go, and wrong as a schedule: it delays capacity-risk exposure
by the whole 4–6 week calibration window for no gain, because the signals that
find the *first* ceiling — error rate, consumer backlog, resource saturation,
recovery behaviour — need no calibrated SLO at all.

So, two thresholds instead of one gate:

- **Provisional guardrails** (available now): error/timeout rate, monotonically
  growing backlog, CPU/memory/IO saturation, and whether the system recovers when
  load returns to baseline. A ramp stopped by any of these is a real finding and
  a real ceiling — it just is not an *SLO* ceiling.
- **The SLO ceiling** (after Track 1.3): the same ramp re-scored against the
  approved predicates. Only this one goes in the release decision.

T3 runs in parallel with both. T4 is the pre-production sweep.

`Env`: **L** = docker-local, **S** = staging.

### Track 1 — Make the SLOs measurable, then calibrate them

The targets are unvalidated because nothing has ever *measured* them, not because
nobody looked. Calibration is a measurement problem before it is a judgement
problem. Order matters here: 1.1 is a code change, 1.0b and 1.2 are the two
evidence sources, 1.3 is the decision.

**The numbers currently in `sli-slo.md` are drafts, and this programme is what
settles them.** §0.2 says so — achievable-first starting values, 4–6 weeks of
observation, then adjust and seek approval — and nothing has been through that
yet. So no run in this track scores *against* a target; they produce the
distribution a target is chosen *from*. Two consequences worth stating plainly:

- **A bound must land on a real histogram boundary**
  (`o11y.DefaultLatencyBuckets()` = `{.005 .01 .025 .05 .1 .25 .5 1 2.5 5 10}`).
  A bound between two of them is not computable — 300 ms can only be read as 250
  or 500. This rules out most round numbers before anyone argues about them, and
  it belongs in `sli-slo.md`'s calibration section.
- **The load test sets a floor; observation sets the commitment.** A lab number
  says what is achievable under a chosen shape on chosen hardware. Let it veto a
  target that is not even reachable, and let the observational window decide where
  inside the reachable range to sit.

| # | Item | Why it is first | Output |
|---|---|---|---|
| **1.0** | **Write the SLO-1a recording rule against counters that already exist** | A rules change, not a code change: `message_worker_persistence_total` ÷ `message_gatekeeper_messages_total{result="accepted"}`. Puts a third of J1 into the calibration window immediately, weeks ahead of the rest | SLO-1a under observation |
| **1.0b** | **First SLO-measuring run — loadgen as traffic source, production counters as the instrument** | Produces run-window numbers for **SLO-3/4/5/7**, an approximate indicator for **1a**, and a defensible one-sided bound for **1b/2**, with **no loadgen code change** (SLO-4/5 need #337 merged first): the docker-local overlay already scrapes the o11y SDK endpoint on every service plus a JetStream exporter. Answers the question calibration actually needs — *is the drafted target reachable at all* — before anyone commits to it. Method and per-SLO verdict: [`slo-measurement-map.md`](slo-measurement-map.md) §7; operator runbook: [`first-slo-run-runbook.md`](first-slo-run-runbook.md) | Achievability evidence for 7 of 9 SLOs |
| **1.1** | **Ship the remaining P2 work** (#337 landed, so P1 is done) | SLO-1b and 2 remain unmeasurable, and SLO-2 has nothing even close — the existing processing histogram excludes the stream wait, which is the interval SLO-2 is about. After reading the code the work is smaller than the roadmap line suggests: **four instrument families to add, one already exists, one to drop**, and **two carry a measurable hot-path cost** — an unconditional room-meta lookup in the gatekeeper and an unconditional `msg.Metadata()` parse in broadcast-worker, both of which the brief requires benchmarking. Scope, call sites, tests and acceptance: [`p2-implementation-task.md`](p2-implementation-task.md); rationale and the "what not to add" list: [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md) | SLO-1a/1b/2 become computable |
| **1.2** | **Observational calibration window (4–6 weeks, sli-slo §0.2)** | Run the counters against *real staging traffic and the soak*, with **no paging**, and record the achieved distribution per SLI | The empirical p50/p95/p99 and good-ratio per journey |
| **1.3** | **Set bounds and targets from 1.0b + 1.2, not from feel** | A target is defensible when it is (achieved distribution) − (headroom), with the error budget the business will actually tolerate, and a **bound that a histogram can actually compute**. Right now they are round numbers chosen by feel: #337 already moved SLO-5 onto a real bucket boundary (300 ms → 250 ms, 99% → 95%), but that fixed *computability*, not *defensibility* — nothing has yet shown 95% at 250 ms is reachable, and 1.0b exists to produce that evidence | Approved SLO bounds + targets, `Revisit date` filled in |
| **1.4** | **SLO assertion mode in loadgen** (sli-slo §10 "required before *validates*") | Counts `eligible` / `good` / `missing-after-deadline` against the **production predicates** over isolated run-window deltas, with warm-up drain + baseline snapshot | Runs can gate on a **client-observed** ledger of logical messages instead of eyeballing. That is a different instrument from the production counters, and it is what G4 cannot be: loadgen knows which logical message it sent, so it has no redelivery double-count — what it lacks is the production boundary |
| **1.5** | **Re-run the completed soak + failure round under assertion mode** | The two finished programs produced evidence but no SLO verdict. Replaying them with 1.4 is cheap and converts them into a pass/fail record | The first **client-observed logical-message verdict** — loadgen knows which message it sent, so no attempt-counting bias, but it is not the production boundary. Not a "we meet our SLOs" statement: that needs a production-side measurement G4 explicitly cannot provide |

> **Interim rule until 1.1 lands:** every J1 verdict is loadgen-L1 observational.
> Say "E2E publish→delivery p99 was X" — never "SLO-2 passed". They are different
> boundaries (sli-slo §2).

### Track 2 — Find the ceilings

Nothing here is new tooling; `max-rps` and `daily` already do it. What is missing
is that no one has run them to a breach on a production-like backend, and that
the results have to be reported as **headroom vs current prod load**, not as
absolutes (capacity-test-plan §Method).

| # | Item | Question | Driver | Env | Gates |
|---|---|---|---|---|---|
| **2.1** | **J1 send-path ceiling + bottleneck attribution** | msg/s at first SLO breach; which component saturates first | `max-rps --workload=messages --inject=frontdoor --steps=100,250,500,1k,2k,5k` | L then S | — / G1 |
| **2.2** | **Concurrent-user ceiling** | How many daily-IM users per site | `daily --steps=1k,2k,5k,10k` | L then S | G1 |
| **2.3** | **Mongo send-path write ceiling** (T1-4/T1-5) | Where the per-send write amplification saturates Mongo | 2.1 with a mention share, asserting on Mongo L2/L3 | S | G1, G2 |
| **2.4** | **Read-path ceilings** | Enter-channel, enter-thread, mark-read, read-receipt | `max-rps --workload=history\|thread-read\|room-read\|read-receipt` | L then S | G1 |
| **2.5** | **Presence ceiling + storm** | Max online population; largest survivable reconnect storm | `presence-capacity`, `presence-storm` | L then S | G1 |
| **2.6** | **Login and search ceilings** | SLO-3 / SLO-7 / SLO-8 under load | `max-rps --workload=login\|search` | L then S | G1 |
| **2.7** | **Recovery-surge ceiling** | After a fault clears, does the backlog drain exceed what storage absorbs | Failure-round replay with the drain measured | S | round-1 evidence |

**Report every ceiling in the same four fields**, or it is not a result
(end-to-end-plan §7): sustainable ceiling · first-saturating component · failure
mode past the knee · whether it recovers when load returns to baseline.

### Track 3 — Extreme scenarios

Full derivation, arithmetic and per-item test design in
[`extreme-scenarios.md`](extreme-scenarios.md). Ranked here by priority only.

| # | Scenario | Dependency | Coverage today | Effort |
|---|---|---|---|---|
| **3.1** | **X1** — unbounded thread partition, full re-scan on every reply (O(N²) rows; 15 s timeout → `MaxDeliver` drop) | Cassandra | **None** — and the soak plan documents this path as bounded to 99 rows, which is wrong | New seed shape + ramp |
| **3.2** | **X5** — morning login / reconnect storm (`subscription.list` last-message fan-out, unbounded in aggregate) | history-service + Cassandra | **None** — `daily` warms up, which is the opposite shape | New loadgen mode |
| **3.3** | **X2** — large-room notification fan-out (⌈N/512⌉ presence RPCs/msg) **+ the Valkey-miss variant** | NATS, presence-service, Mongo | Partial — `max-room-size` gates backlog but never looks at presence-service or the Valkey fallthrough | Extra scrape targets + one variant |
| **3.4** | **X3** — `@all` in a large room (N-doc `UpdateMany`, repeated on edit) | MongoDB | T1-5, unasserted | Mention-storm variant of 2.3 |
| **3.5** | **X6a** — deployed TWCS `compaction_window_size` vs `MESSAGE_BUCKET_HOURS` | Cassandra | **Config assertion, not a test.** Do this first — it decides whether the completed soak's compaction evidence is interpretable | Minutes |
| **3.6** | **X6b** — hot-room partition size (bucket is a fixed *time* window; no row cap) | Cassandra | `max-room-size --rooms-per-size=1` exists; never held long enough to fill a bucket | Long single-room hold |
| **3.7** | **X4** — key-rotation storm on member removal (N × `key.get`) | room-service + Mongo | None | Small addition to 3.3 |
| **3.8** | **X9 — largely mitigated, keep only the budget confirmation.** The premise (an immediate `Nak()` burning `MaxDeliver` in milliseconds) is wrong — see correction 7 below. What is left is confirming the *real* budgets under a long outage: gatekeeper and room-worker at the default `MaxDeliver=6` (~12.6 min client-side), and message-worker at **17** / broadcast-worker at **18**, raised by `stream.WithOutageRetryBudget` to span an hour | NATS | Possibly covered by failure round 1 — **check whether max-delivery advisories were collected**; if not, re-run | Re-run with an advisory consumer |
| **3.9** | **X10** — sparse/aged history walk | Cassandra | None — soak has no historical backfill | New seed span |
| **3.10** | **X7** — reaction MAP width / collection tombstones (soak F4) | Cassandra | None | Isolated keyspace |
| **3.11** | **X8** — cross-site membership burst through the `MaxAckPending=1` FIFO lane | NATS federation | **Blocked** — single-site driver | Needs D4 |

### Track 4 — Pre-production sweep

Answering *"what else before prod"*. These are not capacity questions; they are
"we will find out the hard way otherwise" questions.

| # | Item | Why it belongs before prod |
|---|---|---|
| **4.0** ⬆ | **Rolling restart / deploy under load — promoted to P1, run it in Track 2's window** | This document calls a deploy at peak "the most common production event" and then scheduled it last. A consumer-loop terminal error leaves a durable unread behind a green readiness probe, and services **exit** on a failed stream or consumer lookup, so a deploy during any dependency wobble crash-loops. It needs no new tooling: restart one worker at a time during any Track 2 run and watch backlog, terminal outcomes and the ledger |
| **4.1** | **Deployment-shape validation**: per-service CPU/memory requests and limits, replica counts, HPA behaviour under 2.1's load | Every ceiling measured so far is against *staging* pod sizing. A ceiling is meaningless without the resource envelope it was measured in, and `terminationGracePeriodSeconds` (30 s) vs the 25 s shutdown budget has never been exercised under load |
| **4.3** | **Cold-start / crash-loop behaviour** | Services **exit** when the initial NATS connection, JetStream context, stream lookup or consumer creation fails. A restart during a dependency blip crash-loops. Read as its own scenario, never as steady-state degradation |
| **4.4** | **Data-volume rehearsal**: run reads against a dataset aged to a realistic size, not a fresh one | Every read measured so far hit shallow, dense, run-generated data. Production reads hit a year of history. This is the single largest fidelity gap in all completed work |
| **4.5** | **Backup / restore and Cassandra disk-growth projection** | Soak plan §5 requires the disk model be projected under the **no-TTL production assumption**, where storage grows unbounded. Confirm the projection and the reclaim path (G5) before prod, not after |
| **4.6** ⬆ | **Multi-site federation — P1 if production launches multi-site, P3 if not. Decide which, now** | This is the one item whose priority is a *product* question, and leaving it in the pre-production sweep quietly assumed the answer. SLO-9 is entirely unvalidated, no loadgen traffic drives it, `outbox-worker` and `inbox-worker` carry no consumer or domain metrics at all, and X8's arithmetic suggests the 30 s bound may be unreachable for a bulk membership change at realistic inter-site RTT. If prod is multi-site at launch, federation is a **launch blocker**, not a smoke test, and the two-site topology plus the P4 outbox counters move into Track 1/2 |
| **4.7** | **o11y overhead A/B at the chosen sampler ratio** | Required once per release (sli-slo §10). Cheap, and it protects every other number |
| **4.8** | **Alerting dry-run**: burn-rate alerts (sli-slo §7) fired against a real breach | An SLO with no working alert is a dashboard. Validate the 14.4×/6×/1× windows against a deliberately-induced breach from 2.1 |
| **4.9** | **Capacity headroom statement** | Convert 2.1–2.6 into "we have N× headroom over projected launch load", with the projection written down. This is the artefact a go/no-go actually needs |

---

## 4. Gate backlog

> **Identifiers used across these documents.** **Gn** — a gate: external work
> with a named owner that blocks programme items (this section). **PRE-n** — a
> precondition for one specific run, an operator checklist item
> ([`first-slo-run-runbook.md`](first-slo-run-runbook.md) §1). **P1/P2/P3** — an
> instrumentation *priority tier*, i.e. how urgent a piece of missing telemetry
> is ([`p2-instrumentation-spec.md`](p2-instrumentation-spec.md)). A gate and a
> precondition can name the same work: G1 is PRE-3, G2 is PRE-7.

Gates are the schedulable unit for everything above that is blocked. Ordered by
how much they unblock. **⬆ marks the ones promoted since rev 1.** G4 was "nice to
have for hard-gating" while the programme was exploratory; now that the question
is *"are our SLO numbers right"*, it is the critical path. **G2 is promoted in rev
3** — the soak defaults turn out to be a stress shape, not a baseline (see 1.0b),
so it now gates the first SLO run rather than following it. **G9 closed on
2026-08-30** when #337 merged.

**With G9 closed, the binding constraint on the first SLO run is no longer app
code — it is G6.** The backlog observer that `t0-async` and `t2` depend on lives
inside the loadgen pod and dies with it, so a run cannot mark either boundary
without the JetStream exporter or an equivalent out-of-band reader
([`first-slo-run-runbook.md`](first-slo-run-runbook.md) PRE-4). That is infra work,
and it is now the thing standing between here and a first number.

| Gate | Work | Unblocks | Owner |
|---|---|---|---|
| **G1** | Isolated staging tenant: dedicated `SITE_ID`, Mongo DB, Cassandra keyspace, NATS account | All of Track 2, every staging item in Track 3 | Infra + us |
| **G2** ⬆ | Confirm workload-model inputs: I8 meaning, I10 scope, **I12**; and S1–S4 (fan-out, concurrent members, notification eligibility, cross-site share). Then split the soak presets three ways — `realistic`, `hot-room`, `stress` | **Now gates 1.0b**, not just B1/B3. At the chart defaults, I12 derives to **4 320 messages per active user per day** and the Zipf shape puts **20.8% of all sends into one room** (51% into ten). The soak's own `logSoakAssumptions` already logs this as `provisional: true`. A number measured under that shape is not "the system at expected load" | Product + infra |
| **G3** | Managed pre-run coordination: peak load declared, blast radius recorded, abort thresholds agreed, L3 dashboards confirmed | Every staging run. **Now higher-stakes than before** — Track 2 ramps to a breach, unlike the two completed programs | Us → infra |
| **G4** ⬆ | **P2 J1 counters — narrowed** (gatekeeper `messages_canonical_published_total{broadcast_path}` + `_duplicate_total`; broadcast-worker `broadcast_channel_enqueue_total{outcome}`, `broadcast_channel_enqueue_age_seconds` from the JetStream metadata timestamp, `_age_invalid_total{reason}`). Implementable brief: [`p2-implementation-task.md`](p2-implementation-task.md) | Makes SLO-1a/1b/2 **computable and calibration-ready** in B1 — **not hard-gateable**. Every numerator here is consumer-side and counts *delivery attempts*, so compensating errors cancel: one logical message lost and another processed twice under a failed Ack reads as `good=2 / valid=2 = 100%`, with the backlog draining to zero and the P3 lag signal clean. A hard gate needs either **P7's logical-outcome dedup ledger**, or a per-window redelivery-bias bound that the window can actually produce — and where neither exists, the verdict is `INCONCLUSIVE`, not a number. Until then every J1 verdict stays loadgen-L1 observational | App |
| **G5** | Cassandra storage control: pick and verify **one** of — run-scoped disposable keyspace with snapshot clearing, or bounded TTL + storage budget (both over an isolated keyspace) | B4 (repeat runs), D3 | Owner decision + infra |
| **G6** ⬆ | JetStream exporter on staging with `{is_consumer_leader="true"}` recording rules; custom oldest-pending-age monitor (P3) | The enforcement backstop for every async SLO in Tracks 1–3 — **and, since G9 closed, the blocker on Track 1.0b itself**: without it neither `t0-async` nor `t2` can be marked | Infra |
| **G7** | ES: run-scoped index, named owner, expiry, verified teardown **and** an ES telemetry contract (shards, thread-pool rejection, circuit breaker, merge, watermarks) | D1 | Us |
| **G8** | Valkey: run-scoped key namespace / ownership marker, expiry, verified post-teardown cleanup | D2 | Us |
| ~~**G9**~~ | ~~P1 RPC server duration histogram~~ — **closed by #337, merged 2026-08-30 (`bf0ea62`)** | Server-side SLO-4/5 (as a proxy — the timer stops at `Respond`, tracked as issue #393); also gives `daily`'s dormant service-error arm a real counter to point at | ✅ App |
| **G10** | Storage locality + node affinity answers for Mongo/Cassandra/ES/Valkey | Makes IO-bound ceilings non-provisional (environments §7) | Infra |

---

## 5. Execution rules that apply to every run

These are not new criteria — they are the ones easiest to get wrong.

1. **Start the ramp at the declared baseline.** `end-to-end-plan.md` §4 requires
   the ramp to begin at I1 (100 msg/s). loadgen's default
   `--steps=500,1k,2k,5k,10k` starts five times above it and cannot observe a
   breach below 500. Always pass explicit steps
   (`--steps=100,250,500,1k,2k,5k`), then bisect between the last passing and
   first failing step.
2. **Structure every SLO-asserting run as warm-up → send window → settle
   window.** Denominator counts the send window; async numerators wait out to
   the max SLO deadline plus a scrape margin.
3. **Read `miss%` before the percentiles.** A saturated pipeline drops its
   slowest messages, so percentiles *improve* as the system gets worse.
4. **A MongoDB read-capacity number requires a declared working set.** All three
   send-path reads are cache-absorbed (`GATEKEEPER_SUB_CACHE_TTL`,
   history-service `readcache`). State the room/user working set and intended
   cache-hit distribution, then require the observed Mongo command rate to match
   it — otherwise the result is INCONCLUSIVE for Mongo regardless of RPC rate.
5. **Backlog outranks latency.** A run where latency looks fine but a durable's
   `num_pending` climbs monotonically found a bottleneck.
6. **A degraded generator invalidates the window; it does not fail the system.**
   INCONCLUSIVE (GC pause, emit underrun, saturation, dispatch ratio <95%,
   loadgen NATS disconnect) never counts as a pass and never stops a ramp.
7. **`MESSAGE_BUCKET_HOURS` identical on every reader and writer**, and matching
   the TWCS window. A mismatch reads exactly like data loss.
8. **`ENCRYPTION_ENABLED=true` and `ATREST_ENABLED=true`** for acceptance runs;
   plaintext is a diagnostic A/B only.
9. **Record `run_id`, sampler ratio, preset, seed and steps** with every result,
   and retain evidence 24–72 h before teardown.
10. **Repeatability contract — one run is not a result.** The programme had no
    rule for this, so a single ceiling could be environment noise reported as a
    finding. Minimum:
    - **Near the knee, repeat 3 times.** The last passing step and the first
      failing step each get three runs; a step that passes twice and fails once
      is **not** a ceiling, it is the knee's width.
    - **Report the median and the spread**, never a single number. If the spread
      across three runs exceeds ~10% of the value, the result is INCONCLUSIVE
      until the variance is explained — that is the same bar
      `capacity-test-plan.md` already sets for an unexplained ceiling, made
      countable.
    - **Compare like with like**: same box, same image digest, same preset, same
      seed, same sampler ratio, and neighbour activity recorded for each run.
      Numbers from different hosts are not comparable at all.
    - **A regression claim needs two runs on each side**, before and after.
    - **When the run is being compared against a target, the margin must exceed
      the spread.** A median that clears a drafted target by less than the
      run-to-run spread has not met it — the target is inside the environment's
      noise. This is sharper than the 10%-of-value bar above and catches cases
      that bar passes; both apply. Worked example:
      [`first-slo-run-report.md`](first-slo-run-report.md) Part 4.

---

## 6. SLO traceability

| SLO | Highest-ranked item covering it | Enforceable today? |
|---|---|---|
| SLO-1a (persist) | A1/A2 → B1; reconciled by A3/B2 soak ledger | Loadgen read-back only — **G4** for the ratio |
| SLO-1b / SLO-2 (channel broadcast enqueue) | A1/A2 → B1 | **No** — G4 is the enforced boundary; loadgen L1 measures a different, downstream boundary |
| SLO-3 (login) | A9 | Yes, client-side; auth-leg proxy per sli-slo §3 |
| SLO-4 (enter channel) | A5 → B1 | Loadgen L1 only — **G9** for server-side |
| SLO-5 (enter thread) | A5 → B1 | Loadgen L1 only — **G9** |
| SLO-6 (push handoff) | D5 | No |
| SLO-7 (search availability) | A9 | Yes (partial-failure only; full outage needs the prober backstop) |
| SLO-8 (search latency) | A9 | Client-side only — needs the `status` label (P4) |
| SLO-9 (federation) | D4 | No — single-site driver |

---

## 7. Corrections found while writing this

Recorded here rather than silently fixed; each is a one-line edit to the
owning document.

1. **`common/sli-slo.md` §10 is stale on three rows.** It lists SLO-3 as
   "missing — auth is a stub" and SLO-7/8 as "missing — search workload", but
   `max-rps --workload=login` and `--workload=search` both exist
   (`tools/loadgen/maxrps_login.go`, `maxrps_search.go`) and score the spec's
   predicates. The remaining SLO-8 gap is the service-side `status` label, not
   the workload.
2. **loadgen's default message ramp steps contradict `end-to-end-plan.md` §4.**
   See execution rule 1. Either the default should become I1-anchored or the
   plan should state that explicit steps are mandatory.
3. **`performance/capacity-test-plan.md` calls the search workload "the
   Elasticsearch capacity workload".** It drives search-service request/reply;
   with no ES telemetry contract (G7) a breach cannot be attributed to
   Elasticsearch. Worth narrowing to "the search *query-path* workload".

Added in rev 2, from the code review behind
[`extreme-scenarios.md`](extreme-scenarios.md):

4. **`soak/cassandra-soak-plan.md` §1 describes the reply-count scan as bounded
   to `Cap`(99).** `pkg/threadcount` has no cap: it scans the whole
   `thread_messages_by_thread` partition with no `LIMIT`, paging at 5000, under a
   15 s timeout its own comment calls a *"backstop for an unbounded partition"*.
   **Fixed in this branch**, because a soak designed from that sentence
   under-tests the path by the full thread length.
5. **`soak/cassandra-soak-plan.md` §D pins `MESSAGE_BUCKET_HOURS` at envDefault
   `72` with line references that no longer resolve.** Every service defaults to
   **360** (`message-worker/main.go:46`,
   `history-service/internal/config/config.go:53`, `bot-message-worker/main.go:37`).
6. **`docker-local/cassandra/migrations/2026-05-twcs-message-tables.cql:40` sets
   `compaction_window_size` to 72 while the init DDL sets 360.** A cluster built
   from the migration and never re-`ALTER`ed runs a 5× mismatch against the bucket
   window. Both files carry a comment saying the two MUST match.
7. **`failure/nats-jetstream.md` overstated the gatekeeper's retry behaviour.**
   It described an "immediate `Nak()` against `MaxDeliver=5`" burning the budget
   "in seconds". The handler uses `jsretry.Nak` with `DefaultBackoff`
   (1s/5s/30s/2m/10m) and `MaxDeliver` defaults to 6, so the budget is ~12.6
   minutes. **Fixed in this branch**; X9's severity drops accordingly.
8. **`sli-slo.md` §8 lists SLO-1a's numerator as outstanding P2 work.**
   `message_worker_persistence_total{message_kind,result}` already exists and
   records at every persist site. Tracked in
   [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md) §1B.
9. **Every failure doc assumed `MaxDeliver=6` everywhere. Two services are much
   higher.** `message-worker` (`main.go:360`) and `broadcast-worker`
   (`main.go:558`) wrap their settings in `stream.WithOutageRetryBudget`, which
   raises the cap until the client-side schedule spans
   `stream.OutageRetryWindow` = 1 hour — **17 and 18 deliveries**, roughly two
   hours of redelivery, not twelve minutes. `message-gatekeeper` and
   `room-worker` take the plain default and are 6. This changes how long a
   failure test must run before a terminal drop is even possible, and it changed
   the runbook's `t2` cap from ~20 min to ~2 h 16 min. **Fixed in this branch**
   across `failure/nats-jetstream.md`, `failure/cassandra.md`,
   `extreme-scenarios.md` (X1) and the runbook.
10. **X1's drop threshold was wrong for the same reason.** It read "terminally
   drops after five redeliveries"; the path runs in `message-worker`, so it is
   seventeen. The longer budget makes the drop slower, **not less total** — each
   attempt re-runs the same full-partition scan and times out again.

---

## 8. Sibling documents

- [`README.md`](README.md) — program index
- [`extreme-scenarios.md`](extreme-scenarios.md) — code-derived worst-case shapes (Track 3)
- [`p2-instrumentation-spec.md`](p2-instrumentation-spec.md) — what to build for Track 1.1, and what deliberately not to build
- [`slo-measurement-map.md`](slo-measurement-map.md) — every journey's path hop by hop, which instrument sits where, and which segments are dark
- [`common/sli-slo.md`](common/sli-slo.md) — acceptance criteria
- [`common/workload-model.md`](common/workload-model.md) — shared inputs (I1–I13)
- [`common/environments-and-data-ownership.md`](common/environments-and-data-ownership.md) — what may be pushed, blast radius, cleanup
- [`performance/end-to-end-plan.md`](performance/end-to-end-plan.md) — scenario inventory and run structure
- [`performance/capacity-test-plan.md`](performance/capacity-test-plan.md) — ramp-to-breach methodology
- [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md) — Run A specification
- [`failure/overview.md`](failure/overview.md) — fault campaign program
- [`loadgen/observation.md`](loadgen/observation.md), [`loadgen/dashboard-contract.md`](loadgen/dashboard-contract.md) — evidence contracts
