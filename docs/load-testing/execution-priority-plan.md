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
| **Status** | Draft — for review |
| **Answers** | In what order do we run the load tests, and what has to be true first |
| **Does NOT answer** | What "pass" means (§ sli-slo), how to ramp (§ capacity-test-plan), how to read a fault result (§ failure/) |

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
2. **De-risk the expensive window before you book it.** The 3-day Cassandra
   soak is the single most expensive item in the program; its harness
   (evidence ledger, WAL, observers, at-rest DEK preflight) gets a short
   local smoke run first, because a mechanism failure discovered on day 2 of
   a staging soak costs the whole window.
3. **A blocked item is scheduled as its gate, not as a test.** Elasticsearch
   capacity is not "later" — it is "after a run-scoped index with verified
   teardown exists". The gate is the schedulable unit.

---

## 2. Readiness snapshot (verified against the code, 2026-08-27)

What exists decides what can be *asserted*, not just what can be *driven*.

| Capability | State | Consequence for scheduling |
|---|---|---|
| loadgen `messages`/`thread`/`history`/`thread-read`/`room-read`/`read-receipt`/`login`/`search` ramps | **Exists** | Wave A can start today |
| loadgen `soak` (Cassandra Run A + room/member/user/search/presence lanes, durable ledger) | **Exists** | Run A is implementation-ready; gated on environment only |
| loadgen `daily`, `max-room-size`, `members-*`, `presence-*` | **Exists** | Wave A |
| P1 — `rpc_server_duration_seconds` metrics middleware in `pkg/natsrouter` | **Absent** (no metrics middleware in `pkg/natsrouter/middleware.go`) | SLO-4/5 cannot be hard-gated from production counters; gate on loadgen L1 only. Also why `daily`'s `service_errors` verdict arm is permanently zero |
| P2 — `messages_canonical_published_total`, `broadcast_channel_enqueue_total`, `broadcast_channel_enqueue_age_seconds` | **Absent** (no occurrence in the repo) | SLO-1a/1b/2 have **no enforceable server-side boundary**. Every J1 run this wave gates on loadgen L1 E2E correlation, which is a *different, downstream* boundary — record it as observational, never as "SLO-2 passed" |
| `search_service_requests_total{kind,status}` | **Exists** | SLO-7 scorable (partial-failure only) |
| `search_service_request_duration_seconds` `status` label (P4) | **Absent** — histogram carries `kind` only | SLO-8 client-side only (loadgen `--workload=search` scores it); not enforceable from production recording rules |
| JetStream Prometheus exporter with `is_consumer_leader` filtering (P3) | Local overlay only (`nats-exporter` sidecar) | Backlog is the primary enforcement signal for every async SLO — a staging run without it has no backstop |
| loadgen `search_index` observer | **Refused at startup** by design (soak bodies analyze to one token) | Search index-convergence loss is unobservable; do not claim search E2E coverage |
| Isolated staging `SITE_ID` / Mongo DB / Cassandra keyspace | **Unconfirmed** (environments §7) | Blocks every staging SLO-asserting run — counters are shared and monotonic, so isolation is the only denominator control |
| Cassandra disk-reclaim branch (disposable keyspace **or** TTL + budget) | **Neither exists** (environments §5) | Blocks *repeated* and *pathological* Cassandra runs, not the first Run A |
| ES run-scoped index + ES telemetry contract | **Neither exists** | Blocks ES capacity outright |
| Valkey run-scoped key namespace + expiry + verified cleanup | **Absent** | Blocks Valkey capacity execution (damage lands during the run, not at result time) |
| Cross-site federation traffic in loadgen | **Absent** — single-site | SLO-9 unschedulable this round |
| `PUSH_NOTIFICATION` recipient observer | **Absent** | SLO-6 unschedulable this round |

---

## 3. The priority ladder

`Env`: **L** = docker-local (`tools/loadgen/deploy`), **S** = staging.
Ranks within a wave may run in parallel; waves are ordered by dependency.

### Wave A — unblocked, runs today (docker-local)

Numbers are box-relative, not absolute (environments §1). The purpose is
mechanism validation, bottleneck attribution, and a regression baseline.

| # | Item | Question it answers | Driver | Cost | Pass / output |
|---|---|---|---|---|---|
| **A1** | **J1 send-path ceiling + bottleneck attribution** (T1-7, T1-4, T1-6) | Where does the whole send chain break, and which component saturates first? | `max-rps --workload=messages --preset=realistic --inject=frontdoor --steps=100,250,500,1k,2k,5k` | ~45 min | Largest all-signals-pass step + `BOTTLENECK:` line. **This output re-ranks everything below it** |
| **A2** | **J1 fixed-load baseline at I1** | At the declared peak (100 msg/s), is latency in budget and is every durable's backlog flat? | `run --preset=realistic --rate=100`, then `daily --preset=daily-heavy` | ~30 min | `final_pending == 0`, `miss% ≈ 0`, error rate in budget. Becomes the regression reference |
| **A3** | **Soak-harness smoke** | Does the evidence ledger, WAL, observer set and at-rest-DEK preflight actually work before we book the staging window? | `seed --workload=soak` → `soak` with `SOAK_RUN_DURATION=30m`, `SOAK_LEDGER_DIR` set → `teardown --workload=soak` | ~1 h | Dispatch ratio ≥95% per lane, zero `loadgen_failure_invalidations_total`, `unverified` under `max(3, 0.001×eligible)`, WAL flush p95 flat. **Prerequisite for B2/B3** |
| **A4** | **Notification O(N) + broadcast fan-out** (T2-8, T2-9) | How large can a room get before notification-worker's per-message O(N) backs up? | `seed-botroom` → `max-room-size --preset=botroom-medium --rate=200` | ~1 h | `ANSWER: max room size = N` + first tripping signal. Run `--rooms-per-size=1` separately for the Cassandra hot-partition / Mongo room-doc contention probe |
| **A5** | **Enter-channel / enter-thread read cost** (T2-10, SLO-4/5) | Does the bucket walk hold its latency bound, and how deep does it get? | `max-rps --workload=history` and `--workload=thread-read` on `history-medium`, then `history-large` | ~1 h | Per-endpoint p95/p99 + **bucket-walk depth** distribution. Compare blended (`history`) vs focused (`thread-read`) |
| **A6** | **room-service aggregation reads** (T2-11) | Do the read-receipt / mark-as-read `$lookup` paths hold up? | `seed-roomread` → `max-rps --workload=room-read`; `seed --workload=read-receipt` → `max-rps --workload=read-receipt` | ~1 h | Max RPS per RPC; a knee well below the message path is a finding against the aggregation |
| **A7** | **Concurrent-user ceiling** | How many daily-IM users does one site sustain, and what breaks first? | `seed PRESET=daily-heavy` → `daily --steps=1k,2k,5k,10k` | ~1.5 h | `ANSWER: N` + `Next limit:`. Note the two known fixture limitations (large-room gatekeeper rejects, dormant service-error arm) before reading it |
| **A8** | **Presence capacity + reconnect storm** | Max concurrent online population; largest survivable reconnect storm | `presence-capacity --steps=10k,20k,50k`; `presence-storm --storm-mode=graceful` then `silent` | ~1.5 h | Max N held without false-offlines; largest fraction recovering within SLO |
| **A9** | **Login (SLO-3) and search (SLO-7/8) client-side** | Do the two already-measurable SLOs hold under load? | `max-rps --workload=login`; then `--workload=search` **after** A1/A2 have produced indexable traffic | ~45 min | SLO-3 / SLO-7 / SLO-8 good-ratio columns. SLO-8 is client-side only until the `status` label lands |
| **A10** | **Member-add pipeline** | Does the ROOMS → room-worker lane sustain rate, and does per-room cost grow with room size? | `seed-members PRESET=members-heavy` → `members-sustained RATE=1000`; `members-capacity --target-size=5000` on `members-capacity-xl` | ~1 h | `final_pending == 0` at rate; size-bucket latency table flat or explained |
| **A11** | **o11y overhead A/B** | Does observability cost throughput or latency? | A1 repeated with `O11Y_ENABLED` on/off and a fixed `OTEL_TRACES_SAMPLER_ARG` | ~1 h | Delta in max RPS and p95. Required once per release |

### Wave B — staging, gated on isolation and infra coordination

| # | Item | Question | Driver | Gates | Cost |
|---|---|---|---|---|---|
| **B1** | **J1 SLO validation at expected load** (Run 1 Baseline) | Does the system meet its SLO predicates at 100 msg/s + 700 reads/s on production-like backends? | `run`/`daily` at I1, warm-up → send → settle | G1, G2, G4, G6 | half day |
| **B2** | **Cassandra Run A — acute** (T1-1, phase 1) | Do the schema and access patterns hold under concurrent realistic load? | `soak` for a few hours | G1, G3, G5, A3 | ~1 day |
| **B3** | **Mongo send-path write capacity** (T1-4, T1-5) | Where does the send-path write amplification (`UpdateRoomLastMessage`, `AdvanceSubscriptionLastSeen`, `SetSubscriptionMentions`) saturate Mongo? | `max-rps --workload=messages --preset=realistic` with `@all` mention share, asserting on Mongo L2/L3 | G1, G2 | ~1 day |
| **B4** | **Cassandra Run A — endurance** (3 days) | Does compaction backlog, disk growth or latency drift over a multi-day window? | `soak` `SOAK_RUN_MODE=continuous` | B2 green, G5, multi-day window | 3 days + retention |

### Wave C — failure campaigns (`failure/`), each under continuous soak traffic

Run in a **separate** run from B4's evidence soak — a fault window inside the
soak contaminates exactly the compaction/disk evidence the soak exists to
produce. Ordered by dependency criticality (environments §6).

| # | Item | Fault classes | Doc |
|---|---|---|---|
| **C1** | NATS / JetStream | Leader loss, election, reconnect, slow consumer, recovery surge | [`failure/nats-jetstream.md`](failure/nats-jetstream.md) |
| **C2** | MongoDB | Primary loss/election, majority loss, slow query, 2-min planned outage overlay | [`failure/mongodb.md`](failure/mongodb.md) §5 |
| **C3** | Cassandra | Replica loss, LOCAL_QUORUM unavailable, slow coordinator, ambiguous write | [`failure/cassandra.md`](failure/cassandra.md) |

Each needs: an external fault injector and its timestamps, the ledger enabled
(`SOAK_LEDGER_DIR`, `SOAK_RECIPIENT_OBSERVER_ENABLED=true`), and the
recovery-classification window from
[`loadgen/dashboard-contract.md`](loadgen/dashboard-contract.md).

### Wave D — blocked or out of round (scheduled as their gate)

| # | Item | Blocked by | Unblock = |
|---|---|---|---|
| **D1** | Elasticsearch capacity | No run-scoped index with verified teardown; no ES telemetry contract | G7 |
| **D2** | Valkey capacity (incl. cache-fallthrough amplification onto Mongo/ES) | No run-scoped key namespace / expiry / verified cleanup | G8 |
| **D3** | Cassandra pathological F1–F6 | Disk-reclaim branch undecided; isolated keyspace missing | G5 |
| **D4** | Federation SLO-9 (outbox FIFO per-peer, T2-12) | loadgen is single-site | Cross-site traffic in loadgen + a two-site staging topology |
| **D5** | Push SLO-6 | No `PUSH_NOTIFICATION` recipient observer | P4 + observer |
| **D6** | Search index-convergence (E2E) | `search_index` observer refused at startup | Per-message searchable marker in soak payloads |
| **D7** | Collector / OTLP outage | Unowned — no campaign asserts it | Assign an owner |
| **D8** | Frontend last mile, spike scenarios | Out of scope this round | P6/P6b probers |

---

## 4. Gate backlog

Gates are the schedulable unit for everything above that is blocked. Ordered by
how much they unblock.

| Gate | Work | Unblocks | Owner |
|---|---|---|---|
| **G1** | Isolated staging tenant: dedicated `SITE_ID`, Mongo DB, Cassandra keyspace, NATS account | B1, B2, B3, B4, all of Wave C | Infra + us |
| **G2** | Confirm workload-model inputs: I8 meaning, I10 scope, I12; and S1–S4 (fan-out, concurrent members, notification eligibility, cross-site share) | B1, B3 — and every "is this rate realistic" argument | Product + infra |
| **G3** | Managed pre-run coordination: peak load declared, blast radius recorded, abort thresholds agreed, L3 dashboards confirmed | B1–B4, Wave C | Us → infra |
| **G4** | **P2 J1 counters** (`messages_canonical_published_total{broadcast_path}`, `broadcast_channel_enqueue_total`, `broadcast_channel_enqueue_age_seconds` from the JetStream metadata timestamp, `_age_invalid_total{reason}`) | Hard-gating SLO-1a/1b/2 in B1. Until then every J1 verdict is loadgen-L1 observational | App |
| **G5** | Cassandra storage control: pick and verify **one** of — run-scoped disposable keyspace with snapshot clearing, or bounded TTL + storage budget (both over an isolated keyspace) | B4 (repeat runs), D3 | Owner decision + infra |
| **G6** | JetStream exporter on staging with `{is_consumer_leader="true"}` recording rules; custom oldest-pending-age monitor (P3) | The enforcement backstop for every async SLO in B1–B4 and Wave C | Infra |
| **G7** | ES: run-scoped index, named owner, expiry, verified teardown **and** an ES telemetry contract (shards, thread-pool rejection, circuit breaker, merge, watermarks) | D1 | Us |
| **G8** | Valkey: run-scoped key namespace / ownership marker, expiry, verified post-teardown cleanup | D2 | Us |
| **G9** | **P1 natsrouter metrics middleware** (`rpc_server_duration_seconds{subject_pattern, errcode_category}`) | Server-side SLO-4/5; also revives `daily`'s dormant service-error verdict arm | App |
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

---

## 8. Sibling documents

- [`README.md`](README.md) — program index
- [`common/sli-slo.md`](common/sli-slo.md) — acceptance criteria
- [`common/workload-model.md`](common/workload-model.md) — shared inputs (I1–I13)
- [`common/environments-and-data-ownership.md`](common/environments-and-data-ownership.md) — what may be pushed, blast radius, cleanup
- [`performance/end-to-end-plan.md`](performance/end-to-end-plan.md) — scenario inventory and run structure
- [`performance/capacity-test-plan.md`](performance/capacity-test-plan.md) — ramp-to-breach methodology
- [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md) — Run A specification
- [`failure/overview.md`](failure/overview.md) — fault campaign program
- [`loadgen/observation.md`](loadgen/observation.md), [`loadgen/dashboard-contract.md`](loadgen/dashboard-contract.md) — evidence contracts
