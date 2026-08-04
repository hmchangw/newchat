# End-to-End Load Test Plan — chat platform

> System-level load & capacity plan. The acceptance criteria are the SLOs in
> [`sli-slo.md`](sli-slo.md); shared inputs live in
> [`../common/workload-model.md`](../common/workload-model.md) and
> [`../common/environments-and-data-ownership.md`](../common/environments-and-data-ownership.md).
> Component-level Cassandra work is in [`../cassandra/soak-test-plan.md`](../cassandra/soak-test-plan.md).

| | |
|---|---|
| **Status** | Draft — for review |
| **Author** | Michelle Leu |
| **Driver** | `tools/loadgen` (NATS surface) |
| **Validates** | that current **code / schema / usage-pattern design** meets the SLOs and stays healthy under realistic load, **before** production |
| **Does NOT validate** | the frontend/last-mile (render), true broker breakpoints on shared managed services, cross-site federation at scale (single-site loadgen), or auth/login (stub) — see §7 |
| **A "pass" means** | the exercised scenario met its SLO predicate/target over an isolated run window — an evidence-based readiness signal, **not** a blanket production certification |

**Scope note — loadgen-driven, no frontend.** Every scenario is driven through
the **NATS surface** by `tools/loadgen`. Anything that requires the real
frontend (websocket client, decrypt/render, browser login) is **out of scope
this round** and is tracked as the observational last mile in `sli-slo.md` §2.

---

## 0. How this relates to the SLOs

The SLOs are the **acceptance criteria**; *capacity* = the load at which an SLO
first breaks. Two threshold modes (from `sli-slo.md` §10), applied here:

- **Hard gate** — reuse the production SLO **predicate and target** (e.g. SLO-4 =
  95% within 500 ms), evaluated over an **isolated, quiescent run window**, never
  the 28-day aggregate rule.
- **Engineering headroom** — a separately-named, stricter latency-only guardrail
  (~50–70% of the bound) that *warns* before the SLO breaks; only a hard-gate
  miss fails a release.

**Run structure (required for every SLO-asserting run):** `warm-up → send window
→ settle window`. The denominator counts the **send window**; async numerators
(persist, enqueue, forward) are waited out to the **max SLO deadline + a scrape
margin** so tail-end in-flight events are not misjudged as failures. Traffic
isolation is mandatory — a dedicated test `SITE_ID` or Prometheus tenant (see
`environments-and-data-ownership.md`).

**Today vs enforced.** Most J1/J2 service-side SLI metrics are still on the
roadmap (`sli-slo.md` §8 P1–P4); **loadgen's own L1 correlation already measures
E2E latency**, so near-term runs gate on loadgen L1 while the service-side SLI
metrics land in parallel.

---

## 1. Which test types we run

| Type | Question | Load shape | We run it? | On what |
|---|---|---|---|---|
| **Fixed-load / soak** | holds up at expected peak over time? | pinned at peak | ✅ **primary** | all (incl. managed) |
| **Capacity / ramp-to-breach** | where's the ceiling, what breaks first? | step up until an SLO breaks | ✅ selective | **Mongo** (dedicated), app services, self-hosted ES/Valkey; **Cassandra bounded**; **not** shared NATS to breakpoint |
| **Pathological / schema-stress** | does a worst-case shape break the schema? | deliberate bad shapes, isolated | ✅ targeted | Cassandra F1–F6 (isolated keyspace) |
| **o11y overhead A/B** | does observability cost throughput/latency? | same run, pillar on/off/sampler | ✅ | `sli-slo.md` §8 P-refs; owns `o11y-performance-and-sampling.md` §5 |
| **Resilience / fault injection** | a dependency dies/slows — what degrades? | fixed load + injected fault | ⏳ later | `resilience-test-plan.md` |
| **Spike** | absorb a surge / reconnect storm? | sudden burst | ⏳ later | presence-storm exists; broader later |

**Near-term set = fixed-load soak (SLO validation) + bounded capacity
(Mongo/app/self-hosted) + targeted Cassandra schema-stress + o11y A/B.**
Resilience and spike are phased after.

---

## 2. Worked example — the same journey, two test types (J1 "send a message")

To make the abstract concrete, here is **one journey** exercised two ways:

| | **Capacity (ramp-to-breach)** | **Fixed-load soak** |
|---|---|---|
| **Question** | "at how many msg/s does SLO-2 miss 1 s? what backs up first?" | "at 100 msg/s for N hours, does any SLO drift?" |
| **Load shape** | 500 → 1k → 2k → 5k msg/s, hold each step, stop at first breach | pinned at the expected peak (100 msg/s, workload-model I1) |
| **loadgen** | `max-rps --workload=messages` | `daily` / `soak` at a fixed rate |
| **Verdict** | largest step where **all** SLO signals held = capacity; `BOTTLENECK:` names the culprit | SLO held for the whole run **and** consumer lag / Mongo write latency / memory did not drift |
| **What it can't tell you** | endurance / slow drift (it's short per step) | the ceiling (you never push past peak) |

Both use the **same realistic mix** (channel/DM/thread split, message-size
distribution, room-size Zipf); only the **rate profile** and the **question**
differ. This is the template for every other journey.

---

## 3. Prioritized scenario inventory

Priority = (journey/service criticality) × (likelihood current code/schema has a
problem) × (still-unvalidated). Columns: **SLO** it validates · **SLI metric
(prod)** today (✅ exists / 🔧Pn to build, per `sli-slo.md` §8) · **loadgen**
scenario (✅ exists / ~partial / ✗).

### Tier 1 — most urgent (critical path + unvalidated + plausible risk)

| # | Scenario | Code anchor | Stresses | SLO | SLI metric | loadgen |
|---|---|---|---|---|---|---|
| T1-1 | **Cassandra schema soak (Run A)** | soak-test-plan (unrun) | Cassandra write/read/compaction/disk | — (feeds 1a) | L2 op-duration ✅ · L3 ✅ | ✅ `soak` |
| T1-4 | **Mongo send-path writes** | broadcast-worker `UpdateRoomLastMessage` + `AdvanceSubscriptionLastSeen` per send | Mongo writes | 1a/1b (underlying) | 🔧; L2 mongo ✅ | ✅ `messages` (no Mongo assert) |
| T1-5 | **`SetSubscriptionMentions` UpdateMany fan-out** | broadcast-worker; @all → many sub docs | Mongo write amplification | 1b | 🔧; L2 ✅ | ~ `realistic` (mentions, unasserted) |
| T1-6 | **gatekeeper 2–3 Mongo reads/send** | `GetSubscription` + `FindUserByID` (+ `GetRoomMeta`) | Mongo reads (blocks E1) | 1a/1b | 🔧; L2 ✅ | ✅ `messages` |
| T1-7 | **J1 full-chain E2E @ peak** | gatekeeper→canonical→workers | all | **1a/1b/2** | 🔧 P2; **loadgen L1 ✅** | ✅ `messages`/`daily` |

### Tier 2 — critical but likely sturdier / needs setup

| # | Scenario | Code anchor | Stresses | SLO | SLI metric | loadgen |
|---|---|---|---|---|---|---|
| T2-8 | **notification O(room size)/msg** | `GetMembers` + O(N) filter + ⌈N/100⌉ publishes | NATS + cache/Mongo | 6 | 🔧 P4 | ✅ `botroom` (gates notif backlog) |
| T2-9 | **broadcast big-room fan-out + interest-map** | DM/thread per-recipient; channel via NATS; PR #139 subject split | NATS fan-out / gateway memory | 1b/2 | 🔧 + NATS exporter (P3) | ~ `botroom`/`daily`; interest-map needs exporter |
| T2-10 | **history bucket-walk (enter channel)** | `LoadHistory` multi-bucket | Cassandra read | **4** | 🔧 P1; L1 ✅ + walk-depth | ✅ `history` |
| T2-11 | **room-service `$lookup` aggregation reads** | read-receipt / member / sub enrichment | Mongo aggregation | 4/5 (partial) | 🔧 P1 | ✅ `read-receipt`/`room-read` |
| T2-12 | **outbox FIFO federation throughput** | per-peer `MaxAckPending=1` serializes membership | NATS cross-site | **9** | 🔧 P4 + exporter | ✗ (single-site) |

### Tier 3 — pathological / resilience (later)

Cassandra F1–F6 (tombstone storm, hot partition, TWCS pollution, reaction MAP
width — isolated keyspace) · dependency fault injection (→ `resilience-test-plan.md`)
· search (ES) capacity, auth/login, reconnect/presence storms.

---

## 4. Cassandra: acute (now) vs endurance (deferred)

The staging window currently allows only a **few hours**, not the 3-day soak the
component plan (`../cassandra/soak-test-plan.md` D3) assumes. A few hours is a
**load test, not a soak** — split accordingly:

| Phase 1 — acute (run now) | Phase 2 — endurance (deferred, needs multi-day window) |
|---|---|
| schema/access-patterns correct under concurrent realistic load | compaction backlog accumulation over time |
| steady-state SSTables-per-read, per-read tombstones-scanned | disk-growth trend projection |
| immediate read/write latency, timeouts, errors | slow memory/resource leaks |
| acute hot-partition / wide-partition read degradation | **TWCS 72h window sealing** (even 3 days seals ~1) · gc_grace (≥10d) tombstone eviction |

Phase 1 directly serves the primary goal ("confirm the schema/usage design won't
misbehave before prod"); the time-dependent questions are explicitly **deferred**,
not silently claimed. Pathological F1–F6 remain isolated-keyspace follow-ons.

---

## 5. Observability (three layers)

Same model as the Cassandra plan; the SLI is read from L2 where it exists, else
loadgen L1.

| Layer | Source | Use |
|---|---|---|
| **L1 loadgen** | the driver's own correlation | per-RPC / E2E latency, throughput, error rate, `BOTTLENECK:` attribution (cAdvisor+Prom) |
| **L2 o11y** | service metrics → `:2112` (`:8200` in k8s) | the service-side SLI metrics (`sli-slo.md` §8); DB op durations |
| **L3 server** | infra Grafana / provider dashboards | DB/broker internals + per-node — **the only view for managed services** (see §6) |

Assertion mode counts `eligible` / `good` / `missing-after-deadline` over the
send window with the settle boundary (§0), using the **same production source
counters and good/valid predicates** evaluated over **isolated run-window
deltas** (not the 28-day aggregate rule, not loadgen-local metrics), with a
**drain/reset after warm-up** so warm-up completions can't inflate the numerator.

---

## 6. Managed vs self-hosted (blast radius) — summary

Full treatment in [`../common/environments-and-data-ownership.md`](../common/environments-and-data-ownership.md).
In short: **NATS / Mongo / Cassandra are managed; ES / Valkey are self-hosted;
no dedicated test cluster is provisioned.** Mongo is dedicated to us (CPU
pressure OK); Cassandra is semi-conservative (realistic load OK, pathological
isolated); NATS raw-throughput is not the concern — **our usage patterns** are;
ES/Valkey we own and may drive to capacity in isolation. Managed-service runs
require pre-communication with infra and a recorded blast radius.

---

## 7. Out of scope this round / to confirm

- **Frontend / last mile** (render, websocket, browser login) — loadgen-only.
- **SLO-3 login** — auth is a loadgen stub.
- **Federation at scale (SLO-9)** — loadgen is single-site.
- **Search E2E (SLO-7/8)** — no search workload in loadgen yet (addable).
- **Workload-model inputs** — reuse Cassandra I1–I13 as the baseline; confirm
  I10 (global vs busiest site), I12 (msgs/active-user/day), I8 (reaction meaning).

---

## 8. Sibling documents

- [`sli-slo.md`](sli-slo.md) — acceptance criteria (SLOs).
- [`capacity-test-plan.md`](capacity-test-plan.md) — ramp-to-breach methodology *(stub)*.
- [`resilience-test-plan.md`](resilience-test-plan.md) — dependency fault-injection matrix *(stub)*.
- [`../common/environments-and-data-ownership.md`](../common/environments-and-data-ownership.md) — managed/self-hosted, blast radius, data ownership.
- [`../common/workload-model.md`](../common/workload-model.md) — shared inputs *(stub, seeds from Cassandra I1–I13)*.
