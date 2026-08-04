# Environments & Data Ownership

> Shared input for every system-level load test. Defines where tests run, which
> dependencies are managed vs self-hosted, the blast radius on shared
> infrastructure, and the data-ownership/cleanup rules. Consumed by
> [`../system/end-to-end-load-test-plan.md`](../system/end-to-end-load-test-plan.md),
> `capacity-test-plan.md`, and `resilience-test-plan.md`.

| | |
|---|---|
| **Status** | Draft — for review |
| **Key constraint** | No dedicated test cluster is provisioned for the managed dependencies; tests run against **shared** managed instances. |

---

## 1. Environment tiers

| Tier | What it is | Dependencies | Use |
|---|---|---|---|
| **Local (docker-local)** | `tools/loadgen/deploy` brings up every service + NATS/Mongo/Cassandra/Valkey/ES **as containers** | all containerized, disposable | mechanism/functional runs, per-endpoint capacity on a single box (numbers are box-relative, not absolute) |
| **Staging (managed-backed)** | real services against the **managed** NATS/Mongo/Cassandra + **self-hosted** ES/Valkey | production-like backends | SLO validation, managed-service behavior, blast-radius-bounded capacity |

loadgen today targets **docker-local** by default; validating managed-service
behavior requires pointing a run at the staging backends. The two tiers answer
different questions — box-relative capacity (local) vs production-representative
SLO/behavior (staging).

---

## 2. Dependency classification & who owns observability

| Dependency | Hosting | Dedicated to us? | L3 (server internals) visibility |
|---|---|---|---|
| **NATS / JetStream** | Managed | shared | provider / infra Grafana + JetStream exporter (consumer state) |
| **MongoDB** | Managed | **yes (dedicated)** | provider / infra Grafana |
| **Cassandra** | Managed | shared (semi) | infra Grafana (per-node often missing on shared clusters) |
| **Elasticsearch** | **Self-hosted** | ours | ours to instrument (cAdvisor + ES metrics) |
| **Valkey** | **Self-hosted** | ours | ours to instrument (cAdvisor + Valkey metrics) |

The **L1/L2/L3 observability model** (`end-to-end-load-test-plan.md` §5) matters
here: for **managed** services we do **not** own L3 — server internals come from
the provider/infra dashboards, so capacity findings depend on infra
coordination. For **self-hosted** ES/Valkey we own L3 and can read container
resource ceilings directly.

---

## 3. What may be pushed, and how hard

Because there is no dedicated managed test cluster, the concern is **not** other
tenants but **not corrupting our own data / not taking down our own staging**.
Per-dependency stance:

| Dependency | May drive to capacity? | Stance & rationale |
|---|---|---|
| **MongoDB** | **Yes** | Dedicated to us — CPU pressure is acceptable. It is on the **send path both ends** (gatekeeper `GetSubscription`/`FindUserByID`; broadcast-worker `UpdateRoomLastMessage`/`AdvanceSubscriptionLastSeen`; `SetSubscriptionMentions` fan-out write), so a write-load capacity test is both possible and valuable. Isolate data (own test DB / `SITE_ID`). |
| **Cassandra** | **Bounded** | Semi-conservative. Realistic soak (Run A) is fine; **pathological** shapes only in an **isolated keyspace**. Non-destructive by decision; TWCS pollution of a shared keyspace must be avoided. |
| **NATS / JetStream** | **Not to broker breakpoint** | Raw broker throughput is robust and not the concern — **our usage patterns** are (consumer lag/ack-pending under fan-out, gateway interest-map memory, notification O(N), outbox FIFO). Validate *consumer keep-up + bounded interest-map* at expected + headroom load; do not ramp the shared broker to failure. |
| **Elasticsearch** | **Yes (isolated)** | Self-hosted — may drive to index/query capacity in an isolated index; size the test node to prod ratios. |
| **Valkey** | **Yes (isolated)** | Self-hosted, best-effort cache — may drive to cluster capacity in isolation; not correctness-critical. |

---

## 4. Managed-service pre-run coordination (required)

Before any staging run that materially loads a **managed** dependency:

- [ ] State the **expected peak load** and duration to the infra/platform team.
- [ ] Record the **blast radius**: which shared instances/keyspaces/streams the
      run touches, and what else lives there.
- [ ] Confirm the managed service's **scaling behavior matches prod** (provisioned
      throughput / autoscale / burst credits) so capacity numbers are meaningful.
- [ ] Agree **abort thresholds** and a rollback/stop plan (who can pull the plug).
- [ ] Confirm **L3 metric availability** on the provider/infra dashboards for the
      run window (see §2 — we don't own managed L3).
- [ ] Confirm **data isolation** (dedicated `SITE_ID` / DB / keyspace / test index).

This mirrors the Cassandra plan's "Items to Confirm With Infrastructure"
(`../cassandra/soak-test-plan.md` §7), generalized to every managed dependency.

---

## 5. Data ownership & cleanup

Driving through real services writes to **every** backend, not just the one under
test. Ownership classes and rules (generalized from the Cassandra plan's D6/§3):

| Class | Examples | Rule |
|---|---|---|
| **Borrowed** | real Mongo `users` | Read-only; **never deleted**. Persist selected active-user IDs in the run manifest so restarts don't shift the population. |
| **Mongo test-owned** | rooms, subscriptions, room keys, thread rooms/subs | Recorded in a run-prefixed ownership ledger; teardown verifies ownership then deletes in paced batches (uses existing service indexes, no teardown-only index). |
| **Cassandra test-owned** | messages, pins, reactions | Retained by default; `TRUNCATE` only for an isolated disposable keyspace with explicit confirmation. No row-by-row delete on shared keyspaces. |
| **Other side effects** | NATS streams, Elasticsearch, Valkey | Not auto-removed; **record the accepted staging blast radius before the run**. |

Prefer a dedicated `SITE_ID` / Mongo DB / keyspace / ES index. Retain evidence
24–72h before cleanup.

---

## 6. Dependency criticality — summary

Full **failure-mode matrix** (dead / degraded / slow → impact + backstop) is in
[`../system/resilience-test-plan.md`](../system/resilience-test-plan.md). In
short, criticality ranks: **NATS (send path + federation) > MongoDB (control
plane + send path) > Cassandra (history only, off the send path) ≈
Elasticsearch (search only) > Valkey (best-effort cache, graceful degrade)**.
Two consequences for load testing:

- **Cassandra and Valkey outages do not block message-send** (message-worker
  persists async off `MESSAGES-CANONICAL`; Valkey falls through to Mongo). A
  Cassandra/Valkey load problem shows up as **read/notify degradation**, not send
  failure.
- **A Valkey outage amplifies Mongo/ES load** (room-meta and restricted-room
  caches fall through) — a resilience scenario worth measuring, not just a
  graceful no-op.

---

## 7. To confirm with infrastructure

- Managed NATS/Mongo/Cassandra: version, topology/RF, per-node spec, staging vs
  prod node counts, throughput ceilings, maintenance windows, L3 dashboard access.
- Self-hosted ES/Valkey: staging node sizing vs prod ratio.
- Whether a quiescent/dedicated **test `SITE_ID`** (traffic isolation for the SLO
  assertion window, `end-to-end-load-test-plan.md` §0) is available on staging.
