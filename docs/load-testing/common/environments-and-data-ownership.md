# Environments & Data Ownership

> Shared input for every system-level load test. Defines where tests run, which
> dependencies are managed vs self-hosted, the blast radius on shared
> infrastructure, and the data-ownership/cleanup rules. Consumed by
> [`../performance/end-to-end-plan.md`](../performance/end-to-end-plan.md),
> `../performance/capacity-test-plan.md`, and the failure-testing program under `../failure/`.

| | |
|---|---|
| **Status** | Draft — for review |
| **Key constraint** | NATS/JetStream, MongoDB, and Cassandra each run as a **cluster dedicated to us**, so no other tenant's data shares them. They are **co-located on shared Kubernetes nodes**, so isolation is two-axis: dedicated at the data plane, shared at the resource plane. See §2. |

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

**Isolation has two independent axes, and they do not agree.** Read both before
deciding what a run may do and what its numbers are worth.

- **Data isolation** — whose data lives in the instance. Dedicated means a
  failure or a destructive operation cannot corrupt another tenant's data.
- **Resource isolation** — whose CPU, memory, disk IO, and network the instance
  competes for. Shared Kubernetes nodes mean contention in **both** directions:
  a saturating run can starve co-located pods, and a neighbour can distort the
  run's own measurements.

| Dependency | Hosting | Data isolation | Resource isolation | L3 (server internals) visibility |
|---|---|---|---|---|
| **NATS / JetStream** | Managed, own cluster | **Dedicated** | shared k8s nodes; pods carry CPU/memory requests and limits | provider / infra Grafana + JetStream exporter (consumer state) |
| **MongoDB** | Managed, own cluster | **Dedicated** | shared k8s nodes; pods carry CPU/memory requests and limits | provider / infra Grafana |
| **Cassandra** | Managed, own cluster | **Dedicated** | shared k8s nodes; pods carry CPU/memory requests and limits. **Storage locality unconfirmed** - see §7 | provider / infra Grafana |
| **Elasticsearch** | **Self-hosted** | ours | shared k8s nodes; **placement unconfirmed** - see §7 | ours to instrument (cAdvisor + ES metrics) |
| **Valkey** | **Self-hosted** | ours | shared k8s nodes; **placement unconfirmed** - see §7 | ours to instrument (cAdvisor + Valkey metrics) |

**What requests and limits do and do not bound.** Configured CPU and memory
requests/limits bound those two resources per pod, which removes most of the
node-level contention risk. They bound **neither disk IO nor network
bandwidth**. For a Cassandra soak, whose subject is compaction and disk
behaviour, IO is the axis that matters most and the one the limits do not
cover - hence the open storage-locality question in §7.

**Owning a dependency is not the same as isolating it.** For Elasticsearch and
Valkey, "ours" answers who operates and instruments them, not whose CPU, disk,
or network they compete for. They run on the same Kubernetes substrate, so
their resource isolation is unconfirmed on the same three questions as the
databases: affinity or taints, storage class, and network placement. Until that
placement evidence exists, a capacity result for either is **provisional**, and
accepting one requires node-level utilisation and neighbour-activity evidence
for the run window.

**Current operating assumption:** staging utilisation is low enough that
neighbour interference is not expected to affect results. This is an
observation about present conditions, not a structural guarantee; a run that
produces an unexplained ceiling or latency step should check node-level
neighbour activity before the result is trusted.

The **L1/L2/L3 observability model** (`../performance/end-to-end-plan.md` §5) matters
here: for **managed** services we do **not** own L3 — server internals come from
the provider/infra dashboards, so capacity findings depend on infra
coordination. For **self-hosted** ES/Valkey we own L3 and can read container
resource ceilings directly.

---

## 3. What may be pushed, and how hard

Because each managed dependency is a **cluster dedicated to us**, driving one to
capacity cannot corrupt another tenant's data. The residual concerns are
therefore (a) not corrupting our own data or taking down our own staging, and
(b) node-level resource pressure reaching pods co-located on the same Kubernetes
nodes. CPU and memory requests/limits bound (b) for those two resources; disk IO
and network are not bounded by them. Per-dependency stance:

| Dependency | May drive to capacity? | Stance & rationale |
|---|---|---|
| **MongoDB** | **Yes** | Dedicated to us — CPU pressure is acceptable. It is on the **send path both ends** (gatekeeper `GetSubscription`/`FindUserByID`; broadcast-worker `UpdateRoomLastMessage`/`AdvanceSubscriptionLastSeen`; `SetSubscriptionMentions` fan-out write), so a write-load capacity test is both possible and valuable. Isolate data (own test DB / `SITE_ID`). |
| **Cassandra** | **Bounded** | The cluster is ours, so TWCS pollution and disk growth are not a cross-tenant risk; the bound is a **decision**, not a hosting constraint. Realistic soak (Run A) is fine; **pathological** shapes only in an **isolated keyspace** so a bad shape does not contaminate the soak's own evidence. Non-destructive by decision (soak plan D1). Storage locality is unconfirmed (§7), so IO-bound findings are provisional. |
| **NATS / JetStream** | **Not to broker breakpoint** | Raw broker throughput is robust and not the concern — **our usage patterns** are (consumer lag/ack-pending under fan-out, gateway interest-map memory, notification O(N), outbox FIFO). Validate *consumer keep-up + bounded interest-map* at expected + headroom load; do not ramp the broker to failure - not because it is shared, but because a broker breakpoint is not the question this program asks. |
| **Elasticsearch** | **Blocked pending isolation** | Self-hosted, and driving it to index/query capacity is acceptable *in principle* - but the isolated index this depends on does not exist yet (§7): there is no run-scoped index, owner, expiry, or cleanup verification. Until reads and writes can target a dedicated test index whose deletion teardown verifies, a capacity run writes into persistent shared state, mixes stale documents into later search results, and distorts segment/cache/index-size behaviour across runs. Resolving the index target is a preflight requirement, not a run-time detail. |
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
(`../soak/cassandra-soak-plan.md` §7), generalized to every managed dependency.

---

## 5. Data ownership & cleanup

Driving through real services writes to **every** backend, not just the one under
test. Ownership classes and rules (generalized from the Cassandra plan's D6/§3):

| Class | Examples | Rule |
|---|---|---|
| **Borrowed** | real Mongo `users` | Read-only; **never deleted**. Persist selected active-user IDs in the run manifest so restarts don't shift the population. |
| **Mongo test-owned** | rooms, subscriptions, room keys, thread rooms/subs | Recorded in a run-prefixed ownership ledger; teardown verifies ownership then deletes in paced batches (uses existing service indexes, no teardown-only index). |
| **Cassandra test-owned** | messages, pins, reactions | Retained by default; `TRUNCATE` only for an isolated disposable keyspace with explicit confirmation. No row-by-row delete on a keyspace holding evidence another run depends on. **Retention here is not a cleanup path** - see the disk-reclaim requirement below. |
| **Other side effects** | NATS streams, Elasticsearch, Valkey | Not auto-removed; **record the accepted staging blast radius before the run**. |

Prefer a dedicated `SITE_ID` / Mongo DB / keyspace / ES index. Retain evidence
24–72h before cleanup.

**Cassandra retention has no disk-reclaiming path, and the volume is not small.**
At the planned 100 msg/s a three-day Run A produces roughly 26 million message
operations before mirrored rows, pins, and reactions. The default is to retain
that data, and the 24–72h window above is a retention rule that mandates no
Cassandra cleanup at all. Even the permitted `TRUNCATE`/`DROP` path may not
reclaim disk, because Cassandra can preserve the data in auto-snapshots.
Repeated runs - pathological F1-F6 especially - can therefore accumulate
SSTables or snapshots, distort the compaction and disk-growth evidence that the
soak exists to produce, and eventually exhaust node storage.

Before a repeated or pathological run, one of these must hold, and which one is
an owner decision recorded in §7:

- a run-scoped disposable keyspace (or cluster) plus post-retention teardown
  that clears the applicable snapshots and verifies disk and compaction return
  to baseline; or
- a bounded TTL and an explicit storage budget, both verified before execution,
  **and still an isolated keyspace**.

Keyspace isolation is not optional in either branch. §3 already restricts
pathological shapes to an isolated keyspace, and a TTL bounds how long rows live
without bounding whose evidence they sit beside - a shared keyspace under the
TTL branch would still contaminate another run's compaction and disk-growth
observations. The TTL branch trades the disposable keyspace for a bounded
lifetime, never for shared storage.

Neither branch exists today.

---

## 6. Dependency criticality — summary

Full **failure-mode matrix** (dead / degraded / slow → impact + backstop) is in
[`../failure/overview.md`](../failure/overview.md) §6.1. In
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
  assertion window, `../performance/end-to-end-plan.md` §0) is available on staging.
- **Storage locality for MongoDB and Cassandra**: node-local disk or
  network-attached volumes. This is the highest-value open item for the Cassandra
  soak - CPU/memory limits do not bound IO, so on shared network-attached storage
  the compaction and disk-growth observations the soak exists to produce are not
  attributable to our workload alone.
- **Node affinity or taints** keeping other workloads off the database nodes. If
  present, resource isolation is effectively dedicated too and the neighbour
  caveats in §2 and §3 can be dropped.
- **Placement evidence for the self-hosted Elasticsearch and Valkey** - the same
  three questions, so their capacity numbers stop being provisional (§2).
- **Run-scoped isolation and cleanup for the persistent state teardown does not
  reach.** §5 classes NATS streams, Elasticsearch data, and Valkey keys as "not
  auto-removed", and the implemented teardown covers Mongo ownership plus an
  optional Cassandra `TRUNCATE` only. There is no run-scoped stream, index, or
  key prefix, no named owner, no expiry, and no post-run cleanup verification for
  those three. The 24-72h evidence window is a retention rule, not a cleanup
  mechanism: it does not prevent a later run reading stale state, nor Valkey
  eviction of pre-existing keys, whose refill lands as extra Mongo and
  Elasticsearch load. Deciding this needs both a loadgen teardown change and a
  policy call on whether a capacity run may touch shared persistent state at all,
  so it is recorded here rather than settled in this document.
- **Which Cassandra disk-reclaim path applies** - a run-scoped disposable
  keyspace with verified snapshot clearing, or a bounded TTL with an explicit
  storage budget (§5). Both branches still require an isolated keyspace.
  Neither exists today, and repeated or pathological runs need one before they
  can execute.
