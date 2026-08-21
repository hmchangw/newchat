# Capacity Test Plan — chat platform *(stub)*

> **Status: outline.** Ramp-to-breach methodology. Acceptance criteria:
> [`sli-slo.md`](../common/sli-slo.md). What may be pushed and how hard:
> [`../common/environments-and-data-ownership.md`](../common/environments-and-data-ownership.md) §3.

## Purpose

Find the load at which each SLO first breaks (= capacity) and **what breaks
first**, for the parts of the system that may be safely driven to failure.

## Scope — bounded by hosting (see environments §3)

| Target | Ramp-to-breach? | Note |
|---|---|---|
| **App services** (gatekeeper / workers) | ✅ | `max-rps` per workload; `BOTTLENECK:` attribution |
| **MongoDB** | ✅ | dedicated cluster — send-path write capacity. Shared k8s nodes, but pods carry CPU/memory requests and limits |
| **Elasticsearch** | ⚠️ blocked | Self-hosted, sized to prod ratio, but **two preconditions are unmet**: no run-scoped isolated index with verified teardown (`../common/environments-and-data-ownership.md` §7), and no Elasticsearch telemetry contract - cAdvisor alone cannot separate application saturation from shard failures, thread-pool rejection, circuit-breaker trips, merge pressure, or disk watermarks, so a search SLO can breach with no way to name what broke first. Do not report an Elasticsearch capacity result while the required ES series are absent or stale |
| **Valkey** | ⚠️ conditional | Self-hosted, best-effort cache, not correctness-critical, so the Elasticsearch blockers above do **not** apply to it. Do not start a capacity run until a run-scoped key namespace or ownership marker, an expiry, and verified post-teardown cleanup exist. These gate **execution**, not result acceptance: the damage lands during the run, because a Valkey capacity run without them evicts pre-existing keys and the refill arrives as extra Mongo and Elasticsearch load, contaminating neighbouring measurements before any result is judged. The evidence-retention window is not a cleanup mechanism (§7) |
| **Cassandra** | ⚠️ bounded | dedicated cluster; the bound is a decision, not a hosting constraint. Realistic + isolated-keyspace pathological only. Repeated or pathological runs are blocked until §5's storage-control branch is selected and verified - a disposable keyspace with verified snapshot clearing, or a bounded TTL with a storage budget, both over an isolated keyspace (`../common/environments-and-data-ownership.md` §5, §7). Storage locality unconfirmed, so IO-bound ceilings are provisional |
| **NATS/JetStream** | ❌ broker breakpoint | dedicated cluster, but a broker breakpoint is not the question — validate consumer keep-up + bounded interest-map at expected + headroom |

## Method *(to expand)*

- Ordered RPS steps, per-step warm-up → hold → cooldown, stop at first SLO breach
  (loadgen `max-rps`). Report largest passing step + first-failing signal.
- INCONCLUSIVE guard when the load box, not the SUT, is the limit (GC pause /
  emit-underrun) — already in loadgen.
- INCONCLUSIVE guard on the **SUT side** as well: the databases run on shared
  Kubernetes nodes, so a ceiling is only valid for the neighbour state during the
  run. CPU and memory are bounded by pod requests/limits, but **disk IO and
  network are not**. Record node-level neighbour activity for the run window; an
  unexplained ceiling or latency step is INCONCLUSIVE until it reproduces.
- Capacity is reported as **headroom vs current prod load**, not an absolute.

## loadgen coverage today

`max-rps --workload=messages|history|thread|thread-read|read-receipt|room-read|search`,
`members-capacity`, `botroom` (max-room-size), `presence-capacity`. The search
workload drives search-service request/reply load (`tools/loadgen/maxrps_search.go`)
and is the Elasticsearch capacity workload - do not treat ES as uncovered. The
remaining gap is federation (loadgen is single-site). See
[`end-to-end-plan.md`](end-to-end-plan.md) §3.
