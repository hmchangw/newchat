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
| **MongoDB** | ✅ | dedicated — send-path write capacity |
| **Elasticsearch / Valkey** | ✅ (isolated) | self-hosted, sized to prod ratio |
| **Cassandra** | ⚠️ bounded | realistic + isolated-keyspace pathological only; not to failure on shared |
| **NATS/JetStream** | ❌ broker breakpoint | validate consumer keep-up + bounded interest-map at expected + headroom, not broker-to-failure |

## Method *(to expand)*

- Ordered RPS steps, per-step warm-up → hold → cooldown, stop at first SLO breach
  (loadgen `max-rps`). Report largest passing step + first-failing signal.
- INCONCLUSIVE guard when the load box, not the SUT, is the limit (GC pause /
  emit-underrun) — already in loadgen.
- Capacity is reported as **headroom vs current prod load**, not an absolute.

## loadgen coverage today

`max-rps --workload=messages|history|thread|thread-read|read-receipt|room-read`,
`members-capacity`, `botroom` (max-room-size), `presence-capacity`. Gaps: search
(ES), federation (single-site). See `end-to-end-load-test-plan.md` §3.
