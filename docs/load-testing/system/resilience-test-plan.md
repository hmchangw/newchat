# Resilience Test Plan — chat platform *(stub)*

> **Status: outline + dependency criticality matrix.** Fault-injection scenarios
> are a later phase (`end-to-end-load-test-plan.md` §1). This stub fixes the
> matrix that drives them. Acceptance criteria: [`sli-slo.md`](sli-slo.md).
> Environment/blast-radius rules: [`../common/environments-and-data-ownership.md`](../common/environments-and-data-ownership.md).

## Purpose

Under a fixed realistic load, inject dependency faults (**dead / degraded /
slow**) and measure which journeys/SLOs hold, which degrade gracefully, and
which fail — validating the codebase's degradation design before prod.

## Dependency criticality & failure-mode matrix

Grounded in `docs/research/dependency-instability-impact.md` and the code scan
(`end-to-end-load-test-plan.md` §3). "On send path?" is the key axis — an outage
off the send path cannot fail message-send.

| Dependency | Host | Criticality | On send path? | **Dead** | **Degraded / half-dead** | **Slow** | App-layer backstop |
|---|---|---|---|---|---|---|---|
| **NATS/JetStream** | Managed | **Critical** | **Yes** (+ federation) | whole platform stalls: send, broadcast, federation | partial consumer lag; gateway interest-map pressure | E1/E2 latency up, consumers back up | `MaxReconnects=-1`; consumer-lag alert; **no app fallback** |
| **MongoDB** | Managed (dedicated) | **Critical** | **Yes** (both ends) | send fails (gatekeeper sub check), broadcast writes fail | primary stepdown → brief write failure / rollback risk (driver-default concern) | send E1 latency up (`GetSubscription`/`FindUserByID`) | tight ctx deadlines; roommetacache absorbs some reads |
| **Cassandra** | Managed (semi) | **Medium** | **No** (history only) | history reads (J2 enter-channel) fail; **send unaffected** (async worker) | LocalQuorum → reads hard-fail on quorum loss | enter-channel/thread latency up (SLO-4/5) | send decoupled from Cassandra; gocql retry/speculative *(recommended)* |
| **Elasticsearch** | Self-hosted | **Medium** | No | search unavailable (SLO-7/8); **no fallback** | partial-shard → partial results | search latency up (SLO-8) | health/probe; search isolated from core journeys |
| **Valkey** | Self-hosted | **Low** | No | full fall-through: room-meta → Mongo, restricted-room → Mongo/ES; **amplifies Mongo/ES load**, no correctness loss | partial miss → partial amplification | slight send/search latency rise | L1→L2→Mongo ReadThrough; fail-open |

**Key design facts this validates:**
- Cassandra/Valkey outages must **not** fail message-send (decoupling + fail-open).
- A Valkey outage should show as **elevated Mongo/ES load**, not correctness loss
  — worth measuring the amplification, not assuming a free no-op.
- NATS/Mongo are the true single points on the send path — degraded-mode behavior
  there is the highest-value resilience scenario.

## Planned scenarios *(to expand)*

- Per dependency: kill / add latency / drop-and-reconnect under fixed load; assert
  which SLOs hold and that fail-open paths behave as designed.
- **Valkey-down amplification**: measure the Mongo/ES load delta.
- **NATS peer-down (federation)**: per-peer FIFO isolation under a stuck peer
  (needs multi-site — currently a loadgen gap, SLO-9).

*(ES failure-mode detail was not in the dependency research doc — added here from
the code scan; expand with an ES-specific degraded-shard scenario.)*
