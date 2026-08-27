# Load Testing Documentation

Entry point for newchat load, performance, and failure testing. Documents are
organized by **what kind of test they describe**, so the shared inputs and the
loadgen implementation contracts stay separate from the programs that consume
them.

```text
docs/load-testing/
|-- common/       shared inputs and acceptance criteria (used by every program)
|-- soak/         sustained realistic load, non-destructive
|-- failure/      dependency fault campaigns under continuous load
|-- performance/  end-to-end SLO validation and ramp-to-breach capacity
`-- loadgen/      the load generator's own implementation contracts
```

## Start Here

| You want to know | Read |
|---|---|
| What "pass" means for any run | [`common/sli-slo.md`](common/sli-slo.md) |
| What traffic we model | [`common/workload-model.md`](common/workload-model.md) |
| What may be pushed, and how hard | [`common/environments-and-data-ownership.md`](common/environments-and-data-ownership.md) |
| Does the system meet its SLOs at expected load, and where is the ceiling | [`performance/end-to-end-plan.md`](performance/end-to-end-plan.md) |
| Does the Cassandra schema hold up under sustained realistic load | [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md) |
| Does the system stay correct and recoverable when a dependency breaks | [`failure/overview.md`](failure/overview.md) |
| How the load generator records evidence | [`loadgen/observation.md`](loadgen/observation.md) |
| In what order to run all of it, and what blocks each item | [`execution-priority-plan.md`](execution-priority-plan.md) |

## The Three Programs

All three assert against the same acceptance criteria in
[`common/sli-slo.md`](common/sli-slo.md); they differ in the question they ask.

| Program | Question | Documents |
|---|---|---|
| **Soak** | Does a component hold up under sustained realistic load? | [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md) |
| **Performance / capacity** | Does the system meet its SLOs at expected load, and at what load does an SLO first break? | [`performance/end-to-end-plan.md`](performance/end-to-end-plan.md), [`performance/capacity-test-plan.md`](performance/capacity-test-plan.md) |
| **Failure** | When a dependency dies, degrades, or slows, does the system stay correct, observable, and recoverable? | [`failure/overview.md`](failure/overview.md) |

**Recovery surge** — the backlog replay after a fault is removed — is the one
scenario both the failure and capacity programs can claim. Its mechanics
(redelivery, hinted handoff, retry storms) belong to
[`failure/overview.md`](failure/overview.md); whether the drain exceeds what
storage and downstream services absorb is a capacity question.

## Failure Testing

- [`failure/overview.md`](failure/overview.md) — what the three dependency
  documents share: cross-dependency write paths, failure classes, and the rules
  for reading a result.
- Dependency documents — [`failure/nats-jetstream.md`](failure/nats-jetstream.md),
  [`failure/mongodb.md`](failure/mongodb.md),
  [`failure/cassandra.md`](failure/cassandra.md), and the
  [NATS/JetStream failure-test topology](failure/topology.drawio).
- NATS metric inventory —
  [`failure/nats-metrics-contract.md`](failure/nats-metrics-contract.md)
  (infrastructure, service, advisory, and loadgen metrics).

The MongoDB and Cassandra client metric inventory lives with the other
observability specifications:
[Storage Dependency Metrics](../specs/o11y/storage-dependency-metrics.md).

## Load Generator Contracts

`tools/loadgen` is the single driver for every program. Its own contracts live
under [`loadgen/`](loadgen/):

- [`loadgen/observation.md`](loadgen/observation.md) — the durable evidence
  ledger and the workload lanes implemented today.
- [`loadgen/dashboard-contract.md`](loadgen/dashboard-contract.md) — the
  query-time evidence/impact/correctness dimensions a dashboard must report.
- [`loadgen/runtime-api.md`](loadgen/runtime-api.md) — the disabled-by-default
  runtime `pause`/`resume`/`status` skeleton.
