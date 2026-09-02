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
|-- slo/          making the SLOs measurable, then calibrating them
`-- loadgen/      the load generator's own implementation contracts
```

## Start Here

**If you are about to run something**, read in this order — each one assumes the
previous:

| # | Read | For |
|---|---|---|
| 1 | [`common/sli-slo.md`](common/sli-slo.md) | What "pass" means. Still the acceptance contract; the calibration programme has not amended it yet |
| 2 | [`common/workload-model.md`](common/workload-model.md) | What traffic we model, and which inputs are still provisional |
| 3 | [`common/environments-and-data-ownership.md`](common/environments-and-data-ownership.md) | What may be pushed, how hard, and who owns the data afterwards |
| 4 | [`execution-priority-plan.md`](execution-priority-plan.md) | **What to run next and what is blocking it.** The scheduling document — four tracks, the gate backlog, and the rules that apply to every run |
| 5 | The plan for the run you are doing | [`slo/first-run-runbook.md`](slo/first-run-runbook.md), [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md), [`performance/end-to-end-plan.md`](performance/end-to-end-plan.md), or [`failure/overview.md`](failure/overview.md) |

**If you have a specific question:**

| You want to know | Read |
|---|---|
| In what order to run everything, and what blocks each item | [`execution-priority-plan.md`](execution-priority-plan.md) |
| Which instrument sits on which hop, and what is dark | [`slo/measurement-map.md`](slo/measurement-map.md) |
| How to run the first SLO-measuring soak | [`slo/first-run-runbook.md`](slo/first-run-runbook.md) |
| What that run has to hand back | [`slo/first-run-report.md`](slo/first-run-report.md) — a filled-in worked example |
| Why SLO-1b and SLO-2 cannot be measured yet | [`slo/p2-instrumentation-spec.md`](slo/p2-instrumentation-spec.md) — rationale only |
| What to build to fix that, as an implementable task | [`slo/p2-implementation-task.md`](slo/p2-implementation-task.md) |
| The worst-case load shapes, read out of the code | [`extreme-scenarios.md`](extreme-scenarios.md) |
| Does the Cassandra schema hold up under sustained realistic load | [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md) |
| Does the system stay correct and recoverable when a dependency breaks | [`failure/overview.md`](failure/overview.md) |
| How the load generator records evidence | [`loadgen/observation.md`](loadgen/observation.md) |

### Two things worth knowing before you read anything else

- **The SLO targets are not settled.** They were drafted by judgement and have
  never been validated. The `slo/` programme exists to replace them with numbers
  measured off a real run. Until [`execution-priority-plan.md`](execution-priority-plan.md)
  Track 1.3 lands an amendment, [`common/sli-slo.md`](common/sli-slo.md) stays
  the binding contract for any **gating** run — and the first calibration run
  gates nothing.
- **Three identifier prefixes, three meanings.** `Gn` is a **gate** — external
  work blocking a programme item, listed in the plan's gate backlog. `PRE-n` is a
  **precondition** for one specific run, in that run's runbook. `P1`/`P2`/`P3`
  is an **instrumentation priority tier** — how urgent a missing metric is. A
  gate and a precondition can name the same work (G1 is PRE-3, G2 is PRE-7).

## The Four Programs

All four assert against the same acceptance criteria in
[`common/sli-slo.md`](common/sli-slo.md); they differ in the question they ask.
The fourth is the newest and, right now, the one gating the others: until the
SLOs are measurable, the other three produce evidence but no verdict.

| Program | Question | Documents |
|---|---|---|
| **Soak** | Does a component hold up under sustained realistic load? | [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md) |
| **Performance / capacity** | Does the system meet its SLOs at expected load, and at what load does an SLO first break? | [`performance/end-to-end-plan.md`](performance/end-to-end-plan.md), [`performance/capacity-test-plan.md`](performance/capacity-test-plan.md) |
| **Failure** | When a dependency dies, degrades, or slows, does the system stay correct, observable, and recoverable? | [`failure/overview.md`](failure/overview.md) |
| **SLO calibration** | Can we measure our SLOs at all — and what values are actually reachable? | [`slo/first-run-runbook.md`](slo/first-run-runbook.md), [`slo/measurement-map.md`](slo/measurement-map.md), [`slo/p2-implementation-task.md`](slo/p2-implementation-task.md) |

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
