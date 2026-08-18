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

## The Three Programs

All three assert against the same acceptance criteria in
[`common/sli-slo.md`](common/sli-slo.md); they differ in the question they ask.

| Program | Question | Plans |
|---|---|---|
| **Soak** | Does a component hold up under sustained realistic load? | [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md) |
| **Performance / capacity** | Does the system meet its SLOs at expected load, and at what load does an SLO first break? | [`performance/end-to-end-plan.md`](performance/end-to-end-plan.md), [`performance/capacity-test-plan.md`](performance/capacity-test-plan.md) |
| **Failure** | When a dependency dies, degrades, or slows, does the system stay correct, observable, and recoverable? | [`failure/overview.md`](failure/overview.md) |

**Recovery surge** — the backlog replay after a fault is removed — is the one
scenario both the failure and capacity programs can claim. Its mechanics
(redelivery, hinted handoff, retry storms) belong to
[`failure/overview.md`](failure/overview.md) §6; whether the drain exceeds what
storage and downstream services absorb is a capacity question. Owner assignment
for that overlap is still open.

## Failure Testing

- [`failure/overview.md`](failure/overview.md) — the shared NATS/JetStream,
  MongoDB, and Cassandra program: service coverage, campaign lifecycle, evidence
  model, acceptance gates.
- Dependency plans — [`failure/nats-jetstream.md`](failure/nats-jetstream.md),
  [`failure/mongodb.md`](failure/mongodb.md),
  [`failure/cassandra.md`](failure/cassandra.md), and the
  [NATS/JetStream failure-test topology](failure/topology.drawio).
- NATS campaign readiness —
  [`failure/nats-metrics-contract.md`](failure/nats-metrics-contract.md) (required
  infrastructure, service, advisory, and loadgen metrics),
  [`failure/observability-deployment.md`](failure/observability-deployment.md)
  (scraping, recording rules, dashboards, annotations, preflight, retention), and
  [`failure/nats-campaign-runbook.md`](failure/nats-campaign-runbook.md) (the first
  operator-ready campaign, scenarios F01/F02/F03/F04/F07).

The shared MongoDB and Cassandra metric inventory stays in the observability
specification area:
[Storage Dependency Metrics and Dashboard Contract](../specs/o11y/storage-dependency-metrics.md).
Those metrics are a service-side emission contract, read by whoever instruments
the service, not only by whoever runs a campaign.

**Ownership boundary.** Loadgen generates continuous production-shaped traffic
and records observable effects. It does **not** inject or schedule dependency
faults. The campaign operator performs the declared fault independently and
records its timestamps; loadgen continues through baseline, disruption, and
recovery without changing traffic merely because the dependency state changed.

## Load Generator Contracts

`tools/loadgen` is the single driver for every program. Its own contracts live
under [`loadgen/`](loadgen/):

- [`loadgen/observation.md`](loadgen/observation.md) — the durable evidence
  ledger and the workload lanes implemented today.
- [`loadgen/evidence-platform.md`](loadgen/evidence-platform.md) — the
  generalized operation, observer, manifest, timeline, verdict, and report
  contracts required to close the remaining gaps.
- [`loadgen/dashboard-contract.md`](loadgen/dashboard-contract.md) — the
  query-time evidence/impact/correctness dimensions a dashboard must report.
- [`loadgen/runtime-api.md`](loadgen/runtime-api.md) — the disabled-by-default
  runtime `pause`/`resume`/`status` skeleton.
- [`loadgen/run-a-implementation-plan.md`](loadgen/run-a-implementation-plan.md)
  — the task-by-task engineering plan for the Run A harness and its Kubernetes
  deployment assets.

Run B/C pathological and direct-CQL experiments remain deferred and are not part
of the Run A implementation plan.

## Open Prerequisites

Items that block a meaningful run but are not owned by the load generator.
They will not resolve on their own — each needs an owner outside this repo's
application code.

### Observability parity between the local overlay and the real test cluster

`tools/loadgen/deploy/prometheus/prometheus.yml` scrapes three sources beyond
loadgen's own series: the `prometheus-nats-exporter` sidecar on `:7777`
(JetStream consumer backlog), the o11y SDK endpoint on `:2112` via Docker
service discovery, and per-service counters on `:9090`.

**That covers the docker-compose path only.** The Helm chart under
`tools/loadgen/deploy/k8s/` carries no scrape config of its own — it relies on
`prometheus.io/scrape` pod annotations and a cluster-wide Prometheus, and those
annotations cover the loadgen pod (`:9099`), not the services under test or the
NATS server.

So a run executed in Kubernetes has **no consumer-backlog signal** unless the
cluster is separately configured for it. That matters more than it sounds:
`docs/load-testing/common/sli-slo.md` §0.1 and §7 make consumer backlog the *primary*
enforcement signal for every asynchronous SLO (1a/1b/2/6/9), because the event
ratios behind those SLOs stay approximate until an exact outcome ledger exists.
A Kubernetes run without it can report healthy latency while a worker falls
steadily behind.

Required before a Kubernetes run is trustworthy — all ops/IaC, none of it
application code:

1. A `prometheus-nats-exporter` (run with `-jsz=all`) against the test
   cluster's NATS monitoring endpoint, scraped by the cluster Prometheus.
2. Cluster Prometheus scraping the services under test on `:2112` (o11y SDK)
   and `:9090` (per-service counters).
3. Confirmation that loadgen's `BOTTLENECK_PROM_URL` points at a Prometheus
   that can see all of the above — bottleneck attribution reads from it, and a
   Prometheus that only sees loadgen will attribute every bottleneck to the
   load box.

Until these exist, treat Kubernetes results as latency-and-throughput only, and
do not read a green run as evidence that the asynchronous pipeline kept up.
