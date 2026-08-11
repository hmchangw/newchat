# Load Testing Documentation

This directory is the entry point for newchat load and performance testing.
It keeps shared workload assumptions and operational guidance separate from
component-specific and system-wide test plans.

## Two programs, one set of acceptance criteria

Testing is organized as two complementary programs. They ask different
questions and must not be merged, but both assert against the same SLOs.

| Program | Question it answers | Entry point |
|---|---|---|
| **Performance** | Does the system meet its SLOs at expected load, and where is the capacity ceiling? | [`system/end-to-end-load-test-plan.md`](system/end-to-end-load-test-plan.md) |
| **Failure** | When a dependency fails, does the system stay correct, observable, and recoverable? | [`failure/overview.md`](failure/overview.md) |

[`system/sli-slo.md`](system/sli-slo.md) is the platform's user-facing SLI/SLO
specification and the single source of acceptance criteria for **both**.

## Current Plans

**Shared inputs** — [`common/workload-model.md`](common/workload-model.md)
(production traffic inputs) and
[`common/environments-and-data-ownership.md`](common/environments-and-data-ownership.md)
(environment tiers, managed vs self-hosted blast radius, data ownership and
cleanup).

**System-level (performance)**

- [`system/sli-slo.md`](system/sli-slo.md) — SLI/SLO specification.
- [`system/end-to-end-load-test-plan.md`](system/end-to-end-load-test-plan.md) —
  scenario inventory, prioritization, and how the test types relate.
- [`system/capacity-test-plan.md`](system/capacity-test-plan.md) —
  ramp-to-breach methodology, bounded by what each dependency may be pushed to.
- [`system/resilience-test-plan.md`](system/resilience-test-plan.md) — pointer
  into the failure program, kept so the test-type taxonomy has no dead end.

**Failure testing**

- [`failure/overview.md`](failure/overview.md) — shared terminology, dependency
  criticality matrix, campaign lifecycle, loadgen requirements, acceptance
  gates, and combined campaigns.
- [`failure/nats-jetstream.md`](failure/nats-jetstream.md),
  [`failure/mongodb.md`](failure/mongodb.md),
  [`failure/cassandra.md`](failure/cassandra.md) — per-subsystem fault
  mechanics and campaigns.
- [`../specs/o11y/storage-dependency-metrics.md`](../specs/o11y/storage-dependency-metrics.md)
  — metric names, dashboards, and PromQL. Owned by o11y; the failure plans
  reference it rather than restating it.

**Cassandra component**

- [`cassandra/soak-test-plan.md`](cassandra/soak-test-plan.md) — authoritative
  specification for the Run A pre-production soak.
- [`cassandra/run-a-implementation-plan.md`](cassandra/run-a-implementation-plan.md)
  — task-by-task engineering plan for the Run A load generator and its
  Kubernetes deployment assets.

## Structure

```text
docs/load-testing/
|-- README.md
|-- common/
|   |-- workload-model.md
|   |-- environments-and-data-ownership.md
|   |-- kubernetes-runbook.md          (not yet written)
|   `-- result-report-template.md      (not yet written)
|-- system/
|   |-- sli-slo.md
|   |-- end-to-end-load-test-plan.md
|   |-- capacity-test-plan.md
|   `-- resilience-test-plan.md        (pointer into failure/)
|-- failure/
|   |-- overview.md
|   |-- nats-jetstream.md
|   |-- mongodb.md
|   |-- cassandra.md
|   `-- topology.drawio
`-- cassandra/
    |-- soak-test-plan.md
    `-- run-a-implementation-plan.md
```

Files under `common/` marked *not yet written* should be created only when
their first real content exists. User-facing SLI/SLO definitions belong under
`system/`; the Cassandra plan has component-level acceptance criteria and must
not be treated as an end-to-end SLO certification.

Run B/C pathological and direct-CQL experiments remain deferred and are not
part of the Run A implementation plan.

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
`docs/load-testing/system/sli-slo.md` §0.1 and §7 make consumer backlog the *primary*
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
