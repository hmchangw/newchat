# Resilience Test Plan — chat platform

> **This document has moved.** Dependency failure testing is owned by the
> failure-testing program under [`../failure/`](../failure/overview.md), which
> supersedes the outline that previously lived here.

## Where the content went

| What you are looking for | Where it lives now |
|---|---|
| Dependency criticality, and what dead / degraded / slow does to each dependency | [`../failure/overview.md`](../failure/overview.md) §6.1 |
| Shared failure classes, campaign lifecycle, acceptance gates, combined campaigns | [`../failure/overview.md`](../failure/overview.md) |
| NATS / JetStream fault injection and campaigns (F01–F14) | [`../failure/nats-jetstream.md`](../failure/nats-jetstream.md) |
| MongoDB fault campaigns | [`../failure/mongodb.md`](../failure/mongodb.md) |
| Cassandra fault campaigns | [`../failure/cassandra.md`](../failure/cassandra.md) |
| Storage metric names, dashboards, and PromQL | [`../../specs/o11y/storage-dependency-metrics.md`](../../specs/o11y/storage-dependency-metrics.md) |

## Why this page still exists

The three system-level test types stay distinct, and each answers a different
question about the same platform:

| Test type | Question | Plan |
|---|---|---|
| **Performance / end-to-end** | Does the system meet its SLOs at expected load? | [`end-to-end-load-test-plan.md`](end-to-end-load-test-plan.md) |
| **Capacity** | At what load does an SLO first break, and what breaks first? | [`capacity-test-plan.md`](capacity-test-plan.md) |
| **Resilience / failure** | When a dependency fails, does the system stay correct, observable, and recoverable? | [`../failure/overview.md`](../failure/overview.md) |

All three assert against the same acceptance criteria in
[`sli-slo.md`](sli-slo.md). Keeping this pointer means a reader who arrives from
the test-type taxonomy is not left with a dead end.

## Boundary with the capacity plan

**Recovery surge** — the backlog replay that follows fault removal — is the one
scenario both programs can claim. Treat it as a failure-program scenario whose
*mechanics* (redelivery, hinted handoff, retry storms) belong to
[`../failure/overview.md`](../failure/overview.md) §6, and whose *quantitative
limit* (does drain exceed what storage and downstream services can absorb)
belongs to [`capacity-test-plan.md`](capacity-test-plan.md). Owner assignment for
that overlap is still open.
