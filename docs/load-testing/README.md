# Load Testing Documentation

This directory is the entry point for newchat load and performance testing.
It keeps shared workload assumptions and operational guidance separate from
component-specific and system-wide test plans.

## Current Plans

- [`cassandra/soak-test-plan.md`](cassandra/soak-test-plan.md) is the
  authoritative specification for the Cassandra Run A pre-production soak.
- [`cassandra/run-a-implementation-plan.md`](cassandra/run-a-implementation-plan.md)
  is the task-by-task engineering plan for the Run A load generator and its
  Kubernetes deployment assets.

## Intended Structure

As the test program grows, shared inputs and system-level contracts should be
added without merging them into the Cassandra component plan:

```text
docs/load-testing/
|-- README.md
|-- common/
|   |-- workload-model.md
|   |-- environments-and-data-ownership.md
|   |-- kubernetes-runbook.md
|   `-- result-report-template.md
|-- cassandra/
|   |-- soak-test-plan.md
|   `-- run-a-implementation-plan.md
`-- system/
    |-- sli-slo.md
    |-- end-to-end-load-test-plan.md
    |-- capacity-test-plan.md
    `-- resilience-test-plan.md
```

The directories under `common/` and `system/` should be created only when
their first real document is ready. In particular, user-facing SLI/SLO
definitions belong under `system/`; the Cassandra plan has component-level
acceptance criteria and must not be treated as an end-to-end SLO
certification.

Run B/C pathological and direct-CQL experiments remain deferred and are not
part of the Run A implementation plan.
