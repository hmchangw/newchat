# Loadgen Failure Observation Runtime

This document describes the failure-observation capability implemented by the
continuous Cassandra soak workload. It extends the existing
`seed -> soak -> stopped -> teardown` lifecycle; it is not a separate fault
orchestration mode.

## Execution Model

Loadgen does not inject faults and does not change its traffic profile when a
fault starts. The soak Deployment continuously generates its configured
production-like mix. An operator or an external chaos controller restarts
pods, removes a leader, or injects a network fault independently. Grafana
annotations or the chaos system's timestamps identify the fault window.

```text
seed Job
  -> soak Deployment and warm-up
  -> stable baseline
  -> external fault while the same traffic continues
  -> automatic service recovery and backlog drain
  -> stopped
  -> evidence retention
  -> approved teardown
```

The Helm `phase` value continues to mean deployment lifecycle
(`seed|soak|stopped|teardown`). It is not a fault phase and it does not label
hot-path metrics.

## Implemented Vertical Slice

The first durable operation lane covers user message sends through the real
service path:

```text
loadgen
  -> message-gatekeeper admission reply
  -> MESSAGES_CANONICAL JetStream delivery
  -> message-worker Cassandra write
  -> history-service GetMessageByID reconciliation
```

Before publishing a message, loadgen durably records an intent containing the
message ID, request ID, room, account, timestamps, and a SHA-256 content hash.
Plaintext message content is not written to the ledger. Admission and history
read-back are independent observations, so a reply timeout followed by a
successful read-back remains an availability failure without being
misreported as data loss.

Every generated message operation is eligible. Its terminal result is one of:

- `good`: admission and persisted history both match;
- `bad`: an observation explicitly failed or the persisted record mismatched;
- `missing_after_deadline`: one or more observations were still absent at the
  configured reconciliation deadline.

The invariant for a completed interval is:

```text
eligible = good + bad + missing_after_deadline
```

## Reconciliation Traffic

Reconciliation does not create an unbudgeted reader. A due operation consumes
one slot from the existing soak read lane. When no operation is due, that slot
runs the existing mixed history read. The default 700 reads/s therefore
contains the 100 message read-backs/s rather than silently becoming 800
reads/s.

A missing record or transient history RPC failure is retried on later read
slots until `SOAK_RECONCILE_DEADLINE`. The default is ten minutes. Operators
must set the deadline beyond the longest planned fault plus recovery window.

## Durable Ledger and Restart Recovery

The in-memory ledger is the active correlation index. Its append-only WAL is
stored on a loadgen-only PVC and is authoritative for unresolved operations.
It is never stored in NATS, MongoDB, or Cassandra because those systems are
the fault targets.

The WAL provides:

- durable intent before publish;
- `fsync` on every state transition;
- recovery of unresolved operations after pod replacement;
- repair of an unterminated final record left by a process crash;
- crash-safe compaction to the active set after every 10,000 completed
  operations;
- bounded in-memory capacity with explicit invalidation on overflow or WAL
  failure.

The Helm chart remains a single-replica Deployment with `Recreate`. A
StatefulSet is not required for one writer. The run-specific PVC is mounted at
`/var/lib/loadgen/ledger` and has both Helm keep and Argo CD no-prune
annotations so `phase=stopped` preserves evidence. PVC removal is a separate,
explicit operator action after evidence retention.

An OOM or pod restart still makes traffic/SLO conclusions for the affected
interval inconclusive because offered load and latency observation stopped.
WAL recovery permits data-integrity reconciliation to continue; it does not
turn the interrupted performance interval into a pass.

## Configuration

| Environment variable | Helm value | Default | Purpose |
|---|---|---:|---|
| `SOAK_LEDGER_DIR` | `ledger.mountPath` | empty in the binary; enabled by the Chart | Persistent ledger directory |
| `SOAK_LEDGER_CAPACITY` | `ledger.capacity` | 200,000 | Maximum unresolved in-memory operations |
| `SOAK_RECONCILE_DEADLINE` | `ledger.reconcileDeadline` | 10m | Final missing-data deadline |
| `SOAK_RECONCILE_RETRY_INTERVAL` | `ledger.reconcileRetryInterval` | 1s | Earliest retry after missing/transient read-back |

The Chart defaults to a 20 GiB `ReadWriteOnce` PVC. `ledger.existingClaim`
selects an operator-managed claim. Docker Compose uses the
`loadgen-ledger` named volume.

## Metrics

| Metric | Meaning |
|---|---|
| `loadgen_failure_operations_total{scenario,lane,result}` | Terminal operation outcomes |
| `loadgen_failure_observations_total{scenario,lane,observer,result}` | Admission and Cassandra history observations |
| `loadgen_failure_inflight{scenario,lane}` | Operations awaiting an observation |
| `loadgen_failure_recovered_operations_total` | Unresolved operations loaded from WAL after restart |
| `loadgen_failure_invalidations_total{reason}` | Ledger capacity or WAL failures that invalidate evidence |
| `loadgen_failure_journal_bytes` | Current compacted WAL size |
| `loadgen_nats_connected{pool}` | Instrumented loadgen pool's current NATS connection state |
| `loadgen_nats_connection_events_total{pool,event}` | Connect, disconnect, reconnect, close, and async-error events |
| `loadgen_nats_outage_duration_seconds{pool}` | Client-observed disconnect duration |
| `go_*`, `process_*` | Loadgen runtime and process health |

Operation IDs, message IDs, room IDs, and run IDs are not hot-path metric
labels. Individual unresolved records remain in the WAL; Prometheus receives
only bounded labels.

## Current Boundary

This version is deployable in place of the existing Cassandra soak harness,
but the durable ledger currently covers the user-message admission-to-history
vertical slice. Existing edit, delete, reaction, pin, thread, and history
workloads continue to emit their existing aggregate and sampled verification
metrics. Real bot persistence, migration, MongoDB final-state reconciliation,
federation, and complete JetStream durable/exhaustion observation remain
separate gaps in the subsystem plans.
