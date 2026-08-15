# Loadgen Failure Observation Runtime

The Cassandra soak workload provides continuous, durable observation. It does
not inject faults, schedule campaign phases, or calculate a Go-side verdict.
Operators select fault and recovery windows at dashboard query time while the
same open-loop traffic mix continues through baseline, disruption, and
recovery.

## Durable message observation

Before each message publish attempt, loadgen appends and fsyncs a versioned WAL
intent. After the publish attempt begins, the operation is active even if the
NATS client returns an error because the downstream outcome is ambiguous.
`not_sent` is reserved for a proven local pre-publish failure and uses the
bounded reason `publish_local_error`.

Every admitted message requires independent admission and Cassandra-history
effects. When `SOAK_RECIPIENT_OBSERVER_ENABLED=true`, newly admitted messages
also require an account-attributed recipient event for every subscribed human
recipient. Disabled recipient observation creates no recipient effect and no
recipient `unverified` result.

Terminal results are `good`, `bad`, `unverified`, `not_sent`, and
`missing_after_deadline`. `bad` and authoritative absence retain bounded
reasons such as `admission_rejected`, `history_content_mismatch`,
`history_missing`, `recipient_duplicate`, `recipient_unexpected`,
`recipient_identity_mismatch`, and `recipient_missing`. Exact identifiers stay
in the WAL or retained recipient journals, never Prometheus labels.

The WAL header stores the observer contract. A restart must use the same
configured observer set and recipient enablement. A mismatch fails startup with
an instruction to use a new `SOAK_RUN_ID`; it never silently reinterprets
pending operations. A headerless or contract-less legacy WAL is adopted only
when every pending operation matches the current contract, then atomically
upgraded.

## Traffic independence

The workload remains open-loop. Each pacing event has exactly one outcome:

```text
intended = dispatched
         + scheduler_underrun
         + lane_saturation
         + global_saturation
```

The configured target is a gauge and is not substituted for the exact intended
counter. Downstream latency never reduces the configured target or introduces
backpressure; overload is reported as lane/global saturation. Reconciliation
borrows at most `SOAK_RECONCILE_READ_SHARE` from the existing read lane and does
not add an unbudgeted reader.

Ledger capacity, WAL, observer, and queue failures degrade evidence but do not
stop safe traffic. The single-replica `Deployment` uses `Recreate` with a
retained `ReadWriteOnce` PVC, so unresolved operations resume after replacement.

## Configuration

| Environment variable | Default | Purpose |
|---|---:|---|
| `SOAK_LEDGER_DIR` | empty | Persistent WAL and recipient-journal directory |
| `SOAK_LEDGER_CAPACITY` | `200000` | Maximum unresolved in-memory operations |
| `SOAK_RECONCILE_DEADLINE` | `10m` | Authoritative absence deadline |
| `SOAK_RECONCILE_RETRY_INTERVAL` | `1s` | Earliest history retry |
| `SOAK_RECONCILE_READ_SHARE` | `0.5` | Maximum read-lane reconciliation share |
| `SOAK_RECIPIENT_OBSERVER_ENABLED` | `false` | Opt in to exact recipient observation |
| `SOAK_RECIPIENT_OBSERVER_QUEUE` | `8192` | Bounded recipient callback queue |
| `SOAK_RECIPIENT_OBSERVER_CONNECTIONS` | `32` | Bounded account-attributed NATS connection pool |

Helm exposes the same opt-in as
`recipientObserver.enabled|queue|connections`. Queue and connection bounds are
validated even while disabled.

The dashboard interpretation contract is defined in
[`failure-testing/dashboard-evidence-contract.md`](failure-testing/dashboard-evidence-contract.md).
