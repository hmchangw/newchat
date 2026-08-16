# Loadgen Failure Observation Runtime

The Cassandra soak workload provides continuous, durable observation. It does
not inject faults, schedule campaign phases, or calculate a Go-side verdict.
Operators select fault and recovery windows at dashboard query time while the
same open-loop traffic mix continues through baseline, disruption, and
recovery.

## Durable message observation

Before each message publish attempt, loadgen appends a versioned WAL intent and
waits for its grouped fsync durability barrier. Concurrent intents may share a
barrier for up to 10 milliseconds. Non-intent lifecycle records use the same
bounded group-commit writer and are fsynced by the next barrier, the 10 ms
flush timer, compaction, or shutdown. After the publish attempt begins, the operation is active even if the
NATS client returns an error because the downstream outcome is ambiguous.
`not_sent` is reserved for a proven local pre-publish failure and uses the
bounded reason `publish_local_error`.

Every admitted message requires independent admission and Cassandra-history
effects. When `SOAK_RECIPIENT_OBSERVER_ENABLED=true`, newly admitted messages
also require an account-attributed recipient event for every subscribed human
recipient. Disabled recipient observation creates no recipient effect and no
recipient `unverified` result.

Recipient expectations retain a bounded route (`room` or `user`), set source,
and completeness flag in the WAL. The room observer treats one global and one
local copy of the same logical room event as one delivery; a repeated copy on
the same route remains duplicate evidence. Locally tracked thread followers
survive recent-message eviction. An externally discovered thread parent has an
incomplete follower set, so absence and unexpected-recipient claims are
`unverified` rather than false violations.

Ordinary expected recipient deliveries remain in bounded memory and are not
fsynced individually. Exact anomalies are durably recorded when observed;
authoritative missing-recipient sets and terminal `unverified` markers are
flushed as batches. Every positive result has a completed sidecar barrier before
it can be recorded in the ledger. A failed barrier downgrades the result to `unverified`,
marks the observer down, and invalidates the evidence interval. Durable positive
sidecars are replayed after restart; ordinary recovered recipient operations
without such evidence remain `unverified` because pre-restart observer-health
coverage cannot be reconstructed.

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

WAL and sidecar performance are directly observable through
`loadgen_failure_wal_append_duration_seconds`,
`loadgen_failure_wal_flush_duration_seconds`,
`loadgen_failure_wal_flush_batch_size`,
`loadgen_failure_wal_appends_total`,
`loadgen_failure_evidence_flush_duration_seconds`, and
`loadgen_failure_evidence_records_total`. Rising WAL flush latency together
with falling dispatch ratio or lane/global saturation identifies evidence I/O
as a loadgen bottleneck. Sidecar flush errors or latency are independent from
the recipient callback queue and invalidate positive absence claims.

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
| `SOAK_LEDGER_EPOCH` | `v1` | Evidence-journal identity, separate from the run ID |
| `SOAK_MEMBER_MUTATION_RATE` | `2` | Member add/remove cycles per second |
| `SOAK_ROOM_MUTATION_RATE` | `1` | Rename and mute toggles per second, alternating |
| `SOAK_ROOM_READ_RATE` | `20` | Room reads per second; also funds reconciliation |
| `SOAK_ROOM_CREATE_RATE` | `0.05` | Room creations per second until the budget is spent |
| `SOAK_ROOM_CREATE_BUDGET` | `2000` | Total rooms the create lane may add in one run |
| `SOAK_ROOM_CREATE_SIZE` | `5` | Members a created room starts with |
| `SOAK_ROOM_RECONCILE_READ_SHARE` | `0.5` | Maximum room-read share reconciliation may claim |
| `SOAK_MEMBER_QUARANTINE_MAX` | `10000` | Bounded parked room/account pairs awaiting a re-probe |
| `SOAK_READ_RECEIPT_RATE` | `5` | Mark-read operations per second |
| `SOAK_PRESENCE_RATE` | `30` | Presence signals per second |
| `SOAK_PRESENCE_CONNECTIONS` | `2000` | Simulated presence connections held online |
| `SOAK_PRESENCE_QUERY_SHARE` | `0.1` | Share of presence slots spent verifying instead of signalling |
| `SOAK_PRESENCE_QUERY_BATCH` | `50` | Accounts per batch presence query |
| `SOAK_PRESENCE_SETTLE` | `5s` | Grace before a published signal is compared |
| `SOAK_PRESENCE_TTL` | `5m` | Connection TTL past which an absence is legal |

Helm exposes the same opt-in as
`recipientObserver.enabled|queue|connections`. Queue and connection bounds are
validated even while disabled.

The dashboard interpretation contract is defined in
[`failure-testing/dashboard-evidence-contract.md`](failure-testing/dashboard-evidence-contract.md).

## Room and member lanes

The continuous soak also exercises room and member state. The lanes ride the
same `phase: seed -> soak -> stopped` lifecycle; there is no extra Deployment
and no extra phase.

| Lane | Operations | Ledger |
|---|---|---|
| `member_mutation` | `member_add`, `member_remove` | `admission` + `room_state` |
| `room_mutation` | `room_rename`, `mute_toggle` | `admission` + `room_state` |
| `room_create` | `room_create` | `admission` + `room_state` |
| `read_receipt` | `message_read` | `admission` + `room_state` |
| `room_read` | member list, rooms-info batch, subscription list, read receipts | none |
| `presence` | hello / ping / activity / bye, batch query | none |

`room_read` is read-only. A read has no expected side effect to reconcile, so
it emits latency, error, and result metrics and creates no ledger operation.
`presence` is a special case covered below.

### The room_state observer

Room and member effects are reconciled through one observer with two sources:

1. **room-service RPC** — the path a real client uses.
2. **MongoDB primary read** — the authoritative arbiter.

The store settles disagreements. room-service returning nothing can mean its
read path is degraded, while a primary read that finds the state proves the
write landed, so an absence claim requires the authoritative source to agree or
to be unreachable. Reads are explicitly routed at the primary because the
shared soak client connects with `SecondaryPreferred`; a lagging secondary
would otherwise report a completed write as missing and turn replication lag
into a false data-loss claim during exactly the failures being measured.

Rename and mute additionally separate "the change did not land" from a state
that could not legally occur — a name nobody asked for, or a subscription that
vanished — so a lost mutation is never confused with a corrupted one.

### Result rules

- `missing_after_deadline` requires the admission observer to have recorded
  `good`. An ambiguous request that left no state is `unverified`, never a
  data-loss claim.
- `not_sent` is reserved for failures proven not to have left the process: a
  body that could not be encoded, or a request never attempted. A timeout or a
  dropped connection stays ambiguous.
- Mutations are never resent. They are not idempotent — a replayed remove drops
  a member the first attempt already removed and a replayed mute toggle
  restores the state it just changed — so ambiguity is settled by
  reconciliation, not by retrying.
- A mutation the ledger refuses is not sent at all. Message traffic keeps
  flowing when the ledger degrades because holding the offered load matters
  more; a room mutation does the opposite, because untracked state drift cannot
  be reconciled afterwards.

### Candidate cycling and quarantine

Each room owns a ring of reusable accounts. A candidate is added, verified,
then removed and returned to the ring, so room size oscillates by one and the
pool never runs dry. A per-room lease and a per-account mute lease keep two
conflicting operations off the same target.

A mutation that ends `unverified` leaves the real state unknown. Acting on a
guess would re-add an existing member or toggle a mute twice, so the pair is
parked and re-probed from the room-read lane until its state is known again.

Quarantine overflow is a **reversible traffic degradation**, reported through
`loadgen_soak_room_pool_degraded` and `loadgen_soak_room_pool_exhausted_total`.
It never invalidates the ledger: invalidation is a one-way, process-lifetime
latch, so using it here would make every later fault window in a multi-day run
inconclusive. Operators should gate on offered load instead — a lane whose
`loadgen_soak_dispatched_total` falls below 90% of
`loadgen_soak_configured_rate` over a window makes that window inconclusive for
that lane only.

### Room creation budget

Created rooms accumulate in MongoDB with no safe delete path before teardown,
so the lane stops once `SOAK_ROOM_CREATE_BUDGET` is spent while every other
room and member lane keeps running. A confirmed room is stamped with the run's
ownership marker and appended to an ownership chunk, because teardown only
deletes rooms carrying that marker and only reaches rooms a chunk lists.

Created rooms join the room-read mix only. They stay out of the message and
history lanes, which build their recipient expectations, room distribution, and
pinned catalog once at startup — adding rooms mid-run would shift the traffic
profile that a fault window is compared against, and a new room's encryption
key is provisioned asynchronously.

### Read receipts

`messageRead` is a synchronous room-service write to `subscriptions.lastSeenAt`,
so it is ledger-tracked like any other mutation. The cursor only ever moves
forward, which makes it verifiable without trusting loadgen's clock: the lane
records the previously confirmed cursor as the operation's baseline and the
observer compares it against the new value. Both timestamps are written by the
server, so clock skew between loadgen and room-service cannot produce a
verdict.

The first mark-read on a subscription has no baseline yet. Any cursor present
at reconciliation settles it `good`; from then on the baseline is known and a
cursor that failed to advance is real loss. A cursor that moved *backwards* is
a `mismatch`, not a lost write — the monotonic guarantee was violated.

The lane shares the room-read reconciliation budget, so its rate is included in
the startup capacity check and in the chart's equivalent guard.

### Presence

Presence is deliberately outside the evidence ledger. Its signals — hello,
ping, activity, bye — are core NATS fire-and-forget publishes: they are
buffered client-side during an outage and flushed on reconnect, so a successful
publish proves nothing about delivery and a failed one proves nothing about
loss. Recording them as ledger operations would manufacture verdicts from
non-evidence.

What *is* evidence is the batch query. The lane keeps its own view of the
connections it announced and periodically asks presence-service for the same
set, comparing the answer. Two windows are excluded from comparison because a
disagreement there is legal rather than a fault:

- **Settle** (`SOAK_PRESENCE_SETTLE`) — a just-published signal has not
  necessarily been applied yet.
- **TTL** (`SOAK_PRESENCE_TTL`) — past the connection TTL, presence-service is
  entitled to have expired the connection on its own.

Outcomes land in `loadgen_soak_presence_checks_total{result}`; the announced
population is reported as `loadgen_soak_presence_connections`. Accounts and
connection IDs stay out of labels.

### Sampled consumers

The soak polls `ConsumerInfo` for every durable its lanes feed, so a backlog
building behind any lane is visible without reading NATS directly:

| Stream | Durable | Fed by |
|---|---|---|
| `MESSAGES` | `message-gatekeeper` | send |
| `MESSAGES-CANONICAL` | `message-worker`, `broadcast-worker`, `notification-worker`, `message-sync` | send |
| `ROOMS` | `room-worker` | member, rename, create |
| `ROOMS` | `notification-worker-room-event-invalidate` | mute |
| `INBOX` | `spotlight-sync`, `user-room-sync` | member |

Stream and durable are Prometheus labels, so both are allowlisted and a typo
fails at construction rather than minting an unbounded label.

A consumer that is not deployed is a **reversible** condition, never a ledger
invalidation: the sampler sets `loadgen_consumer_up` to 0 and counts a bounded
`loadgen_consumer_sample_errors_total{reason}`. Treat a window where a required
consumer reports `up == 0` as inconclusive for the lanes that feed it, and
nothing more.

Backlog is not a complete loss signal on its own. `search-sync-worker` Acks and
drops a message whose payload fails to decode or build an action, so that class
of loss leaves the consumer at zero pending — only the `search_index` observer
sees it.

### Ledger epoch

The run ID owns the seeded topology; the epoch owns the evidence journal. The
WAL path is `{SOAK_LEDGER_DIR}/{runId}.{epoch}.wal`.

A contract change makes an existing journal for the same run ID incompatible,
which is a hard startup failure rather than a silent downgrade. Bumping
`ledger.epoch` starts a fresh journal on an unchanged topology, so upgrading
the image no longer forces a re-seed of the whole room set. Journals from
earlier epochs stay on disk as evidence, are never replayed, and are counted in
`loadgen_failure_abandoned_journals`. Treat one reconciliation deadline either
side of an epoch switch as `INCONCLUSIVE`.

### Reconciliation budget

Room and member reconciliation and quarantine probes borrow `room_read` slots
under `SOAK_ROOM_RECONCILE_READ_SHARE`, so verification adds no unbudgeted
request rate and a fault-time backlog cannot starve the room read mix. Startup
refuses a configuration whose room-read rate cannot retire the mutations the
lanes produce; without that check the unresolved backlog grows without bound
and every mutation eventually expires unverified.

### Metrics

| Metric | Meaning |
|---|---|
| `loadgen_soak_room_candidates{state}` | Member candidates by lifecycle state |
| `loadgen_soak_room_quarantine_probes_total{result}` | Parked-pair re-probe outcomes |
| `loadgen_soak_room_pool_exhausted_total{reason}` | Mutations skipped for lack of a usable target |
| `loadgen_soak_room_pool_degraded` | Reversible candidate-pool degradation flag |
| `loadgen_soak_room_create_budget_remaining` | Rooms the create lane may still add |
| `loadgen_soak_room_state_source_total{source,result}` | Room-state observer outcomes per source |
| `loadgen_soak_presence_signals_total{signal}` | Presence signals published, by kind |
| `loadgen_soak_presence_checks_total{result}` | Batch-query comparison outcomes |
| `loadgen_soak_presence_connections` | Connections the lane currently claims online |
| `loadgen_failure_abandoned_journals` | Retained journals from earlier epochs |

The existing `loadgen_failure_operations_total`,
`loadgen_failure_observations_total`, `loadgen_failure_inflight`, and the
`loadgen_soak_*` pacing families cover the new lanes through their existing
`lane` and `observer` labels. Room IDs, accounts, and operation IDs remain WAL
and log content only.
