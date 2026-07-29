# SP1a (core) — Message-slice DR feed, single site

> **Scope discipline.** This is the *core* first slice of SP1a, deliberately
> narrowed to **one origin site → the backup, message history only**. It is the
> Cassandra counterpart to SP1b (operational slice). Everything else in the
> failover program — the operational slice (SP1b), multi-site fan-out, backup
> serving (SP2), routing (SP3), detection (SP4), failback replay (SP5) — is
> explicitly **out of scope** and lands in its own PR.
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§4.1, §6.4, §6.5)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP1a)
> - **Sibling:** `docs/superpowers/specs/2026-07-29-sp1b-operational-slice-dr-feed.md`

## 1. Goal

The backup site continuously receives **one origin site's message history** and
materializes it into the backup's own `siteID`-namespaced Cassandra under the
same bucketed schema, so the backup stays *warm* — a recent, queryable copy of
that site's messages.

Nothing in this slice *serves* that data. The only observable outcome: messages
persisted at the origin site appear, shortly after, in the backup's Cassandra.

## 2. Decision — replicate at the event layer, not the storage layer

In this codebase Cassandra is written through exactly one door: the
`MESSAGES_CANONICAL_{siteID}` stream, persisted by `message-worker`. That single
ingress is what makes the event-layer approach clean.

- **Rejected — Cassandra-native multi-DC replication.** Adding the backup as
  another DC per keyspace fuses each site's independent Cassandra into
  cross-region rings, fighting the "each site runs its own NATS / Mongo /
  Cassandra independently" architecture and the per-`siteID` backup model.
- **Rejected — application dual-write** (message-worker writes both Cassandras).
  Synchronous cross-region write on the hot path, no durable buffer, couples
  availability.
- **Chosen — source the canonical stream** (design §4.1). The backup runs a
  `message-worker`-shaped consumer that sources the whole
  `MESSAGES_CANONICAL_{siteID}` over the gateway and persists into its **own**
  Cassandra via the same `Store.SaveMessage` / `SaveThreadMessage`. No
  cross-region cluster; reuses existing persistence code; the canonical stream
  itself doubles as the durable restore log (§5).

**The canonical event stream is the replication channel** — we don't replicate
Cassandra, we replay the events that build it.

## 3. Topology

```
[ origin site A ]                              [ backup site ]
  MESSAGES_CANONICAL_{A}  ──►  DR_MESSAGES_{A}  ──►  message-worker-style  ──►  Cassandra
   (single source of truth)     (JetStream,           consumer                  (siteID-
                                 sourced x-site)       (SaveMessage)             scoped)
```

- **Sourced cross-gateway** — a backup-local `DR_MESSAGES_{A}` stream that
  JetStream-**sources** the origin's whole `MESSAGES_CANONICAL_{A}` (durable
  buffering + automatic resume; decoupled from momentary remote unavailability),
  same rationale as SP1b's `DR_OPLOG` sourcing.
- **Consumer at the backup** persists into the backup's own Cassandra, `siteID`-
  scoped, under the identical bucketed schema.

## 4. Why replay is safe (falls out of the existing schema)

The message schema makes replay near-worry-free — three properties, all true
today (design §6.4):

- **Idempotent** — keyed on `(room_id, bucket, message_id)`, so a re-applied
  write is a no-op upsert; JetStream `Nats-Msg-Id` absorbs redelivery.
- **Order-independent** — the clustering key is the message's own `created_at`
  (stamped at original send), so *any* replay order still sorts correctly on read.
- **Buckets line up** — `bucket = floor(created_at / windowMs)` uses the original
  `created_at`, so a replayed message lands in the exact partition it would have
  occupied had A never died.

This trio is why the message slice needs **no conflict reconciliation and no
origin-echo handling** (unlike SP1b's failback hook): "just replay it" is correct.

### 4.1 Hard invariant — shared bucket window

Backup and every origin site **MUST** share `MESSAGE_BUCKET_HOURS` (default 72).
A mismatch makes writes and reads target different `(room_id, bucket)` partitions
and silently lose data — this is already mandated repo-wide (CLAUDE.md §
Cassandra); this slice inherits and re-asserts it. Thread replies use
`thread_messages_by_thread` (partitioned by `thread_room_id` alone, no bucket), so
they carry no bucket-window dependency.

## 5. Retention — two tiers (do not couple them)

- **Read window (Cassandra):** the backup's Cassandra keeps only the lifeboat
  window (~72h) needed to *serve* history during an outage.
- **Restore log (the stream):** the `DR_MESSAGES_{siteID}` / canonical stream is
  the durable restore log; size its `MaxAge` **≥ max tolerated outage**
  (days/weeks — 1× with no fan-out, cheap).
- **Rule:** restore (SP5) reads from the **stream**, never the 72h Cassandra
  window — a longer outage would age the earliest messages out of Cassandra
  before the origin returns. The two numbers are independent.

## 6. The forward gap (noted, not solved here)

Messages the origin persisted just before crashing that never reached the backup
(the RPO tail) are **not lost** — they stay durable in the origin's own Cassandra
and reappear when clients are pointed home; remote members recover them via the
client's normal history/gap fetch (design §6.7). This slice does not attempt to
close the gap; it only records that the gap is benign.

## 7. Out of scope (explicit — each its own PR)

- **SP1b** — operational slice → backup Mongo (sibling spec).
- **Multi-site** — running this per-`siteID` for every origin site (this core is one site).
- **SP2** — backup *serving* (history read path pointed at the backup Cassandra).
- **SP3 / SP4** — routing override, health detection.
- **SP5** — failback replay of the restore log into the origin's front door.
- **SP6** — ops/IaC: gateway peering, stream `MaxAge` sizing, replication-lag alerting.

## 8. Testing (TDD, per CLAUDE.md §4)

- **Unit** — consumer/persist handler table tests (channel message + thread reply
  → correct `SaveMessage` / `SaveThreadMessage` call; malformed payload; store
  error → Nak vs poison), mocked store; assert idempotent re-delivery is a no-op.
- **Integration** (`//go:build integration`, `testutil.NATS` +
  `testutil.CassandraKeyspace`) — publish canonical events → source →
  persist → read back from the backup Cassandra; assert bucket placement matches
  the origin (same `MESSAGE_BUCKET_HOURS`), and that a replayed duplicate does not
  double-write.
- **Coverage** — ≥80% floor; ≥90% for the persist/handler business logic.

## 9. Open sub-decisions (call out in the plan)

1. **Deploy shape** — MODE-configured reuse of `message-worker` (a "replicate"
   mode) vs a thin new DR consumer service. *Leaning: reuse* (matches the repo's
   unified-worker convention and SP1b's reuse decision).
2. **`DR_MESSAGES_{siteID}` ownership** — new per-site stream bootstrapped via the
   standard `BOOTSTRAP_STREAMS` convention; which service owns creation, and
   whether it is the same owner as SP1b's `DR_OPLOG_{siteID}`.
3. **Backup Cassandra namespacing** — separate keyspace per `siteID` vs a
   `siteID`-scoped table/partition scheme (align with SP1b's Mongo namespacing
   choice for a consistent per-site story).
