# SP1b (core) — Operational-slice DR feed, single site

> **Scope discipline.** This is the *core* first slice of SP1b, deliberately
> narrowed to **one origin site → the backup, operational state only**. It
> resolves design open-question §11.1 ("DR feed mechanism for operational
> state"). Everything else in the failover program — the message slice (SP1a),
> multi-site fan-out, backup serving (SP2), routing (SP3), detection (SP4),
> failback replay (SP5) — is explicitly **out of scope** and lands in its own
> PR.
>
> - **Governing design:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md` (§4.1, §11.1)
> - **Roadmap:** `docs/superpowers/plans/2026-07-28-cross-site-failover-roadmap.md` (SP1b)

## 1. Goal

The backup site continuously receives **one origin site's operational state**
(rooms, subscriptions, members, and the user roster slice) and materializes it
verbatim into a `siteID`-namespaced Mongo, so the backup stays *warm* — a
recent, queryable copy of that site's lifeboat-servable state.

Nothing in this slice *serves* that data. The only observable outcome: writes at
the origin site's Mongo appear, shortly after, in the backup's Mongo under that
site's namespace.

## 2. Decision — reuse the oplog CDC pipeline

Resolves §11.1 (**Mongo change-streams vs a new backup-directed event fan-out**)
in favour of **Mongo change-streams**, by reusing the existing, tested oplog CDC
components rather than building a new fan-out:

- **`data-migration/oplog-connector`** — a change-stream tailer that emits a
  `model.OplogEvent` envelope (dedup `EventID` = `Nats-Msg-Id`, op, collection,
  opaque whole-document `FullDocument`, `SiteID`, `ClusterTime`) to a NATS
  stream.
- **`data-migration/oplog-direct-transfer`** — an applier whose `targetStore`
  (`UpsertByID` / `DeleteByID`, keyed by native `_id`) writes each event verbatim
  into a destination Mongo.

### 2.1 Why change-streams, not the OUTBOX federation pattern

The OUTBOX pattern is **membership-driven**: `room-service` / `room-worker`
publish per-destination events only for state destined for a *peer site*
(`member_added` / `member_removed` / `room_renamed` + subscription-state), and
only for rooms that have cross-site membership. DR needs the **opposite
property**: the *whole* site's operational state, membership-independent —
including purely local rooms that never federate to anyone.

Tailing the Mongo oplog captures **every** write to the watched collections
regardless of membership or event type, with three properties the OUTBOX lane
cannot give us:

1. **Local rooms covered by construction** — the exact gap the OUTBOX lane leaves.
2. **`CrossSite` rides free** — whole-document replication copies `Room.CrossSite`
   verbatim, satisfying design §4.1 / §7 with zero extra work.
3. **Zero producer changes** — room-service / room-worker / user-service are
   untouched; no risk of a missed write path, now or in future.

## 3. Topology

Mirrors the existing connector → NATS → transfer split, applied cross-site:

```
[ origin site A ]                         [ backup site ]
  Mongo (rooms, subscriptions,
   room_members, users)
        │ change stream (local)
        ▼
  oplog-connector  ──►  DR_OPLOG_{A}  ──►  oplog-direct-transfer  ──►  Mongo
   (watches op collections)  (JetStream,      (applier)                (siteID-
                              sourced x-site)                            scoped)
```

- **Tailer runs at the origin site**, co-located with that site's Mongo — the
  change-stream connection stays local (robust; no cross-region change stream).
- **Events cross the gateway as JetStream** — `DR_OPLOG_{A}` is sourced into the
  backup (durable buffering + automatic resume; decoupled from momentary remote
  unavailability), same rationale as SP1a's canonical sourcing.
- **Applier runs at the backup**, writing into the `siteID`-scoped Mongo.

## 4. What is replicated — the lifeboat operational slice

Whitelisted collections (defense-in-depth: both the connector's watch set and the
applier's `collections` guard):

| Collection | Why it's in the lifeboat slice |
|-----------|--------------------------------|
| `rooms` | Room existence, `CrossSite` locality, metadata to serve/list rooms |
| `subscriptions` | Who is subscribed to what — authorize + `subscription.list` |
| `room_members` | Membership — authorize send/receive |
| `thread_rooms` | Thread rooms parallel channel rooms |
| `thread_subscriptions` | Thread subscription state |
| `users` (roster slice) | Identity slice needed to authorize/serve (not full user-service parity) |

Explicitly **excluded** from this core: messages (SP1a, Cassandra), and anything
not needed to authorize + serve send/receive in existing rooms.

## 5. Self-contained events — `updateLookup`

The migration connector opens its change stream with **no** `updateLookup`
("native oplog only") because its co-located transfer re-reads the source Mongo
by `_id` for `update` ops (`oplog-direct-transfer/handler.go:resolveBySourceLookup`).

For a **cross-site DR** feed that back-read would be a cross-region round-trip
from the backup to the origin's Mongo on every update — a coupling we do not
want. The DR connector therefore opens the change stream with
`fullDocument: "updateLookup"` so `update` events carry the post-image inline and
the feed is **self-contained**: the backup applier never reads back to the origin.

## 6. Idempotency & ordering

- **Idempotent** — `EventID` set as `Nats-Msg-Id` (JetStream dedup) + `_id`-keyed
  upsert/delete absorb redelivery.
- **Per-document order preserved** — change streams deliver a document's events in
  oplog order; a single applier (or a per-`_id` FIFO, `MaxAckPending=1`, matching
  the OUTBOX membership-lane precedent) preserves apply order so a later update
  can't be overtaken by an earlier one.

## 7. The failback origin hook (cheap insurance for SP5)

SP5 (failback) must replay **only backup-authored writes** back to a recovered
site — never echo the origin's own replicated data back at it. To make that a
clean "tail this namespace/marker over this window" instead of a forensic diff,
this core bakes in the separation now:

- Backup-authored operational writes are **distinguishable from origin-replicated
  writes** — via a dedicated backup namespace and/or an origin marker on the
  document.
- **Precedent already in-repo:** `oplog-connector/source_mongo.go` already carries
  a `federation.origin` `$match` filter that drops foreign-origin insert/replace.
  The same shape (an origin marker + a change-stream `$match`) is the mechanism
  SP5 will filter on.

This slice only guarantees the **separation exists and is queryable**; SP5 owns
the replay, conflict policy, and cutover.

## 8. Out of scope (explicit — each its own PR)

- **SP1a** — message slice → backup Cassandra.
- **Multi-site** — running this per-`siteID` for every origin site (this core is one site).
- **SP2** — backup *serving* (JWT minting, send/receive/history read paths).
- **SP3 / SP4** — routing override, health detection.
- **SP5** — failback replay, origin filtering *use*, conflict reconciliation.
- **SP6** — ops/IaC: gateway peering, stream `MaxAge` sizing, replication-lag alerting.

## 9. Testing (TDD, per CLAUDE.md §4)

- **Unit** — applier handler table tests (insert/replace/update/delete →
  upsert/delete; collection-guard skip; poison vs transient), mocked `targetStore`.
  Connector collection-selection + `updateLookup` config tests.
- **Integration** (`//go:build integration`, `testutil.MongoDB` + `testutil.NATS`)
  — change event at a source Mongo → published to `DR_OPLOG_{siteID}` → applied →
  read back verbatim from the backup Mongo under the site namespace, including a
  `CrossSite=true` room asserting the flag survives round-trip.
- **Coverage** — ≥80% floor; ≥90% for the applier/handler business logic.

## 10. Open sub-decisions (call out in the plan)

1. **Deploy shape** — MODE-configured reuse of the existing
   `oplog-connector` / `oplog-direct-transfer` binaries (matching the repo's
   unified-worker convention) vs thin new DR services. *Leaning: reuse.*
2. **`DR_OPLOG_{siteID}` ownership** — new per-site stream bootstrapped via the
   standard `BOOTSTRAP_STREAMS` convention; which service owns creation.
3. **Backup namespacing** — separate Mongo *database* per `siteID` vs a
   `siteID`-prefixed collection scheme (feeds the §7 origin hook).
