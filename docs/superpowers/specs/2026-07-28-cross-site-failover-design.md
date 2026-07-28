# Cross-Site Failover — Shared Warm Backup Site ("Lifeboat")

**Status:** Design / brainstorm output — not yet planned or implemented.
**Date:** 2026-07-28

## 1. Problem

Each site is a fully independent silo: its own NATS, MongoDB, Cassandra,
Elasticsearch, Valkey, and MinIO. A user has a **home site** (resolved via
`portal-service`'s `account → siteID → {natsUrl, baseUrl}` directory), and their
identity, rooms, subscriptions, and message history live **only** in that home
site's databases. Cross-site federation today forwards *events* (membership,
subscription-state, messages) via OUTBOX→INBOX — it does **not** replicate any
site's full state anywhere.

Consequently, when a site goes down, its users are simply offline: a standby
site can accept a WebSocket connection but holds none of the displaced user's
rooms, subscriptions, or history. There is no way to keep basic chatting working.

**Goal:** when any single active site is down, its users can connect to a backup
site and continue *basic chatting* — send/receive in rooms they already belong
to, and read recent history — with acknowledged data surviving and healing back
when the home site recovers.

## 2. Scope

**In scope (lifeboat functionality):**
- Send and receive messages in rooms the user is **already** a member of.
- Read **recent** message history (default window: 72h, matching
  `MESSAGE_BUCKET_HOURS`).

**Out of scope during failover (deferred, degraded):**
- Creating rooms / DMs, membership changes, role changes.
- Search, admin, media upload, presence parity.
- Full-fidelity history beyond the recent window.

Constraining failover to **append-only message traffic in existing rooms**
is what makes both replication and recovery-time reconciliation tractable.

## 3. Decision

**Topology: N active sites → 1 dedicated warm backup site ("the lifeboat").**

Every active site continuously ships its lifeboat data slice to the single
backup site. The backup holds a recent, degraded copy of **every** site's
essential state, namespaced by `siteID`. On a site-A outage, `portal-service`
reroutes A's accounts to the backup's NATS URL, and the backup **impersonates
site A** — serving A's rooms/subs/membership/recent-history and accepting new
messages — until A recovers, at which point outage-window writes reconcile back
and portal flips A's accounts home.

**Why this topology:** N sites → 1 target is **N replication relationships, not
a full mesh**, and **one** warm deployment to operate instead of a standby per
site. It fits the existing architecture because data is already **`siteID`-scoped
end-to-end** (rooms/subs/members carry `SiteID`; streams are `<STREAM>_<siteID>`),
so the backup simply runs per-origin-site namespaces.

**Replication mechanism: Variant A — reuse the event federation.** The backup is
modeled as "a peer subscribed to everything." It receives each site's
membership / subscription / room-metadata events over the OUTBOX→INBOX lanes
that already exist and materializes them with the existing `inbox-worker`
apply logic, and it runs a `message-worker`-style consumer on each site's
canonical message stream to persist recent history into its own Cassandra.
This adds mostly configuration plus one "materialize-everything" consumer set —
no new storage-replication subsystem.

Rejected alternative — **Variant B (storage-layer replication):** Cassandra
multi-DC (backup as an extra DC per ring) + Mongo change-stream/replica
shipping. Higher fidelity but N multi-DC rings and a Mongo replication pipeline —
more machinery and more than the lifeboat scope needs. Retained as the growth
path if fidelity requirements ever exceed the event-derived slice.

**Two knobs, at recommended defaults:**
- **Failover trigger:** automatic health-detection with a manual operator
  override.
- **RPO:** asynchronous replication — seconds of potential loss on the very last
  in-flight messages. Synchronous/quorum cross-site replication is not worth its
  cost for lifeboat scope.

## 4. Architecture

```
   Active Site A ──┐   (membership/sub/room events via OUTBOX→INBOX)
   Active Site B ──┼──►  Backup "Lifeboat" Site
   Active Site C ──┘   (canonical messages via cross-site consumer)
                          │  materializes per-siteID namespaces:
                          │    Mongo: rooms, subs, members, users (slice)
                          │    Cassandra: recent history (72h window)
                          │
   portal-service ────────┘  routing brain / split-brain fence:
     account → (home site if healthy | backup if home down)
```

### 4.1 Backup site — materialization
- Runs a per-origin-site set of consumers. For each active `siteID`, it
  consumes that site's federated membership/subscription/room events (the lanes
  `inbox-worker` already understands) and applies them with the **same
  insert/delete/upsert semantics** `inbox-worker` uses today, into a
  `siteID`-namespaced Mongo.
- Runs a `message-worker`-style consumer against each site's
  `MESSAGES_CANONICAL_{siteID}` (cross-site), persisting messages into its own
  Cassandra under the same bucketed schema. History retention on the backup is
  capped to the lifeboat window.
- Holds the **identity slice** (user roster + the room/subscription state) needed
  to authorize and serve `send`/`receive` — not full user-service parity.

### 4.2 Routing brain — `portal-service`
- Becomes the **single source of truth** for "who serves account X *right now*."
  Normal: returns the home site. On A-down: returns the backup's NATS URL for
  A's accounts.
- This single authority is also the **split-brain fence**: A and the backup can
  never both be designated to serve A's users. Failback flips the designation
  atomically after reconciliation drains.
- Frontend already connects to a NATS URL it is handed and re-queries portal on
  connection failure; the reroute reuses that path.

### 4.3 Identity
- The backup must mint NATS JWTs for **any** site's accounts, so it needs the
  account signing NKeys. This is the design's largest security trade-off (one
  deployment can impersonate every site's users).
- **Recommended:** a shared org-level signing scheme or KMS-fronted keys, rather
  than copying raw per-site NKeys onto the backup.

## 5. Data flow

### 5.1 Steady state (all sites up)
Active sites federate their events to the backup exactly as they federate to
peers today; the backup materializes them. The backup serves **no** live client
traffic — it is warm, not hot.

### 5.2 Failover (site A down)
1. Health detection marks A down (auto, with manual override).
2. `portal-service` switches A's accounts to resolve to the backup's coordinates.
3. Displaced clients reconnect to the backup; backup `auth-service` mints their
   JWTs.
4. Users send/receive in existing rooms and read recent history, served from the
   backup's materialized copy of A. New messages persist to the backup's
   Cassandra (and flow through the backup's canonical pipeline for delivery).

### 5.3 Failback (site A recovers)
1. A comes back but is **not yet** re-designated as serving its accounts (portal
   still points them at the backup).
2. The backup **replays its outage-window messages** for A back into A over the
   canonical/federation path. Because messages are **append-only with unique IDs
   and Cassandra bucketing**, replay is **idempotent and conflict-free** (unique
   `Nats-Msg-Id` / message-ID dedup absorbs duplicates).
3. Once A has caught up, portal atomically flips A's accounts home; clients
   reconnect to A.

## 6. Reconciliation

Lifeboat scope makes reconciliation simple: since room/DM creation and
membership changes are **out of scope** during failover, the only new writes at
the backup are **append-only messages**. There is no membership divergence to
merge — only message backfill, which the existing dedup + bucketing make safe to
replay. A periodic anti-entropy sweep (as in the membership-federation design
§3.4) can serve as an unconditional backstop for any messages missed by the
replay.

## 7. Failure modes

| Vector | Handling |
|---|---|
| **Backup itself down when a site fails** | Backup is a failover SPOF; it MUST be HA within itself (multi-AZ). Documented limit, not silently degraded. |
| **Correlated multi-site outage** | Backup is sized for **one site down at a time**; concurrent multi-site failure degrades further (documented ceiling, alerting). |
| **Split brain (A + backup both serving A)** | Prevented by portal-service as sole routing authority; failback flips only after reconciliation drains. |
| **Last in-flight messages at outage instant** | Async RPO ⇒ seconds of potential loss accepted for lifeboat scope. |
| **Identity key compromise on backup** | Mitigated by shared/KMS-fronted signing rather than raw NKey copies; backup hardened accordingly. |
| **Replay duplicates on failback** | Idempotent: message-ID / `Nats-Msg-Id` dedup + append-only bucketed writes. |

## 8. Non-goals

- No room/DM creation, membership, admin, search, presence, or media parity
  during failover (lifeboat scope only).
- No synchronous/zero-RPO cross-site replication.
- No full-fidelity byte-for-byte DB replica of any site (that is Variant B, a
  future growth path).
- No active-active (multi-primary) serving — the backup is warm-passive and
  serves only while a home site is down.

## 9. Open questions / follow-ups

1. **History window** exact size and per-room caps on the backup's Cassandra.
2. **Health-detection mechanism** and the manual-override control surface
   (likely portal-service-owned).
3. **Identity key custody** — concrete shared-signing / KMS scheme for the
   backup minting cross-site JWTs.
4. **Backup capacity sizing** — headroom target (largest single site) and the
   documented multi-site-outage degradation behavior.
5. **Failback cutover protocol** — precise drain/flip sequencing and how clients
   are signaled to reconnect home.
6. **Reconciliation cadence** — replay-on-recovery plus the periodic
   anti-entropy backstop interval.
