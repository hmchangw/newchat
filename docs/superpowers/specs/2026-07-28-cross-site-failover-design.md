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

**Replication mechanism: Variant A — event-derived materialization, over a
dedicated backup feed.** The backup is modeled as "a peer that holds a copy of
everything." Two channels feed it, and the distinction is load-bearing:

- **Messages** — a `message-worker`-style consumer sources each site's **whole**
  `MESSAGES_CANONICAL_{siteID}` (1× per message, *before* the ×F fan-out) and
  persists recent history into the backup's own Cassandra under the same bucketed
  schema.
- **Operational state** (rooms / subscriptions / members / user slice) — a
  **dedicated, membership-independent DR feed** that ships *every* room's state
  to the backup, applied with the same `inbox-worker`-style insert/delete/upsert
  semantics.

**Why not reuse the existing OUTBOX→INBOX federation for operational state:** that
federation is **membership-driven** — it only fires when a room has a member on
*another active site*. Same-site ("local") rooms produce no federation events at
all, and (post the local/global subject work — see §7) they are the **majority**.
Reusing federation would therefore leave the backup blind to most rooms. The DR
feed is a distinct, backup-directed, unconditional whole-site channel — carried
below the client-facing interest graph (JetStream / Mongo change-streams), so it
neither depends on nor perturbs room locality. This pulls the *operational-state*
slice partway toward Variant B mechanics while the *message* slice stays purely
event-derived.

Rejected alternative — **Variant B (full storage-layer replication):** Cassandra
multi-DC (backup as an extra DC per ring) + full Mongo change-stream/replica
shipping for *all* collections. Higher fidelity but N multi-DC rings and a broad
Mongo replication pipeline — more machinery and more than the lifeboat scope
needs. Retained as the growth path if fidelity requirements ever exceed the
event-derived slice.

**Two knobs, at recommended defaults:**
- **Failover trigger:** automatic health-detection with a manual operator
  override.
- **RPO:** asynchronous replication — seconds of potential loss on the very last
  in-flight messages. Synchronous/quorum cross-site replication is not worth its
  cost for lifeboat scope.

## 4. Architecture

```
   Active Site A ──┐   dedicated DR feed (unconditional, all rooms):
   Active Site B ──┼──►    · whole MESSAGES_CANONICAL_{site}  (1×, no fan-out)
   Active Site C ──┘       · rooms/subs/members/user-slice    (change-stream/feed)
                          │
                          ▼  Backup "Lifeboat" Site
                          │  materializes per-siteID namespaces:
                          │    Mongo: rooms, subs, members, users (slice)
                          │    Cassandra: recent history (72h read window)
                          │
   portal-service ────────┘  routing brain / split-brain fence:
     account → (home site if healthy | backup if home down)
```

### 4.1 Backup site — materialization
- Runs a per-origin-site set of consumers. For each active `siteID`:
  - **Messages:** a `message-worker`-style consumer sources the whole
    `MESSAGES_CANONICAL_{siteID}` and persists into the backup's own Cassandra
    under the same bucketed schema. The Cassandra **read window** is capped to the
    lifeboat window (72h); the **restore log** (the canonical stream itself) is
    retained far longer — see §6.
  - **Operational state:** a dedicated DR feed ships *every* room's
    room/subscription/member state (not just cross-site rooms), applied with the
    same insert/delete/upsert semantics `inbox-worker` uses today, into a
    `siteID`-namespaced Mongo.
- Holds the **identity slice** (user roster + the room/subscription state) needed
  to authorize and serve `send`/`receive` — not full user-service parity.
- Materializes the room **`CrossSite` locality flag** (see §7) so the backup's
  broadcast path picks the correct subject prefix and `subscription.list` returns
  the right locality to reconnecting clients.

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
> **Resolved (2026-08-12) — the trade-off below does not apply.** This section
> assumed **per-site NATS accounts** (so the backup would concentrate every
> site's signing keys — "World 2"). Production actually runs **one shared
> org-level NATS account** for all clients ("World 1"): the chat `account` is a
> *tag* on a scoped user JWT, and one signing key is trusted at every site. The
> backup therefore mints in the *same* account every site uses — **not**
> impersonation, no per-site keys — so it needs no special key custody and
> reuses `auth-service` unchanged. See `specs/2026-08-11-sp2-backup-identity-jwt-minting.md`.
> The only surviving identity task is the shared-template `chat.local.room.>`
> grant (§7). The original per-site framing is retained below for history.
- ~~The backup must mint NATS JWTs for **any** site's accounts, so it needs the
  account signing NKeys. This is the design's largest security trade-off (one
  deployment can impersonate every site's users).~~
- ~~**Recommended:** a shared org-level signing scheme or KMS-fronted keys, rather
  than copying raw per-site NKeys onto the backup.~~

## 5. Data flow

### 5.1 Steady state (all sites up)
Active sites stream their DR feed (§4.1) to the backup, which materializes it.
This is a **separate, unconditional, backup-directed channel** — not the
membership-driven OUTBOX→INBOX peer federation, which skips same-site rooms. The
backup serves **no** live client traffic — it is warm, not hot.

### 5.2 Failover (site A down)
1. Health detection marks A down (auto, with manual override).
2. `portal-service` switches A's accounts to resolve to the backup's coordinates.
3. Displaced clients reconnect to the backup; backup `auth-service` mints their
   JWTs.
4. Users send/receive in existing rooms and read recent history, served from the
   backup's materialized copy of A. New messages persist to the backup's
   Cassandra (and flow through the backup's canonical pipeline for delivery).

### 5.3 Failback (site A recovers)
See §6 — the restore/failback protocol is detailed there.

## 6. Failback & data restoration

When A returns, everything written at the backup during the outage must land in
A (A is the permanent home of those rooms and messages). Because lifeboat scope
confined new writes to **append-only messages** (plus best-effort read-state),
restoration is a **replay, not a merge**.

### 6.1 What diverged
Only two kinds of writes happened at the backup while it impersonated A:
1. **New messages** into existing rooms — append-only, globally unique IDs
   (`idgen.GenerateMessageID`), written to the backup's Cassandra *and* its
   canonical stream for A.
2. **Read-state** — `lastSeenAt` / read watermarks / receipts, monotonic
   (max-wins).

Explicitly **not** diverged: rooms, memberships, roles, profiles — those RPCs
were blocked at the backup. No topology divergence ⇒ **no merge-conflict class**.

### 6.2 The restore log
Because the backup ran A's canonical pipeline during the outage, it already holds
a **durable, ordered JetStream log of exactly the outage writes** (A's canonical
namespace on the backup). Restore = **replay that log into A's front door**: the
events re-enter A's normal `message-worker` (persist) → `search-sync-worker`
(index), as if freshly federated.

### 6.3 Failback choreography (ordering prevents split-brain and races)
1. **A's infra recovers, but portal still points A's accounts at the backup.** A
   is "up but not serving." Users keep writing to the backup — a stable source
   while draining.
2. **Replay the outage log backup → A** into A's `MESSAGES_CANONICAL_{A}`;
   runs continuously, chasing A's lag toward zero.
3. **Backfill read-state** as monotonic (`$max`-guarded) updates — order-
   independent, best-effort. Skipping it only re-surfaces a few messages as
   "unread"; never data loss.
4. **Verify convergence** — a directional, outage-window-scoped anti-entropy diff
   (latest message / counts per `(room, bucket)`) between the backup's A-copy and
   A; reconcile anything replay missed. This is the §3.4-style backstop, narrowed
   to one window and one direction.
5. **Flip portal → home (A)** once lag ≈ 0. Clients reconnect to A, re-read
   `subscription.list` (now current), and resume — reusing the reconnect-self-heal
   path (§7). The replay stream stays alive briefly past the flip to sweep any
   tail writes (safe — idempotent).
6. **Tear down** the backup's A-impersonation; retain the outage log until
   convergence is confirmed, then GC.

### 6.4 Why replay is safe
- **No duplicates:** Cassandra keys on `(room_id, bucket, message_id)` ⇒
  idempotent upsert; JetStream `Nats-Msg-Id` (= DedupID) absorbs redelivery.
- **Order-independent:** clustering key is the message's own `created_at` (stamped
  at original send time), so any replay order still sorts correctly and history
  reads by timestamp stay correct.
- **Buckets line up:** `bucket = floor(created_at / window)` with the original
  `created_at` ⇒ messages land in the same bucket they'd have occupied had A never
  died (requires backup and A share `MESSAGE_BUCKET_HOURS` — already mandated).
- **No double-delivery:** a remote member on a healthy site who saw a message live
  during the outage is not re-notified; a re-fired broadcast is deduped client-side
  by message ID.

### 6.5 Retention — two tiers, and the long-outage case
- **Fast path:** ordered stream replay.
- **Backstop:** state-diff anti-entropy (§6.3 step 4) re-derives anything missed.
- **Retention rule:** the restore log is the backup's **canonical stream**, not
  its 72h Cassandra read window. Since canonical is 1× (no fan-out) and small,
  size that stream's `MaxAge` **≥ the max tolerated outage** (days/weeks is cheap).
  Do **not** depend on the 72h read window for restore — a longer outage would age
  its earliest messages out before A returns.

### 6.6 Operational must-dos on the backfill path
- **Suppress push notifications** — replayed messages are stale; `notification-
  worker` skips events flagged as replay.
- **Suppress (or client-dedup) re-broadcast** — restore is about durability in A,
  not re-delivering to users who have moved on.

### 6.7 The forward gap (A's last pre-crash writes)
Messages A persisted just before crashing that never replicated to the backup
(the RPO tail) were **never lost** — they are durable in A's own Cassandra, merely
invisible during the outage, and reappear when users are pointed home. Remote
members who missed them recover via the client's normal history/gap fetch.

## 7. Compatibility with local/global room subjects

A separate design (`.../2026-07-28-local-global-room-subjects-design.md`) reduces
NATS gateway interest-map memory by routing **same-site ("local") rooms** onto a
`chat.local.room.{id}.>` prefix that the platform team **denies at the leaf node**
(never advertised across the supercluster), while cross-site ("global") rooms keep
the propagated `chat.room.{id}.>`. Locality is a sticky `CrossSite` flag on the
`Room` doc: global iff ≥1 member's home site ≠ `room.SiteID`.

The two designs are compatible, and split cleanly by locality:

- **Global rooms** stay on the propagated prefix; during A's outage the backup
  publishes globally and delivery reaches both the displaced member (intra-cluster
  on the backup) and any member on a still-healthy site (via the backup→peer
  gateway — requires the backup to be a full supercluster gateway peer). These are
  exactly the rooms the membership-driven federation already covers.
- **Local rooms** need only **intra-cluster** delivery on the backup: because
  "local" ⇒ all members share one home site, they **all fail over to the same
  backup together**, so the `chat.local.` leaf filter (which only blocks
  *cross-gateway* interest) is irrelevant to their delivery.

Classification is **home-site-based**, so a user being transiently relocated to
the backup during failover does **not** perturb any room's `CrossSite` flag.

**Integration requirements (must hold when both ship):**
1. **Backup auth-service must grant `chat.local.room.>` subscribe** in the JWTs it
   mints for displaced users. Missing it silently breaks subscription to local
   rooms — i.e. the majority of the lifeboat's own promise. Highest-risk coupling.
2. **Backup must materialize *and* honor `CrossSite`** — carried on the DR feed
   (§4.1), read by the backup's broadcast path for prefix choice and returned on
   `subscription.list`.
3. **Leaf-node `chat.local.>` deny must also apply to the backup's leaf**, and
   displaced clients must connect to the backup's own NATS (intra-cluster) — which
   the portal reroute already does.

**Synergy:** the reduction's reconnect-self-heal (reconnect → re-read
`subscription.list` → subscribe on the correct prefix) *is* the failover/failback
reconnect path; and the per-user tree (`chat.user.{account}.>`) stays global in
its Phase 1, so reroute signaling and the locality-transition nudge still work
through the backup.

## 8. Performance & sizing

**The structural fact that makes this affordable:** DR replication rides the
**canonical plane (1× per message)**, not the **fan-out plane (×F, F≈100)**. The
backup sources one copy per message and persists it; the ×F broadcast
amplification happens only at *serving* time, and only for the one failed site
during an actual failover.

- **Replication cost ∝ M** (message count) — paid continuously by the backup.
- **Serving cost ∝ M × F** — paid only for 1 site, only during failover.

Reference load (`docs/nats-traffic-estimation.md`): ~4.5M msg/day per site
(~52/s avg, ~250–500/s peak), canonical payload ~0.5–1.5 KB.

**Steady state (N sites healthy):**

| Resource | Load on the backup | Magnitude |
|---|---|---|
| Ingest compute | ~N `message-worker`-equivalents + N materializers, 24/7 | N × ~52/s avg; Cassandra shrugs |
| WAN egress (each site→backup) | 1× canonical + tiny op events | ~50 KB/s avg, ~0.25–0.5 MB/s peak **per site** — trivial |
| Cassandra storage | 72h window × aggregate volume | ~13 GB/site × N (bounded by window) |
| Mongo storage | operational state for all servable rooms | not windowed by default — main storage lever |

Ingest is sized to the **aggregate of N sites**, but on the cheap (1×) plane, so
it is a modest continuous load — not a ×F blow-up.

**Failover burst (the real peak):** backup = (continued N−1-site ingest) + (full
live serving of 1 site, *now* with ×F fan-out) + (a reconnect thundering herd:
TLS + JWT-mint storm + `subscription.list` bootstrap storm). Size the **serving**
tier for one (largest) site's peak; size the **ingest** tier for the N-site
aggregate — two different capacity numbers. Mitigate the herd with client
reconnect backoff/jitter, rate-limited/pre-warmed auth, and pre-warmed Valkey.

**Latency:** steady-state replication is async and off every user's critical path
(zero user-facing impact; the lag *is* the RPO). At failover, displaced users'
perceived latency = their RTT to the backup — a function of backup geography.

**Relation to the reduction (different resources):** the reduction saves gateway
**interest-map memory** (an ~O(clients × rooms) all-to-all RAM cost); DR
replication costs backup **CPU/IO/disk ∝ Σ throughput** to **one** destination
plus a cheap per-site WAN stream. DR does not re-inflate the interest graph. The
one honest note: local-room canonical messages now leave their home site for the
first time (a single 1× copy to the backup) — new inter-site traffic, but cheap.

**Risks & levers:**
- **Silent RPO decay** if the backup can't keep up with aggregate ingest — its lag
  grows and the DR guarantee quietly weakens. **Monitor per-site replication lag**
  (reuse the membership-federation lag signal) and alert.
- **Warm-standby idle waste** — N-way ingest runs 24/7 while serving ~nothing. If
  RTO can relax, a **colder** standby (buffer + replay-on-promote) cuts continuous
  compute, trading RTO for cost.
- **Biggest storage lever is Mongo, not Cassandra** (already windowed). Windowing
  Mongo to recently-active rooms bounds it but risks a failover user not finding a
  long-idle room — a scope decision.

## 9. Failure modes

| Vector | Handling |
|---|---|
| **Backup itself down when a site fails** | Backup is a failover SPOF; it MUST be HA within itself (multi-AZ). Documented limit, not silently degraded. |
| **Correlated multi-site outage** | Backup is sized for **one site down at a time**; concurrent multi-site failure degrades further (documented ceiling, alerting). |
| **Split brain (A + backup both serving A)** | Prevented by portal-service as sole routing authority; failback flips only after reconciliation drains. |
| **Last in-flight messages at outage instant** | Async RPO ⇒ seconds of potential loss accepted for lifeboat scope. |
| **Identity key compromise on backup** | World 1: the backup holds no per-site keys — it uses the one shared account signing key already present on every site's `auth-service`, so it is no more key-exposed than any site. Reducing that shared key's exposure (fleet-wide remote signer) is a separate security project, not a failover concern (§4.3). |
| **Replay duplicates on failback** | Idempotent: message-ID / `Nats-Msg-Id` dedup + append-only bucketed writes. |
| **Silent RPO decay** (backup can't keep up with N-aggregate ingest) | Per-site replication-lag monitoring + alert (§8); the lag *is* the live RPO. |
| **Outage longer than history window** | Restore uses the canonical **stream** log (`MaxAge` ≥ max outage), not the 72h Cassandra read window (§6.5); anti-entropy backstop re-derives the rest. |
| **Local room not materialized on backup** | DR feed is unconditional/whole-site (§4.1), not membership-driven federation, so same-site rooms are covered. |

## 10. Non-goals

- No room/DM creation, membership, admin, search, presence, or media parity
  during failover (lifeboat scope only). This constraint is **load-bearing**: it
  is what keeps failback a replay rather than a bidirectional merge (§6).
- No synchronous/zero-RPO cross-site replication.
- No full-fidelity byte-for-byte DB replica of any site (that is Variant B, a
  future growth path).
- No active-active (multi-primary) serving — the backup is warm-passive and
  serves only while a home site is down.

## 11. Open questions / follow-ups

1. **DR feed mechanism** for operational state — Mongo change-streams vs a new
   backup-directed event fan-out; and how `CrossSite` rides it (§4.1, §7).
2. **History read window** size and per-room caps on the backup's Cassandra, vs
   the (longer) canonical restore-log `MaxAge` (§6.5).
3. **Health-detection mechanism** and the manual-override control surface
   (likely portal-service-owned).
4. ~~**Identity key custody** — concrete shared-signing / KMS scheme for the
   backup minting cross-site JWTs.~~ **Moot (World 1, 2026-08-12):** one shared
   NATS account ⇒ no per-site keys, no custody problem; the backup reuses
   `auth-service` unchanged. Only the `chat.local.room.>` grant (§7) remains —
   see `specs/2026-08-11-sp2-backup-identity-jwt-minting.md`.
5. **Backup capacity sizing** — separate ingest (N-aggregate) vs serving
   (largest single site) targets, and documented multi-site-outage degradation.
6. **Failback cutover protocol** — precise drain/flip sequencing, tail-sweep
   window, and how clients are signaled to reconnect home (§6.3).
7. **Reconciliation cadence** — replay-on-recovery plus the periodic anti-entropy
   backstop interval.
8. **Per-site replication-lag monitoring** — the RPO-decay signal (§8).
