# Local/Global Room Subject Separation

**Date:** 2026-07-28
**Status:** Design — approved for planning
**Scope:** Phase 1 — room-scoped subjects only

## Problem

All chat clients connect under a **single shared NATS account** (auth-service mints
every user JWT against one `issuer_account`; the `{account}` token in
`chat.user.{account}.>` is an application-level subject token, not a NATS account).

In a NATS supercluster, gateway **interest** propagation is per NATS account and is
all-or-nothing: once an account flips to interest-only mode, *every* subscription in
it is propagated into *every* peer gateway's interest map. Because all users share one
account, each connected client's per-room subscriptions (`chat.room.{roomID}.>`) are
replicated into every site's gateway interest map — including the large majority that
belong to rooms whose members are all on a single site and therefore never need
cross-site delivery. That replicated interest state is the memory pressure we want to
remove.

Cross-site real-time message delivery **currently rides core gateway interest**: a
remote member (`bob@siteB` in `roomR@siteA`) subscribes `chat.room.roomR.>`, and
`broadcast-worker@siteA`'s publish crosses the A→B gateway because B propagated
interest in that subject. Same-site rooms pay the same propagation cost for no benefit.

## Goal

Separate client-facing **room-scoped** subjects into a **local** namespace and a
**global** namespace so that same-site rooms' interest is *not* advertised across the
supercluster, while cross-site rooms continue to deliver over the gateway as they do
today.

**Infrastructure boundary:** the platform/NATS team filters the designated **local**
prefix at the **leaf node** (denies it from interest advertisement across the
supercluster). This design covers only the **application code** that must route each
publish and subscribe to the correct namespace and classify each room correctly.

## Non-Goals (Phase 1)

- Splitting the per-user subject tree (`chat.user.{account}.>`). It stays fully global,
  unchanged. (Future enhancement.)
- Demoting a room from global back to local (`global → local`). Rooms are **sticky
  global**. (Future enhancement.)

## Core Principles

1. **Global is the fail-safe default.** Misclassifying a *global* room as *local*
   silently breaks remote delivery (interest is filtered, the publish never crosses).
   Misclassifying a *local* room as *global* only wastes a little interest — no
   correctness bug. So the system defaults to global whenever unsure and only routes to
   local when it is certain the room is same-site-only. The existing `chat.room.>`
   subjects remain the global namespace, so any un-updated caller degrades to
   "works, no savings."

2. **The persisted flag is the source of truth; the live event is only a latency
   optimization.** Correctness never depends on any best-effort NATS event being
   received — only promptness does. Every client reads each room's locality from its
   normal bootstrap (`subscription.list`); the per-user transition event merely lets an
   already-connected client re-subscribe sooner.

## Classification

A room is **global iff at least one member's home site differs from `room.SiteID`**,
otherwise **local**.

Persisted as a sticky tri-state pointer on the `Room` document:

```go
// Room document (pkg/model)
// nil = unclassified; &true = cross-site; &false = confirmed same-site.
// Missing == nil == GLOBAL (fail-safe); only an explicit false routes local.
CrossSite *bool `json:"crossSite,omitempty" bson:"crossSite,omitempty"`
```

Phase 1 sets an explicit true/false at every create/add path and never clears
true (sticky global); `nil` only ever occurs on a not-yet-classified room.

The classification signal already exists at the right place: room-worker/room-service
decide to federate a `member_added` across sites at exactly the points where a member's
site differs from the room's site. We persist that decision rather than inventing new
logic.

## Subject Namespaces

The unit is the entire `chat.room.{roomID}.>` subtree — one prefix decision per room
covers `RoomEvent`, `RoomMsgStream`, and `RoomMetadataUpdate`.

| Locality | Namespace | Gateway behavior |
|----------|-----------|------------------|
| Global (default) | `chat.room.{roomID}.>` (unchanged) | Advertised / propagated |
| Local | `chat.local.room.{roomID}.>` | Denied at leaf node (site-local) |

`chat.local.>` is a clean top-level prefix for the leaf-node deny rule.

### `pkg/subject` changes

The room-scoped builders take a locality argument instead of gaining parallel
ad-hoc builders. Proposed signature (final form — bool vs a small `Locality` type — to
be settled in the plan; a bool is sufficient):

```go
func RoomEvent(roomID string, global bool) string
func RoomMsgStream(roomID string, global bool) string
func RoomMetadataUpdate(roomID string, global bool) string
```

`global == true` → `chat.room.{roomID}.…`; `global == false` →
`chat.local.room.{roomID}.…`. All three derive from a single internal base helper so
the prefix rule lives in one place. Table tests cover both localities for each builder.

## Write Path (who sets the flag)

The flag is set true at the same points that already decide cross-site federation:

- **Room creation** (channel & DM) — set `crossSite=true` when the initial member set
  spans sites. A DM between two different-site users is born global.
- **Member add** (room-worker **and** room-service) — if the added member's site ≠
  `room.SiteID` and the room is not already global, set `crossSite=true`.
- **Member remove** — no-op in phase 1 (sticky).

## Publisher Routing

Every `chat.room.{roomID}.>` publisher reads `crossSite` and picks the prefix:

- **broadcast-worker** — add `CrossSite` to `roommetacache.Meta` (and its Mongo
  projection). The hot-path fan-out already loads this `Meta` per room, so the prefix
  choice is a free cache read.
- **room-service / room-worker** — the metadata-update and room-event publishers read
  the flag from the `Room` doc they already load.

Publishers default to global if the flag is absent/unknown (fail-safe).

## Client Routing & Transition

- **Steady state:** the client learns each room's locality from a new field on the room
  payload returned by `subscription.list`:

  ```go
  // SubscriptionRoom (pkg/model) — tri-state; absent == nil == global (fail-safe)
  CrossSite *bool `json:"crossSite,omitempty" bson:"-"`
  ```

  It subscribes each room to `chat.local.room.{id}.>` or `chat.room.{id}.>` accordingly.

- **Transition (local → global):** when a room's flag flips, the client learns the new
  `crossSite=true` on its next `subscription.list` and re-subscribes that room to the
  global prefix; the flag is authoritative. The newly-added remote member reads
  `crossSite=true` from its initial `subscription.list` and subscribes global directly.
  The real-time gap for same-site members still on the local prefix is closed
  server-side by the grace window below, so correctness never depends on a proactive
  push. (An earlier design pushed a per-user nudge to prompt immediate re-subscribe;
  it was dropped in review — mass-nudging every subscriber is heavy for large rooms and
  the grace window already covers the gap.)

- **Offline / dropped clients self-correct on bootstrap.** An offline client isn't
  subscribed to anything; on reconnect it re-reads `subscription.list`, sees
  `crossSite=true`, and subscribes global from the first subscription. A client
  disconnected as a slow consumer likewise recovers via the reconnect + fresh
  `subscription.list`.

- **Transition grace window closes the flip gap server-side.** When a *previously
  confirmed same-site* room (`crossSite==false`) flips to cross-site, the flip time is
  recorded as `Room.CrossSiteAt`, and for the locality grace window afterward every
  publisher emits the room's `.event` to **both** the local and global subjects (see
  `pkg/subject.RoomEventTargets`). The window is `ROOM_LOCALITY_GRACE` (default 1 week,
  `subject.DefaultRoomLocalityGrace`) — long enough that essentially every client
  reconnects and re-subscribes the room within it, so it decays to zero double-publishing
  once the local audience has drained; all publisher services MUST use the same value. So
  a same-site member still subscribed to the local subject keeps receiving in real time
  until it re-subscribes — the gap depends only on the client reconnecting/reconciling
  within the window, not on any proactive push. The
  double-publish is bounded (only rooms actively flipping, only for the window) and
  site-local on the leaf-filtered subject, so the steady-state interest-map savings are
  preserved. Rooms *born* cross-site get no `CrossSiteAt` and no grace (they never had a
  local audience). Any residual gap past the window is still recovered by the client's
  normal history/gap fetch.

## Auth / JWT Permission (platform-team touchpoint)

Client JWT `Sub allow` currently lists `chat.room.>`. It must **also** allow
`chat.local.room.>`. This is the auth-service scoped-signing-key **template** (mirrored
in `docker-local/setup.sh` and `docs/client-api.md §2.1`), which is infra-owned; the
code-side comment in `auth-service/handler.go` documents the effective grants and must be
updated to match. Flagged as a required platform-team change. Clients only *subscribe* to
room-scoped subjects (they publish sends on the user-scoped tree), so only the
subscribe grant changes.

## Documentation

- **`docs/client-api.md`:** add the `crossSite` field to the room payload, document the
  local/global room subject namespaces, and update §2.1 with the `chat.local.room.>`
  subscribe grant. Update the derived views (`docs/client-api/request-reply.md`,
  `docs/client-api/events.md`) if touched.

## Testing (TDD)

- **`pkg/subject`:** table tests asserting both localities for `RoomEvent`,
  `RoomMsgStream`, `RoomMetadataUpdate`.
- **Classification write sites:** unit tests (mocked store) at room creation and member
  add in room-worker and room-service — same-site members leave `crossSite=false`, a
  cross-site member sets it true, and it stays true (sticky) on subsequent same-site
  adds.
- **broadcast-worker routing:** `crossSite` in `Meta` → publishes on the correct prefix;
  absent flag → global (fail-safe).
- **Client-facing payload:** `subscription.list` enrichment carries `crossSite`.
- **Integration tests** where a store contract changes (metacache projection, room
  write path), per the repo's testcontainers conventions.

## Rollout Ordering

1. Land `CrossSite` on the model + write path (defaults false; no behavior change yet).
2. Land the `pkg/subject` locality-aware builders (callers still pass `global=true`).
3. Wire publishers and the client payload to the flag.
4. Platform team adds the `chat.local.room.>` subscribe grant and the leaf-node filter.
5. Flip publishers/clients to honor the flag (local rooms move to `chat.local.room.>`).

Because global is the default at every step, each stage is safe to deploy independently;
the memory savings only begin once step 4's grant + filter and step 5 are in place.

## Rollout Runbook: `ROOM_SUBJECT_MODE` migration

This is the operational sequence for cutting a live deployment from all-global room
subjects to the local/global split, driven by the `ROOM_SUBJECT_MODE` gate
(`global` default → `dual` → `local`).

1. Platform adds `chat.local.room.>` to the production subscribe template.
   (No data backfill: deployments start from fresh data, so every room is
   classified `crossSite` at creation/member-add by `room-worker` — there are no
   pre-existing unclassified rooms to migrate.)
2. Deploy all publisher services (`broadcast-worker`, `room-service`, `room-worker`)
   with `ROOM_SUBJECT_MODE=dual` — same-site rooms are published to **both**
   prefixes; old clients (still subscribed on `chat.room.>`) and new clients (already
   subscribed on `chat.local.room.>`) both receive — zero gap.
3. Deploy the updated chat-frontend (subscribes same-site rooms on
   `chat.local.room.>`).
4. Once the frontend rollout completes, set publishers to `ROOM_SUBJECT_MODE=local`
   (drop the redundant global publish for same-site rooms).
5. Platform enables the leaf-node deny for `chat.local.>` from cross-gateway interest
   propagation → interest-map reduction realized.

**Rollback:** at any step, set `ROOM_SUBJECT_MODE=global` — publishers revert to
publishing all rooms on the global prefix only.

**Per-room flip after rollout — bounded by the grace window.** A local→global room
**flip** (a same-site room gains a cross-site member) re-routes an already-connected
client only when it re-subscribes. The **transition grace window** (see Client Routing &
Transition: publishers dual-publish a flipped room to both subjects for the locality
grace window) makes this gapless server-side for any client that re-subscribes within the
window via its normal reconnect / periodic `subscription.list` reconcile. With the default
1-week window this covers essentially every client, since clients re-read
`subscription.list` and re-subscribe on every reconnect (network change, deploy, app
foreground).

**Client contract for the flip:** the client re-routes when it next reads
`subscription.list` and re-subscribes the room by its `crossSite` value — on reconnect or
periodic reconcile. Correctness rests on the persisted flag + history/gap fetch, so a
client is correct as long as it reconnects/reconciles within the grace window; anything
past it is recovered by the normal history fetch. (An earlier design added a per-user
`event.room.update` push to trigger an immediate re-subscribe; it was dropped in review —
mass-nudging every subscriber is heavy for large rooms and the grace window already covers
the gap.)
