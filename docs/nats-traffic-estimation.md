# NATS Traffic Estimation

> Sizing model for a single site's NATS/JetStream traffic. Inputs are parameterized
> so the model can be re-run when assumptions change. All figures are estimates for
> capacity planning, not measured production telemetry.

## 1. Scope

This document estimates NATS message rates, bandwidth, connection-state load, and
JetStream storage for one independent site. It covers:

- The core JetStream streams and request/reply (R/R) endpoints that carry traffic.
- Per-payload size references used in the estimate.
- A traffic model for a **single connection per user** (baseline).
- A traffic model for **multiple concurrent connections per user** (desktop + mobile +
  web tabs).
- **Storage estimation** from per-stream retention (TTL).

Each site runs its own NATS server, so these figures are per-site.

> **Two phases.** A ~2-month **migration phase** (`MIGRATION_OPLOG`, §8) runs first and
> essentially alone; the **steady-state phase** (all other streams, §6–§7, §9) begins
> only after migration completes. The two do not overlap and are never summed.
>
> **Stream inventory** — the streams below match `pkg/stream/stream.go` (canonical
> names/subjects in §2.1); their **traffic volumes** remain estimates pending production
> telemetry. **Cross-site federation** uses a per-site `OUTBOX_{siteID}`
> (`chat.outbox.{siteID}.>`) whose consumer outbox-worker forwards each event to the
> destination's flat `INBOX_{siteID}` (`chat.inbox.{siteID}.>`). **Bot traffic** runs on a
> parallel `BOT_*` pipeline. Both federation and the bot pipeline are folded into the
> steady-state tables (§6.1/§9); the 4.5M/day total = 2.5M human + 2.0M bot (§4 split note).

## 2. Stream & Endpoint Inventory

### 2.1 JetStream streams

Stream names/subjects below reflect the current code (`pkg/stream/stream.go` schema +
`pkg/subject` builders), plus the new-design **`OUTBOX_{siteID}` + flat `INBOX_{siteID}`**
federation and the parallel **`BOT_*`** bot pipeline — both folded into the steady-state
estimate (§6.1/§9). `{siteID}` is the local site; `HR_` is keyed by the central site's ID.

| Stream | Subject(s) | Producer | Consumer(s) | In estimate? |
|--------|------------|----------|-------------|:---:|
| `MESSAGES_{siteID}` | `chat.user.*.room.*.{siteID}.msg.>` | Client | message-gatekeeper | ✅ |
| `MESSAGES_CANONICAL_{siteID}` | `chat.msg.canonical.{siteID}.>` | message-gatekeeper, room-worker (sys msgs) | message-worker, broadcast-worker, notification-worker, search-sync-worker | ✅ |
| `ROOMS_{siteID}` | `chat.room.canonical.{siteID}.>` | room-service | room-worker | ✅ |
| `INBOX_{siteID}` | `chat.inbox.{siteID}.>` | remote sites' `OUTBOX` (sourced), same-site services (local search feed) | inbox-worker, search-sync-worker | ✅ |
| `OUTBOX_{siteID}` | `chat.outbox.{siteID}.>` | room-worker, room-service, message-worker, user-service (cross-site publishers) | outbox-worker → forwards to remote `INBOX_{destSiteID}` | ✅ |
| `PUSH_NOTIFICATION_{siteID}` | `chat.server.notification.push.{siteID}.>` | notification-worker | push-gateway worker → APNs/FCM | ✅ |
| `HR_{centralSiteID}` | `chat.hr.{centralSiteID}.>` | hr-syncer (central site) | search-sync-worker (every fab) | ✅ |
| `SYSTEM_{siteID}` | `chat.system.{siteID}.>` | system services (admin/ops events) | system consumers | ✅ *(negligible, ~10/day)* |
| `MIGRATION_OPLOG_{siteID}` | `chat.migration.oplog.{siteID}.>` | oplog-connector | migration applier (1) | ⏱ §8 phase |
| `BOT_MESSAGES_CANONICAL_{siteID}` | `chat.bot.msg.canonical.{siteID}.>` | Bot Msg Handler | Shared Sync Worker, Bot Broadcast Worker, Bot Notification Worker | ✅ |
| `BOT_PUSH_NOTIFICATION_{siteID}` | `chat.bot.server.notification.push.{siteID}.>` | Bot Notification Worker | Bot Push Notification | ✅ |
| `BOT_PLATFORM_{siteID}` | `chat.bot.event.{siteID}.>` | Broadcast Worker | Bot Webhook Worker | ✅ |

**Bot streams (parallel pipeline).** Bot message traffic is split off the normal
(human) message flow onto a parallel `BOT_*` pipeline so it can be scaled, throttled, and
observed independently:

- `BOT_MESSAGES_CANONICAL_{siteID}` (`chat.bot.msg.canonical.{siteID}.>`) — the bot-side
  analogue of `MESSAGES_CANONICAL`. **Bot Msg Handler** publishes validated bot messages;
  **Shared Sync Worker**, **Bot Broadcast Worker**, and **Bot Notification Worker** consume.
- `BOT_PUSH_NOTIFICATION_{siteID}` (`chat.bot.server.notification.push.{siteID}.>`) — bot
  mobile-push lane. **Bot Notification Worker** publishes; **Bot Push Notification** forwards
  to APNs/FCM.
- `BOT_PLATFORM_{siteID}` (`chat.bot.event.{siteID}.>`) — outbound platform/webhook events.
  **Broadcast Worker** publishes; **Bot Webhook Worker** delivers to external bot platforms.

Bot traffic is sized alongside the human pipeline in §6.1/§9 (params in §4); the 4M/day
total = 2.5M human + 2.0M bot. Payload sizes are in §3.

**Federation via OUTBOX → INBOX.** Cross-site federation uses a per-site `OUTBOX_{siteID}`
stream (`chat.outbox.{siteID}.>`): a service at the origin site publishes each cross-site
event to its **local** OUTBOX, and its consumer **outbox-worker** forwards each event to
the destination's `INBOX_{destSiteID}` (`chat.inbox.{siteID}.>`), where inbox-worker
consumes and applies it to the local DB. `INBOX` carries no `internal`/`external` lane
split — its single flat subject holds both the federated inflow and the same-site
search-indexing feed.

### 2.2 Core NATS delivery subjects (server → client)

Fire-and-forget events clients subscribe to for real-time updates. Publishers verified
against the services (broadcast-worker, room-worker, user-presence-service); every subject
is built by a `pkg/subject` builder (named in parentheses).

| Subject (builder) | Publisher | Purpose |
|-------------------|-----------|---------|
| `chat.room.{roomID}.event` (`RoomEvent`) | broadcast-worker (message CRUD, reactions, pins, thread metadata, member changes); room-worker (room rename) | Channel/group room event delivery — the single combined event stream for a room |
| `chat.user.{account}.event.room` (`UserRoomEvent`) | broadcast-worker | Per-user delivery for DM / BotDM rooms and thread-subscription events |
| `chat.user.{account}.event.subscription.update` (`SubscriptionUpdate`) | room-worker | Subscription added / removed (room join / leave). inbox-worker deliberately does **not** re-publish — the origin site's room-worker already did |
| `chat.user.{account}.event.room.key` (`RoomKeyUpdate`) | room-worker (via `pkg/roomkeysender`) | Encrypted room-key material pushed to members |
| `chat.user.presence.state.{account}` (`PresenceState`) | user-presence-service | Presence state broadcast — online / away / offline / manual override |

> **Stale subjects removed (survey, 2026-07).** Prior drafts listed
> `chat.room.{roomID}.stream.msg`, `chat.user.{a}.stream.msg`, and
> `chat.room.{roomID}.event.metadata.update`. Those builders (`RoomMsgStream`,
> `UserMsgStream`, `RoomMetadataUpdate`) are no longer referenced by any service —
> message and per-message metadata delivery were consolidated onto the single combined
> `chat.room.{roomID}.event` / `chat.user.{account}.event.room` event. Presence delivery
> also moved from the placeholder `chat.user.{account}.event.presence` to
> `chat.user.presence.state.{account}`. User notifications are **not** a core delivery
> subject: they are handled by **notification-worker** via the push lane
> (`PUSH_NOTIFICATION_{siteID}`, §2.1) → APNs/FCM, so the former
> `chat.user.{account}.notification` (`Notification`) subject is dropped here. The traffic
> model (§6.2) sizes message + metadata as **one** combined `chat.room.{roomID}.event`
> delivery (~1.3KB), matching the consolidated event.

### 2.3 Request/Reply endpoints

Synchronous NATS request/reply (client publishes to `…request.…`, awaits an `_INBOX.>`
reply). Surveyed from the service registrations (`pkg/natsrouter` `Register`/`RegisterNoBody`
+ replying `QueueSubscribe`); mirrors `docs/client-api/request-reply.md`. **Bold** responses
are the heavy ones (return arrays/lists). The presence connection-lifecycle ops
(hello/ping/activity/bye) are `RegisterVoid` **publishes** (no reply); they're grouped into
the Presence R/R row below for completeness and flagged as publishes.

**Client-facing** (`chat.user.{account}.request.…`), by owning service — these are the four
groups sized in §6.3:

| Service (§6.3 group) | Subject families (after the `…request.` prefix) | Heaviest response |
|----------------------|--------------------------------------------------|-------------------|
| history-service (Message history R/R) | `room.{roomID}.{siteID}.msg.{history,next,surrounding,get,get.ids,edit,delete,pin,unpin,pinned.list,react,thread,thread.parent}` (13) | **`LoadHistoryResponse{Messages[]}`** (also next / surrounding / get.ids / thread / thread.parent / pinned.list) |
| user-service (Subscription R/R) | `user.{siteID}.{me,status.getByName,profile.getByName,status.set,subscription.list,subscription.getChannels,subscription.getDM,subscription.getByRoomID,subscription.count,subscription.setAppSubscription,apps.list,thread.list,thread.unread.summary}` (13) | **`PagedSubscriptionListResponse{Subscriptions[]}`** (list / getChannels); `ThreadListResponse`, `AppsListResponse` |
| room-service (Room R/R) | `room.{roomID}.{siteID}.{member.add,member.remove,member.role-update,member.list,member.statuses,subscription.mentionable,message.read,message.thread.read,message.read-receipt,mute.toggle,favorite.toggle,room.rename,key.get,app.tabs,app.cmd-menu,teams.call,teams.meeting}` · `room.{siteID}.create` · `orgs.{orgID}.{siteID}.members` · `teams.{siteID}.call.user` (20) | **`ListRoomMembersResponse`** / `ListOrgMembersResponse` / `MentionableSubscriptionsResponse` |
| search-service (Search R/R) | `search.{siteID}.{messages,rooms,apps,users}` (4) | **`SearchMessagesResponse`** |
| user-presence-service (Presence R/R) | `presence.{siteID}.manual.set` (R/R) · `chat.user.presence.{siteID}.query.batch` (R/R) · `event.presence.{siteID}.{hello,ping,activity,bye}` (publish¹) (6) | **`PresenceQueryResponse`** (batch) |

**Server-to-server** (`chat.server.request.…` / peers; not client-facing, not sized in §6.3):

| Subject | Owner | Response |
|---------|-------|----------|
| `chat.server.request.room.{siteID}.info.batch` | room-service | **`RoomsInfoBatchResponse`** |
| `chat.server.request.room.{siteID}.thread.info.batch` | room-service | **`ThreadRoomInfoBatchResponse`** |
| `chat.server.request.room.{siteID}.key.ensure` | room-service | `RoomKeyEnsureResponse` |
| `chat.server.request.room.{siteID}.restricted` | room-service | `StatusWithRequestReply` (admin) |
| `chat.server.request.room.{siteID}.create.dm` | room-worker | `SyncCreateDMReply` |
| `chat.server.request.thread.{siteID}.subscription.list` | history-service | **`ThreadSubscriptionListResponse`** |
| `chat.server.request.presence.{siteID}.query.batch` | user-presence-service | **`PresenceQueryResponse`** (peer leaf) |
| `chat.migration.internal.{siteID}.msg.{edit,delete}` | history-service | `MigrationAck` (migration-phase only) |

¹ `hello`/`ping`/`activity`/`bye` are client→server publishes (fire-and-forget, no reply)
on `chat.user.{account}.event.presence.{siteID}.{op}` — connection up / keepalive /
active-inactive / tab-close. Each ~1/user/day (= `C_hc`); grouped in the Presence R/R row
for inventory completeness, delivered to watchers in §6.2.

## 3. Payload Size Reference

| Category | Streams / Endpoints | avg | max |
|----------|---------------------|-----|-----|
| Message JetStream | MESSAGES, MESSAGES_CANONICAL | 500B–1.5KB | ~20KB |
| Push notification | PUSH_NOTIFICATION | ~0.8KB | ~15KB |
| HR sync | HR (≈ `model.User`: ~12 fields) | ~0.5KB | ~1KB |
| Migration oplog | MIGRATION_OPLOG | ~130KB | — |
| Room JetStream | ROOMS | 200–400B | ~5KB |
| Federation | INBOX, OUTBOX | 200–400B | ~5KB |
| Bot message JetStream | BOT_MESSAGES_CANONICAL | 500B–1.5KB | ~20KB |
| Bot push notification | BOT_PUSH_NOTIFICATION | ~0.8KB | ~15KB |
| Bot platform event | BOT_PLATFORM | 200B–1KB | ~5KB |
| Message R/R | history, search-messages | 15–50KB | 100KB+ |
| Room R/R | RoomsInfoBatch, CreateRoom, member.list | 2–20KB | ~65KB |
| Subscription R/R | subscription.get*, member-list | 100–300KB | ~700KB |
| Presence R/R | manual.set, query.batch (batch of ~P states) | 0.2–3KB | ~5KB |
| Presence publish | hello / ping / activity / bye (client → server) | ~50–200B | ~0.5KB |

## 4. Parameters (locked inputs)

| Parameter | Symbol | Value |
|-----------|--------|-------|
| Users | U | 20,789 |
| Subjects subscribed per connection | S | 100 (user + room) |
| Presence subjects watched per connection | P | **20** |
| Human messages per day (pipeline) | M | 2,500,000 *(user↔room 2.0M + user→bot 0.5M via broadcast-worker→BOT_PLATFORM). 4.5M total = 2.5M human + 2.0M bot.* |
| Human messages fanned ×F to rooms | M_room | 2,000,000 *(user↔room only; botDM/mention go to BOT_PLATFORM, no ×F)* |
| Recipients per room (fan-out) | F | 100 |
| Subscription R/R ops per day per connection | R_sub | **10** |
| Message-history R/R ops per day per connection | R_hist | **150** |
| Room R/R ops per day per user | R_room | **100** |
| Member add/remove ops per day per user *(member-change subset of R_room)* | R_member | **20** |
| Search ops per day per user | R_search | ~5 |
| Presence status changes per day per user | C_pres | ~10 |
| Presence health-check ops per day per user (hello/ping/activity/bye) | C_hc | 4 |
| Presence batch-query (initial-state) ops per day per user | R_presq | ~5 |
| Cross-site fan-out (remote sites per federated query) | N | ~3 |
| Federated fraction of member changes (→ OUTBOX / INBOX inflow) | f_fed | ~20% |
| Push notifications per day (human room msgs, = M_room) | M_push | 2,000,000 |
| HR sync records per daily run (burst @ 100 msg/s) | M_hr | 40,000 |
| Migration oplog QPS (sustained 24/7, 130KB payload, 1 consumer) | Q_mig | 200 |
| Bot-originated messages per day (→ BOT_MESSAGES_CANONICAL) | M_bot | 2,000,000 |
| Bot push notifications per day (= bot canonical input) | M_bot_push | 2,000,000 |
| User→bot events per day (→ BOT_PLATFORM, forwarded by broadcast-worker) | M_bot_platform | 500,000 |
| Bot fan-out per message (room delivery, core-NATS) | F_bot | 100 |
| Peak factor (business-hours concentration) | k | ~4× |

> **R_member** is the member-change slice of the 100 room ops/day: `R_member = 20` drives
> the ROOMS + INBOX streams + sys-message fan-out; **OUTBOX** (and the symmetric INBOX
> federated inflow) carries only the cross-site subset `× f_fed`. The remaining ~80 room ops
> are read-only Room R/R. Lines tagged *(member-driven)* scale linearly with R_member.

> **4.5M/day traffic split** (bot pipeline folded into §6.1/§9): user↔room 2.0M and user→bot
> 0.5M flow through `MESSAGES`→`MESSAGES_CANONICAL` (M = 2.5M; broadcast-worker forwards the
> 0.5M user→bot subset to `BOT_PLATFORM`, no ×F). Bot-originated 2.0M enters via Bot Msg
> Handler → `BOT_MESSAGES_CANONICAL`. Total = 2.5M human + 2.0M bot = 4.5M.

## 5. Methodology — ingress vs. fan-out

NATS traffic is **not** the inbound action count; it is dominated by fan-out (delivery
to subscribers). Each message flows:

```
Client ─pub→ MESSAGES ─→ gatekeeper ─pub→ CANONICAL ─→ 4 consumers
                                                       └─ broadcast-worker ─pub→ chat.room.{id}.event (+DM chat.user.{a}.event.room) ──→ ×F members
                                                       └─ broadcast-worker ─pub→ room metadata (lastMessageAt) ──→ ×F members
                                                       └─ notification-worker ─pub→ PUSH_NOTIFICATION → push gateway → APNs/FCM
```

Per message: **~7 JetStream hops** (2 stores + 5 consumer deliveries, persisted) plus
**~2×F core deliveries** (room stream + metadata). The broadcast-worker publishes once
per subject; the multiplication happens at the NATS delivery layer.

`PUSH_NOTIFICATION` and `HR` are **terminal JetStream streams** — a single
worker consumes and forwards out-of-band (APNs/FCM, HR DB), so they incur no ×F NATS
fan-out.

## 6. Single-Connection Baseline — per stream & endpoint

One NATS connection per user (connections = U = 20,789). Peak ≈ avg × k. "Ops/day"
counts publishes + consumer deliveries (JetStream), deliveries (core), or requests (R/R).

### 6.1 JetStream streams

| Stream | Driver | Pub/day | Deliveries/day | Ops/day | avg msg/s | Payload | Bytes/day |
|--------|--------|--------:|---------------:|--------:|----------:|---------|----------:|
| `MESSAGES` | M (client → gatekeeper) | 2.5M | 2.5M | 5M | 58 | 1KB | 5 GB |
| `MESSAGES_CANONICAL` | (M + member sys) × (1 pub + 4 consumers) | 2.92M | 11.7M | 14.6M | 169 | 1KB | 14.6 GB |
| `ROOMS` *(member-driven)* | R_member × U | 0.42M | 0.42M | 0.83M | 10 | 0.4KB | 0.33 GB |
| `INBOX` *(member-driven)* | (local feed R_member×U + federated inflow ×f_fed) × 2 consumers | 0.50M | 1.0M | 1.5M | 17 | 0.3KB | 0.45 GB |
| `OUTBOX` *(member-driven)* | R_member × U × f_fed (1 pub + outbox-worker) | 0.08M | 0.08M | 0.17M | 2 | 0.3KB | 0.05 GB |
| `PUSH_NOTIFICATION` | M_push × (1 pub + 1 consumer) | 2M | 2M | 4M | 46 | 0.8KB | 3.2 GB |
| `HR` | M_hr × (1 pub + 1 consumer); 100 msg/s burst | 40K | 40K | 80K | ~1 (200/s burst) | 0.5KB | 0.04 GB |
| `SYSTEM` | system/admin events (~10/day) | 10 | 10 | 20 | ~0 | — | ~0 GB |
| `BOT_MESSAGES_CANONICAL` | M_bot × (1 pub + 3 consumers) | 2M | 6M | 8M | 93 | 1KB | 8 GB |
| `BOT_PUSH_NOTIFICATION` | M_bot_push × (1 pub + 1 consumer) | 2M | 2M | 4M | 46 | 0.8KB | 3.2 GB |
| `BOT_PLATFORM` | M_bot_platform × (1 pub + Bot Webhook Worker) | 0.5M | 0.5M | 1M | 12 | 1KB | 1 GB |
| **JetStream subtotal** | | | | **~39M** | **~454** | | **~36 GB** |

`MESSAGES_CANONICAL` pub = 2.5M human messages + ~0.42M member-change system messages.
**OUTBOX** carries only the **cross-site subset** of member changes (`f_fed ≈ 20%` — those
touching a federated room); its lone consumer **outbox-worker** forwards each to the
destination site's `INBOX`. **INBOX** carries the local-origin search feed (all member
changes, `R_member × U`) **plus** the federated inflow from remote outbox-workers (`×f_fed`),
both on `chat.inbox.{siteID}.>` (×2 consumers: inbox-worker applies, search-sync-worker
indexes). Bot streams (`BOT_*`) are the parallel bot
pipeline (4M split in §4); bot room fan-out is core-NATS (§6.2 note), not shown here.
`MIGRATION_OPLOG` is **excluded** — separate ~2-month phase (§8).

### 6.2 Core delivery subjects (server → client, fanned out ×F or ×P)

| Subject | Driver | Pub/day | Deliveries/day | avg msg/s | Payload | Bytes/day |
|---------|--------|--------:|---------------:|----------:|---------|----------:|
| `RoomEvent`/`UserRoomEvent` — message + `lastMessageAt`, **human + bot** (`chat.room.*.event` +DM `chat.user.*.event.room`) | (M_room + M_bot) × F | 4.0M | 400M | 4,630 | ~1.3KB | 520 GB |
| member-change — sys msg on `chat.room.*.event`, member event on `chat.room.*.event.member` *(member-driven)* | R_member × U × F | 0.42M | 41.6M | 481 | 0.4KB | 17 GB |
| `chat.user.*.event.subscription.update` (`SubscriptionUpdate`) | R_room × U (read/mute/favorite; per-user) | 2.08M | 2.08M | 24 | 0.5KB | 1 GB |
| `chat.user.*.event.room.key` (`RoomKeyUpdate`) *(member-driven)* | R_member × U × F ÷ 2 (rotation on member-remove only) | 0.21M | 21M | 241 | 0.2KB | 4.2 GB |
| `chat.user.presence.state.*` (`PresenceState`, status broadcast) | U × C_pres × P | 0.21M | 4.16M | 48 | 0.15KB | 0.6 GB |
| `chat.user.*.event.presence.{siteID}.{hello,ping,activity,bye}` — **client→server** (ingress; §2.3), watcher fan-out modeled here | U × C_hc × P | 0.083M | 1.66M | 19 | ~0.05KB | 0.08 GB |
| `chat.room.*.event.typing` | active room only | — | — | — | — | *(ignored)* |
| **Core delivery subtotal** | | | **~470M** | **~5,440** | | **~543 GB** |

**Reconciling with §2.2 — the message/metadata/member-change rows.**

- **message + metadata** are **one** delivery: the `chat.room.{roomID}.event` (`RoomEvent`) /
  `chat.user.{account}.event.room` (`UserRoomEvent`) event carries both the message body and
  the room's `lastMessageAt` (for sidebar reordering) — there is **no dedicated metadata
  subject** (the old `RoomMetadataUpdate` / `…event.metadata.update` builder is unused). Covers
  **both human and bot** messages: the Bot Broadcast Worker delivers bot-originated messages
  (`M_bot`) to room members on the same event, so the driver is `(M_room + M_bot) × F` — one
  `×F` row at ~1.3KB (~1KB body + ~0.3KB metadata).
- **member-change** spans **two** subjects. The *system message* (`members_added` /
  `member_removed`) is a real `model.Message` published to `chat.msg.canonical.{siteID}.created`
  (MESSAGES_CANONICAL, so it also rides the §6.1 CANONICAL member-sys pub) and delivered by
  broadcast-worker on `chat.room.{roomID}.event` / `chat.user.{account}.event.room` — the
  same path as a normal message. The *member event* (`member_added` / `member_removed`) is
  published **directly** by room-worker to `chat.room.{roomID}.event.member`
  (`RoomMemberEvent`), which clients subscribe to. That `.event.member` subject is
  server→client but **not yet in §2.2** (survey gap). The single `×F` here is a lower bound —
  sys-msg and member-event each fan out to ~F members.

- **`SubscriptionUpdate`** (`chat.user.*.event.subscription.update`) is **per-user**, not a
  room fan-out (×1, to the affected account's own connections). It fires on member add/remove
  **and** on that user's own `read` / `mute` / `favorite` / `role` / `rename` actions — so it
  scales with **`R_room`** (the state-mutating room ops), *not* `R_member`. `R_room × U` here
  is an upper bound: read-only room RPCs (member.list, statuses, mentionable) don't fire it.
- **`RoomKeyUpdate`** (`chat.user.*.event.room.key`) is **member-driven**: only a member
  **remove** rotates the room key (an add just hands the current key to the new member), so
  ~half of the `R_member` ops trigger a full ×F rotation — `≈ R_member × U × F ÷ 2`, ~21M
  deliveries/day at ~0.2KB (E2E rooms).

The presence health-check row is a **client→server publish** (`RegisterVoid`, §2.3) modeled
here for its optional watcher fan-out, not a true server→client subject.

Bot room delivery (Bot Broadcast Worker → members, `M_bot × F` ≈ ~200M deliveries/day) is
now **included** in the message row above — bots post to rooms and fan out to members on the
same `RoomEvent`/`UserRoomEvent` as human messages.

Presence status delivery = ~4.16M/day (`C_pres = 10` changes × `P = 20` watchers), on
`presence.state.*`. The health-check row is **client-published** (each connection publishes
hello/ping/activity/bye to its own presence subject); each op is modeled as fanning out to
the user's P watchers like a status change, but with a tiny ~50B payload it adds negligible
bytes (and if these are server-only keepalives with no fan-out, the delivery count drops
~20× — §11).

### 6.3 Request/Reply endpoints

Client-facing (`chat.user.{account}.request.…`, scales with D). Groups per §2.3.

| Endpoint group (service) | Driver | Req/day | req/s | Resp payload | Bytes/day |
|--------------------------|--------|--------:|------:|-------------|----------:|
| Subscription R/R (user-service) | R_sub × U | 0.21M | 2.4 | 150KB | 31 GB |
| **Message history R/R (history-service)** | R_hist × U | 3.12M | 36 | 30KB | **94 GB** |
| Room R/R (room-service) | R_room × U | 2.08M | 24 | 10KB | 21 GB |
| Search R/R (search-service) | R_search × U | 0.10M | 1.2 | 30KB | 3 GB |
| Presence R/R (user-presence-service) | (R_presq + 1) × U | 0.12M | 1.4 | 3KB | 0.3 GB |
| **Client R/R subtotal** | | **~5.6M** | **~65** | | **~149 GB** |

Server-to-server (`chat.server.request.…`, **server-side — flat with D**, derived from the
ops that trigger each; see §2.3). Kept out of the client subtotal so §7's ×D scaling
doesn't inflate it.

| Endpoint | Driver | Req/day | req/s | Resp payload | Bytes/day |
|----------|--------|--------:|------:|-------------|----------:|
| room `info.batch` enrichment | ≈ R_sub × U (1 per subscription-list) | 0.21M | 2.4 | 10KB | 2.1 GB |
| `key.ensure` / `create.dm` | ≈ R_member × U (member-change) | 0.42M | 5 | 0.3KB | 0.1 GB |
| `thread.subscription.list` (cross-site) | thread.list × N | 0.12M | 1.4 | 8KB | 1.0 GB |
| presence `query.batch` peer (cross-site) | query.batch × N | 0.31M | 3.6 | 3KB | 0.9 GB |
| **Server-to-server subtotal** | | **~1.1M** | **~12** | | **~4 GB** |

### 6.4 Totals

| Layer | Ops or deliveries/day | avg msg/s | Bytes/day |
|-------|----------------------:|----------:|----------:|
| JetStream streams (incl. `BOT_*`, server-side/flat) | ~39M | ~454 | ~36 GB |
| Core delivery subjects (scales with D) | ~470M | ~5,440 | ~543 GB |
| Client R/R (req + resp, scales with D) | ~11M | ~130 | ~149 GB |
| **TOTAL (steady-state, D=1)** | **~0.52B/day** | **~6,020/s avg · ~24,100/s peak** | **~0.73 TB/day** |

Plus **server-to-server R/R** (§6.3) — server-side, **flat with D** — ~2.1M ops/day (req +
resp), ~25/s, ~4 GB/day per site. Like the JetStream layer it does not scale with D (§7);
the ×D total in §7 scales only Core delivery + Client R/R.

Excludes `MIGRATION_OPLOG` (separate phase — §8).

### 6.5 Connection state

> **Connection calculation.** Subscription interests = connections × avg subscriptions
> per connection. Example: **50,000 connections × 100 subscriptions = 5,000,000 (5M)
> interests.** (Note: 5,000 × 100 = 500K — to reach 5M you need 50,000 connections, the
> multi-device scenario in §7.) Presence adds `connections × P` more interests.

At the single-connection baseline: 20,789 connections × (S=100 + P=20) =
**~2.5M subscription interests**.

### 6.6 Bottlenecks (steady-state)

- **Message delivery = ~520 GB/day & ~400M deliveries/day (~4,630/s)** — **human + bot**
  messages fanned ×F (one combined event with metadata); the dominant term for *both*
  bandwidth (~71% of bytes) and message rate, driven by F = 100 on (M_room + M_bot) = 4.0M.
- **Message history R/R = ~94 GB/day** — the largest R/R term now that R_room dropped to
  100/day (Room R/R ~21 GB). R/R is ~32% of steady-state bytes.
- **Member-change fan-out = ~42M deliveries/day** — driven by `R_member = 20/day/user`
  (sys-message + member-event broadcast ×F); **room-key rotation** adds another ~21M/day
  (member-remove only → ×F), and OUTBOX/INBOX federation ~1.7M ops/day.

### 6.7 Optimization levers

- **Message fan-out (×F) is the top bandwidth term** (~507 GB/day, human + bot). Metadata already rides
  the message event (no separate delivery to coalesce), so the remaining levers are reducing
  effective fan-out (e.g. don't deliver to idle/very-large rooms in real time) or trimming
  the combined payload.
- **Keep subscription-list payloads lean** — at R_sub = 10/day the endpoint is no longer
  dominant, but its 150KB response still makes each call expensive on reconnect bursts.

## 7. Multiple Connections Per User

Real users connect from several clients at once — e.g. **1 desktop + 1 mobile + 3 web
tabs = 5 concurrent connections** (`D = 5`). Each connection is an independent NATS
subscriber that re-subscribes and fetches its own state.

### 7.1 What scales with D and what stays flat

| Scales linearly with D | Stays flat (independent of D) |
|------------------------|-------------------------------|
| All core deliveries (message, metadata, member-change, presence) | Message ingress (user sends from one client) |
| Subscription, history, room, search, **presence** R/R (client-facing) | JetStream pipeline & terminal streams (server-side) |
| Connections & subscription interests | `MIGRATION_OPLOG`, `PUSH`, `HR`, **server-to-server R/R** (§6.3, server-side, not client-facing) |

Rule of thumb: **ingress and server-side processing are flat; delivery/egress, R/R, and
connection state scale with D.** Effective fan-out becomes `F × D = 500` per message.

### 7.2 D=1 vs. D=5

| Layer | D=1 deliveries/day | D=5 deliveries/day | D=1 bytes/day | D=5 bytes/day |
|-------|-------------------:|-------------------:|--------------:|--------------:|
| JetStream streams (incl. bot) | ~39M | ~39M *(flat)* | ~36 GB | ~36 GB |
| Core delivery | ~470M | ~2,350M | ~543 GB | ~2,715 GB |
| Client R/R (req+resp) | ~11M | ~56M | ~149 GB | ~745 GB |
| Server-to-server R/R | ~2.1M | ~2.1M *(flat)* | ~4 GB | ~4 GB |
| **TOTAL (steady-state)** | **~0.52B** | **~2.45B** | **~0.73 TB** | **~3.50 TB** |
| **avg / peak msg/s** | ~6.0k / ~24k | **~28k / ~113k** | | |

Connection state at D=5: **~104k connections** × (100 + 20) = **~12.5M subscription
interests**. Excludes `MIGRATION_OPLOG` (server-side, does not scale with D — §8).

### 7.3 Takeaways for multi-device

- Traffic scales **~linearly with D** (~5×), because the only flat term (ingress + terminal
  streams) is a tiny fraction of the total.
- **Message delivery dominates at D=5 (~2.5 TB/day, ~74% of bytes)** — the per-message
  fan-out (human + bot, message + metadata in one event), multiplied across every connected
  device. Reducing effective fan-out (§6.7) is the highest-value lever.
- **Reconnect storms multiply by D**: a restart triggers up to `U × D ≈ 104k`
  simultaneous subscription-list fetches (150KB each) — jitter/rate-limit to avoid a
  thundering herd.

## 8. Migration Phase — MIGRATION_OPLOG (standalone)

`MIGRATION_OPLOG_{siteID}` runs as a distinct **~2-month migration phase that precedes
live traffic** — the steady-state streams (§6–§7) and their storage (§9) carry
essentially no load until migration completes. The two phases do **not** overlap, so
these figures are reported on their own and must never be summed with the steady-state
tables.

Parameters: Q_mig = 200 msg/s sustained 24/7 · payload 130KB · 1 consumer (applier) ·
TTL 8 hr.

### 8.1 Traffic

| Flow | Rate | Per day | Bytes/s | Bytes/day |
|------|-----:|--------:|--------:|----------:|
| Publish (`chat.migration.oplog.{siteID}.>`) | 200 msg/s | 17.28M | 26 MB/s | ~2.25 TB |
| Consumer delivery (×1 applier) | 200 msg/s | 17.28M | 26 MB/s | ~2.25 TB |
| **Total** | **400 msg/s** | **34.56M** | **~52 MB/s** | **~4.49 TB/day** |

Sustained 24/7 with no business-hours peaking (avg = peak). At ~208 Mbps each way it is
still bandwidth-led (130KB payload) rather than a message-rate problem.

### 8.2 Storage

| TTL | Retained msgs | Logical |
|-----|--------------:|--------:|
| 8 hr | 5.76M | ~0.75 TB |

`= 200 msg/s × 28,800 s × 130KB`. TTL is the only storage lever — keep it as short as
the migration tolerates.

### 8.3 Sizing implications

- **Isolate it.** Put MIGRATION_OPLOG on its own stream/account or dedicated NATS nodes
  and disk so its ~52 MB/s and ~0.75 TB footprint cannot starve the live chat cutover
  that follows.
- **Provision for the phase, then reclaim.** After the ~2-month window the stream can be
  retired and its ~0.75 TB disk + network headroom returned to steady-state growth.
- **Does not scale with device count** — it is server-to-server migration plumbing.

## 9. Storage Estimation (steady-state, per-stream TTL)

JetStream storage at steady state ≈ `publish-rate × retention (TTL) × payload`. Figures
**logical** bytes per the stream's TTL (replication is out of scope here).

| Stream | TTL | Pub/day | Payload | Retained msgs | Logical storage |
|--------|-----|--------:|---------|--------------:|----------------:|
| `MESSAGES` | 8 hr | 2.5M | 1KB | 0.83M | 0.8 GB |
| `PUSH_NOTIFICATION` | 8 hr | 2.0M | 0.8KB | 0.67M | 0.5 GB |
| `HR` | 8 hr | 40K (daily burst) | 0.5KB | 40K | 0.02 GB |
| `SYSTEM` | 8 hr | 10 | — | negligible | ~0 GB |
| `MESSAGES_CANONICAL` | 1 day | 2.92M | 1KB | 2.92M | 2.9 GB |
| `INBOX` | 7 day | 0.50M | 0.3KB | 3.5M | 1.1 GB |
| `OUTBOX` | 7 day | 0.08M | 0.3KB | 0.59M | 0.2 GB |
| `ROOMS` | 1 day | 0.42M | 0.4KB | 0.42M | 0.2 GB |
| `BOT_MESSAGES_CANONICAL` | 1 day | 2.0M | 1KB | 2.0M | 2.0 GB |
| `BOT_PUSH_NOTIFICATION` | 8 hr | 2.0M | 0.8KB | 0.67M | 0.5 GB |
| `BOT_PLATFORM` | 1 day | 0.5M | 1KB | 0.5M | 0.5 GB |
| **TOTAL (logical)** | | | | | **~9 GB** |

Excludes `MIGRATION_OPLOG` storage (separate phase — §8).

Notes:
- `MESSAGES_CANONICAL` (2.9 GB) and `BOT_MESSAGES_CANONICAL` (2.0 GB) are the canonical
  sources of truth; at **1-day** retention their footprint is modest. `INBOX` (1.1 GB, 7-day)
  is next; the steady-state storage total is ~9 GB (1-day canonical retention keeps it low).
- `HR` is a once-daily 40K burst; with an 8 hr TTL the whole batch is retained for
  ~8 hr then expires. Peak ingest is 100 msg/s during the ~7-minute burst.
- For TTL < 1 day, retained ≈ `(pub/day) × (TTL_hours / 24)`.

## 10. Per-Fab Traffic Summary

Each fab is an independent site (its own NATS). The steady-state model (§6) decomposes
into a **per-message** component (delivery fan-out — ~71% of bytes, ~77% of deliveries)
and a **per-user** component (R/R, presence, member events — ~40% of bytes); since message
volume itself scales with users, each fab's total scales ~linearly with its user count.
`Msg/day` below is the **human pipeline** volume (2.5M for Fab 1); the bot pipeline (§6.1)
and server-to-server R/R are included in Deliveries/Traffic and also scale ∝ users. Fab 1
reproduces §6.4 exactly; the rest scale by user count (all other parameters — F, P, R_sub,
R_member, etc. — held equal across fabs).

Figures are **steady-state, single connection per user (D=1)**, and **exclude
`MIGRATION_OPLOG`** (separate phase — §8). Per-fab numbers are **not summed** — size each
site independently. Peak ≈ 4× avg.

| Fab | Users | Msg/day | Deliveries/day | avg msg/s | peak msg/s | Traffic/day | avg MB/s |
|-----|------:|--------:|---------------:|----------:|-----------:|------------:|---------:|
| Fab 1 | 20,789 | 2.50M | ~523M | 6,050 | 24,210 | 0.73 TB | 8.5 |
| Fab 2 | 12,150 | 1.46M | ~306M | 3,540 | 14,150 | 0.43 TB | 4.9 |
| Fab 3 | 2,922 | 0.35M | ~73M | 850 | 3,400 | 0.10 TB | 1.2 |
| Fab 4 | 2,078 | 0.25M | ~52M | 600 | 2,420 | 0.07 TB | 0.8 |
| Fab 5 | 17,061 | 2.05M | ~429M | 4,970 | 19,870 | 0.60 TB | 7.0 |
| Fab 6 | 4,138 | 0.50M | ~104M | 1,200 | 4,820 | 0.15 TB | 1.7 |
| Fab 7 | 2,199 | 0.26M | ~55M | 640 | 2,560 | 0.08 TB | 0.9 |
| Fab 8 | 3,244 | 0.39M | ~82M | 940 | 3,780 | 0.11 TB | 1.3 |
| Fab 9 | 4,492 | 0.54M | ~113M | 1,310 | 5,230 | 0.16 TB | 1.8 |
| Fab 10 | 4,754 | 0.57M | ~120M | 1,380 | 5,540 | 0.17 TB | 1.9 |
| Fab 11 | 5,537 | 0.67M | ~139M | 1,610 | 6,450 | 0.19 TB | 2.3 |
| Fab 12 | 4,356 | 0.52M | ~110M | 1,270 | 5,070 | 0.15 TB | 1.8 |
| Fab 13 | 2,227 | 0.27M | ~56M | 650 | 2,590 | 0.08 TB | 0.9 |
| Fab 14 | 5,215 | 0.63M | ~131M | 1,520 | 6,070 | 0.18 TB | 2.1 |

Per-fab byte split holds at the §6.4 ratio for every site: **core delivery ~75%**, R/R
~21%, JetStream streams (incl. bot) ~5%. For multi-device (D), scale Deliveries/day,
Traffic/day, and MB/s by the rule in §7 (≈ ×D); `MIGRATION_OPLOG` per fab is per §8 and
independent of D.

## 11. Caveats

- **R_member = 20/day/user** is the member-change slice of the 100 room ops (§4); all
  *(member-driven)* lines (ROOMS, OUTBOX, INBOX, member fan-out) scale with it.
- **Presence** assumes ~10 event-driven status changes/user/day (`C_pres`); a heartbeat
  design would explode this.
- **Presence health-check** (hello/ping/activity/bye, C_hc = 4/user/day) is modeled as
  publishing to `chat.user.{account}.event.presence.{siteID}.{op}` and fanning out to the
  user's P watchers (~1.66M deliveries/day, negligible bytes). If these are server-only
  keepalives with no watcher fan-out, the delivery count drops ~20× (to ~83k/day).
- **Cross-site federation is now modeled** (§6.1/§9): a per-site `OUTBOX_{siteID}`
  (`chat.outbox.{siteID}.>`) whose consumer **outbox-worker** forwards each event to the
  destination's `INBOX_{siteID}` (`chat.inbox.{siteID}.>`). OUTBOX carries only the cross-site
  subset of member changes (`≈ R_member × U × f_fed`, f_fed ~20%); the symmetric INBOX inflow
  scales the same. Volumes are estimates pending telemetry.
- **PUSH_NOTIFICATION / HR / BOT_* / MIGRATION_OPLOG streams are defined in
  `pkg/stream/stream.go`** (names/subjects per §2.1); their **traffic volumes** here remain
  estimates — revisit against production telemetry.
- **MIGRATION_OPLOG is a separate ~2-month phase (§8)** — reported standalone and never
  summed with steady-state. At ~4.5 TB/day traffic and ~0.75 TB storage it dwarfs
  the entire steady-state load (~0.73 TB/day) while running; isolate it and reclaim the
  capacity after cutover.
- **Message delivery is the dominant steady-state cost** (~71% of bytes, ~4,630 msg/s) —
  **human + bot** messages in one combined event, fanned ×F = 100 on (M_room + M_bot) = 4.0M.
  Reducing effective fan-out (§6.7) is the highest-value lever.
- **Bot pipeline (`BOT_*`) is folded into the steady-state tables** (§6.1 traffic, §9
  storage, §4 params) — the 4.5M total = 2.5M human + 2.0M bot (§4 split note). Bot room
  fan-out is core-NATS (~190M deliveries/day, §6.2 note) and not yet in the bytes.
- **Peak factor k ≈ 4** assumes 80% of traffic in a 10-hour window with 2× intra-window
  peaking. Figures are first-order; validate against production telemetry.
