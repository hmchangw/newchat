# Bot Messaging Pipeline

**Status:** Design spec for review. Not an implementation plan.

## 1. Overview

Introduce a bot messaging pipeline parallel to the existing user pipeline. Bots (internal, first-party) call `bot-platform-service` over HTTP. The service authenticates, rate-limits, deduplicates, and bridges each write over NATS. Two downstream flows:

- **Message flow** (send-in-room, send-DM): NATS req/reply to a new `bot-msg-handler` service, which validates, enriches, publishes to `BOT_MESSAGES_CANONICAL_{siteID}`, and returns the canonical `Message` synchronously. Four consumers persist, index, deliver, and notify — reusing extracted `pkg/broadcast` and `pkg/notify` libraries shared with the user pipeline.

- **Room management** (create-room, add/remove-member): NATS req/reply to a new `bot-room-service` (parallel to bot-msg-handler; sync RPC, direct Mongo writes). It publishes cross-site membership events via the **existing** `OUTBOX_{siteID}` + `outbox-worker` via `pkg/outbox`. Existing user-facing `room-service` is unchanged.

Bot messages and user messages share the same Cassandra table (`messages_by_room`) and the same Elasticsearch index, distinguished by `Participant.IsBot=true` on `Sender`. No new `authorKind` field.

## 2. Motivation

- **Sync semantics for bots**: bot SDKs are HTTP-based; the send-message contract must return server-assigned `messageID` and `createdAt` synchronously so callers can reference the message immediately.
- **Traffic isolation from user flow**: separate JetStream stream and worker deployments so a bot batch job or retry storm cannot starve user-message delivery.
- **Reuse, not duplication**: broadcast and notification logic is identical bot ↔ user; extract into `pkg/broadcast` and `pkg/notify` and diverge only in the thin consumer wrapper.
- **First-class idempotency**: bot HTTP clients retry aggressively; the pipeline must dedupe at ingress so retries never produce duplicate downstream messages, even across pods and mid-flight retries.
- **Same storage surface**: history reads interleave bot + user messages natively (single Cassandra table, single ES index) — no union queries, no schema divergence.
- **Reuse existing federation machinery**: bot room management ops go through a dedicated `bot-room-service`, but its cross-site membership events publish to the existing `OUTBOX_{siteID}` via `pkg/outbox` and are forwarded by the existing `outbox-worker` — inheriting durable retry, per-destination FIFO for order-sensitive events, and the destination-side apply logic already in `inbox-worker`.

## 3. Scope

**In scope (this spec):**

| Op class | Endpoint (BP service) | Flow |
|---|---|---|
| Send message to room | `POST /api/v1/rooms/{roomID}/messages` | Message flow |
| Send DM to user | `POST /api/v1/dms/{userID}/messages` | Message flow |
| Create channel room (bot=owner, optional initial members + orgs) | `POST /api/v1/rooms` | Room management |
| Add members / orgs to room (batch) | `POST /api/v1/rooms/{roomID}/members/add` | Room management |
| Remove members / orgs from room (batch) | `POST /api/v1/rooms/{roomID}/members/remove` | Room management |

**Batch semantics**: both add and remove are `POST` with a body specifying `{userIds: [...], orgIds: [...]}`. This avoids the HTTP DELETE-with-body ambiguity of REST clients and keeps the path-parameter/body model consistent across add and remove.

**Explicitly deferred** (future specs, not in this design):

- Edit/delete message, reactions
- Room rename, room delete, ownership transfer
- History reads (deferred; direct client-facing `history-service` remains the read path)
- Multiple bot tiers, third-party bot marketplace

## 4. Architecture

### 4.1 Message flow — send to room / send DM

```text
Bot HTTP client
  │  POST /api/v1/rooms/{roomID}/messages   OR   POST /api/v1/dms/{userID}/messages
  │  headers: x-user-id, x-auth-token   (bot server does NOT send Idempotency-Key; BP derives opID internally per §9.0 for its own sentinel)
  ▼
Istio ingress (existing)
  │  TLS termination, connection-limit per source, body cap (~32 KiB),
  │  optional local_ratelimit as coarse per-pod ceiling
  ▼
bot-platform-service                                        (3-5 pods, Gin)
  │  serves both v1.0 external bot APIs and internal v2 APIs
  │  ├─ auth middleware        (validate x-auth-token, extract Participant identity)
  │  ├─ rate-limit middleware  (per-caller + global Valkey buckets, per endpoint)
  │  ├─ idempotency middleware (Valkey SET NX sentinel, 30s TTL; no cached response — retries re-execute after sentinel expires)
  │  └─ handler → NATS request/reply
  ▼
NATS request subject (via supercluster / NATS gateway routing):
  chat.server.bot.request.room.{siteID}.{roomID}.msg.send    (send-to-room)
  chat.server.bot.request.dm.{siteID}.{userID}.msg.send          (send-DM)
  ▼
bot-msg-handler                                             (n pods, queue-group)
  │  ├─ For DM: resolve targetUserID → DM roomID via idgen.BuildDMRoomID(botID, userID)
  │  │           ensure DM room via bot-room-service RPC (idempotent; called on every DM)
  │  ├─ lookup room (Mongo, cached)
  │  ├─ authorize: bot must be a member (checked via subscription store, cached)
  │  ├─ validate content (≤ 20 KiB non-empty; mentions valid; card payload valid)
  │  ├─ enrich: server messageID, createdAt, Sender = {ID: botID, IsBot: true, AppID, AppName, Account}
  │  ├─ JS publish → BOT_MESSAGES_CANONICAL_{siteID}   with MsgID = messageID (BP-generated, §9.0)
  │  │              (await ack; failure → return errcode.Internal → BP returns 5xx → bot safe-retries)
  │  └─ reply with full canonical Message
  ▼
BOT_MESSAGES_CANONICAL_{siteID}
  subject: chat.bot.canonical.{siteID}.created
  │
  ├───────────────┬──────────────────────┬─────────────────────┐
  ▼               ▼                      ▼                     ▼
bot-msg-worker  search-sync-worker    bot-broadcast-worker  bot-notification-worker
(Cassandra)    (shared, extended;    (pkg/broadcast wrapper) (pkg/notify wrapper)
               new consumer)                                    │
                                                                ▼
                                                    BOT_PUSH_NOTIF_{siteID}
                                                    subject: chat.bot.notification.push.{siteID}.>
                                                                │
                                                                ▼
                                                        bot-push-service (thin wrapper, APNs/FCM)
```

### 4.2 Room management — create room / add member / remove member

```text
Bot HTTP client
  │  POST /api/v1/rooms                                    (create room, bot=owner)
  │  POST /api/v1/rooms/{roomID}/members/add               (add members / orgs, batch)
  │  POST /api/v1/rooms/{roomID}/members/remove            (remove members / orgs, batch)
  ▼
Istio ingress → bot-platform-service (same middleware chain)
  │
  │  NATS request subject:
  │    chat.server.bot.request.room.{siteID}.create
  │    chat.server.bot.request.room.{siteID}.{roomID}.member.add
  │    chat.server.bot.request.room.{siteID}.{roomID}.member.remove
  ▼
bot-room-service                                            (2-3 pods, queue-group)
  │  parallels bot-msg-handler: sync req/reply, direct Mongo writes
  │  ├─ authorize (create: any authenticated bot; add/remove: bot must be room owner)
  │  ├─ Mongo write (rooms collection, subscriptions collection) — direct
  │  ├─ system message emission via publish to BOT_MESSAGES_CANONICAL
  │  │    ("Bot X added Alice" as Type=sysmsg; delivered by existing bot workers)
  │  └─ outbox.Publish(...) via existing pkg/outbox
  ▼
EXISTING OUTBOX_{siteID}  →  existing outbox-worker  →  chat.inbox.{destSiteID}.external.{member_added|member_removed}
  ▼
existing inbox-worker at remote site → remote Mongo subscription updates
```

**Federation reuse**: bot-room-service publishes to the existing `OUTBOX_{siteID}` via `pkg/outbox.Publish(...)`. Room management op volume is low (hundreds/day); events are indistinguishable at destinations from user-triggered membership events; existing outbox-worker per-destination lanes handle them without modification. No new streams, no new subjects for federation.

**Existing `room-service` is unchanged** — bot-room-service is a fully independent parallel service, matching the bot-msg-handler pattern for messages.

### 4.3 Cross-site federation summary

| Path | Used by | Guarantee |
|---|---|---|
| **A. OUTBOX + outbox-worker** | Membership events from room management flow (`member_added`, `member_removed`) via bot-room-service | At-least-once, retried forever, per-destination FIFO for `OrderedEventTypes` |
| **B. NATS supercluster subject routing (WS live delivery)** | Bot message live delivery to remote-site members via `subject.RoomEvent(roomID)` / `subject.UserRoomEvent(account)` — supercluster routes these subjects to WS receiver sessions at every site with subscribers. No cross-site content event exists. | Best-effort live push — missed on gateway outage; content persists at origin Cassandra; reads route to the room's origin site via the site-scoped request subject `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids` (supercluster routes), content is not copied to reader's site |

Bot messages inherit user-flow Path B semantics. See §12 (Known Limitations).

## 5. New services

### 5.1 bot-platform-service

HTTP → NATS bridge. Single service, all bot HTTP endpoints in scope.

| Aspect | Value |
|---|---|
| Deployment | 3-5 pods (230 peak rps ÷ 3 pods = ~77 rps/pod, comfortable; scale via HPA on CPU) |
| HTTP | Gin, `:8080`, `/healthz` |
| API surface | Both v1.0 external bot APIs and internal v2 APIs, unified through the same middleware chain |
| Auth | `x-user-id` + `x-auth-token` bearer-token headers. BP validates the session against its own Mongo `sessions`-like collection (Meteor / Rocket.Chat pattern). No JWT, no external auth RPC per request. See §7. |
| Dependencies | NATS, Valkey (rate-limit + idempotency) |
| Middleware order | auth → rate-limit → idempotency → handler |
| Outbound (msg flow) | NATS req/reply on `chat.server.bot.request.room.*.*.msg.send` and `chat.server.bot.request.dm.*.*.msg.send` → bot-msg-handler (site-scoped) |
| Outbound (room management flow) | NATS req/reply on `chat.server.bot.request.room.*.>` → bot-room-service (site-scoped) |
| NATS routing | Via NATS supercluster / gateway (site-scoped subjects) |
| Config | Standard `caarlos0/env` (see §11) |
| NATS request timeout | 3s (invariant: > idempotency sentinel TTL is NOT required; see §9.3) |

### 5.2 bot-msg-handler

NATS **request/reply** service. Modeled after `room-service` — the real sync req/reply template in this codebase. NOT modeled after `message-gatekeeper`, which is an async JetStream consumer that fires-and-forgets a reply publish, not a true req/reply RPC.

| Aspect | Value |
|---|---|
| Deployment | 3-5 pods (headroom over expected ~230 rps peak) |
| Transport | NATS req/reply, queue-group `bot-msg-handler` |
| Subjects consumed | `chat.server.bot.request.room.*.*.msg.send`, `chat.server.bot.request.dm.*.*.msg.send` |
| Dependencies | NATS + JetStream, MongoDB (subscriptions), Valkey (subscription cache) |
| Max NATS payload | Read `nc.MaxPayload()` at connect; enforce per-reply, matches `room-service` pattern |
| Publish target | JS publish to `chat.bot.canonical.{siteID}.created` with `MsgID = messageID` for JS-layer dedup |
| Reply | Full canonical `Message` envelope on success, typed `*errcode.Error` via `errnats.Reply` on failure |

**Per-request work:**

1. Extract caller `Participant` from NATS message header `X-Bot-Identity` (BP forwards it; see §7). BP also generates `messageID = idgen.GenerateMessageID()` and `createdAt = time.Now().UTC()` and forwards them in NATS headers `X-Bot-Message-ID` and `X-Bot-Created-At` — bot-msg-handler uses those verbatim and does NOT regenerate. If BP retries, a fresh (messageID, createdAt) pair is fine: Cassandra's compound PK dedups exact duplicates and any genuine retry-after-in-flight is already blocked by BP's Valkey sentinel (§9.1) so bot-msg-handler never runs concurrently for the same opID.
2. **Subscription-first lookup** — the room document lives at exactly one origin site, but the caller's subscription (which carries the room's origin `siteID`) lives at the caller's local site:
   - Read `subscriptions` by `(roomID, botID)` from local Mongo (for send-to-room). Cache in a local LRU with 60s TTL (§5.2.1). Missing → `errcode.Forbidden(reason: "not_a_room_member")`.
   - For DM: derive `roomID = idgen.BuildDMRoomID(botID, targetUserID)` and look up the subscription. If missing → the DM has never been created from this side; call bot-room-service local RPC `chat.server.bot.request.room.{siteID}.dm.ensure` to create the local DM room + subscription and fan out a `member_added` to the target user's site (see §17).
3. **Room-info fetch (cross-site request)** — the room doc lives only at its origin site (`subscription.SiteID`). bot-msg-handler issues a NATS req/reply to that origin site's bot-room-service on `chat.server.bot.request.room.{originSiteID}.get` with `{roomID}` and receives back the authoritative room record (name, type, ownerID, restricted flag). If `originSiteID == localSiteID`, the request routes locally by NATS; otherwise the supercluster routes to the origin site. Result cached in the same LRU with 60s TTL. Missing → `errcode.NotFound(reason: "room_not_found")`.
4. Content validation and mention canonicalization:
   - `len(content) > 0 && len(content) <= 20*1024`.
   - **Mentions canonicalized**: for every `Mention` in the request, keep `mention.ID` and IGNORE all other client-supplied fields (account, isBot, appID, appName, engName, companyName). Resolve the ID via the cached subscription store — must be a member of the target room; reject with `errcode.BadRequest(reason: "mention_invalid")` otherwise. Then overwrite the mention Participant with server-side authoritative fields.
   - Card payload structure valid (schema check on `Card`).
   - **No attachments allowed** — bots send text and cards only. Enforced by `DisallowUnknownFields` at decode time (§14.1).
5. Enrich the canonical envelope: `ID = messageID`, `CreatedAt = createdAt` (both from the NATS headers set in step 1), `Sender` populated from `X-Bot-Identity`. Thread-reply fields (`ThreadParentMessageID`, `ThreadParentMessageCreatedAt`, `TShow`) copied through from the request body if present.
6. JS publish: `js.Publish(subject.BotCanonicalCreated(siteID), data, jetstream.WithMsgID(messageID))` — messageID doubles as MsgID for JS dedup within the 5-min window. Await ack with 2s timeout. Failure → return `errcode.Internal` (BP surfaces 5xx; bot safe-retries).
7. Reply with full canonical `Message`.

**Failure modes:** see §10 (Error mapping).

#### 5.2.1 Cache invalidation

Cache is TTL-only. **60s hard TTL** on every entry (subscription + room-info); no explicit invalidation via events.

Consequence: a bot removed from a room can send messages for up to 60s after the removal (stale-read window). Authorization check is correct at the moment it runs; the stale window is short and matches the read-your-own-writes consistency window room-service already accepts. Explicit event-driven invalidation was considered and rejected — it would add a new consumer surface for bot-msg-handler subscribing to internal-lane INBOX events that don't exist today.

### 5.3 bot-msg-worker

Consumes canonical stream, writes to Cassandra at the **origin site only**. Writes mirror `message-worker`'s pattern — dual-table for main-room messages, dedicated thread table for thread replies, at-rest encryption via the shared cipher when `ATREST_ENABLED=true`.

**Cross-site attribution**: bot-msg-worker does NOT publish any cross-site event for message content. Message content is **never** federated cross-site as an event. This mirrors reality of the user pipeline — no `message_created` inbox event type exists in the codebase; `inbox-worker` binds `external.>` only and has no handler for such an event; `message-worker` only publishes `thread_subscription_upserted` cross-site (not content). Cross-site live delivery to remote-site members is handled entirely by NATS supercluster WS subject routing — see §5.4 (bot-broadcast-worker) and §16.1.

| Aspect | Value |
|---|---|
| Deployment | **2 pods** (single durable consumer, deploys for HA not throughput) |
| Consumer | Durable pull, `MaxWorkers=100`, semaphore + `PullMaxMessages=200` |
| Consumer name | `bot-msg-worker` |
| Filter subject | `chat.bot.canonical.{siteID}.created` |
| AckWait | 30s |
| MaxDeliver | 5 (matches worker convention; after 5 deliveries the message is Ack-dropped as poison) |
| BackOff | Exponential [1s, 2s, 5s, 10s, 30s] |
| Bucket math | Reuses `pkg/msgbucket`, matches user flow bucketing |
| Idempotency at write layer | Cassandra primary key `(room_id, bucket, created_at, message_id)` — a redelivered event with same messageID is a no-op INSERT if the row is already written |
| At-rest encryption | Reuses `atrest.Cipher` (same instance/config as `message-worker`); when enabled, body columns (`msg`, `attachments`, `card`, `card_action`, `quoted_parent_message` body) are encrypted into `enc_payload` + `enc_meta`, plaintext columns bound NULL |

**Per-event work** — dual-table write (matches `message-worker.CassandraStore.SaveMessage` in `message-worker/store_cassandra.go`, wired via `pkg/msgbucket`):

1. If this is a main-room message (no `ThreadParentMessageID`): `UnloggedBatch` of two `INSERT`s in a single coordinator round-trip:
   - `INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, ...)` — bucket = `msgbucket.Of(msg.CreatedAt)`.
   - `INSERT INTO messages_by_id (message_id, created_at, room_id, sender, ...)` — same columns.
2. If this is a thread reply (`ThreadParentMessageID != ""`): mirror `message-worker.SaveThreadMessage`:
   - `INSERT INTO messages_by_id` (unbucketed, keyed by message_id) — every reply is retrievable by ID.
   - `INSERT INTO thread_messages_by_thread` (partitioned by `thread_room_id`, clustered by `created_at`).
   - If `TShow == true`: additionally `INSERT INTO messages_by_room` for the parent room, so the reply also renders in the channel timeline (the "also send to channel" flag).
3. **Encryption** — when the cipher is enabled: encrypt body columns via `cipher.Encrypt(ctx, roomID, enc)` before the batch; bind `enc_payload` and `enc_meta.nonce` in every INSERT above; bind the plaintext body columns as NULL. `sys_msg_data` is NOT encrypted (metadata).
4. Reply is not applicable — bot-msg-worker is an async consumer; the request/reply happens upstream in bot-msg-handler.

That's it — no cross-site publish. Reads route to the room's origin site via the site-scoped request subject `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids` (supercluster routes to the origin site's history-service); content is not copied to reader's site. Cross-site OUTBOX federation exists only for read state (`lastSeenAt`/unread), never for content.

**Behavior on Cassandra failure**: Nak with backoff → JS redelivers. Permanent failure (e.g. schema violation) → Ack with `errcode.Permanent(err)`, log at ERROR, emit `bot_msg_worker_permanent_error_total` counter.

### 5.4 bot-broadcast-worker

**New service — built via copied consumer skeleton + slim bot-specific handler (text/card only).** Not a wrapper around a shared library. Rationale: the existing user `broadcast-worker` handles 6 event types, thread fanout, encryption via `pkg/roomkeysender`. Extracting a shared `pkg/broadcast` `Broadcast(ctx, *Message) error` would be a rebuild, not an extraction — the interface doesn't fit.

Instead: bot-broadcast-worker copies the JetStream consumer skeleton from the user broadcast-worker (durable pull, semaphore, ack/nak/backoff, graceful shutdown) and pairs it with a slim handler that only handles bot-message shape (text + card, no attachments, no thread events, no encryption).

| Aspect | Value |
|---|---|
| Deployment | **2-3 pods** (sized for expected fanout ~4.6 MB/s per pod at 230 peak msg/s × 20-member rooms; scale up if load-testing exposes the 20 KiB worst-case) |
| Consumer | Durable pull, `MaxWorkers=100`, semaphore + `PullMaxMessages=200` |
| Consumer name | `bot-broadcast-worker` |
| Filter subject | `chat.bot.canonical.{siteID}.created` |
| AckWait | 30s |
| MaxDeliver | 5 (matches worker convention) |
| BackOff | Exponential [1s, 2s, 5s, 10s, 30s] |

**Publish targets** — core NATS subjects consumed by WS gateways / receiver sessions:

- Room-scoped broadcast: `subject.RoomEvent(roomID)` → `chat.room.{roomID}.event` (verified in `pkg/subject/subject.go:263`)
- Per-user direct notifications (e.g. DM alerts): `subject.UserRoomEvent(account)` → `chat.user.{account}.event.room` (verified in `pkg/subject/subject.go:267`)

**Cross-site delivery** — done entirely by NATS supercluster subject routing, not by bot-broadcast-worker code:

- bot-broadcast-worker publishes to `subject.RoomEvent(roomID)` regardless of member site.
- The NATS supercluster routes these subjects to WS receiver sessions at ALL sites where subscribers exist for that subject.
- Result: a bot message published at site A reaches WS sessions of subscribers at site B via supercluster routing — no bot-specific cross-site code needed.
- **Zero cross-site publish logic in bot-broadcast-worker.** This mirrors the user pipeline.

**Sysmsg cross-site**: not applicable. Bot room management sysmsgs are LOCAL-ONLY at the room's origin site (matches user `room-worker/handler.go:489`). Remote-site members learn membership from the `member_added` OUTBOX event; the narrative sysmsg does not federate.

### 5.5 bot-notification-worker

**New service — built via copied skeleton + slim bot-specific handler.** Not a wrapper around a shared library. Same reasoning as bot-broadcast-worker: extracting `pkg/notify` would be a rebuild not an extraction, and infrastructure isolation is a design goal (a bot-notification incident must not touch user push delivery).

Publishes push events **directly** to `BOT_PUSH_NOTIF_{siteID}` — no intermediate batching stream.

| Aspect | Value |
|---|---|
| Deployment | **1 pod** — matches existing user `notification-worker` deployment convention |
| Consumer | Durable pull, `MaxWorkers=100` |
| Consumer name | `bot-notification-worker` |
| Filter subject | `chat.bot.canonical.{siteID}.created` |
| AckWait | 30s |
| MaxDeliver | 5 (matches worker convention) |
| Publishes to | `chat.bot.notification.push.{siteID}.{kind}` on `BOT_PUSH_NOTIF_{siteID}` |
| Batch max_payload | `NATS_MAX_PAYLOAD_BYTES=262144` config, enforced by emitter (matches user `notification-worker`) |

**Trade-off**: single-pod deployment means no HA — a pod crash pauses notification emission until the pod is rescheduled. Downstream persistence and broadcast continue (their workers have their own pods). Accepted trade-off inherited from user notification-worker convention.

### 5.6 bot-push-service

Consumes `BOT_PUSH_NOTIF_{siteID}`, dispatches to APNs/FCM. Thin wrapper mirroring the user push notification service.

| Aspect | Value |
|---|---|
| Deployment | 2 pods |
| Consumer | Parallel pull, `MaxWorkers=100`, semaphore + `PullMaxMessages=200` (high-throughput pattern) |
| Ordering | **Best-effort per-device**. See ordering note below. |
| Consumer name | `bot-push-service` |
| Filter subject | `chat.bot.notification.push.{siteID}.>` |

**Ordering — honest statement**: JetStream distributes messages randomly across the 2 pods via the queue group, so per-user in-process sharding within a pod cannot guarantee global per-recipient FIFO — the same recipient's pushes can land on different pods. This is an inherent property of the deployment shape.

We accept this because APNs and FCM already don't guarantee inter-device ordering (each device's push channel has its own ordering, and different devices for the same user are independent). What the pipeline provides:

- **Best-effort per-device**: near-instantaneous same-pod dispatch means most pushes for the same device arrive in order in practice.
- **No strict cross-pod per-user FIFO**: not claimed.

If strict per-user FIFO is later required (rare — chat doesn't typically need it), options are: (a) partition subjects by userID hash (`chat.bot.notification.push.{siteID}.user.{userIDShard}.>`) with per-shard durable consumers pinned to specific pods, (b) single-pod deployment (loses HA). Neither is required for v1.

### 5.7 search-sync-worker (existing service, extended)

`search-sync-worker` today binds a consumer to `MESSAGES_CANONICAL_{siteID}`. **A JetStream consumer binds to exactly one stream — you cannot filter across streams.** Approach:

**Add a second durable consumer** in the existing search-sync-worker process, bound to `BOT_MESSAGES_CANONICAL_{siteID}`. Same handler code path (index into same ES index); bot vs user distinguished by `sender.isBot` filterable field on the ES document.

Two consumers in one worker process; single deployment; single ES index; zero org-sync branch for bots (bots don't own orgs).

### 5.8 bot-room-service

Sync NATS req/reply service for bot room management ops. Modeled after `room-service` (the real sync req/reply template). **Direct-write role**: bot-room-service writes `rooms` / `subscriptions` directly to Mongo.

**Attribution correction**: user `room-service` does NOT write these collections — it validates + publishes to `ROOMS` canonical stream, and `room-worker` materializes them. There is no existing `room-service.store.Insert*` API for rooms/subscriptions to reuse. Bot room management ops need direct-write semantics because they are sync req/reply — waiting for a room-worker materialization would defeat the "reply with the created room" contract. The room/subscription write code lives in bot-room-service's own `store_mongo.go` (per the standard service layout); if user-flow room-worker later needs to reuse it, extract to `pkg/roomstore` at that point — not speculatively.

| Aspect | Value |
|---|---|
| Deployment | 2-3 pods (low volume; sized for HA not throughput) |
| Transport | NATS req/reply, queue-group `bot-room-service` |
| Subjects consumed | `chat.server.bot.request.room.*.create`, `chat.server.bot.request.room.*.*.member.add`, `chat.server.bot.request.room.*.*.member.remove`, `chat.server.bot.request.room.*.get` (cross-site room-info fetch called by bot-msg-handler; site-scoped), `chat.server.bot.request.room.{siteID}.dm.ensure` (internal, called by bot-msg-handler) |
| Dependencies | NATS + JetStream, MongoDB (rooms, subscriptions), Valkey (identity cache, room membership cache) |
| Publish target (federation membership) | `pkg/outbox.Publish(...)` → existing `OUTBOX_{siteID}` with `member_added`/`member_removed` event types (existing `OrderedEventTypes` lane) |
| Publish target (sysmsg — local only) | `chat.bot.canonical.{siteID}.created` with `Type=sysmsg` — delivered to LOCAL room members by bot workers. Sysmsgs are NOT federated cross-site via OUTBOX — matches user `room-worker` behavior (`room-worker/handler.go:489` publishes the sysmsg only to the local canonical stream). |
| Reply | Structured envelope for each op (e.g. `{roomID, createdAt}` for create, `{added: [userIDs]}` for add); typed `*errcode.Error` on failure |
| Max NATS payload | `nc.MaxPayload()` at connect, enforce per-reply |

**Handled operations** (add/remove covers both individual users AND orgs; org expansion resolves org membership at request time via user-service RPC):

| Op | NATS subject | Behavior |
|---|---|---|
| Create channel room | `chat.server.bot.request.room.{siteID}.create` | See §5.8.1. Body: `{name, topic, members:[userID], orgs:[orgID]}`. |
| Ensure DM room | `chat.server.bot.request.room.{siteID}.dm.ensure` | Compute `roomID = idgen.BuildDMRoomID(bot, targetUser)`. Insert local `rooms` doc (this site is the DM's origin — see §17). Insert bot's local subscription. If targetUser on remote site: `outbox.Publish(member_added)` for the target carrying both participant accounts in payload — target's site upserts a subscription only, NOT a room doc (see §6.1). |
| Add members / orgs (batch) | `chat.server.bot.request.room.{siteID}.{roomID}.member.add` | Body: `{userIds:[...], orgIds:[...]}` — matches HTTP `POST /api/v1/rooms/{roomID}/members/add`. Verify bot is room owner. Expand orgs → users. **Diff against current subscriptions**: `newlyAdded = requested \ existing`. If `newlyAdded` is empty → return success with empty diff, no side effects. Otherwise: upsert local subscriptions for `newlyAdded`; per remote destination `outbox.Publish(member_added)` carrying accounts; emit ONE local sysmsg listing `newlyAdded`. |
| Remove members / orgs (batch) | `chat.server.bot.request.room.{siteID}.{roomID}.member.remove` | Body: `{userIds:[...], orgIds:[...]}` — matches HTTP `POST /api/v1/rooms/{roomID}/members/remove`. Verify bot is room owner. Diff similarly: `actuallyRemoved = requested ∩ existing`. If empty → return success with empty diff. Otherwise: delete local subscriptions; per remote destination `outbox.Publish(member_removed)`; emit ONE local sysmsg. Bot cannot remove itself via this op. |
| Get room | `chat.server.bot.request.room.{siteID}.get` | Body: `{roomID}`. Reads local `rooms` by `_id`, replies with the room record. Cross-site fetch: bot-msg-handler at other sites requests this against the room's origin site (routed by NATS supercluster). |

**5.8.1 Create channel room**

`POST /api/v1/rooms` accepts optional `members: [userID]` and `orgs: [orgID]` arrays in the body. Handling:

1. Generate `roomID = idgen.GenerateID()`. Insert `rooms` doc with `Owner = bot Participant`, `CreatedByBot = caller.ID`. This site becomes the room's origin (`SiteID = localSiteID` on the doc).
2. Insert owner subscription for the bot.
3. For each individual `userID` in the request's `members` array AND each user resolved from each `orgID` in `orgs` (org expansion via existing user-service RPC):
   - Insert local subscription (idempotent upsert on `(roomID, userID, siteID)`).
   - If target is on a remote site: `outbox.Publish(...)` `member_added` per destination. Target site upserts a subscription for that user; it does NOT provision a local room doc (rooms live only at origin — see §17, §6.1).
4. Emit ONE local sysmsg listing the initial members and orgs (`js.Publish` to local `BOT_MESSAGES_CANONICAL_{siteID}` with `MsgID = "bot-create:" + roomID`). Sysmsg is NOT federated cross-site; remote-site members receive the room's creation state via the `member_added` event, and the sysmsg only renders for local members.
5. Reply `{id, owner, members, orgs, createdAt}`.

**Idempotency**: relies on BP's Valkey sentinel (§9.1) — no `createOpID` field, no `bot_room_mgmt_ops` collection, no unique index. BP's sentinel serializes retries within the 30s window; beyond that a retry produces a fresh room, which is the same behavior as `POST /channels.create` in Rocket.Chat and is acceptable at this volume (hundreds of room creates/day).

**5.8.2 Add / remove — no durable idempotency collection**

There is no `bot_room_mgmt_ops` collection. BP's Valkey sentinel is the only in-flight guard. If a retry lands after the sentinel expires:

- The diff step (`newlyAdded = requested \ existing`) makes duplicate adds a no-op — re-adding a member already in the room is skipped, and the sysmsg is only emitted when `len(newlyAdded) > 0`.
- Duplicate `outbox.Publish(member_added)` events are deduped downstream by `inbox-worker`'s `$setOnInsert` on subscription upsert (matches user pipeline behavior).
- Remove is symmetric: `actuallyRemoved = requested ∩ existing`; a retry after the members are already gone is a no-op.

Together, this collapses retries to the same terminal state without a durable-op collection.

**5.8.3 Federation publish — direct `outbox.Publish`, no transactional outbox**

bot-room-service uses **direct `outbox.Publish(...)`** for cross-site membership events, matching how `room-worker` publishes membership events in the user pipeline. No transactional outbox collection, no relay goroutine, no cross-site sysmsg event.

*Crash gap (inherited from user pipeline)*: if bot-room-service crashes between Mongo commit and OUTBOX publishes, remote sites may not receive the membership event. This is **the same crash-window property that user-flow `room-worker` has today** — documented in §17.1 as an inherited limitation. A cross-pipeline fix (transactional outbox pattern spanning both pipelines) is a future initiative outside this spec's scope.

*Local sysmsg publish failure*: logged, emits `bot_room_service_sysmsg_publish_failures_total` for alerting. Operation still returns HTTP 200 because the state write succeeded — matches user room-worker's sysmsg emission semantics.

**Authorization rules:**

| Op | Rule |
|---|---|
| Create channel room | Any authenticated bot |
| Ensure DM room | Any authenticated bot (target user must exist) |
| Add member(s) / org(s) | Bot must be room owner |
| Remove member(s) / org(s) | Bot must be room owner; bot cannot remove itself |

**Bounded batch limits + endpoint-specific timeout budgets** (must be enforced pre-handler so latency is bounded):

| Op | Max userIds per request | Max orgIds per request | Max expanded users (after org resolution) | BP NATS request timeout |
|---|---|---|---|---|
| Create channel room | 100 | 5 | 500 | 15s |
| Add member(s) / org(s) | 100 | 5 | 500 | 15s |
| Remove member(s) / org(s) | 100 | 5 | 500 | 15s |
| Ensure DM room | 0 (single target from subject) | 0 | 1 | 3s |

Batches exceeding the limits are rejected with `errcode.BadRequest(reason: "batch_too_large", metadata: {"max": N, "attempted": X})`. Org expansion happens inside bot-room-service; if the resolved user set exceeds "Max expanded users", the whole op is rejected before any Mongo write.

**Cache invalidation**: bot-room-service maintains a small room+membership cache (60s TTL) for the ownership check on add/remove. Same TTL-only discipline as bot-msg-handler (§5.2.1).

## 6. Reused services (no changes required)

| Service | Role in bot pipeline |
|---|---|
| `room-service` | **Unchanged**. Bot room management ops go through the new `bot-room-service`, not this one. |
| `room-worker` | **Unchanged**. Consumes user membership events + emits sysmsgs on user side; bot sysmsgs are emitted directly by bot-room-service via the bot canonical stream. |
| `outbox-worker` | **Unchanged**. Forwards membership events cross-site via existing per-destination lanes. Bot-triggered events use the same `ConcurrentEventTypes` / `OrderedEventTypes` partition. |
| `inbox-worker` | Applies remote membership events to local DB. For DM `member_added` (`event.roomType == "dm"`), upsert the target user's subscription only — do NOT create a local rooms doc (DM origin lives at one site, matches channel rooms; see §6.1). |
| `auth-service` | Existing surface for chat-frontend login flows. **BP does NOT call auth-service on bot HTTP requests** — session validation is local to BP's Mongo. auth-service already calls BP the other way (`POST /api/v1/auth/validate` in `auth-service/bpvalidator.go`) when it needs to validate a bot session during NATS callout. No new coupling introduced. |

### 6.1 inbox-worker changes (in-scope for this spec)

**Attribution correction**: inbox-worker binds `chat.inbox.{siteID}.external.>` **only** (see `inbox-worker/main.go` — `cc.FilterSubjects = subject.InboxExternalAll(siteID)`). It NEVER sees the internal lane, and no `message_created` event is ever routed to any INBOX by any producer — bot or user. Message content does not pass through inbox-worker at all.

**Second correction**: `idgen.BuildDMRoomID` is a separator-less concat and **is not reversible**. There is no `idgen.IsDMRoomID` or `SplitDMRoomID` helper. Events must **carry both participant accounts in the payload**; the destination MUST NOT reconstruct parties from the roomID.

**DM `member_added` handling — subscription-only at target site** (the only change needed to inbox-worker for this spec):

Rooms live at exactly one origin site (see §17). When a target site receives `member_added` for a DM room whose origin is another site, it must NOT create a local rooms doc — only upsert the target user's subscription pointing back to the origin.

- Trigger: `chat.inbox.{siteID}.external.member_added` arriving at the destination site.
- Payload requirement: `MemberAddedEvent` carries `{roomID, roomType, addedUser: {id, account, siteID}, addedByUser: {id, account, siteID}, originSiteID, ...}`. Both participants' accounts/IDs are in the payload, plus `originSiteID` = the site that owns the room doc. Origin publishers (bot-room-service DM ensure, existing user DM path) MUST populate these fields.
- inbox-worker behavior on `member_added` for a DM room (`event.roomType == "dm"`):
  - Upsert the target user's subscription: `subscriptions.UpdateOne({roomID, userID: addedUser.id}, $set: {siteID: event.originSiteID, ...}, $setOnInsert: {createdAt: event.Timestamp}, upsert: true)`.
  - The `siteID` field on the subscription points to the DM's origin site, so history reads and room-info fetches from this site route back there.
  - **Do NOT insert a local `rooms` doc.** The room lives only at `event.originSiteID`.
- Idempotency: subscription upsert keyed on `(roomID, userID)` is a no-op on duplicate delivery.

**No message-content race at inbox-worker**: earlier drafts described a NAK-and-retry protocol for `message_created` at inbox-worker. That's removed — content never passes through inbox-worker (there is no `message_created` external-lane event, and content is not federated as an event). The only remaining ordering property is a bounded **client-side rendering window**: a remote-site WS client may receive a bot's live message via `subject.RoomEvent(roomID)` before it has learned membership via `member_added` propagation. Chat clients handle this the same way they handle all cross-site membership races today.

**No `bot_room_mgmt_sysmsg` handling.** Bot room management sysmsgs are LOCAL-ONLY at the room's origin site — see §16.1 and §5.8. inbox-worker has no new event-type handler to add for the bot pipeline; the only bot-driven change is the DM `member_added` behavior above.

## 7. Sender identity resolution

Every bot HTTP request carries **`x-user-id` + `x-auth-token`** — a simple bearer-token model, **not JWT**. `x-user-id` is a 17-char Meteor/Rocket.Chat-format user ID identifying the bot account; `x-auth-token` is a session token issued by BP at login time. BP is itself the authority for token validation — sessions are stored in BP's Mongo (Meteor pattern: `sessions`-like collection storing `sha256(authToken)` alongside `userId`, `expiresAt`).

**Attribution note**: auth-service, when it needs to validate a bot session (e.g., during NATS callout for chat-frontend login "as" a bot), calls BP via HTTP `POST /api/v1/auth/validate` and receives back a `Principal` — see `auth-service/bpvalidator.go`. This is the inverse of BP calling auth-service. For BP's own HTTP endpoints, no cross-service auth call is needed at all: BP looks up the session locally.

**Resolution path (BP internal, per request):**

```text
BP auth middleware:
  1. Read x-user-id + x-auth-token headers (both required; missing → 401).
  2. hashedToken = sha256(x-auth-token)
  3. Local Mongo lookup on the sessions collection:
       sessions.FindOne({ hashedToken, userId: x-user-id })
       - Missing / expired / userId mismatch → errcode.Unauthenticated(reason: "invalid_token") → 401
       - Found → session doc contains the validated userId
  4. Load full bot Participant from the users collection (or cached):
       users.FindOne({ _id: userId })
       → Participant{ ID, Account, AppID, AppName, IsBot, EngName, CompanyName }
  5. Require Participant.IsBot == true → else errcode.Forbidden(reason: "not_a_bot") → 403.
  6. Attach Participant to NATS request header `X-Bot-Identity` (JSON-encoded) for bot-msg-handler / bot-room-service to consume.
```

**Session validation caching**: none required. The session lookup is a single Mongo query keyed by `(hashedToken, userId)` with a supporting compound index. At ~1ms per lookup and 230 peak rps, that's 230 tiny queries/sec — trivial. If load ever justifies caching, add a **short (~5s) in-process LRU** keyed by `hashedToken → Participant`; anything longer weakens suspension propagation.

**Suspension propagation — immediate, no broadcast needed**:

- Suspension = delete the session doc from Mongo (BP admin ops or auth-service triggers this).
- Next request from the bot → Mongo lookup returns nothing → 401.
- **Immediate** (bounded by Mongo replication lag, typically single-digit ms). No cache TTL, no revocation broadcast, no `chat.auth.bot.revoked.*` subject.

**Deprecation note (informational)**: bot NATS JWTs (issued via callout for chat-frontend login "as" a bot account) are being feature-flagged off in favor of a separate bot-dev frontend. This spec's auth model doesn't depend on that feature-flag state — BP's `x-auth-token` bearer flow is the sole auth for the pipeline.

**Trust model — NATS account permissions** (deployment-side, not an in-code check): the internal-cluster NATS account topology restricts publish permission on `chat.server.bot.request.>` on a per-subject basis so that `X-Bot-Identity` (and any other request header) cannot be forged by an internal-cluster client that shouldn't be issuing that call:

| Subject | Publish permission granted to |
|---|---|
| `chat.server.bot.request.room.{siteID}.{roomID}.msg.send` | `bot-platform-service` only |
| `chat.server.bot.request.dm.{siteID}.{userID}.msg.send` | `bot-platform-service` only |
| `chat.server.bot.request.room.{siteID}.create` | `bot-platform-service` only |
| `chat.server.bot.request.room.{siteID}.{roomID}.member.add` | `bot-platform-service` only |
| `chat.server.bot.request.room.{siteID}.{roomID}.member.remove` | `bot-platform-service` only |
| `chat.server.bot.request.room.{siteID}.get` | `bot-msg-handler` only |
| `chat.server.bot.request.room.{siteID}.dm.ensure` | `bot-msg-handler` only |

bot-msg-handler and bot-room-service consume from these subjects but only the listed accounts may publish. bot-msg-handler / bot-room-service verify at the code layer:

- Request arrived on a `chat.server.bot.request.>` subject (subject-based trust boundary)
- `X-Bot-Identity` header present (missing → reject)
- Body's Sender field is IGNORED — always overwritten from the header

**Invariant**: `caller.IsBot == true` for every request into BP. A session whose `users` record shows `IsBot == false` gets `errcode.Forbidden(reason: "not_a_bot")` at the auth middleware.

**Display name**: `UserDisplayName` on the enriched Message = `AppName` verbatim (skip the compose helper — bots have a single canonical brand name).

## 8. Rate limiting

**Single layer** in `bot-platform-service` middleware. Skip ingress-level rate limiting for internal traffic (Envoy `local_ratelimit` optional as a coarse per-pod ceiling; not a design requirement).

**Per-endpoint × two-bucket** Valkey token bucket. Both must pass:

| Bucket | Key format | Purpose |
|---|---|---|
| Per-caller | `ratelimit:{endpoint}:{caller.ID}` | Fair share between bots |
| Global | `ratelimit:{endpoint}:global` | Pipeline resource protection |

**Starting limits (tune from production):**

| Endpoint | Per-caller (rps) | Global (rps) |
|---|---|---|
| `POST /api/v1/rooms/{roomID}/messages` | 50 | 1000 |
| `POST /api/v1/dms/{userID}/messages` | 30 | 300 |
| `POST /api/v1/rooms` | 1 | 20 |
| `POST /api/v1/rooms/{roomID}/members/add` | 5 | 50 |
| `POST /api/v1/rooms/{roomID}/members/remove` | 5 | 50 |

**Implementation:**

- Valkey Lua script implementing atomic token-bucket refill + consume (well-known pattern).
- **Cluster co-location**: both keys per request share a hash tag: `ratelimit:{{sendMessage}}:global` and `ratelimit:{{sendMessage}}:bot-42` → both hash to the slot for `sendMessage`. This lets a single Lua EVAL touch both atomically.
- Rejection returns `errcode.TooManyRequests(reason: "rate_limited")` with `Retry-After` header set to the bucket's refill window.
- Metrics: `bot_rate_limit_hit_total{endpoint, reason=("caller"|"global")}` — no per-caller cardinality (bot count is small but growing; keep label set fixed).

## 9. Idempotency

**Bot servers do NOT send `Idempotency-Key`.** The legacy contract is `x-user-id` + `x-auth-token` only; requiring a new header would break existing bot deployments. BP derives the idempotency key **internally** from request contents so retries after HTTP timeout are safely deduped without any client-side change.

### 9.0 Composite operation identity (BP-derived, single source of truth)

Derived once by BP after auth + rate-limit, before the idempotency middleware. Used as the BP Valkey sentinel key (§9.1). The JetStream `MsgID` on the canonical publish is the BP-generated `messageID` (unique per opID within the sentinel window), and `outbox.Publish(...)` uses its own per-destination `dedupID`. There is no fan-out of the raw opID beyond the sentinel key.

```text
bodyHash = sha256(canonicalizeJSON(requestBody))                    // stable across whitespace / key-order
timeBucket = floor(unix_seconds / 60)                               // 60-second retry window
opID = sha256(siteID + ":" + endpoint + ":" + resourceID + ":" + caller.ID + ":" + bodyHash + ":" + timeBucket)
```

- `siteID`: current site (prevents cross-site collision).
- `endpoint`: `sendRoom` | `sendDM` | `createRoom` | `addMember` | `removeMember`.
- `resourceID`: roomID (send-room / add / remove), targetUserID (send-DM), or `""` (create-room).
- `caller.ID`: authenticated bot ID from the session-validated `x-user-id` (see §7).
- `bodyHash`: SHA-256 of the JSON-canonicalized request body.
- `timeBucket`: floor(now / 60s). Makes the opID stable within a 60-second window, so a retry after HTTP timeout (typically 0-30s) produces the same opID as the original request and dedupes downstream.

**Behavior properties:**

- **Legacy bot timeout-retry within 60s of original**: identical body + same time bucket → same opID → same Valkey sentinel key → in-flight retries return 409. Retry-safe. ✅
- **Intentional identical request > 60s apart**: same body but different `timeBucket` → different opID → both requests go through as distinct operations. ✅
- **Intentional identical request within 60s** (bot sends "OK" twice back-to-back): same body + same bucket → same opID → **second request collapses into first while first is in-flight** (returns 409 in-flight). After the first completes and the 30s sentinel expires, a follow-up retry re-executes and creates a second message. Rare in practice; bots that need to send truly-identical content within 60s can vary it slightly (e.g., trailing zero-width space).
- **Client-supplied `Idempotency-Key` (future opt-in)**: if a header is present, BP uses `sha256(header value)` in place of `bodyHash` and drops the `timeBucket` term. Not required in v1 but the derivation is compatible.

The opID drives both the BP in-flight sentinel key and the JetStream `MsgID` on the canonical publish (bot-msg-handler uses `messageID` for MsgID — one messageID per opID within the sentinel window).

### 9.1 Sentinel-only Valkey pattern (no response cache)

Executed by BP idempotency middleware after rate-limit, before handler.

**Key format** — all endpoints use the composite `opID`:

| Endpoint | Key |
|---|---|
| `POST /api/v1/rooms/{roomID}/messages` | `idem:` + opID |
| `POST /api/v1/dms/{userID}/messages` | `idem:` + opID |
| `POST /api/v1/rooms` | `idem:` + opID |
| `POST /api/v1/rooms/{roomID}/members/add` | `idem:` + opID |
| `POST /api/v1/rooms/{roomID}/members/remove` | `idem:` + opID |

**Protocol** — sentinel only, no cached response envelope:

```text
Phase 1: SET NX <key> "processing"  EX 30
  ├─ OK   → we own this request; proceed to handler; on completion (200 or handler-returned typed error) delete the key
  └─ FAIL → return errcode.Conflict(reason: "in_flight", Retry-After: 1)
```

- A retry that arrives while the original is still processing gets 409 + `Retry-After` and backs off.
- Once the original completes and the sentinel is deleted (or expires at 30s), a retry re-executes the request normally. Correctness of the retried outcome is provided by downstream dedup (Cassandra PK on `(room_id, bucket, created_at, message_id)`, `outbox.Publish` `dedupID`, `member_added` `$setOnInsert`, add/remove diff logic) — not by a cached BP response.
- **No response body is cached.** A retry after the sentinel expires produces a fresh 200 with the retry's own outcome; the "response replay" property is not offered.

### 9.2 Sentinel TTL selection

Sentinel TTL = **30 seconds**. Chosen to be strictly greater than the sum of:

- BP → NATS request timeout: 3s (msg handler) / 15s (room management ops)
- bot-msg-handler / bot-room-service max latency (Mongo + JS publish + ack): ~10s worst case
- Small safety margin

For room-management ops (`create`, `member.add`, `member.remove`) the sentinel TTL is `60s` to cover the 15s BP request timeout plus worst-case org expansion.

If a handler takes longer than the sentinel TTL (indicative of a serious problem), the sentinel expires and a retry proceeds. Downstream idempotency (Cassandra PK, `member_added` upsert diff, outbox `dedupID`) keeps the outcome consistent.

### 9.3 Sentinel deletion policy

| Handler result | Delete sentinel? |
|---|---|
| 200 success | Yes — free the key so a later retry can send a fresh request |
| 4xx typed error (validation, auth, permission) | Yes — the payload is deterministically bad; a retry with the same body will get the same error and the sentinel adds no value |
| 5xx transient error (Mongo down, NATS timeout) | **No — let it expire at 30s.** Original handler may still be running past BP's 3s NATS timeout; deletion would let a retry start while the first attempt is still writing. The 30s TTL absorbs the overlap window and forces retries to wait until the original is truly abandoned. |
| 5xx internal (invariant violation, panic) | No — same as transient |

**Rationale for keeping sentinel on 5xx**: BP's NATS timeout (3s) is shorter than bot-msg-handler's worst-case latency (10s). Without the sentinel-held window, a 5xx-and-retry pattern would let two concurrent handlers run for the same opID.

## 10. Error mapping

Every failure returns a typed `*errcode.Error` from the handler; `errnats.Reply` / `errhttp.Write` marshals to the wire envelope.

| Failure | Category | Reason | HTTP | Notes |
|---|---|---|---|---|
| Missing/invalid `x-auth-token` | Unauthorized | `invalid_token` | 401 | Auth middleware |
| `x-user-id` doesn't map to a bot | Forbidden | `not_a_bot` | 403 | Auth middleware |
| Duplicate in flight | Conflict | `in_flight` | 409 | Retry-After: 1 |
| Rate-limited (caller) | TooManyRequests | `rate_limited_caller` | 429 | Retry-After set from bucket |
| Rate-limited (global) | TooManyRequests | `rate_limited_global` | 429 | Retry-After set from bucket |
| Content empty or > 20 KiB | BadRequest | `content_invalid` | 400 | metadata: attempted length |
| Mention references non-member | BadRequest | `mention_invalid` | 400 | metadata: offending userID |
| Room not found | NotFound | `room_not_found` | 404 | Post-authz |
| Bot not a room member (send) | Forbidden | `not_a_room_member` | 403 | |
| Bot not a room owner (admin) | Forbidden | `not_a_room_owner` | 403 | |
| Target user for DM not found | NotFound | `dm_target_not_found` | 404 | |
| Mongo unreachable | Internal | (collapsed) | 500 | Server-side logged with cause |
| NATS req/reply timeout | Unavailable | `handler_timeout` | 503 | Retry-After: 2 |
| JS publish failed | Internal | (collapsed) | 500 | |
| Reply would exceed NATS max_payload | Internal | `response_too_large` | 500 | Rare with 20 KiB content cap |

## 11. Configuration

All services use `caarlos0/env` typed structs. Env var prefixes below.

### 11.1 bot-platform-service

| Env | Default | Required | Notes |
|---|---|---|---|
| `SITE_ID` | — | yes | |
| `HTTP_PORT` | `8080` | | |
| `NATS_URL` | — | yes | |
| `VALKEY_ADDRS` | — | yes | Cluster-mode |
| `MONGO_URI` | — | yes | For `sessions` + `users` collection lookup (see §7) |
| `MONGO_DATABASE` | `chat` | | |
| `RATE_LIMIT_SEND_ROOM_CALLER_RPS` | `50` | | |
| `RATE_LIMIT_SEND_ROOM_GLOBAL_RPS` | `1000` | | |
| (similar for each endpoint) | | | |
| `BOOTSTRAP_STREAMS` | `false` | | Not applicable — BP doesn't own streams |
| `LOG_LEVEL` | `info` | | |

Timeouts and TTLs (BP → NATS request 3s / 15s per endpoint, idempotency sentinel 30s / 60s per endpoint, cache TTLs 60s) are code constants, not env-tunable — they encode invariants against handler worst-case latency (§9.2) and would break the system if drifted.

### 11.2 bot-msg-handler

| Env | Default | Required |
|---|---|---|
| `SITE_ID` | — | yes |
| `NATS_URL` | — | yes |
| `MONGO_URI` | — | yes |
| `MONGO_DATABASE` | `chat` | |
| `MESSAGE_BUCKET_HOURS` | `72` | Must match bot-msg-worker + message-worker (existing constraint) |
| `BOOTSTRAP_STREAMS` | `false` | envDefault |
| `BOOTSTRAP_` prefix per stream | | Owning service: bot-msg-handler owns `BOT_MESSAGES_CANONICAL_{siteID}` bootstrap |
| `LOG_LEVEL` | `info` | |

No Valkey dependency: bot-msg-handler holds only an in-process LRU (60s TTL) for subscription + room-info lookups. Cross-site room-info fetch uses NATS req/reply against the room's origin site, not a shared cache.

### 11.2.1 bot-msg-worker

| Env | Default | Required |
|---|---|---|
| `SITE_ID` | — | yes |
| `NATS_URL` | — | yes |
| `CASSANDRA_HOSTS` | — | yes |
| `CASSANDRA_KEYSPACE` | `chat` | |
| `MESSAGE_BUCKET_HOURS` | `72` | Must match bot-msg-handler |
| `ATREST_ENABLED` | `false` | envDefault — when `true`, encrypts body columns via the shared `atrest.Cipher` (same as message-worker). Wire this up only after the at-rest cipher rollout is complete site-wide. |
| `MAX_WORKERS` | `100` | |
| `LOG_LEVEL` | `info` | |

### 11.3 Workers (bot-msg-worker, bot-broadcast-worker, bot-notification-worker, bot-push-service)

Standard consumer config env vars per existing worker services: `NATS_URL`, `MAX_WORKERS=100`, `SITE_ID`, backend-specific config (Cassandra, Valkey, push credentials, etc.).

## 12. Stream definitions

### 12.1 New: BOT_MESSAGES_CANONICAL_{siteID}

| Field | Value |
|---|---|
| Name | `BOT_MESSAGES_CANONICAL_{siteID}` |
| Subjects | `["chat.bot.canonical.{siteID}.>"]` (`>` allows `.edited`, `.deleted` in future) |
| Retention | Limits-based, MaxAge = 7d (matches user `MESSAGES_CANONICAL`) |
| Duplicate window | 5m (uses `MsgID = messageID`, BP-generated per §9.0) |
| Storage | File-backed |
| Replicas | 3 |
| Owning service (bootstrap) | `bot-msg-handler` (per CLAUDE.md: publisher owns bootstrap in dev; ops/IaC owns in prod) |

### 12.2 New: BOT_PUSH_NOTIF_{siteID}

| Field | Value |
|---|---|
| Name | `BOT_PUSH_NOTIF_{siteID}` |
| Subjects | `["chat.bot.notification.push.{siteID}.>"]` |
| Retention | Limits-based, MaxAge = 1d |
| Storage | File-backed |
| Owning service (bootstrap) | `bot-notification-worker` |
| Producer | `bot-notification-worker` publishes push events directly (no intermediate stream) |
| Consumer | `bot-push-service` (thin wrapper, mirrors user push notification service) |

### 12.3 Reused streams (unchanged)

| Stream | Bot pipeline usage |
|---|---|
| `INBOX_{siteID}` | Cross-site bot events (`member_added`, `member_removed` only) land on `chat.inbox.{siteID}.external.{eventType}` alongside user events. No `message_created` event — bot message content is not federated as an event; live delivery is via NATS supercluster subject routing (§16.1). Sysmsgs stay local at origin, so no new event type. No new partition. |
| `OUTBOX_{siteID}` | Room management membership events published by `bot-room-service` via `pkg/outbox.Publish(...)` — same subjects/lanes as user-triggered events. |

## 13. Subject definitions

### 13.1 New subject builders (`pkg/subject`)

**Convention**: **all bot req/reply — external ingress and internal service-to-service — lives under `chat.server.bot.request.>`**. Matches the existing `chat.server.request.>` prefix. Streams live under `chat.bot.>`, mirroring the user pipeline's `chat.room.canonical.*` / `chat.server.notification.push.*` split (streams don't use the `chat.server.request.*` prefix).

**Req/reply subjects (all bot RPCs; ingress + internal):**

| Function | Output | Caller |
|---|---|---|
| `ServerBotMsgRoomSend(siteID, roomID)` | `chat.server.bot.request.room.{siteID}.{roomID}.msg.send` | BP (from bot server) |
| `ServerBotDMSend(siteID, userID)` | `chat.server.bot.request.dm.{siteID}.{userID}.msg.send` | BP (from bot server) |
| `ServerBotRoomCreate(siteID)` | `chat.server.bot.request.room.{siteID}.create` | BP (from bot server) |
| `ServerBotRoomMemberAdd(siteID, roomID)` | `chat.server.bot.request.room.{siteID}.{roomID}.member.add` | BP (from bot server) |
| `ServerBotRoomMemberRemove(siteID, roomID)` | `chat.server.bot.request.room.{siteID}.{roomID}.member.remove` | BP (from bot server) |
| `ServerBotRoomGet(siteID)` | `chat.server.bot.request.room.{siteID}.get` | bot-msg-handler → bot-room-service at room's origin site (cross-site room-info fetch) |
| `ServerBotRoomDMEnsure(siteID)` | `chat.server.bot.request.room.{siteID}.dm.ensure` | bot-msg-handler → bot-room-service (local; DM origin is always bot's site) |
| `ServerBotWildcard()` | `chat.server.bot.request.>` | Wildcard for consumer registration |

**Streams / event subjects:**

| Function | Output |
|---|---|
| `BotCanonicalCreated(siteID)` | `chat.bot.canonical.{siteID}.created` |
| `BotCanonicalWildcard(siteID)` | `chat.bot.canonical.{siteID}.>` |
| `BotPushNotification(siteID, kind)` | `chat.bot.notification.push.{siteID}.{kind}` |
| `BotPushNotificationWildcard(siteID)` | `chat.bot.notification.push.{siteID}.>` |

**Federation**: room management flow reuses existing `chat.outbox.{origin}.{dest}.{eventType}` subjects from `pkg/subject.Outbox(...)` and existing `member_added` / `member_removed` event types. **No new OUTBOX event type is introduced.**

## 14. Model changes and wire schemas

**No new fields on `Message`.** Reuse `pkg/model.Message` and `pkg/model/cassandra.Message`. Bot vs user distinction lives entirely on `Participant.IsBot` (+ `AppID`, `AppName`) on `Sender`.

### 14.1 BotSendMessageRequest

```go
// In pkg/model/bot.go (new file)
type BotSendMessageRequest struct {
    Content     string          `json:"content"`
    Mentions    []Participant   `json:"mentions,omitempty"`
    Card        *cassandra.Card `json:"card,omitempty"`
    // Sender NOT accepted from request body — bot-msg-handler populates from
    // NATS header X-Bot-Identity (forwarded by BP).
    // NO IdempotencyKey field — bot servers do NOT send Idempotency-Key
    // header. BP derives an opID internally per §9.0 for its own sentinel.
    // BP generates messageID + createdAt and forwards them to bot-msg-handler
    // via NATS headers X-Bot-Message-ID and X-Bot-Created-At.
    // NO Attachments field — bots send text and cards only (see §3).
}
```

`roomID` (send-to-room) or `targetUserID` (DM) extracted from NATS subject; not in body.

**Content-type restriction**: bots may send `Content` (text, ≤20 KiB), `Mentions`, and `Card`. Bots may NOT upload or reference attachments — the `Message.Attachments` field on the canonical envelope is always empty for bot-originated messages.

**Strict JSON decoding**: BP configures its JSON decoder for bot request bodies with `Decoder.DisallowUnknownFields()` (net/http default is permissive silence). Any unknown field — including a stray `attachments` array — causes decode failure returned as `errcode.BadRequest(reason: "unknown_field")`. Without this, Go silently ignores unknown JSON fields and the "attachments rejected" guarantee is unenforceable at the struct level.

### 14.2 HTTP request/response examples

**Send message to room** — request:

```http
POST /api/v1/rooms/room-abc/messages HTTP/1.1
x-user-id: bot-42
x-auth-token: eyJhbGc...
Content-Type: application/json

{
  "content": "Deployment succeeded ✓",
  "mentions": [
    {"id": "user-99", "account": "alice", "isBot": false}
  ]
}
```

*Bot server does NOT send `Idempotency-Key`. BP derives the opID internally per §9.0 from `(siteID, endpoint, roomID, caller.ID, sha256(body), floor(now/60s))`.*

Response 200:

```json
{
  "id": "0197a4f8c2d7c9aabc9e0123",
  "roomId": "room-abc",
  "userId": "bot-42",
  "userAccount": "bot-42-account",
  "userDisplayName": "Payroll Bot",
  "content": "Deployment succeeded ✓",
  "mentions": [{"id": "user-99", "account": "alice", "isBot": false}],
  "createdAt": "2026-07-15T14:23:11.427Z"
}
```

**Send DM** — request identical structure at `POST /api/v1/dms/{targetUserID}/messages`.

**Create room** *(idempotency derived internally; bot server does not send `Idempotency-Key`)*:

```http
POST /api/v1/rooms HTTP/1.1
x-user-id: bot-42
x-auth-token: eyJhbGc...
Content-Type: application/json

{
  "name": "deployments",
  "topic": "CI/CD notifications",
  "members": ["user-1", "user-99"]
}
```

Response 200:

```json
{
  "id": "abc123def456ghi78",
  "name": "deployments",
  "owner": {"id": "bot-42", "isBot": true, "appId": "app-123", "appName": "PayrollBot"},
  "members": ["bot-42", "user-1", "user-99"],
  "createdAt": "2026-07-15T14:23:11.427Z"
}
```

**Add members / orgs (batch)** *(no `Idempotency-Key` header — BP derives internally)*:

```http
POST /api/v1/rooms/room-abc/members/add HTTP/1.1
x-user-id: bot-42
x-auth-token: eyJhbGc...
Content-Type: application/json

{"userIds": ["user-42", "user-77"], "orgIds": ["org-eng"]}
```

Response 200: `{"added": {"userIds": ["user-42", "user-77", ...expanded from org-eng], "orgIds": ["org-eng"]}}`

**Remove members / orgs (batch)** *(no `Idempotency-Key` header — BP derives internally)*:

```http
POST /api/v1/rooms/room-abc/members/remove HTTP/1.1
x-user-id: bot-42
x-auth-token: eyJhbGc...
Content-Type: application/json

{"userIds": ["user-42"], "orgIds": []}
```

Response 200: `{"removed": {"userIds": ["user-42"], "orgIds": []}}`

## 15. Ordering semantics

Bot messages inherit user-flow ordering semantics: **best-effort within timestamp resolution, no strict per-room FIFO**.

**What's actually true**:

- Within a single bot's sequential HTTP calls (each awaiting the previous), timestamps are monotonic per pod but NOT necessarily across pods: two rapid calls from the same bot can be load-balanced to different BP pods whose clocks may differ by a few ms (NTP-bounded). This can produce ms-scale timestamp inversions.
- Concurrent calls from the same bot (rare but allowed) have no ordering guarantee at all.
- Same-site and cross-site delivery use concurrent consumers (`MaxWorkers=100`), so downstream fanout order is not preserved.
- Clients render sorted by `createdAt`. Because timestamps are ms-resolution, two events within the same ms tie-break by messageID — display order is deterministic but not necessarily call order for near-simultaneous events.

**Explicit non-guarantees**:

- No per-room FIFO on broadcast.
- No monotonic-timestamp guarantee across concurrent calls or across pods.
- No cross-recipient delivery-order guarantee.

Rationale: identical to user flow; introducing bot-specific FIFO would create a stronger guarantee for bots than users on the same delivery path, plus throughput bottleneck per busy room. If strict per-room FIFO is later required, it's a cross-pipeline upgrade covering both user and bot flows.

See §17.1 for the transient rendering-window window.

## 16. Cross-site federation

### 16.1 Path B — cross-site live delivery via NATS supercluster subject routing

**Correction over earlier drafts**: earlier revisions claimed bot-msg-worker (or bot-broadcast-worker) publishes `message_created` events to remote INBOXes. That is wrong on two counts:

- No `InboxMessageCreated` event type exists in the codebase (`grep` returns nothing).
- `inbox-worker` binds `external.>` only and has no handler for message content.

**Reality — how cross-site live delivery actually works**:

- Message content is **never** federated cross-site as an event.
- bot-msg-worker writes to Cassandra at the **origin site only**. Remote sites do NOT hold a copy in their local Cassandra.
- bot-broadcast-worker publishes to `subject.RoomEvent(roomID)` / `subject.UserRoomEvent(account)` — core NATS subjects.
- The **NATS supercluster routes these subjects to WS receiver sessions at every site** where subscribers exist. That's how a bot message reaches a remote-site user's browser in real time: the WS gateway subscribes to `chat.room.{roomID}.event`, the supercluster delivers the publish across the gateway link, the WS gateway forwards to the client.
- **Zero bot-specific cross-site code required.** Mirrors user pipeline exactly.

**Remote-site history reads**: since content isn't stored at remote sites, when a remote-site user opens history for a room whose messages live at another site, the read routes to the room's origin site via the site-scoped request subject `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids` (supercluster routes to origin site's history-service). Content is not copied to reader's site. Cross-site OUTBOX federation exists only for read state (`lastSeenAt`/unread), never for content.

**Sysmsg note**: bot room management sysmsgs are LOCAL-ONLY — they publish to the origin site's `BOT_MESSAGES_CANONICAL` and are delivered to local room members by the standard bot workers. They are NOT federated cross-site via OUTBOX. This matches user `room-worker`'s behavior (`room-worker/handler.go:489` publishes the sysmsg only to `subject.MsgCanonicalCreated(h.siteID)`). Remote-site members learn about the membership change from the `member_added` / `member_removed` OUTBOX event; the "who did what" narrative sysmsg is a same-site render concern.

### 16.2 Path A (OUTBOX) — bot room management ops

- Bot room management ops (create room / add / remove member) route through `bot-platform-service` → `bot-room-service`.
- `bot-room-service` writes local Mongo, then calls `outbox.Publish(...)` per `pkg/outbox/outbox.go`.
- `outbox-worker` forwards to remote INBOX via existing per-destination lanes.
- Order-sensitive events (`member_added`, `member_removed`) ride the existing `OrderedEventTypes` per-destination FIFO lane (`MaxAckPending=1`). Order preservation is inherent to existing infra.

**No new OUTBOX event type is introduced.** Bot pipeline reuses existing `member_added` and `member_removed`.

## 17. Cross-site DM lifecycle

Cross-site DM (bot on site A → user on site B) is supported in v1. Design principle: **rooms live at exactly one site** — the site that first creates the DM owns the room document, and the far side only holds a subscription pointing back to that origin (matches channel rooms).

**Flow:**

```text
site A (bot's site — becomes DM origin)         site B (target user's site)
---------------------------------------         ---------------------------
1. bot-msg-handler subscription-first lookup:
   subscriptions.FindOne({roomID=BuildDMRoomID(bot, user), userID=botID})
   - Hit → subscription.SiteID tells us the DM origin site; proceed to send.
   - Miss → this bot has never DMed this user from here.
             Call bot-room-service local chat.server.bot.request.room.{siteID}.dm.ensure.

2. bot-room-service (at site A) — first-DM path:
   a. Insert local rooms doc { _id: BuildDMRoomID(bot,user), type: "dm",
      siteID: A, participants: [bot, targetUser] }.
   b. Insert bot's local subscription (siteID=A).
   c. Resolve targetUser.siteID via user-service.
      If targetUser.siteID != A:
        outbox.Publish(member_added) — payload carries
        { roomID, roomType: "dm", addedUser: targetUser, addedByUser: bot, siteID: A (= DM origin) }
        → outbox-worker → site B INBOX.

                                             3. inbox-worker at site B receives member_added:
                                                event.roomType == "dm"
                                                → upsert LOCAL subscription only
                                                   { roomID, userID: targetUser.ID, siteID: A }
                                                → does NOT provision a rooms doc — DM origin is A, not B
                                                   (matches channel-room behavior: rooms live at one site)

4. bot-msg-handler at site A publishes the message to BOT_MESSAGES_CANONICAL.
   bot-msg-worker writes to Cassandra at site A (origin-only; no cross-site content event).
   bot-broadcast-worker at site A publishes to subject.RoomEvent(roomID) /
   subject.UserRoomEvent(targetUser.account).

                                             4b. NATS supercluster routes those subjects to WS receiver
                                                 sessions at site B (where the target user has an open WS).
                                                 Live push delivered. No content event ever hits site B's INBOX.

5. Reply to bot immediately (no waiting on site B).
```

**Why this works:**
- **Rooms live at exactly one site** — the site that first ensures the DM (here, site A) owns the room doc; matches channel-room behavior. Site B never has a local rooms record for this DM.
- Site B's subscription carries `siteID = A`, so history reads and room-info lookups from site B route to site A via the existing site-scoped request subjects.
- Live delivery works because NATS supercluster routes `subject.RoomEvent(roomID)` across gateway links to any WS receiver session subscribed to that subject, regardless of which site the session is on.
- Content is persisted at the origin site's Cassandra only. Reads route to the room's origin site via `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids` (supercluster routes); content is not copied to reader's site. Cross-site OUTBOX federation exists only for read state (`lastSeenAt`/unread), never for content.

**What if the target user DMs back from site B before their site's subscription is provisioned?** The target user's client resolves the DM by the same deterministic `BuildDMRoomID` and issues a subscription lookup; if the local subscription is missing, site B falls back to the standard user-flow "create DM" path — which today writes a local rooms doc on site B and a `member_added` back to A. That would create **two competing DM origins** for the same deterministic roomID. This is a pre-existing user-pipeline concern (both-sides-DM-simultaneously race) and is out of scope here; in v1 we accept that whoever's `member_added` arrives first at the peer wins the subscription's `siteID`, and message content routes to whichever site actually holds the row.

**Cost**: inbox-worker changes to upsert a target-user subscription on `member_added` for DM rooms (`event.roomType == "dm"`), pointing `subscription.SiteID` at the DM origin from `event.originSiteID`. No local rooms doc at the target site. See §6.1.

## 17.1 Known limitations

- **Cross-site live-delivery gap (not a durability gap)**: cross-site live WS delivery is best-effort via NATS supercluster subject routing (`subject.RoomEvent(roomID)`). If the gateway link to a remote site is down at publish time, the remote WS clients miss the live push for that specific message. **Content is not lost** — it remains durably persisted at the origin site's Cassandra. A history refresh at the remote client routes to the origin site via the site-scoped request subject `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids` (supercluster routes); content is not copied to reader's site. Cross-site OUTBOX federation exists only for read state, never for content. Identical property to user pipeline today.
- **Mongo-to-OUTBOX admin-op crash window**: bot-room-service writes Mongo state THEN calls `outbox.Publish(...)`. A crash between the two leaves remote sites without the membership event for this specific op. **Identical to user `room-worker` behavior today.** A cross-pipeline fix (transactional outbox spanning both pipelines) is a future initiative outside this spec's scope.
- **Transient rendering-window disorder**: parallel broadcast/inbox can deliver messages out of order at the WS layer. Client-side timestamp sort papers over it. Rare visibility window is ~sub-second.
- **First-cross-site-DM window (client-side rendering only)**: on the first DM to a remote user, the WS live push (via `subject.RoomEvent(roomID)`) can arrive at the target user's WS session before `member_added` has propagated via OUTBOX to their site's inbox-worker (and thus before their local subscription is upserted). The bounded consequence is a brief client-side rendering window where the client receives a message for a room its state doesn't yet know about — chat clients handle this identically to any cross-site membership race. Content is persisted at the DM origin site, subscription is upserted from the `member_added` event within a bounded window.
- **Both-sides-DM-simultaneously race**: if both parties initiate the same DM from opposite sites within the OUTBOX propagation window, each site briefly writes its own local rooms doc with the same deterministic `roomID`. Whoever's `member_added` arrives first at the peer sets `subscription.SiteID` for the target; message content routes to whichever site actually holds the row. Pre-existing user-pipeline concern; not addressed in this spec.
- **60s stale-cache window on bot removal**: bot removed from room can send for up to 60s until bot-msg-handler cache TTL expires. Bounded.

## 18. Package additions (in-scope)

**No new shared packages.** Bot workers are built via copied JS-consumer skeleton + slim bot-specific handlers; `pkg/broadcast` and `pkg/notify` extractions were considered and dropped (user workers can't be cleanly extracted).

- `pkg/broadcast` extraction: dropped — user broadcast-worker's actual scope (same-site WS fanout for 6 event types + thread fanout + roomkey encryption) doesn't align with bot needs. Bot-broadcast-worker is built from a copied consumer skeleton + slim text/card handler.
- `pkg/notify` extraction: dropped — user notification-worker handles preferences, mobile push resolution, thread signals, and other user-flow concerns. Bot-notification-worker copies the skeleton + slim bot-shape handler. Infrastructure isolation is a design goal (bot notification incidents cannot touch user push delivery).
- `pkg/roomstore` extraction: also dropped. Earlier revisions introduced it as a shared home for room+subscription CRUD, with the hope that user room-worker would migrate onto it later. That's speculative — it violates the "no premature abstraction" rule. The room/subscription write code lives in bot-room-service's own `store_mongo.go` (per the standard per-service file layout). If user room-worker ever needs to share it, extract at that point.

Phase 1 (BP service) begins immediately — no Phase 0 gate around library extraction.

## 19. Observability

### 19.1 Metrics (Prometheus, via pkg/obs)

**bot-platform-service:**
- `bot_http_requests_total{endpoint, code}`
- `bot_http_request_duration_seconds{endpoint}` (histogram, buckets: 5ms, 25ms, 100ms, 500ms, 2s)
- `bot_rate_limit_hit_total{endpoint, reason}` (`reason` = `caller` | `global`)
- `bot_idempotency_hit_total{endpoint, outcome}` (`outcome` = `in_flight` | `fresh`)
- `bot_auth_failures_total{reason}`

**bot-msg-handler:**
- `bot_msg_handler_requests_total{op, code}` (`op` = `send_room` | `send_dm`)
- `bot_msg_handler_duration_seconds{op}`
- `bot_msg_handler_room_cache_hits_total`
- `bot_msg_handler_publish_failures_total`

**bot workers (each):**
- `nats_consumer_num_pending{consumer}` (from JetStream monitoring)
- `bot_worker_processed_total{worker, outcome}` (`outcome` = `ok` | `retry` | `poison`)
- `bot_worker_processing_duration_seconds{worker}`
- `bot_broadcast_bytes_out{subject}` (labeled by publish subject family: `RoomEvent` | `UserRoomEvent`; NATS supercluster handles cross-site routing)

**No per-caller labels** (Prometheus cardinality guard).

### 19.2 Traces (OpenTelemetry via pkg/obs)

Single trace spans:
- Ingress → BP handler → NATS request → bot-msg-handler → JS publish → BP response
- Downstream: JS consume → worker handler (per worker; separate trace linked via `messagingID`)

### 19.3 Logs (slog JSON)

All BP handlers log per request: `requestID`, `botID`, `endpoint`, `roomID` (if applicable), `opIDPrefix` (first 8 hex chars of the BP-derived opID from §9.0 — safe to log since opID is a hash of hash inputs, not raw content), `status`, `latencyMS`. **Never log content bodies, tokens, or attachment bytes.**

### 19.4 Alerts

- Consumer lag > 10k pending or > 5min behind on any bot worker
- BP p99 latency > 500ms sustained 5min
- `bot_rate_limit_hit_total{reason=global}` rate > 10/s (indicates undersized global cap)
- `bot_msg_handler_publish_failures_total` rate > 1/s
- Broadcast worker bandwidth > 50 MB/s sustained (headroom before the 92 MB/s worst-case)

## 20. Testing strategy

### 20.1 Unit tests (per CLAUDE.md TDD requirements)

Every handler and store method: happy path + error paths + edge cases + invalid input. Coverage floor 80%; 90%+ for handlers.

Notable table-driven tests:

- BP middleware chain: auth failure, rate-limit hit (each bucket), idempotency each phase
- bot-msg-handler validation: content length boundaries, invalid mentions, missing room, missing membership
- Idempotency race matrix from §9.3 as a test table

### 20.2 Integration tests (`//go:build integration`)

Per CLAUDE.md `pkg/testutil` conventions:

- `bot-platform-service/integration_test.go`: real NATS + Valkey, mocked auth-service and bot-msg-handler. Full middleware chain end-to-end.
- `bot-msg-handler/integration_test.go`: real NATS + Mongo. Publish + consume canonical round-trip.
- `bot-msg-worker/integration_test.go`: real NATS + Cassandra via `testutil.CassandraKeyspace`.
- `bot-broadcast-worker/integration_test.go`: real NATS. Verify same-site WS fanout via `subject.RoomEvent`/`subject.UserRoomEvent`. Cross-site delivery is a supercluster-routing property, tested at the network layer, not by the worker unit.

### 20.3 Load test

Custom load-test harness (or reuse `docs/superpowers/specs/2026-04-21-load-test-messaging-workers-design.md`) driving BP HTTP at 500 rps sustained, 5000 rps burst. Verify: latency SLOs held, no dropped messages, Cassandra + ES catch up within 30s post-burst.

### 20.4 Chaos scenarios (manual, pre-production)

- Cassandra down for 5 min → messages queue in stream → drain on recovery, zero loss
- Remote gateway down → cross-site messages lost silently (documented behavior); local delivery unaffected
- BP pod crash mid-request → idempotency retry succeeds with same messageID
- Valkey partial partition → rate-limit or idempotency Lua fails → BP returns 503; bot retries succeed

## 21. Rollout plan

Sequenced phases; each is a separately-shippable milestone.

| Phase | Work | Ships |
|---|---|---|
| 1 | `bot-platform-service` (session-token auth against Mongo `sessions` + rate-limit + idempotency middleware; passthrough handler stubs); NATS subject builders for `chat.server.bot.request.>`; stream bootstrap wiring | BP accepting requests, returning stub responses |
| 2 | `bot-msg-handler` (send-to-room only, no DM yet); `bot-msg-worker` (origin-site Cassandra write only, no cross-site publish); `bot-broadcast-worker` (publishes to `subject.RoomEvent` / `subject.UserRoomEvent`; supercluster handles cross-site routing); `bot-notification-worker`; `bot-push-service`; search-sync-worker second consumer added | Send-to-room end-to-end functional in dev |
| 3 | `bot-room-service` (create-room, add-member/org, remove-member/org, DM-ensure, room.get); room management endpoints wired in BP; cross-site room-info fetch subject `chat.server.bot.request.room.*.get` | Room management ops functional; DM-ensure endpoint available |
| 4 | Same-site DM support (bot-msg-handler DM path calls bot-room-service DM-ensure) | Same-site DMs functional |
| 5 | Cross-site DM support: `inbox-worker` DM auto-provisioning on `member_added` with participants in payload (§6.1); cross-site OUTBOX flow verified | Cross-site DMs functional |
| 6 | Load test, chaos scenarios, observability dashboards, WebSocket compression enabled | Ready for prod rollout |
| 7 | Prod rollout: single site → all sites | Live |

Feature-flag gate is not required — the bot pipeline is a net-new surface; users are unaffected by its presence.

## 22. Documentation updates (same PR as implementation)

Per CLAUDE.md §5:

- `docs/client-api.md` — new top-level section documenting bot HTTP API. Field tables per request/response, JSON examples, error catalog. Reuse `Participant`, `Message` shared types.
- `docs/client-api/request-reply.md` and `docs/client-api/events.md` — bot NATS subject additions if any bot event structs land there.
- `docs/nats-traffic-estimation.md` — existing line for bot JetStream (500B-1.5KB avg, 20 KiB max) updated with 2M/day estimate.
- `docs/nats-subject-naming.md` — bot subject conventions.
- CLAUDE.md — extend the "Event flow" description in §1 with a bot-pipeline paragraph if the pipeline lands.

## 23. Capacity summary (at 2M pubs/day expected volume)

| Metric | Value |
|---|---|
| Average bot publish rate | 23 msg/s |
| Peak bot publish rate (10× burst) | 230 msg/s |
| BOT_MESSAGES_CANONICAL storage (7d) | ~14 GB per site (~1 KB avg) |
| Cassandra write peak | 230 wps (0.7% of 3-node cluster capacity) |
| Broadcast fanout peak (20-member rooms, avg content) | ~4.6 MB/s per pod |
| Broadcast fanout peak (20 members, 20 KiB worst-case content) | ~92 MB/s per pod (mitigation: horizontal scale, compression) |
| Rate-limit headroom over expected peak | ~4× |
| Pipeline sustained ceiling (current infra) | ~5-10k rps |
| Idempotency Valkey footprint | ~1 GB steady state (24h × 2M × ~500B/entry) |

## 24. Resolved decisions

- **HTTP endpoint prefix** — `/api/v1/*`, matching existing `POST /api/v1/login` in `botplatform-service/routes.go`
- **NATS ingress subject prefix** — `chat.server.bot.request.>`, matching existing `chat.server.request.>` convention
- **Attachments** — bots send text + cards only, no attachments (§14.1)
- **Display name** — `AppName` verbatim (§7)
- **Identity resolution** — `x-user-id` + `x-auth-token` bearer flow; BP looks the session up in its own Mongo `sessions` collection (Meteor / Rocket.Chat pattern), then loads the bot `Participant` from `users`. No JWT, no per-request auth-service RPC (§7)
- **Bot suspension propagation** — session deletion in Mongo is immediate; next request's `sessions.FindOne` misses → 401. No revocation broadcast needed (§7)
- **messageID + createdAt generation** — BP generates both and forwards to bot-msg-handler via NATS headers `X-Bot-Message-ID` and `X-Bot-Created-At`. bot-msg-handler uses them verbatim. Cassandra PK dedups exact duplicates; BP's sentinel blocks concurrent retries. No Valkey envelope-mapping mechanism (§5.2)
- **Idempotency — sentinel only, no response cache** — `SET NX idem:{opID} "processing" EX 30`. In-flight retries get 409. Post-completion the sentinel is deleted (200 / 4xx) or expires (5xx). No cached response envelope — retries after the sentinel expires re-execute and downstream dedup (Cassandra PK, `member_added` upsert diff, outbox `dedupID`) keeps the outcome consistent (§9.1)
- **X-Bot-Identity trust** — NATS account permissions restrict `chat.server.bot.request.>` publish per-subject: BP owns publish for the client-facing ingress subjects; bot-msg-handler owns publish for the internal `.get` and `.dm.ensure` subjects (§7)
- **NATS subject convention** — ALL bot req/reply (external ingress + internal service-to-service, all sync) lives under `chat.server.bot.request.>`, matching existing `chat.server.request.>`. Streams live under `chat.bot.>` (`chat.bot.canonical.*`, `chat.bot.notification.push.*`), mirroring the user pipeline's `chat.room.canonical.*` / `chat.server.notification.push.*` split. No `chat.bot.room.*` internal subjects (§13)
- **create-room idempotency** — relies on BP's Valkey sentinel; no `createOpID` field on `rooms`, no Mongo unique index. Retries beyond the sentinel window create a fresh room (matches Rocket.Chat behavior at this volume) (§5.8.1)
- **Add / remove member idempotency** — no `bot_room_mgmt_ops` collection. Handler diffs requested against existing (`newlyAdded = requested \ existing`); duplicate adds are no-ops, sysmsg only emitted when `len(newlyAdded) > 0`. Same for remove. Retry-safe without a durable-op collection (§5.8.2)
- **Bot room management sysmsg — LOCAL ONLY** — origin site publishes sysmsg to local `BOT_MESSAGES_CANONICAL`. NOT federated cross-site via OUTBOX; matches `room-worker/handler.go:489` which publishes only to local canonical stream. Remote members learn membership from the `member_added` event. No new OUTBOX event type (§16.1, §5.8)
- **Mongo-to-OUTBOX atomicity** — direct `outbox.Publish(...)` after Mongo commit, matching user `room-worker` pattern. Crash-gap documented as inherited limitation in §17.1 (§5.8.3)
- **DM scope** — same-site AND cross-site both supported (§17)
- **DM lifecycle when a party is deleted/deprovisioned** — DM room preserved as historical record; matches existing user DM behavior
- **DM origin is single-site** — the site that first ensures the DM owns the room doc; target site holds only a subscription pointing back to origin (matches channel-room behavior). inbox-worker's DM `member_added` handling upserts subscription only, does NOT create a local rooms doc (§6.1, §17)
- **bot-msg-handler lookup order — subscription-first** — read local `subscriptions` by `(roomID, botID)` first → `subscription.SiteID` locates the room's origin site → cross-site NATS req/reply on `chat.server.bot.request.room.{originSiteID}.get` fetches the authoritative room doc. Room lives at exactly one site (§5.2)
- **bot-msg-worker writes** — dual-table (`messages_by_room` + `messages_by_id`) via UnloggedBatch for main-room messages; adds `thread_messages_by_thread` for thread replies; at-rest encryption via shared `atrest.Cipher` when `ATREST_ENABLED=true`. Matches `message-worker.CassandraStore.SaveMessage` behavior (§5.3)
- **MaxDeliver** — `5` on all bot worker consumers (not `-1`); after 5 deliveries the message is Ack-dropped as poison, metric `bot_msg_worker_permanent_error_total` incremented (§5.3)
- **Bot-removal vs last-message ordering** — accept transient overlap; matches user semantics
- **Ordering semantics** — best-effort within timestamp resolution; no per-room FIFO; no cross-call monotonic timestamp guarantee; clients render by `createdAt` (§15)
- **Push per-user FIFO** — best-effort per-device (matches APNs/FCM reality); no strict cross-pod per-user FIFO claimed (§5.6)
- **Bot notification behavior** — identical to user notifications (no silent-by-default)
- **Storage sharing** — same `messages_by_room` / `messages_by_id` / `thread_messages_by_thread` Cassandra tables + same ES index; `Participant.IsBot` distinguishes (no `authorKind`)
- **Federation stream reuse** — bot room management ops use existing `OUTBOX_{siteID}` + `pkg/outbox` contract with existing `member_added` / `member_removed` event types; no bot-specific OUTBOX and no new event types
- **Shared-code strategy** — `pkg/broadcast` + `pkg/notify` extractions dropped. `pkg/roomstore` also dropped — room/subscription write code lives in bot-room-service's own store; extract only when a second caller (e.g. user room-worker migration) actually needs it (§18)
- **Cross-site message content is NOT published as an event** — no `message_created` inbox event type exists. bot-msg-worker writes content to origin-site Cassandra only. Live delivery to remote-site WS sessions happens via NATS supercluster subject routing on `subject.RoomEvent(roomID)` / `subject.UserRoomEvent(account)`. Reads route to the room's origin site via the site-scoped request subject `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids` (supercluster routes); content is not copied to reader's site. Cross-site OUTBOX federation exists only for read state (`lastSeenAt`/unread), never for content. (§5.3, §5.4, §16.1)
- **HPA scaling metric for sync services** (`bot-platform-service`, `bot-msg-handler`, `bot-room-service`) — CPU target 70% for v1; standard K8s built-in metric, no custom-metric adapter required. These services are CPU-bound at throughput (JSON marshal, sha256 for session lookup, Mongo BSON, Valkey Lua). Revisit with Phase 6 load-test data
- **DM authorization** — unrestricted for internal bots in v1; add per-user bot-block list only if abuse emerges
- **Bot lifecycle / provisioning** — **out of scope for this spec**; assumes existing auth-service already provisions bot user records (marked `IsBot: true` on the `users` doc) and issues session tokens the same way it does for humans

## 25. Non-goals

- Third-party bot marketplace; multi-tenant bot isolation
- Per-bot rate-limit tiers (single tier for all internal bots)
- Ingress-level rate limiting via ext_authz / ext_proc / global RLS service
- Separate Cassandra table or ES index for bot messages
- New `authorKind` field on `Message` (use existing `Participant.IsBot`)
- Silent-by-default bot notifications (bot notifications behave identically to user notifications)
- Bot-specific per-room FIFO cross-site delivery (bot messages match user-flow ordering semantics)
- Bot-specific OUTBOX stream; bot room management ops reuse the existing `OUTBOX_{siteID}` + `pkg/outbox` contract via bot-room-service
- Edit/delete message, reactions, room rename, room deletion, ownership transfer (future specs)
