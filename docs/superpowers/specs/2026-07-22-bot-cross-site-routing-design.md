# Bot Cross-Site Routing Design

**Status:** approved
**Author:** claude (bot-implementation-8d2ctl)
**Date:** 2026-07-22
**Scope:** 5 bot HTTP endpoints in `botplatform-service` (BP) and their NATS routing to `bot-message-handler` / `bot-room-service`.

## 1. The rule

**Every bot operation runs at the room's home site.** The message canonical stream (`BOT_MESSAGES_CANONICAL_{siteID}`), the Cassandra write, the `rooms` collection mutation, and the local sysmsg all happen at the site that owns the room. BP is the routing router — it reads its local Mongo subscription for `(botAccount, roomID)` (or `(botAccount, otherAccount)` for DMs) and forwards the NATS request to that site's downstream service.

This mirrors the user pipeline exactly: user subscriptions live at the subscriber's home site, each subscription carries the `siteID` of the room it points to, and the room's home site owns the canonical stream. User's `MemberAdd` subject is `chat.user.{account}.request.room.{roomID}.{siteID}.member.add`, with `{siteID}` = the room's home site; each site's `room-service` subscribes to `chat.user.*.request.room.*.{itsOwnSiteID}.member.add`. Bot subjects use the same shape (`chat.server.bot.request.room.{siteID}.{roomID}.member.add`), so no subject rename is required — only the filler value of `{siteID}` changes from BP's own site to the room's home site.

## 2. Ownership

| Component | Owns |
|---|---|
| **botplatform-service (BP)** | session auth, rate limit, idempotency, **subscription lookup**, cross-site routing decision, cross-site room-details fetch for reply enrichment |
| **bot-message-handler** | content validation, canonical publish. Never does DM-ensure; never does cross-site room fetch |
| **bot-room-service** | room CRUD, DM ensure (deterministic ID), OUTBOX for cross-site subscription fanout |

## 3. Endpoint routing (all 5)

Read `sub := subscriptionStore.Find*(botAccount, key)` at the top of each BP handler.

| # | Endpoint | Sub lookup key | Site the RPC runs at | Missing sub |
|---|---|---|---|---|
| 1 | `POST /rooms/:roomID/messages` | `(bot, roomID)` | `sub.SiteID` (room's site) | **403 `not_a_room_member`** |
| 2 | `POST /dms/:userID/messages` | `(bot, otherAccount)` | `sub.SiteID` if found; else `cfg.SiteID` (bot's site — DM auto-created there) | Trigger DM-ensure at `cfg.SiteID` |
| 3 | `POST /rooms` | n/a — creating | `cfg.SiteID` (bot's site — creator's home) | n/a |
| 4 | `POST /rooms/:roomID/members` | `(bot, roomID)` | `sub.SiteID` | **403 `not_a_room_member`** |
| 5 | `DELETE /rooms/:roomID/members` | `(bot, roomID)` | `sub.SiteID` | **403 `not_a_room_member`** |

Rate-limit and idempotency stay BP-local (they gate ingress). Idempotency sentinel key includes `cfg.SiteID` — BP's own site — as the ingress fence, which is correct even when the downstream RPC crosses to another site.

## 4. Scenarios

### Scenario 1 — Send in cross-site room

**Setup:** bot@site1, room@site2, bot is a member.

1. `POST /rooms/{roomID}/messages` → BP@site1.
2. BP@site1 subscription lookup `(bot, roomID)` → `sub.SiteID = site2`.
3. BP@site1 NATS req/reply to `chat.server.bot.request.room.**site2**.{roomID}.msg.send` (crosses the supercluster).
4. bot-message-handler@**site2** validates, publishes to `BOT_MESSAGES_CANONICAL_**site2**`.
5. bot-message-worker@**site2** writes Cassandra@**site2**.
6. `broadcast-worker` (bot deployment)@**site2** fans out to site2 members. Cross-site members (site1, site3 …) receive via the existing bot-canonical → federation lane.

### Scenario 2 — First-time DM to cross-site user

**Setup:** bot@site1, userA@site2, no DM room exists yet.

1. `POST /dms/{userA}/messages` → BP@site1.
2. BP@site1 subscription lookup `(bot, userA)` → not found.
3. BP@site1 → bot-room-service@**site1** on `chat.server.bot.request.room.**site1**.dm.ensure` — DM room created at site1 with deterministic ID `idgen.BuildDMRoomID(bot, userA)`.
4. bot-room-service@site1 upserts bot's subscription in site1 Mongo; publishes `member_added` on the OUTBOX (destination=site2) → inbox-worker@site2 upserts userA's subscription in site2 Mongo pointing to the site1 room.
5. BP@site1 forwards the send to `chat.server.bot.request.dm.**site1**.{userA}.msg.send`.
6. bot-message-handler@site1 publishes canonical@site1, Cassandra@site1.

### Scenario 3 — Subsequent DM (cross-site room already exists)

**Setup:** bot@site1, userA@site2, DM room lives at site2 (userA created it via the user pipeline earlier; bot has a subscription pointing to the site2 room).

1. `POST /dms/{userA}/messages` → BP@site1.
2. BP@site1 subscription lookup `(bot, userA)` → `sub.SiteID = site2`.
3. BP@site1 optionally calls `chat.server.bot.request.room.**site2**.get` for reply-payload enrichment.
4. BP@site1 NATS req/reply to `chat.server.bot.request.dm.**site2**.{userA}.msg.send`.
5. bot-message-handler@**site2** publishes canonical@**site2**, Cassandra@**site2**.

### Scenario 4 — Create channel room

**Setup:** bot@site1, members = [userA@site1, userB@site2].

1. `POST /rooms` → BP@site1.
2. No subscription lookup (creating).
3. BP@site1 → `chat.server.bot.request.room.**site1**.create` — always bot's site.
4. bot-room-service@site1 writes the `rooms` doc + bot's + userA's subscriptions in site1 Mongo.
5. For userB@site2 → OUTBOX `member_added` (destination=site2) → inbox-worker@site2 materializes userB's subscription in site2 Mongo pointing to the site1 room.
6. Local `room_created` sysmsg on `chat.bot.canonical.**site1**.created`.

### Scenario 5 — Add member to same-site room

**Setup:** bot@site1, room@site1, adds userB@site2.

1. `POST /rooms/{roomID}/members` → BP@site1.
2. BP@site1 subscription lookup → `sub.SiteID = site1`.
3. BP@site1 → `chat.server.bot.request.room.**site1**.{roomID}.member.add` (local).
4. bot-room-service@site1 validates and updates `rooms`@site1.
5. For userB@site2 → OUTBOX (destination=site2) → inbox-worker@site2 upserts userB's subscription in site2 Mongo.
6. Local `member_added` sysmsg on `chat.bot.canonical.**site1**.created` (mliu33 T46, implemented in Batch A).

### Scenario 6 — Add member to cross-site room

**Setup:** bot@site1, room@**site2**, adds userA@site1 and userB@site3.

1. `POST /rooms/{roomID}/members` → BP@site1.
2. BP@site1 subscription lookup → `sub.SiteID = site2`.
3. BP@site1 NATS req/reply to `chat.server.bot.request.room.**site2**.{roomID}.member.add` (crosses to site2).
4. bot-room-service@**site2** validates and updates `rooms`@site2.
5. For userA@site1 → OUTBOX (site2, destination=site1) → inbox-worker@site1 upserts userA's subscription in site1 Mongo.
6. For userB@site3 → OUTBOX (site2, destination=site3) → inbox-worker@site3 upserts userB's subscription in site3 Mongo.
7. Local `member_added` sysmsg on `chat.bot.canonical.**site2**.created`.

### Scenario 7 — Remove member from same-site room

**Setup:** bot@site1, room@site1, removes userB@site2.

1. `DELETE /rooms/{roomID}/members` → BP@site1.
2. BP@site1 subscription lookup → `sub.SiteID = site1`.
3. BP@site1 → `chat.server.bot.request.room.**site1**.{roomID}.member.remove` (local).
4. bot-room-service@site1 mutates `rooms`@site1.
5. For userB@site2 → OUTBOX (destination=site2) → inbox-worker@site2 deletes userB's subscription in site2 Mongo.
6. Local `member_removed` sysmsg on `chat.bot.canonical.**site1**.created`.

### Scenario 8 — Remove member from cross-site room

**Setup:** bot@site1, room@**site2**, removes userA@site1.

1. `DELETE /rooms/{roomID}/members` → BP@site1.
2. BP@site1 subscription lookup → `sub.SiteID = site2`.
3. BP@site1 → `chat.server.bot.request.room.**site2**.{roomID}.member.remove` (crosses to site2).
4. bot-room-service@**site2** mutates `rooms`@site2.
5. OUTBOX (site2, destination=site1) → inbox-worker@site1 deletes userA's subscription in site1 Mongo.
6. Local `member_removed` sysmsg on `chat.bot.canonical.**site2**.created`.

## 5. Data model additions

### 5.1 BP subscriptionStore

New store interface in `botplatform-service/store.go`:

```go
type subscriptionStore interface {
    // FindForBot returns the bot's channel-room subscription, nil if not a member.
    FindForBot(ctx context.Context, botAccount, roomID string) (*BotSub, error)
    // FindDMForBot returns the bot's DM subscription with otherAccount, nil if none.
    FindDMForBot(ctx context.Context, botAccount, otherAccount string) (*BotSub, error)
}

type BotSub struct {
    RoomID   string
    SiteID   string          // the routing decision
    RoomType model.RoomType
}
```

`BotSub` is deliberately minimal — BP needs only what it takes to route. For reply-payload enrichment (create-room, add-member responses) BP calls `chat.server.bot.request.room.{sub.SiteID}.get` — no `$lookup`, no read-through to remote Mongo.

### 5.2 Deterministic DM room ID

`bot-room-service.dm.ensure` derives the room ID from `idgen.BuildDMRoomID(caller.Account, targetAccount)` (lexicographic sorted-concat; deterministic regardless of caller order) and uses `upsert(setOnInsert)` for the create. Two concurrent Ensure calls converge on the same `_id`; the loser becomes a no-op. Mirrors the user pipeline's DM race resolution.

## 6. Subject cheat-sheet

All bot subjects already carry `{siteID}` — no rename needed. The change is who fills that slot.

| Subject | Filler |
|---|---|
| `chat.server.bot.request.room.{siteID}.{roomID}.msg.send` | `sub.SiteID` (room's site) |
| `chat.server.bot.request.dm.{siteID}.{targetUserID}.msg.send` | `sub.SiteID` if found; else `cfg.SiteID` on first-DM |
| `chat.server.bot.request.room.{siteID}.create` | `cfg.SiteID` (bot's site) |
| `chat.server.bot.request.room.{siteID}.{roomID}.member.add` | `sub.SiteID` |
| `chat.server.bot.request.room.{siteID}.{roomID}.member.remove` | `sub.SiteID` |
| `chat.server.bot.request.room.{siteID}.dm.ensure` | `cfg.SiteID` (called by BP on first-DM only) |
| `chat.server.bot.request.room.{siteID}.get` | `sub.SiteID` (called by BP for reply enrichment) |

Cross-site NATS routing between sites is an ops/IaC concern (supercluster gateway export/import), identical to the existing INBOX cross-site pattern. No new stream, no new consumer.

## 7. Implementation checklist

| # | Change | Files |
|---|---|---|
| 1 | Delete cross-site room fetcher (superseded — handler always runs at room's site) | `bot-message-handler/room_fetcher.go`, `bot-message-handler/handler.go` (drop `verifyRoomExists` remote branch), `bot-message-handler/main.go` (drop wiring), `bot-message-handler/handler_test.go` |
| 2 | Move DM-ensure trigger from `bot-message-handler` to BP; drop `DMEnsurer` from the handler | `bot-message-handler/dm_ensurer.go` (delete), `bot-message-handler/handler.go` (remove ensure fallback), `bot-message-handler/main.go`; add `botplatform-service/dm_ensurer.go` |
| 3 | Add `subscriptionStore` (Mongo) to BP | `botplatform-service/subscription_store.go`, `subscription_store_test.go`, integration test |
| 4 | Route the 4 non-create endpoints by `sub.SiteID` | `botplatform-service/bot_handlers.go`, `bot_forwarder.go`, `bot_handlers_test.go`, `bot_forwarder_test.go` |
| 5 | Deterministic DM room ID in `bot-room-service.dm.ensure` | `bot-room-service/handler.go` (Ensure path), `handler_test.go` (concurrent-race test) |
| 6 | Update client-facing docs | `docs/client-api.md` |

Each step commits and pushes independently; each keeps the tree green (`make test`, `make lint`, `make sast`).

## 8. Test plan

Unit / handler tests only for the routing change; existing integration tests cover the wire shapes.

1. `botplatform-service/bot_handlers_test.go` — table-driven `(endpoint, sub_present, sub_siteID, cfg_siteID) → expected_outbound_subject`.
2. `botplatform-service/subscription_store_test.go` — Mongo integration test alongside the bot-platform integration suite.
3. `bot-room-service/handler_test.go` — DM ensure concurrent race: two Ensure calls converge on one `_id`.
4. `bot-message-handler/handler_test.go` — delete `room_fetcher` tests; the handler no longer decides cross-site.
