# Thread-Unread Tracking + Per-Recipient Push Badge Counts

**Date:** 2026-08-02
**Status:** Implemented

## Background

Every push notification must carry the recipient's current badge unread count so
the push gateway and clients (desktop app icon, mobile) can render an up-to-date
badge. Badge semantics: **count of rooms with any unread content** (main message
or thread reply) — each room counts once, muted rooms excluded, rendered as
"9+" beyond 9, so counts are capped at 10 on the wire where noted.

Today this is impossible at acceptable cost:

- `subscription.count (unread=true)`'s thread phase fans out
  `GetThreadRoomInfoBatch` RPCs per owning site because thread activity
  (`thread_rooms.lastMsgAt`) lives only at the room's home site and is never
  federated.
- notification-worker has no badge data at all (`PushNotificationEvent` carries
  none), runs at the room's home site, and a recipient's complete subscription
  picture exists only at the recipient's home site.
- Most users' rooms are homed at their own site, so the dominant read path can
  and should be site-local.

Reference point: Rocket.Chat (the system our data was migrated from) solves the
same problem with write-side per-subscription unread state (`unread`,
`tunread[]`) plus a live Mongo aggregation per push (`getBadgeCount`). That
works in a single-Mongo monolith; our multi-site split and async pipeline
require the same write-side idea plus a cache and an RPC hop to the recipient's
home site.

Prior history: `Subscription.ThreadUnread` was removed in the 2026-08-01 PR
because it never had a producer and all consumers had moved to the derived
model. That removal was correct for the requirements of its time. This spec
reintroduces the field's *shape* deliberately — with a real producer, batched
federation, and capped-badge semantics.

## Decisions

1. **`Subscription.ThreadUnread []string` returns** (parent message IDs — the
   `threadId` the client already sends to `message.thread.read`, matching the
   legacy `tunread[]` content). `omitempty`, same json/bson tags as before.
2. **`subscription.count` thread phase goes local**: the
   `ListByAccountInRooms` + per-site `GetThreadRoomInfoBatch` fan-out is
   replaced by a `threadUnread`-non-empty check on the already-fetched page.
   The response contract is unchanged: `{count}` only — no room-ID list on the
   wire. The room phase (local `$lookup` + cross-site `GetRoomsInfo`) is
   unchanged.
3. **Badge counts ride the existing push batches** as
   `PushNotificationEvent.UnreadCounts map[string]int` (account → count capped
   at 10; 10 renders as "9+"). Accounts whose count could not be fetched are
   absent from the map — push still goes out; the client refreshes on open.
4. **notification-worker fetches counts via a new server-to-server batch RPC**
   to user-service at each recipient's home site — the only site that has the
   recipient's full subscription replica set and the cross-site room logic.
   This works uniformly for local and cross-site recipients.
5. **A Valkey badge cache — a per-user SET of unread room IDs, not a counter —**
   makes the per-message hot path Mongo-free. Sets are the correctness
   foundation: `SADD`/`SREM` are idempotent and commutative, so JetStream
   redeliveries, same-room bursts, and concurrent writers all converge;
   "each room counts once" is set cardinality by construction.
6. **`alert` is not revived.** Room-level unread stays derived
   (`lastSeenAt` vs `lastMsgAt`); no alert-recompute coupling returns.

## 1. Data model (`pkg/model`)

- `Subscription.ThreadUnread []string` — json/bson `threadUnread,omitempty`.
- `ThreadReadEvent` regains `NewThreadUnread []string` (the post-`$pull`
  array, nil when empty) so the home replica converges on thread reads. No
  `Alert` field.
- New inbox event `ThreadUnreadAddedEvent` (type `thread_unread_added`):
  `{roomId, parentMessageId, accounts[], timestamp}` — one event per
  destination site per thread reply, `accounts` scoped to that site's
  followers.
- `PushNotificationEvent` gains `UnreadCounts map[string]int
  \`json:"unreadCounts,omitempty"\``.
- New server-to-server contract types: `BadgeCountBatchRequest{RoomID string,
  Accounts []string}`, `BadgeCountBatchResponse{Counts map[string]int}`.

## 2. Write path (message-worker — the only producer)

On each thread reply (skipped for `X-Migration: live`):

1. Build the recipient set: `replyAccounts ∪ visible mentionees − sender`
   (already in hand from the existing reply handling).
2. One local write at the room's home site:
   `UpdateMany {roomId, u.account ∈ recipients} $addToSet {threadUnread: parentMessageID}`.
3. Federation to each recipient's origin site: group recipients by home site
   (users lookup, cache-backed), and for each remote site publish **one**
   `thread_unread_added` outbox event carrying that site's account list.
   inbox-worker applies the same `UpdateMany` on its local replicas. This
   guarantees the user's origin-site replica always converges.
4. `thread_unread_added` joins `outbox.ConcurrentEventTypes` (order-insensitive:
   `$addToSet` is idempotent and commutative; no FIFO lane needed).

Cost: one Mongo `UpdateMany` per reply locally + one outbox event per remote
site that actually has followers. Single-site rooms federate nothing.

## 3. Clear path (resurrected from the 2026-08-01 removal, minus alert)

- `message.thread.read` (room-service): `$pull` the parent ID; when the array
  empties, `$unset` it. Restore `UpdateSubscriptionThreadRead` (returns the
  new array only — no alert return) alongside the existing
  `UpdateThreadSubscriptionRead` in the handler's concurrent write pair.
  Federate `ThreadReadEvent` with `NewThreadUnread` to the user's home site.
- `thread.read.all` (room-service leaf): restore
  `ClearSubscriptionThreadUnreadForAccount` (`$unset` on subs with
  `threadUnread.0`), no alert write. inbox-worker's `ApplyThreadReadAll`
  regains the subscription bulk clear.
- inbox-worker `ApplyThreadRead` regains the subscription write (apply
  `NewThreadUnread`, `$unset` when empty), still gated behind the existing
  `$lt` thread-sub guard.
- Accepted race (unchanged from the legacy design): a reply in flight during a
  read can re-add a just-read thread — the badge over-shows by ≤1 room until
  the next read. Invisible under the 9+ cap; no per-follower read-guard on the
  `UpdateMany` (it would require per-doc `lastSeenAt` from another collection).

## 4. `subscription.count` rework (user-service)

- Internal refactor: `unreadRooms(ctx, account) → []roomID` — the existing
  `countUnread` flow (local page + `GetRoomsInfo` for cross-site rooms) with
  the thread phase replaced by: a read room is unread when its sub's
  `ThreadUnread` is non-empty. Collecting IDs instead of a bare count is free
  (the loop already iterates per sub).
- The `subscription.count` handler returns `{count: len(ids)}` — **wire
  contract unchanged** — and opportunistically reseeds the Valkey badge set
  from `ids` (fail-open; a cache error never fails the RPC). Every desktop
  badge poll thus doubles as cache repair.
- The `threadUnread` projection line returns to the subscription-list
  projection so clients and the count page receive the field.

## 5. Badge batch RPC (user-service, server-to-server)

- Subject: `chat.server.request.user.{siteID}.badge.count.batch` (builder +
  pattern in `pkg/subject`, registered via natsrouter like the existing
  server-to-server handlers).
- Request `{roomId, accounts[]}` (accounts bounded by the push batch size);
  response `{counts: {account: n}}`, each `n = min(SCARD, 10)`.
- Handler per account, via `pkg/badgecache` (all Valkey ops atomic Lua):
  - **Hit**: `SADD badge:{account} roomId` + `EXPIRE` + `SCARD`. Two O(1) ops,
    zero Mongo. The SADD is the accuracy keystone: the triggering room is
    counted even though the async pipeline (broadcast-worker `lastMsgAt`,
    message-worker `threadUnread`) may not have landed yet — the notification
    itself is the invalidation signal, applied atomically with the read.
  - **Miss**: seed = `unreadRooms(account)`; atomically `SADD(seed ∪ {roomId})`
    + `EXPIRE` + `SCARD`. The triggering room is unioned in, so a seed racing
    the async writers still returns the correct count.
- Per-account failures degrade to absence from `counts` (never fail the batch).

## 6. notification-worker changes

- Member cache (`roomsubcache`) projection + `Member` struct gain `siteId`
  (already on the subscription doc). Cache version/namespace bumped so stale
  entries without the field expire naturally.
- After the survivor filter: group survivor accounts by home `siteId`; one
  concurrent `badge.count.batch` per site (existing 5s server-RPC timeout,
  bounded concurrency); merge into `UnreadCounts`; stamp onto each outgoing
  100-account batch. A site RPC failure logs WARN and leaves its accounts out
  of the map — the push must never block on badges.
- This is the worker's first per-event cross-service read; it is bounded at
  one RPC per recipient-site per message (not per recipient) and the Valkey
  hit path keeps the far side O(1).

## 7. `pkg/badgecache` (new shared package)

- Key: `badge:{account}` — Valkey SET of unread room IDs, TTL 24h, owned by
  the account's home site. Cluster client, same conventions as existing Valkey
  usage; hash-tag the account so Lua multi-key ops stay single-slot.
- API: `Bump(ctx, account, roomID) (count int, ok bool)`,
  `Seed(ctx, account, roomIDs, triggerRoomID) (count, ok)`,
  `ClearRoom(ctx, account, roomID)`, `ClearAll(ctx, account)`,
  `Reseed(ctx, account, roomIDs)`. All fail-open: on any Valkey error, `ok =
  false` and callers fall back (RPC → Mongo seed path or omit; clears → no-op,
  TTL repairs).
- **SREM call sites** (where the home site learns of reads):
  - room-service `message.read`: SREM iff the sub's `threadUnread` is empty
    (one field added back to the read projection), only when the reader is
    home-local (`userSiteID == h.siteID`).
  - room-service `message.thread.read` / `thread.read.all` (home-local
    readers): SREM when the array empties / `ClearAll`. When main-read state
    is unknowable locally (cross-site room, no local `lastMsgAt`): SREM
    anyway — a transient under-count is repaired by the next message's SADD; a
    stuck over-count would persist to TTL.
  - inbox-worker `subscription_read` / `thread_read` / `thread_read_all`
    handlers: same rules for federated reads landing at the home site.
  - inbox-worker `member_removed`: `ClearRoom` (cheap, avoids a lingering
    entry until TTL).
- Principle: user-initiated reads bias toward clearing; passive changes
  (mute/leave edge cases) bias toward TTL/reseed repair.

## 8. Accuracy model (race matrix)

| Race | Outcome |
|---|---|
| Notify vs async counter writes | Exact — RPC-carried SADD bypasses pipeline ordering |
| Redelivery / same-room burst | SADD idempotent — room counted once |
| Concurrent rooms | SADDs commute — all counted |
| Fast reader (reads before RPC lands) | ≤1 over until next read of that room / TTL; the per-account Mongo guard that would prevent it is rejected (defeats the O(1) hot path) |
| Read on another device mid-RPC | Either adjacent value; point-in-time badge, next event corrects |
| Seed vs concurrent read | ≤1 over; repaired by TTL and by every `subscription.count` reseed |
| Mute/leave after counting | Over-count until TTL/reseed (reseed applies the active filter); `member_removed` hook narrows it |
| Valkey eviction/restart | Next RPC reseeds from Mongo — correctness never depends on Valkey durability |

Invariant: the Valkey set is a best-effort materialization; Mongo (+
`GetRoomsInfo`) is the source of truth reachable via reseed; every divergence
is ±1-room magnitude and bounded by `max(TTL, next interaction)`; **the
triggering room is always exact at notify time**.

## 9. Rollout & compatibility

- `ThreadReadEvent.NewThreadUnread`: old inbox-workers ignore the unknown JSON
  field; new inbox-workers treat absence as "clear" — identical to the
  pre-removal semantics. Deploy order free.
- `thread_unread_added`: added to `outbox.ConcurrentEventTypes` before any
  producer ships (the partition guard in `outbox.Publish` rejects unknown
  types). inbox-worker gains the handler in the same release as message-worker
  gains the producer; an inbox-worker that predates the type NAKs to redelivery,
  so deploy inbox-worker first.
- `UnreadCounts` on `PushNotificationEvent` is additive; push-notification-
  service ignores unknown fields until its dispatcher consumes them.
- No Mongo migration: `threadUnread` starts empty and grows organically; the
  badge is advisory and converges.

## 10. Documentation

- `docs/client-api.md`: `threadUnread` row returns to the Subscription schema
  table; `message.thread.read` / `thread.read.all` behaviour notes regain the
  subscription-write description (explicitly without alert coupling);
  `subscription.count` notes the thread check is subscription-local.
- Derived views synced (`request-reply.md`); push event and badge RPC are
  server-to-server and documented in `docs/notification-worker-downstream-contracts.md`.
- `data-migration/CDC_COVERAGE.md`: `thread_read` rationale updated (the field
  is message-pipeline-owned again — and this time the pipeline exists).

## 11. Testing

- TDD throughout (red-first for every behavior change).
- Unit: message-worker recipient-set/UpdateMany/outbox grouping; inbox-worker
  `thread_unread_added` + restored thread-read applies; user-service
  `unreadRooms` + count handler + badge RPC (mocked cache + repo);
  notification-worker site-grouping + stamping + degradation.
- Integration: `UpdateMany` fan-out and clears (Mongo), badge cache via
  `testutil.SharedValkeyCluster` + `FlushValkey` cleanup (Lua paths: hit,
  seed, clear, TTL), end-to-end count rework, wire-compat decode of old
  `ThreadReadEvent` payloads.
- Coverage floors per CLAUDE.md.

## Out of scope

- Reviving `alert` or any alert-recompute coupling.
- Cross-site push device-token routing (pre-existing gap; badge counts are
  correct regardless of where the push is dispatched).
- Per-message `room_activity` federation mirrors (rejected: per-message
  fan-out to all peer sites; the badge cap removes the exactness need).
- Changing the `subscription.count` wire contract or badge rendering rules.
- push-notification-service dispatcher work (still a logging stub).
