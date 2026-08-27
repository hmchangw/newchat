# System messages: unread & sidebar-ordering exclusion — design

**Date:** 2026-08-26
**Status:** Approved
**Services:** broadcast-worker, user-service, room-service, chat-frontend (+ `pkg/model`, docs)

## Problem

QA: system events (member added/removed/left, room renamed/restricted, room created,
Teams meeting started) mark rooms unread and re-sort the sidebar for every member.

Root cause: all seven system-message types are published as ordinary `EventCreated`
messages on MESSAGES-CANONICAL; broadcast-worker's `handleCreated` bumps
`rooms.lastMsgAt` unconditionally (`broadcast-worker/handler.go:248`), and one field —
`rooms.lastMsgAt` — drives three unrelated things:

1. **Unread** — `hasUnread` / `subscription.count` / the frontend badge all compute
   `lastMsgAt > lastSeenAt` (`user-service/service/subscriptions.go`,
   `chat-frontend .../selectUnread.js`).
2. **Sidebar ordering** — server (`sortListRows`, key = room `lastMsgAt` ?? `createdAt`)
   and client (`sortByLastMsgDesc`).
3. **History read ceiling** — history-service caps the room-timeline walk at
   `lastMsgAt + 1ms` and starts the Cassandra bucket walk at `bucket(lastMsgAt)`
   (`history-service/internal/service/messages.go:46-53`). A message newer than
   `lastMsgAt` is **invisible on reload**, so system messages must keep bumping it.

The third point rules out "just stop bumping `lastMsgAt`": system messages are
persisted to Cassandra and rendered in the timeline, so the ceiling must keep moving.

## Requirements

1. A system message never marks a room unread for members who have read it, and never
   changes sidebar ordering — for anyone.
2. **Exception:** a member newly added to a room (including at room creation) sees the
   room as unread until she first opens it — even when the room has no user messages.
3. System messages still render in the room timeline, live and after reload.
4. The actor keeps not seeing their own action as unread (status quo: the system
   message's "sender" gets `lastSeenAt` advanced).
5. No data migration. Deploy order is NOT free, though: writers first and
   fully drained, then readers — and that includes room-service at every remote
   site, whose rooms.info reply is a reader's only source of a cross-site room's
   lastUserMsgAt. See Rollout below.

## Rejected approaches

- **Stop bumping `lastMsgAt`; move history's ceiling to server-clock bounds** — changes
  the hot read path for every room (walks empty buckets down from now), touches the
  documented `meta.lastMsgAt` client-hint semantics and preview freshness invariants.
- **Per-recipient unread flags written at event time** — fan-out writes per system
  event and a new consistency surface; requirement 2 does not need it (see below).

## Design

### New field: `rooms.lastUserMsgAt`

`pkg/model.Room` gains `LastUserMsgAt *time.Time` (json+bson `lastUserMsgAt`).
Single writer: broadcast-worker `handleCreated`, alongside `lastMsgAt`.

- `lastMsgAt` — unchanged: newest message of any kind. Still the history ceiling,
  client `meta.lastMsgAt` hint, mark-read early-return, preview freshness key, and
  cross-site `RoomActivityEvent` payload.
- `lastUserMsgAt` — newest **non-system** message (`!model.IsSystemMessageType`).
  The only input to unread and ordering.

### The read rule (every seam)

- **Unread:** compare `lastSeenAt` against `lastUserMsgAt ?? lastMsgAt`.
  No `createdAt` fallback — a pre-deploy room with no messages must not light up.
- **Sort key / `updatedWithinDays` window:** `(lastUserMsgAt ?? lastMsgAt) ?? createdAt`
  (today's structure with the user field preferred).

The `?? lastMsgAt` fallback keeps pre-deploy rooms behaving exactly as today until
they are touched.

### Write path (broadcast-worker)

- `roomLastMessage` gains `SystemMsg bool` (from `model.IsSystemMessageType(msg.Type)`).
- Coalescer `roomLastMsgUpdate` gains `userAt`/`userMsgID`: newest non-system message
  in the flush window, same millisecond+id comparator (`newerRow`) as `lastMsgAt`.
- `lastMessageUpdate` (single + bulk share it):
  - window contains a user message → `$set lastUserMsgAt: userAt` beside `lastMsgAt`
    (unguarded, same accepted regression semantics as `lastMsgAt`);
  - window is system-only → **sticky freeze**, a pipeline `$set` evaluated against the
    pre-update document:
    `lastUserMsgAt = $ifNull($lastUserMsgAt, $cond($lastMsgAt is null-or-missing, $ifNull($createdAt, $$REMOVE), $$REMOVE))`
    — an existing value is kept, and a floor is pinned once for a room that has
    never carried a message (its `createdAt`). A room that already has a
    `lastMsgAt` is left untouched: that timestamp is *unclassified* (pre-deploy it
    was bumped by system messages too), so promoting it would persist a system
    position as user activity and the stickiness would then keep it forever.
    Absent is the safe state — readers coalesce to `lastMsgAt`, which is the
    pre-field behavior.
  - This makes the previews-off mode a pipeline update too (was a plain `$set`; the
    test asserting that shape is updated).
- Everything else in `handleCreated` is untouched: the actor's
  `AdvanceSubscriptionLastSeen`, mention handling, fan-out, preview eligibility.

**Why requirement 2 needs no per-recipient machinery:** a newly added member's
subscription has no `lastSeenAt`, and `unread(nil, ms≠nil)` is already true. The
reference is non-nil the moment the membership system message lands — for a room
with history it is the room's own `lastMsgAt`, reached through the reader's
coalesce; for a brand-new room it is the `createdAt` the freeze pins. Members who
have opened the room have `lastSeenAt` at/past that reference, so later system
events never re-flag them.

**Legacy rooms are not repaired, but nothing wrong is persisted.** A pre-deploy
`lastMsgAt` is an *unclassified* activity time — before this change system messages
bumped it too — so it cannot be promoted into a field whose whole contract is "the
last user message". Hence the freeze declines to write for any room that already
has one. For a legacy room whose newest pre-deploy event was a system message at T2
while its last real user message was T1, a member who had read through T1 still
reads as unread and the room still sorts at T2 — but that is **exactly today's
behavior**, not a regression, and it is not frozen in: it is just the `?? lastMsgAt`
coalesce, and the room's next user message writes the first accurate value the
field ever holds. Repairing T1 itself would need a per-room user-message backfill
from history, which is the migration this design set out to avoid. Rooms created
after the cutover are unaffected: their own `room_created` message freezes
`createdAt`, which is a correct floor.

### Wire addition: `RoomEvent.systemMsg`

In encrypted channels the message (including `type`) is sealed inside
`encryptedMessage`, so a client cannot recognize a system event by type. System-message
`new_message` events carry a plaintext `systemMsg: true` (json `omitempty`), set where
`handleCreated` builds the event. Metadata-stays-clear policy as the preview;
membership/rename facts are already plaintext in `subscription.update` / room events.

### user-service

- `roomsEnrichStages` ($lookup projection + `$addFields`), `roomBaseline` + projection,
  `activeSubscriptionProjection` + `models.ActiveSubscription`, `roomSortKey` +
  `resolveSortKeys` projection + sort-key cache entries: all carry `lastUserMsgAt`.
- `buildListRows`: `sortAt` and the `withinDays` cutoff use the sort rule above.
- `enrichLocal` / `applyRoomInfo` / both branches of `unreadRooms`: `HasUnread` and
  badge membership use the unread rule; the wire `room` object exposes
  `lastUserMsgAt` (`model.SubscriptionRoom`, `model.EnrichedSubscription`).
- Badge cache, `subscription.count`, push `UnreadCounts`: no code change (derived).

### room-service

- `model.RoomInfo` gains `LastUserMsgAt *int64`; `aggregateRoomInfo` maps it (and the
  `ListRoomsByIDs` projection includes it if fields are projected), so cross-site
  subs follow the same rule.
- `messageRead` early-return keeps using `lastMsgAt` (full activity) — unchanged.

### chat-frontend

Summary `lastMsgAt` keeps its name, takes user-activity semantics — `sortByLastMsgDesc`
and `selectUnread` need no change:

- Seeds (`fetchSidebarBuckets`, the `subscription.update`/room-added seam embedding a
  room object) map `room.lastUserMsgAt ?? room.lastMsgAt` into the summary.
- Reducer `MESSAGE_RECEIVED` (both branches):
  `isSystem = evt.systemMsg === true || isSystemMessageType(msg.type) || msg.sysMsgData != null`
  → skip the `lastMsgAt` bump, skip the `unreadCount` increment, leave `summaries`
  untouched (no resort); still append the message and bump `msgRecvSeq`. Preview
  suppression already exists.
- `types.ts`: `lastUserMsgAt?` on the room object, `systemMsg?` on the room event.

### Docs

`docs/client-api.md` + derived views (`request-reply.md`, `events.md`): `hasUnread`
definition, room-object field table (`lastUserMsgAt`), `new_message` event
(`systemMsg` flag + note that system events must not advance unread/ordering).

## Edge cases

- **Actor:** unchanged — their `lastSeenAt` advances with their own system message.
- **Encrypted channels:** covered by `systemMsg` (above).
- **Thread replies:** unchanged. Visible (TShow) replies are user messages → bump
  both fields; hidden replies bump neither (existing behavior).
- **Pre-deploy empty rooms:** no `lastUserMsgAt`, no `lastMsgAt` → unread stays false
  (unchanged); sort stays `createdAt`.
- **Cross-site:** origin site computes both fields; `RoomsInfoBatch` carries both;
  `remote_rooms` / `RoomActivityEvent` untouched (no reader today).

## Rollout

No migration: the field is additive and every reader coalesces
`lastUserMsgAt ?? lastMsgAt`. Sequencing still matters, because the coalesce
treats *any* non-nil `lastUserMsgAt` as authoritative and an old writer cannot
maintain it.

**Required order: broadcast-worker fleet first and fully drained, then
room-service at every site, then the user-service readers — then the client.**

- **Writer overlap is the one unsafe window.** Old and new broadcast-worker
  replicas share the MESSAGES-CANONICAL queue group, so a room's writes can
  interleave: a new replica creates `lastUserMsgAt=t1`, then an old replica
  handles a *later user* message at t2 and advances only `lastMsgAt`. Readers
  keep preferring t1, so a member whose `lastSeenAt` falls between t1 and t2
  reads as caught-up and the room does not resurface. It is self-healing but
  not promptly: the repair is the room's next user message reaching an upgraded
  writer, so a room that goes quiet right after the overlap can hold the stale
  value indefinitely. Draining old writers before the readers go out closes the
  window; there is no reader-side signal that would let it be detected instead.
- **Readers before writers** is otherwise safe — nothing has written the field,
  so `?? lastMsgAt` is exactly today's behavior.
- **Rolling the writer back** after the field has been written, while readers
  stay new, is the same failure as the overlap and does not self-heal at all
  until the writer is redeployed.
- **A lagging remote room-service is a second unsafe window.** A cross-site
  room's `lastUserMsgAt` reaches a reader only through the peer site's
  `rooms.info` reply. An old room-service omits the field even when its own room
  document carries it, so the reader coalesces to `lastMsgAt` and a system event
  after user activity marks cross-site members unread and reorders the room —
  for exactly as long as that one peer lags. Unlike the writer window this is
  not self-healing through message traffic; it clears only when the remote
  room-service is upgraded. Upgrade room-service at **every** site before
  enabling the new user-service readers. Distinguishing "old peer" from "legacy
  room" would need a versioned rooms.info response; the field's absence alone
  cannot tell them apart.
- **New server + old client.** Safe for the badge itself, but not free. An old
  client never reads `hasUnread` and folds unread locally from the
  full-activity `lastMsgAt` it already had, so after any system message its
  local count structurally disagrees with the server's. `useUnreadCount`'s
  reconcile (5-minute interval, `MISMATCHES_BEFORE_RESYNC = 2`) therefore fires
  `resync()` roughly every 10 minutes for as long as the room stays unread on
  one side only. The badge does not flap — resync converges on the server's
  number each time — the cost is a recurring bootstrap refetch per affected
  session. Shipping the client promptly ends it; there is no server-side
  mitigation.

## Testing

TDD per component:

- `pkg/model`: round-trip for the new fields.
- broadcast-worker: coalescer window tests (mixed → both keys correct; system-only →
  freeze shape; previews-off pipeline shape), handler tests (`SystemMsg` flag set,
  `systemMsg` stamped on the event, actor advance unchanged), integration test
  (system message bumps `lastMsgAt` but not `lastUserMsgAt`; freeze heals an existing
  room; new room pins to `createdAt`).
- user-service: mongorepo integration (ordering unaffected by a `lastMsgAt`-only bump;
  `withinDays`), service unit (`hasUnread` false for readers after a system event;
  true for a never-seen member; cross-site via `RoomInfo.lastUserMsgAt`; badge count).
- room-service: `roomsInfoBatch` carries `lastUserMsgAt`.
- chat-frontend: reducer tests (system event appends but does not bump/resort/count;
  `systemMsg`-only gate for encrypted), seed mapping tests.
