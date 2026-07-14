# User Thread-Unread Badge RPC (user-service, cross-site aggregator)

**Date:** 2026-07-01
**Status:** Approved — implemented

## Problem

A client needs a single per-user thread-unread badge that reflects thread
activity across every federation site the user participates in. No client-facing
RPC exists, and nothing aggregates thread-unread across sites.

The 2026-06-09 work added a room-service **leaf**
(`chat.server.request.room.{siteID}.thread.unread.summary`) that computes one
site's thread-unread rollup for one user. It has **no runtime caller** (the
client-facing aggregator was never built). This spec supersedes it: the badge is
computed the **same way room (subscription) unread already is**, so the leaf is
removed.

## Chosen architecture — mirror subscription-unread

Room (subscription) unread is already computed in user-service through a single
`unread(lastSeen, lastMsgAt)` helper (`user-service/service/subscriptions.go`):
local subs compare the user's `lastSeenAt` against a locally-read
`room.lastMsgAt`; cross-site subs compare it against a `room.lastMsgAt` fetched
via the `RoomsInfoBatch` RPC (`GetRoomsInfo`). One rule, two data sources.

The thread badge is the **same pattern one grain down**:

- Thread subscriptions are federated to the user's home site (message-worker
  publishes `InboxThreadSubscriptionUpserted`; room-service publishes
  `InboxThreadRead`), so the home site holds a replica of **every** thread-sub
  the user participates in — carrying `threadRoomId`, `roomId`, `siteId` (the
  room's home site), `lastSeenAt`, and `hasMention`.
- The home site reads those thread-subs and, in the same query, `$lookup`s the
  local `subscriptions` replica to (1) **gate each thread on membership** — a
  thread whose room the account no longer subscribes to is dropped, (2) **drop
  unsubscribed-app threads** (`roomType == botDM AND isSubscribed == false`), and
  (3) **source `roomType`** for the DM tally from that subscription. The gate runs
  **before** the newest-`maxThreadSubscriptions` limit so an inaccessible or
  unsubscribed-app thread cannot crowd out a live one.
- The only per-thread datum the home site still lacks is the *fresh*
  `thread_rooms.lastMsgAt`, authoritative only at the room's home site.
- So user-service reads all accessible thread-subs locally (with roomType
  already joined in), fetches `lastMsgAt` per thread from the owning site via a
  new `ThreadRoomInfoBatch` RPC (modeled on `RoomsInfoBatch`), and computes unread
  with the **same `unread()` helper** — uniformly for every site, local
  included. There is no separate local path and no room-service rollup leaf.

**Design-decision record / accepted trade-offs** (raised and accepted during
design):

1. **Post-read false-"unread" window (cross-site only).** For a cross-site
   thread, the mark-as-read RPC updates `lastSeenAt` at the room's home site
   first and mirrors it to the home replica asynchronously via `InboxThreadRead`.
   Comparing a freshly fetched remote `lastMsgAt` against the lagged home-replica
   `lastSeenAt` can transiently show unread just after a cross-site read, until
   the event replicates. Self-heals within the inbox lag. (Local threads read
   `lastSeenAt` from the same site that serviced the read, so they are unaffected.)
2. **O(threads) fetch for all sites.** Every site — including local — is fetched
   per-thread rather than rolled up server-side, so `thread.info.batch` requests
   are chunked by `maxBatchSize`. This is the cost of a single uniform compute
   path (the removed leaf did local in one aggregation).
3. **`$lookup` on the read path.** `ListByAccount` joins `subscriptions` rather
   than issuing a second query per thread. The join key `(u.account, roomId)` is
   served by the unique subscriptions index and the outer newest-N read by the
   `(userAccount, createdAt)` index; folding membership + app-gate + roomType into
   the one query is what lets the gate run before the limit.

## Client-facing RPC contract

Client-facing NATS request/reply, served by user-service, one subscription per
site. `siteID` is the caller's own home site (mirrors `UserThreadList`).

**Subject** (`pkg/subject/subject.go`, mirroring `UserThreadList`):

```go
func UserThreadUnreadSummary(account, siteID string) string {
    if !isValidAccountToken(account) {
        panic("invalid account token: contains NATS wildcard characters")
    }
    return fmt.Sprintf("chat.user.%s.request.user.%s.thread.unread.summary", account, siteID)
}
func UserThreadUnreadSummaryPattern(siteID string) string {
    return fmt.Sprintf("chat.user.{account}.request.user.%s.thread.unread.summary", siteID)
}
```

**Request / response** (`pkg/model`):

```go
// ThreadUnreadSummaryRequest carries no fields: the account rides the subject.
type ThreadUnreadSummaryRequest struct{}

type ThreadUnreadSummaryResponse struct {
    Unread              bool     `json:"unread"`
    UnreadDirectMessage bool     `json:"unreadDirectMessage"`
    UnreadMention       bool     `json:"unreadMention"`
    LastMessageAt       *int64   `json:"lastMessageAt,omitempty"` // UnixMilli; nil when no responding site reported a thread
    UnavailableSites    []string `json:"unavailableSites,omitempty"`
}
```

## New server-to-server RPC: ThreadRoomInfoBatch (room-service)

Modeled on `RoomsInfoBatch`. Given a set of thread-room IDs, return each one's
`thread_rooms.lastMsgAt`. Pure key→info lookup, no user scoping, no room-type
join (the caller sources room type from the account's subscription).

**Subject** (`pkg/subject/subject.go`):

```go
// Single builder for both the request subject and room-service's handler registration.
func ThreadRoomInfoBatch(siteID string) string {
    return fmt.Sprintf("chat.server.request.room.%s.thread.info.batch", siteID)
}
```

**Request / response** (`pkg/model`, mirroring `RoomsInfoBatchRequest` /
`RoomInfo` with a `Found` flag):

```go
type ThreadRoomInfoBatchRequest struct {
    ThreadRoomIDs []string `json:"threadRoomIds"`
}

type ThreadRoomInfo struct {
    ThreadRoomID string `json:"threadRoomId"`
    Found        bool   `json:"found"`
    LastMsgAt    int64  `json:"lastMsgAt"` // UnixMilli; 0 when Found=false
}

type ThreadRoomInfoBatchResponse struct {
    Threads []ThreadRoomInfo `json:"threads"`
}
```

A requested thread room that does not exist is returned with `Found=false`
(mirrors `RoomInfo.Found`); the caller treats it as "no activity."

## Thread-sub read result (user-service)

`ListByAccount` returns the `subscriptions`-joined rows the badge folds:

```go
type ThreadUnreadRow struct {
    ThreadRoomID string         `json:"threadRoomId" bson:"threadRoomId"`
    SiteID       string         `json:"siteId"       bson:"siteId"`
    RoomType     model.RoomType `json:"roomType"     bson:"roomType"` // from the joined subscription
    LastSeenAt   *time.Time     `json:"lastSeenAt"   bson:"lastSeenAt"`
    HasMention   bool           `json:"hasMention"   bson:"hasMention"`
}
```

## Data flow

```text
client → chat.user.{account}.request.user.{siteID}.thread.unread.summary   (user-service)
  1. threadSubs.ListByAccount(account)    (local Mongo thread_subscriptions replica, $lookup subscriptions)
        → rows []ThreadUnreadRow: {threadRoomId, siteId, roomType, lastSeenAt, hasMention}
          — membership-gated + unsubscribed-apps dropped, then newest 500 by createdAt desc
  2. group rows by siteId; concurrently (no fan-out cap), per site, chunked by maxBatchSize:
        rooms.GetThreadRoomInfoBatch(site, threadRoomIds) → map[threadRoomId]ThreadRoomInfo
        compute per row (shared unread() helper):
          unread   = info.Found && (lastSeenAt == nil || info.LastMsgAt > lastSeenAt(UnixMilli))
          dmUnread = unread && row.RoomType == dm
          mention  = hasMention
          track max(info.LastMsgAt) where Found
  3. aggregate: OR unread / dmUnread / mention across all rows;
     LastMessageAt = max lastMsgAt; failed sites → UnavailableSites
→ ThreadUnreadSummaryResponse
```

## No double-counting

The home replica holds exactly one thread-sub row per `(threadRoomId,
userAccount)` (unique index), each tagged with its room's home `siteId`. Grouping
by `siteId` routes each `threadRoomId` to the one site that authoritatively owns
its `thread_rooms`, so every thread is fetched and counted exactly once.

## Aggregation & per-row rules

| Field | Rule |
|-------|------|
| `unread` | OR of every row's `unread` |
| `unreadDirectMessage` | OR of every row's `dmUnread` |
| `unreadMention` | OR of every row's `hasMention` |
| `lastMessageAt` | `max` of every `Found` row's `lastMsgAt`; nil if none |
| `unavailableSites` | any site whose batch RPC failed (any chunk) |

Per row: `Found=false` contributes nothing. `lastSeenAt == nil` (never seen) ⇒
`unread` when `Found`. `dmUnread` uses the row's joined `roomType`.

## Components

### Additions

1. **`pkg/subject/subject.go`** — add `UserThreadUnreadSummary` / `UserThreadUnreadSummaryPattern`
   (client) and `ThreadRoomInfoBatch` / `ThreadRoomInfoBatchSubscribe` (server).
2. **`pkg/model`** — add `ThreadUnreadSummaryRequest`, `ThreadUnreadSummaryResponse`,
   `ThreadRoomInfoBatchRequest`, `ThreadRoomInfo`, `ThreadRoomInfoBatchResponse`,
   and the join-result struct `ThreadUnreadRow{ ThreadRoomID, SiteID string;
   RoomType model.RoomType; LastSeenAt *time.Time; HasMention bool }` (json/bson
   tags). Add all to `pkg/model/model_test.go` round-trip coverage.
3. **room-service** (`handler.go`, `store.go`, `store_mongo.go`):
   - `store.go` — add `GetThreadRoomInfos(ctx, threadRoomIDs []string)
     ([]ThreadRoomInfoRow, error)` to `RoomStore`, result struct
     `ThreadRoomInfoRow{ ThreadRoomID string; LastMsgAt time.Time }`. Regenerate
     `mock_store_test.go`.
   - `store_mongo.go` — implement with a single projected find:
     `thread_rooms {_id: {$in: ids}}` → `{_id, lastMsgAt}`. Reuses the existing
     `threadRooms` handle (added by the June-9 work, retained). No `rooms` join.
   - `handler.go` — `threadRoomInfoBatch(c, req model.ThreadRoomInfoBatchRequest)
     (*model.ThreadRoomInfoBatchResponse, error)`: reject empty / over
     `h.maxBatchSize` (`errcode.BadRequest`, mirroring `roomsInfoBatch`), 5s
     timeout, build one `ThreadRoomInfo` per requested id (`Found` + UnixMilli
     `LastMsgAt`), debug-log `site_id`/`batch_size`/`request_id`/`latency_ms`.
     Register via `natsrouter.Register(r,
     subject.ThreadRoomInfoBatchSubscribe(h.siteID), h.threadRoomInfoBatch)`.
4. **`user-service/roomclient/client.go`** — add `GetThreadRoomInfoBatch(ctx,
   siteID string, threadRoomIDs []string) ([]model.ThreadRoomInfo, error)` on
   `subject.ThreadRoomInfoBatch(siteID)`, mirroring `GetRoomsInfo` (relay remote
   envelopes via `errcode.Parse`).
5. **`user-service/service/service.go`** —
   - extend the `RoomClient` interface with `GetThreadRoomInfoBatch`;
   - add a `ThreadSubscriptionRepository` interface
     (`ListByAccount(ctx, account string) ([]model.ThreadUnreadRow, error)`) plus
     a `threadSubs` field on `UserService`, threaded through `New(...)`;
   - add `ThreadSubscriptionRepository` to the `//go:generate mockgen` directive;
   - register the handler: `natsrouter.Register(r,
     subject.UserThreadUnreadSummaryPattern(s.siteID), s.GetThreadUnreadSummary)`.
   - Regenerate `user-service/service/mocks`.
6. **`user-service/mongorepo/threadsubscriptions.go`** (new) —
   `ThreadSubscriptionRepo` over the local `thread_subscriptions` collection;
   `NewThreadSubscriptionRepo(db)`; `ListByAccount` is an aggregation: `$match
   {userAccount}` → `$sort {createdAt: -1}` → `$lookup subscriptions` (sub-pipeline
   matches `u.account` + `roomId` via `$expr`, gate `roomType != botDM OR
   isSubscribed == true`, project `roomType`) → `$match {sub != []}` → `$limit
   maxThreadSubscriptions` (500) → `$project {threadRoomId, siteId, lastSeenAt,
   hasMention, roomType: $arrayElemAt}`. The gate before the limit keeps a large
   history from forcing an unbounded read + fan-out while never letting an
   inaccessible/unsubscribed-app thread crowd out a live one. Adds an
   `EnsureIndexes` creating `{userAccount: 1, createdAt: -1}` (ensured
   independently of the collection's other owners) so the newest-N read is
   index-backed with an early limit, not a blocking in-memory sort. Wire into
   `user-service/main.go` (`NewThreadSubscriptionRepo(db)`, `EnsureIndexes`, the
   `_ service.ThreadSubscriptionRepository = (*mongorepo.ThreadSubscriptionRepo)(nil)`
   assertion, and `service.New(...)`).
7. **`user-service/service/threadunread.go`** (new) — `GetThreadUnreadSummary`
   handler: `ListByAccount` (error → `fmt.Errorf(...: %w)` ⇒ `internal`); group
   rows by `siteId`; `WaitGroup` fan-out (no semaphore) over each site's chunked
   `GetThreadRoomInfoBatch` calls with per-site degradation (no errgroup cancel,
   `c.Err()` checks); fold rows into the response via the `unread()` helper, taking
   the DM flag from each row's `roomType`.

### Removals (the superseded leaf — zero runtime callers)

8. Delete the `ThreadUnreadSummary` leaf and its now-dead support:
   - **room-service** — `threadUnreadSummary` handler + its `natsrouter.Register`
     line; `GetThreadUnreadSummary` store method + the `ThreadUnreadSummary`
     result struct; the related `handler_test.go` / `integration_test.go` cases;
     regenerate `mock_store_test.go`. Keep the `threadRooms` handle (reused by
     `GetThreadRoomInfos`). The June-9 `{userAccount, siteId}` index declaration
     may be dropped (a `{userAccount}` index already backs `ListByAccount`); this
     is optional cleanup and does not drop the physical index.
   - **`pkg/subject`** — remove `ThreadUnreadSummary` /
     `ThreadUnreadSummarySubscribe` + their `subject_test.go` cases.
   - **`pkg/model`** — remove `ThreadUnreadSummaryRequest` /
     `ThreadUnreadSummaryResponse` + their `model_test.go` cases.

## Edge cases

- No thread-subs → zero-value response, no RPCs.
- A site's batch RPC fails (any chunk) → that site's rows dropped, site in
  `unavailableSites`.
- All RPCs fail → all-false booleans **and** non-empty `unavailableSites`.
- `Found=false` → treated as no activity.
- `lastSeenAt == nil` → unread when `Found`.
- Thread whose room the account no longer subscribes to → dropped by the join
  (never reaches the fan-out).
- `botDM` thread whose app the account unsubscribed → dropped by the join.
- `ListByAccount` Mongo error → request fails with `internal`.
- Duplicate/blank ids deduped/dropped before the batch RPC.

## Testing (TDD, Red → Green)

- **room-service handler** (`handler_test.go`, mocked `RoomStore`): per-id
  `Found` + `lastMsgAt` (UnixMilli) mapping; empty ids → `BadRequest`; over-limit
  → `BadRequest`; missing thread room → `Found=false`; store-error path.
  Table-driven.
- **room-service store** (`integration_test.go`, `//go:build integration`): seed
  `thread_rooms`; assert `GetThreadRoomInfos` returns `lastMsgAt`, existing ids
  only.
- **roomclient** (`client_integration_test.go`): fake NATS responder for
  `ThreadRoomInfoBatch` → assert decode; error envelope → `errcode.Parse` relay.
- **user-service handler** (`threadunread_test.go`, mocked `RoomClient` +
  `ThreadSubscriptionRepository`, table-driven): multi-site OR/max; null
  `lastSeenAt` ⇒ unread; DM fold from the row's `roomType`; mention independent of
  unread; `Found=false` skipped; one site fails → `unavailableSites`; empty subs →
  zero, no RPC; chunking (>`maxBatchSize` threads on a site → multiple merged
  calls); `ListByAccount` error → internal.
- **user-service mongorepo** (`integration_test.go`): seed `thread_subscriptions`
  + `subscriptions` across sites → `ListByAccount` returns accessible rows with
  the joined `roomType`; membership gate drops threads with no subscription;
  `botDM` + `isSubscribed=false` dropped; empty account → empty slice.
- **model** (`pkg/model/model_test.go`): round-trip all new types; remove the
  deleted `ThreadUnreadSummary*` round-trip cases.

## Docs

- `docs/client-api.md`: add the client `thread.unread.summary` RPC (request: empty body;
  response fields with explicit types; success JSON example; error cases), beside
  the `thread.list` entry.
- `ThreadRoomInfoBatch` is `chat.server.*`, so no client-api entry.
