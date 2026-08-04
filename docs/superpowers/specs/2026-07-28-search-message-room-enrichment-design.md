# Search-Message Room & Sender Enrichment — Design

**Date:** 2026-07-28
**Service:** `search-service`
**Client-facing RPC:** `chat.user.{account}.request.search.{siteID}.messages`

## Problem

The message-search response (`model.SearchMessage`) is a raw projection of the
Elasticsearch `messages-*` index. Per its current design, "display enrichment
(user name, room name) is the client's responsibility — no Mongo enrichment
occurs server-side." Clients want each hit to arrive already carrying:

- a `room` object — `{ id, name, type, appInfo (botDM), hrInfo (dm) }`
- a `sender` object — `{ account, displayName }`
- the `tshow` flag (thread reply also shown in channel)
- `attachments` / `card` (already present today)

This spec adds that enrichment inside `search-service`, resolving names from the
local per-site MongoDB and room names for channels via a server↔server
`room.batch` NATS request.

## Data-locality facts this design relies on

- The searcher's own `subscriptions` doc is replicated to their site regardless
  of the room's origin site, and carries `RoomType` plus `Name` (the DM
  counterpart account / botDM bot account) — the join keys for enrichment.
- The `apps` and `users` collections are **global** (identical on every site,
  direct-transfer replicated), so `appInfo` and `hrInfo` / sender names resolve
  from the **local** Mongo on any site, even for a cross-site counterpart.
- A **cross-site channel's** canonical room name is **not** local — it lives on
  the room's origin site and is fetched via `RoomsInfoBatch`
  (`chat.server.request.room.{siteID}.info.batch`). DM/botDM names never need
  this (derived from counterpart/app).
- `tshow`, `attachments`, `card`, `userAccount`, `userId` are all already in the
  ES `MessageDoc`. `attachments`/`card` are already surfaced on `SearchMessage`;
  `tshow` and `userId` are indexed but currently discarded by the read path.

## Response shape (additive, backward-compatible)

`pkg/model/search.go`:

```go
type SearchMessage struct {
    // ... existing fields (messageId, roomId, siteId, userAccount, content,
    //     createdAt, editedAt, updatedAt, threadParentMessageId,
    //     threadParentMessageCreatedAt, attachments, card) ...
    TShow  bool          `json:"tshow,omitempty"`
    Room   *SearchRoom   `json:"room,omitempty"`
    Sender *SearchSender `json:"sender,omitempty"`
}

type SearchRoom struct {
    ID      string              `json:"id"`
    Name    string              `json:"name"`
    Type    RoomType            `json:"type"`               // channel | dm | botDM | discussion
    AppInfo *AppSubscription    `json:"appInfo,omitempty"`  // botDM only
    HRInfo  *SubscriptionHRInfo `json:"hrInfo,omitempty"`   // dm only
}

type SearchSender struct {
    Account     string `json:"account"`
    DisplayName string `json:"displayName"`
}
```

- `AppInfo` reuses the existing `model.AppSubscription`; `HRInfo` reuses the
  existing `model.SubscriptionHRInfo`, so clients parse the same shapes used by
  the subscription-list responses.
- All new fields are `omitempty` — existing clients are unaffected.

## Naming rules

- `sender.displayName`:
  - human sender → `displayfmt.CombineWithFallback(user.EngName, user.ChineseName, account)`
  - bot sender (`model.IsBot(account)`) → the app's display name (matches the
    rest of the system). Resolved from `apps` by `assistant.name == account`.
- `room.name`:
  - `dm` → counterpart display name via `CombineWithFallback(engName, chineseName, counterpartAccount)`
  - `botDM` → app display name
  - `channel` / `discussion` → `RoomInfo.Name` from `RoomsInfoBatch`
- `room.hrInfo` (dm only) → `SubscriptionHRInfo{account, name(=chineseName), engName}` for the counterpart.
- `room.appInfo` (botDM only) → `AppSubscription` built from the resolved `App`.

## Enrichment flow (`searchMessages` handler)

1. Run the ES query as today; decode `tshow` and `userId` in `messageSearchHit`.
2. Collect distinct `roomIds` and distinct sender `accounts` across the page.
3. **Local Mongo:** load the searcher's `subscriptions` for `(account, roomIds)`
   → `roomId → {RoomType, Name}`.
4. Partition rooms by `RoomType`: DM counterpart accounts, botDM bot accounts,
   channel/discussion roomIds.
5. **Local Mongo batch:**
   - `users` by the union of (DM counterpart accounts + human sender accounts)
     → engName/chineseName per account.
   - `apps` by the union of (botDM bot accounts + bot sender accounts)
     → `App` per bot account.
6. **`room.batch` RPC:** group channel/discussion roomIds by the hit's `siteId`,
   fan out `RoomsInfoBatch` per distinct site (bounded fan-out, mirroring
   `user-service`'s `enrichCrossSite`) → `roomId → name`.
7. Assemble `room`, `sender`, and `tshow` onto each `SearchMessage`.

## New wiring in `search-service`

- Extend the existing `MongoStore` (today `apps`-only) with read methods for
  `subscriptions` (by account + roomIds, projecting `roomId, type, name`) and
  `users` (by accounts, projecting `account, engName, chineseName`). Keep the
  precise-projection rule.
- Add an outbound `roomclient` for `RoomsInfoBatch`, mirroring
  `user-service/roomclient` (context-aware `nc.Request`, 5s timeout,
  `errcode.Parse` on the reply envelope). `search-service` currently makes no
  outbound NATS requests, so this is new client wiring in `main.go`.

## Error handling / degradation

Enrichment is best-effort and never fails the search:

- Missing subscription for a hit → `room.type`/appInfo/hrInfo omitted; still try
  `room.batch` for a name.
- `users`/`apps` miss → that name/hrInfo/appInfo omitted, fall back to account.
- A per-site `RoomsInfoBatch` failure → that site's channel names omitted;
  logged once, other sites unaffected (mirror `enrichCrossSite` degradation).

Follows the existing `lookupApps`/`lookupHRInfo` "degrade to nil" convention.

## Non-goals

- No index/schema change to ES or `search-sync-worker` (all needed fields are
  already indexed; `tshow`/`userId` are simply decoded).
- No change to `attachments`/`card` (already surfaced).
- No new persistence; enrichment is read-time only.
- `room.batch` is not extended to return type/appInfo/hrInfo — those stay
  subscription/collection-derived in `search-service`.

## Tradeoff (acknowledged)

This intentionally moves `search-service` off its "no server-side enrichment"
stance: each search page now issues local Mongo reads plus (for channel hits) a
`RoomsInfoBatch` RPC, adding latency and a room-service dependency to the search
path. Accepted to deliver the enriched `room`/`sender` in one client call.

## Docs to update in the same PR

- `docs/client-api.md` — `SearchMessage` response table + example (new
  `room`, `sender`, `tshow`; reference `AppSubscription`, `SubscriptionHRInfo`).
- `docs/client-api/request-reply.md` — derived view for the search RPC.

## Testing

- Unit (handler, mocked store + mocked room client, table-driven): channel / dm /
  botDM / discussion; bot sender vs human sender; missing subscription; missing
  user/app doc; cross-site channel needing `room.batch`; `room.batch` per-site
  failure; `tshow` true/false; message with attachments/card.
- TDD red→green; ≥80% coverage (target 90% for the enrichment logic).
