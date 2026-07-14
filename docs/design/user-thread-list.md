# Design: User Thread List (cross-site thread inbox)

**Status:** Proposed
**Scope:** New read RPC that returns a user's thread subscriptions across all
sites as a single, globally-ordered, paginated "thread inbox". Each item carries
the thread subscription, its **parent message**, and its **last message**.

---

## 1. Goal & decisions

A user follows threads that live in rooms across multiple federated sites. We
need one query that returns *all* of that user's thread subscriptions, newest
activity first, with enough context to render an inbox row (parent message +
last reply), paginated.

Locked decisions:

| Decision | Choice |
|---|---|
| Result shape | **Unified globally-sorted inbox** — one merged list ordered by last-activity DESC, every item tagged with `siteId`. |
| Filters | **None** — return the user's full thread-subscription list (no all/unread/mention/following variants for v1). |
| Placement | **user-service aggregator** — owns the `chat.user.{account}` entrypoint and the cross-site fan-out (mirrors `ListSubscriptions` / `enrichWithRoomInfo`); each site's **history-service** serves a per-site leaf RPC. |

This is distinct from the existing per-room `GetThreadParentMessages`
(`chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread.parent`), which
lists thread parents **within one room** on **one site**. The new RPC is
**user-global and cross-site**.

---

## 2. Topology

```
client
  │  chat.user.{account}.request.thread.list           (NEW, client-facing)
  ▼
user-service (caller's home site)                       ← aggregator
  │  1. fan-out set = own site + ALL_SITE_IDS (every federation site)
  │  2. fan out (bounded goroutines, like enrichWithRoomInfo)
  │
  ├── chat.server.request.thread.{siteA}.subscription.list   (NEW, server↔server)
  │     ▼  history-service @ siteA  → Mongo (thread_subs) + Cassandra (messages)
  ├── chat.server.request.thread.{siteB}.subscription.list
  │     ▼  history-service @ siteB
  └── … one leaf per candidate site
        │
        ▼
  3. k-way merge by (lastMsgAt, threadRoomId) DESC, take `limit`
  4. build composite cursor, return unified page
```

- **Client → user-service**: `chat.user.{account}.request.user.{siteID}.thread.list`,
  where `{siteID}` is the **caller's own home site** (the site that holds the
  user's federated subscriptions and runs the aggregator). This stays inside the
  existing `chat.user.{account}.request.user.{siteID}.>` family — same shape and
  auth template as `subscription.list` — so no new permission lane is needed.
  Client-facing → **must be documented in `docs/client-api.md`** (see §7).
- **user-service → history-service (per site)**:
  `chat.server.request.thread.{siteID}.subscription.list`. Server-to-server, so
  client JWTs cannot reach it (`chat.server.*`). Mirrors `RoomsInfoBatch`.

New `pkg/subject` builders:

```go
// client-facing (mirrors UserSubscriptionList / …Pattern)
func UserThreadList(account, siteID string) string  // chat.user.{account}.request.user.{siteID}.thread.list
func UserThreadListPattern(siteID string) string    // chat.user.{account}.request.user.{siteID}.thread.list

// server-to-server leaf (mirrors RoomsInfoBatch / …Subscribe)
func ThreadSubscriptionList(siteID string) string   // chat.server.request.thread.{siteID}.subscription.list
func ThreadSubscriptionListSubscribe(siteID string) string // same, for QueueSubscribe
```

> Note on the client subject: every other `chat.user.*` RPC bakes a `siteID`
> into the registration pattern because the handler lives on the site that owns
> the resource. Here the resource is the *user*, so the handler lives on the
> user's home site and registers with that site's literal id, e.g.
> `chat.user.*.request.thread.list` queue-subscribed by user-service. Keep the
> subject site-less for the client (the client always hits its own site); bake
> the site into user-service's subscription only if/when multiple user-service
> shards must be disambiguated. Match whatever `ListSubscriptions` does today.

---

## 3. Which sites to fan out to

The user's thread subscriptions live on **each thread's home site** (the site
that owns the room), and the home site can't reliably enumerate them from local
data — a user's subscriptions for a remote-site room may not be federated to the
home site. So rather than derive the set from local subscriptions, we **fan out
to every configured federation site**:

- The fan-out set is the caller's own site (always, served directly) plus every
  site in the `ALL_SITE_IDS` config (`s.allSiteIDs`), deduped and non-blank —
  the same mechanism `publishStatus` uses for cross-site status federation.
- The home site doesn't need to know in advance which sites hold the user's
  threads; it asks **every** site's history-service, and each one answers for
  its own threads (empty if none). The value-cursor merge unions the results.
- Trade-off: a constant fan-out width (number of federation sites) regardless of
  how many sites actually hold the user's threads — bounded by `maxSiteFanout`
  concurrency, and sites with nothing return an empty page cheaply.
- The local site is queried in-process (no RPC); remote sites get the leaf RPC.
- Bound concurrency with a semaphore (`maxSiteFanout`, currently 8 in
  `enrichWithRoomInfo`).

This reuses an established invariant and needs no new per-user site index.

---

## 4. Pagination — value-based composite cursor

True global pagination across N independent stores can't rely on opaque
per-store page tokens (they can't be globally merged or rewound). We use a
**value-based cursor on the global sort key**, which makes each page stateless
and tolerant of per-site failures.

**Global sort key:** `(lastMsgAt DESC, threadRoomId DESC)` — `threadRoomId` is
the stable tiebreaker for equal timestamps.

**Cursor:** base64(JSON) of the last emitted item's key:

```json
{ "lastMsgAt": 1746518100000, "threadRoomId": "01970a4f8c2d7c9aZZZ" }
```

**Algorithm per page request:**

1. user-service decodes the cursor (absent ⇒ first page).
2. It sends the **same** `(lastMsgAt, threadRoomId)` cursor to **every**
   candidate site, each with the page `limit`.
3. Each site returns up to `limit` of *its* threads strictly older than the
   cursor, already sorted DESC (its local query — see §5).
4. user-service does a k-way merge of all returned candidates, sorts by the
   global key, and emits the top `limit`.
5. `nextCursor` = the key of the **last emitted** item; `hasNext` = any site
   still had more *or* the merged candidate pool exceeded `limit`.

Why this shape:

- **Stateless / no per-site bookkeeping** — the cursor is a single global
  position, not a map of opaque tokens.
- **Failure-tolerant** — if a site is unavailable for one page, the value cursor
  stays valid; a later page can still include that site's threads once it
  recovers (results are eventually complete, never corrupted).
- **Over-fetch is bounded** — each site returns at most `limit`, so the merge
  pool is `≤ limit × N` and only `limit` is emitted.
- Defaults match the codebase: `limit` default 20, max 100.

Edge cases: equal `lastMsgAt` across sites is fully ordered by `threadRoomId`;
because IDs are unique, no item is skipped or duplicated at a page boundary.

### 4.1 Buffering bounds

The aggregator never holds more than **`limit × N_sites`** candidates at once,
and this bound is **independent of pagination depth**:

- Each leaf is contractually capped at `limit` items (`N+1` is fetched only to
  compute `HasMore`), so page transient = `limit × N_sites` (e.g. `limit=20`,
  5 sites → ~100 buffered, 20 emitted; `limit=100` max, 10 sites → ~1000
  buffered, 100 emitted). Freed after the merge.
- Because the cursor is value-based, page 50 issues the same "top `limit` after
  this position" request as page 1 — no site streams everything-up-to-the-cursor
  for the aggregator to scan-and-discard. Deep pages cost the same as the first.

The cost that *does* scale lives one layer down, at the leaf: the `$sort` on the
looked-up `tr.lastMsgAt` (no index can serve it — §5) is a **blocking in-memory
sort** of the user's matching thread-subs on that site. Normal users follow
tens–low-hundreds of threads per site (trivial); only a pathological
mega-follower (thousands+) risks Mongo's 100 MB blocking-sort cap. Keep
`allowDiskUse` **off** so that case errors cleanly rather than silently spilling.

### 4.2 Consistency under concurrent writes

`lastMsgAt` is the **mutable** sort key — a new reply bumps it — and it is
**monotonically non-decreasing** (activity only moves forward). Walking
descending against the value cursor gives:

| Event during a paginated walk | Result |
|---|---|
| Already-emitted thread gets a reply (above cursor) | Moves higher, stays above cursor → **no duplicate**. The already-shown row holds stale `lastMessage`/`lastMsgAt` until refresh. |
| Not-yet-paginated thread gets a reply (below cursor) | Jumps above the cursor → **skipped** for the rest of the walk ("missed item"). |
| Any normal reply | **No duplicates** — monotonic key + descending walk means an item only moves *away* from the cursor, never back below it. |

The "missed item" is **correct semantics for a recency feed**: a bumped thread
now belongs at the head — which the user already scrolled past — not at its old
lower position. The paginated walk is for **backfilling older** threads; the
**head is kept fresh by a live subscription** (existing
`chat.user.{account}.event…` / thread-update events), the standard
infinite-scroll-plus-live-head pattern. Federation adds no new anomaly class —
every site filters by the same global `(lastMsgAt, threadRoomId)` position, so
the behavior matches single-site recency pagination.

**Deletion edge case:** if a thread's last message is deleted and
`thread_rooms.LastMsgAt` is recomputed *backward* to the prior reply,
monotonicity breaks — an emitted thread can drop below the cursor and be
re-fetched as a **duplicate**. Mitigation: a session-level seen-set of
`threadRoomId` (aggregator- or client-side) makes the walk idempotent. Rare, but
worth handling.

---

## 5. Per-site leaf query (history-service)

The leaf must list the user's thread subscriptions on that site, sorted by the
thread's last-activity, after the cursor. The sort field (`lastMsgAt`) lives on
`thread_rooms`, while the user filter (`userAccount`) lives on
`thread_subscriptions`.

**Decision: drive from `thread_subscriptions` and `$lookup` `thread_rooms`.**
Denormalizing `lastMsgAt`/`lastMsgId` onto `thread_subscriptions` was rejected:
it would force a write fan-out across *every* subscriber of a thread on *every*
reply, which is too frequent and too wide a write amplification. Reads here are
far rarer than thread replies, so we pay the cost on the read path instead.

This is a sanctioned `$lookup` exception (the sibling `GetUnreadThreadRooms`
pipeline already `$lookup`s in the other direction and is grandfathered). Add the
required inline `// $lookup justification: …` comment per `CLAUDE.md`.

Aggregation pipeline on `thread_subscriptions`:

```
[
  { $match: { userAccount: U } },                       // index: {userAccount:1}
  { $lookup: { from: "thread_rooms",
               localField: "threadRoomId", foreignField: "_id",
               as: "tr",
               pipeline: [ { $project: { lastMsgAt:1, lastMsgId:1,
                                         parentMessageId:1, roomId:1, siteId:1,
                                         threadParentCreatedAt:1 } } ] } },
  { $unwind: "$tr" },
  { $match: {                                            // value cursor
      $or: [ { "tr.lastMsgAt": { $lt: C.ts } },
             { "tr.lastMsgAt": C.ts, threadRoomId: { $lt: C.id } } ] } },
  { $sort: { "tr.lastMsgAt": -1, threadRoomId: -1 } },
  { $limit: N + 1 }                                      // +1 probes HasMore
]
```

- The `$match` on `userAccount` is index-served (`thread_subscriptions` already
  indexes it); the `$lookup` resolves one `thread_rooms` doc per followed thread
  by `_id` (point lookups). The sort/cursor run on the post-lookup `tr.lastMsgAt`.
- The cost scales with the user's **thread-subscription count on that site**, not
  the collection — acceptable for the read frequency.
- Fetch `N + 1` to set `HasMore` without a second count query.

**Enrichment (per returned subscription):**
- **Messages** — collect the page's distinct `parentMessageId` ∪ `lastMsgId` and
  hydrate with a **single** batched `GetMessagesByIDs` against `messages_by_id`
  (already exists), local to the site. Attach `parentMessage`/`lastMessage`; the
  reply count rides on `parentMessage.TCount` (no separate item field). A thread
  is included only when **both** bodies hydrate — a row missing either (hard-
  deleted, or not yet replicated) is dropped rather than surfaced half-empty.
  (Edge case: a page whose rows are *all* unhydratable returns empty while
  `HasMore` is set; the cross-site aggregator then has no item to derive
  `NextCursor` from and reports `HasNext` without a cursor. Accepted given rarity
  — it needs a whole contiguous page of hard-deleted parents/last-messages — but
  if it ever bites, the leaf would need a fill/continuation step.)
- **Room meta** — `roomName`/`roomType` ride in on the rows via a second
  `$lookup` (on `rooms`) **placed after `$limit`** in the aggregation, so it
  enriches only the ≤`limit+1` page rows with indexed `_id` point reads. This is
  cheaper than a separate per-page round trip and, crucially, avoids bolting a
  non-cacheable `GetRoomsMeta` onto the cached `RoomRepository`. Missing room docs
  degrade to empty name/type (`preserveNullAndEmptyArrays`). DM display-name
  resolution (counterpart/app name, as `user-service.buildListItems` does for the
  subscription list) is **out of scope for v1** — surface the raw `rooms.name`;
  revisit if DM threads need a friendly name.
- **Membership filter (in-pipeline)** — the leaf keeps only threads whose room
  the user is still subscribed to, via a correlated `subscriptions` `$lookup`
  (`u.account` + `roomId`) with a `{$ne: []}` existence match. The room
  subscription is the source of truth for membership: it is purged when the user
  leaves the room, whereas the `thread_subscriptions` rows are not — so this join
  is what makes a departed member's threads disappear. It is **not** a
  `getAccessSince` call and applies **no** `historySharedSince` window — it is a
  pure still-a-member check. It is applied **first**, keyed on the
  `thread_subscriptions` row's own `roomId`, so the `thread_rooms`/`rooms` joins
  run only for threads the user can access; and because it runs **before**
  `$sort`/`$limit`, the page stays a **single keyset fetch** — no post-fetch
  drops, no fill loop — with `HasMore` straight from the repository's `limit+1`
  probe.

**Leaf request / response:**

```go
type ThreadSubscriptionListRequest struct {
    Account    string `json:"account"`              // server-derived, from the client subject
    LastMsgAt  *int64 `json:"lastMsgAt,omitempty"`   // cursor ts; nil = first page
    ThreadRoomID string `json:"threadRoomId,omitempty"` // cursor tiebreak
    Limit      int    `json:"limit"`
}

type ThreadSubscriptionListResponse struct {
    Items   []ThreadListItem `json:"items"`   // ≤ limit, sorted (lastMsgAt,threadRoomId) DESC
    HasMore bool             `json:"hasMore"` // this site has more beyond what it returned
}
```

---

## 6. Response model (`pkg/model`)

```go
type ThreadListItem struct {
    SiteID          string     `json:"siteId"          bson:"siteId"`
    RoomID          string     `json:"roomId"          bson:"roomId"`
    RoomName        string     `json:"roomName"        bson:"roomName"`
    RoomType        RoomType   `json:"roomType"        bson:"roomType"`
    ThreadRoomID    string     `json:"threadRoomId"    bson:"threadRoomId"`
    ParentMessageID string     `json:"parentMessageId" bson:"parentMessageId"`

    // subscription state (this user)
    LastSeenAt *int64 `json:"lastSeenAt,omitempty" bson:"lastSeenAt,omitempty"` // UTC ms
    HasMention bool   `json:"hasMention"           bson:"hasMention"`
    Unread     bool   `json:"unread"               bson:"unread"` // lastMsgAt > lastSeenAt

    // thread activity
    LastMsgAt int64 `json:"lastMsgAt" bson:"lastMsgAt"` // UTC ms — the global sort key

    // enriched bodies
    ParentMessage *Message `json:"parentMessage,omitempty" bson:"parentMessage,omitempty"`
    LastMessage   *Message `json:"lastMessage,omitempty"   bson:"lastMessage,omitempty"`
}
```

Field sources (all resolved on the **owning site**, none cross-site):
- `roomName` / `roomType` — the site's `rooms` collection, joined into the page
  via a post-`$limit` `$lookup` in the aggregation (§5).
- Reply count is **not** a separate field — it already rides on
  `parentMessage.TCount` (Cassandra `messages_by_id`); clients read it there.

Aggregated client response:

```go
type ThreadListResponse struct {
    Items            []ThreadListItem `json:"items"`
    NextCursor       string           `json:"nextCursor,omitempty"` // base64; omit on last page
    HasNext          bool             `json:"hasNext"`
    UnavailableSites []string         `json:"unavailableSites,omitempty"` // sites that failed this page
}
```

`Unread` is computed server-side (`lastMsgAt > lastSeenAt`, or `lastSeenAt ==
nil` ⇒ unread) so clients don't reimplement the comparison.

---

## 7. Cross-site failure handling

Follow `enrichWithRoomInfo`'s **graceful degradation**: a per-site RPC timeout or
error does not fail the whole request. The failed site's threads are simply
absent from this page, and its id is reported in `unavailableSites` so the client
can show a "some sites unavailable" hint and/or retry. Because the cursor is
value-based (§4), a subsequent page request re-queries every site from the same
global position — a recovered site rejoins seamlessly. Use the same per-RPC
timeout budget already used for room-info fan-out (5s) with a slightly larger
overall deadline.

Error envelope for the *aggregate* call (bad cursor, bad limit) uses `errcode`
constructors (`errcode.BadRequest`) and replies via the natsrouter adapter — no
manual logging. Leaf infra failures collapse to `internal` at the leaf boundary
and surface to the aggregator as a failed site, not a client error.

---

## 8. Documentation & tests (per CLAUDE.md)

- **`docs/client-api.md`**: add a "List User Threads" section under the
  `chat.user.` RPCs — request table (`cursor`, `limit`), success response table
  (`items` → `ThreadListItem`, `nextCursor`, `hasNext`, `unavailableSites`), a
  JSON example, error envelope ref, and "Triggered events: None — reply only".
  Add `ThreadListItem` to §3.0 shared schemas; reference `[Message](#message-schema)`.
- **`docs/cassandra_message_model.md`**: unchanged (reads only; no schema
  change). No Mongo schema change either — the leaf `$lookup`s `thread_rooms`
  rather than denormalizing.
- **TDD**:
  - user-service: handler tests for cursor decode/encode, k-way merge ordering,
    page-boundary tiebreak, partial-failure → `unavailableSites`, all sites empty → empty page.
    Inject the per-site client as an interface (`HistoryClient`) so the merge
    logic is tested with fakes — no real NATS.
  - history-service: leaf handler tests over the `$lookup` pipeline (cursor
    filter, limit/`N+1` HasMore, body enrichment incl. missing parent/room
    degradation) with mocked stores; an integration test for the aggregation
    against real Mongo and the `messages_by_id` batch enrichment, incl. the
    membership filter (threads in a room the user has left are excluded).
  - `pkg/model`: round-trip `ThreadListItem` / `ThreadListResponse` in
    `model_test.go`.
  - `pkg/subject`: tests for the new builders.

---

## 9. Build order (incremental, each independently shippable)

1. `pkg/subject` builders + `pkg/model` types (+ round-trip tests).
2. history-service leaf RPC `chat.server.request.thread.{siteID}.subscription.list`
   — `$lookup` pipeline (with justification comment) + membership filter +
   Cassandra enrichment.
3. user-service aggregator: site-set derivation, bounded fan-out, k-way merge,
   composite cursor, `chat.user.{account}.request.thread.list` handler.
4. `docs/client-api.md` update in the same PR as step 3 (client-facing handler).

---

## 10. Resolved decisions & remaining checks

Resolved:

- **Per-site query strategy** → `$lookup` `thread_rooms`, **no denormalization**
  (§5). Rationale: thread replies are far more frequent than thread-list reads;
  denormalizing would write-amplify across all subscribers on every reply. Pay
  the cost on the rare read path.
- **Fan-out site set** → **every configured federation site** (`ALL_SITE_IDS`),
  not derived from local subscriptions (§3). The home site can't reliably
  enumerate the user's remote sites from local data, so it asks all of them —
  each answers for its own threads. Mirrors `publishStatus`.

Remaining implementation checks (not blockers):

- **`MESSAGE_BUCKET_HOURS`**: enrichment uses `messages_by_id` (keyed by id, not
  bucketed), so no bucket-window dependency — confirm during implementation.
- **`thread_subscriptions` index**: confirm `{ userAccount: 1 }` exists (or the
  compound that fronts it) so the leaf's `$match` stays index-served.
