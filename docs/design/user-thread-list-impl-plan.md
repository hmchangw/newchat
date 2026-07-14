# Implementation Plan: User Thread List (cross-site thread inbox)

Companion to `docs/design/user-thread-list.md`. Step-by-step, TDD-first, each
phase independently shippable and committable. All commands go through `make`
(per CLAUDE.md). Every phase = **Red → Green → Refactor → Commit**.

Grounding (verified against the tree):
- `history-service/internal/models.Message` is a **type alias** to
  `pkg/model/cassandra.Message` → the message wire shape is already shared, so
  the aggregator can relay **fully-typed** items.
- Cross-site fan-out pattern to copy verbatim: `user-service/service/subscriptions.go`
  → `enrichCrossSite` (WaitGroup + `sem := make(chan struct{}, maxSiteFanout)`,
  per-site degradation, `c.Err()` cancellation checks).
- Per-site RPC client to copy: `user-service/roomclient/client.go` →
  `GetRoomsInfo` (Request + `errcode.Parse` relay).
- Leaf sibling to copy: `history-service/internal/service/threads.go` →
  `GetThreadParentMessages` (Mongo page → collect IDs → `GetMessagesByIDs` →
  hydrate → access-window filter → redact).
- Leaf Mongo query: a **new** `ThreadSubscriptionRepo` (own the
  `thread_subscriptions` collection + its index) — the query is driven from
  `thread_subscriptions`, so it gets its own repo rather than overloading
  `ThreadRoomRepo`. Copy the `$lookup` style from `pipelines.go`
  (`unreadThreadsPipeline`).
- Registration: `RegisterHandlers` in each service's `service.go`.

---

## Phase 1 — `pkg/subject` builders + `pkg/model` wire types

**Files:** `pkg/subject/subject.go` (+ `subject_test.go`),
`pkg/model/threadlist.go` (+ `pkg/model/model_test.go` round-trip).

**Red.** Add to `subject_test.go`: assert exact strings for the three builders.
Add to `model_test.go`: `roundTrip` cases for the new structs.

**Green.** Subject builders:

```go
// client-facing (place beside UserSubscriptionList / …Pattern)
func UserThreadList(account, siteID string) string {
    if !isValidAccountToken(account) { panic("invalid account token: contains NATS wildcard characters") }
    return fmt.Sprintf("chat.user.%s.request.user.%s.thread.list", account, siteID)
}
func UserThreadListPattern(siteID string) string {
    return fmt.Sprintf("chat.user.{account}.request.user.%s.thread.list", siteID)
}
// server-to-server leaf (place beside RoomsInfoBatch / …Subscribe)
func ThreadSubscriptionList(siteID string) string {
    return fmt.Sprintf("chat.server.request.thread.%s.subscription.list", siteID)
}
func ThreadSubscriptionListSubscribe(siteID string) string {
    return fmt.Sprintf("chat.server.request.thread.%s.subscription.list", siteID)
}
```

> The client subject sits under `request.user.{siteID}.>`, the same lane as
> `subscription.list`, so it inherits that auth template. Confirm
> `ParseUserSubject`'s area allow-list does not need `"thread"` (it only matters
> if a parser path consumes this subject; `natsrouter.Register` extraction does
> not use it).

`pkg/model/threadlist.go` (shared by both services + client wire):

```go
package model

import "github.com/hmchangw/chat/pkg/model/cassandra"

// ThreadListItem is one row of a user's cross-site thread inbox.
type ThreadListItem struct {
    SiteID          string   `json:"siteId"          bson:"siteId"`
    RoomID          string   `json:"roomId"          bson:"roomId"`
    RoomName        string   `json:"roomName"        bson:"roomName"`         // rooms collection
    RoomType        RoomType `json:"roomType"        bson:"roomType"`         // rooms collection
    ThreadRoomID    string   `json:"threadRoomId"    bson:"threadRoomId"`
    ParentMessageID string   `json:"parentMessageId" bson:"parentMessageId"`
    LastSeenAt      *int64   `json:"lastSeenAt,omitempty" bson:"lastSeenAt,omitempty"` // UTC ms
    HasMention      bool     `json:"hasMention"      bson:"hasMention"`
    Unread          bool     `json:"unread"          bson:"unread"`
    LastMsgAt       int64    `json:"lastMsgAt"       bson:"lastMsgAt"` // UTC ms — global sort key
    // reply count is not a field — clients read parentMessage.TCount
    ParentMessage   *cassandra.Message `json:"parentMessage,omitempty" bson:"parentMessage,omitempty"`
    LastMessage     *cassandra.Message `json:"lastMessage,omitempty"   bson:"lastMessage,omitempty"`
}

// Leaf (server↔server) wire.
type ThreadSubscriptionListRequest struct {
    Account            string `json:"account"`                      // server-derived by the aggregator
    CursorLastMsgAt    *int64 `json:"cursorLastMsgAt,omitempty"`    // nil = first page
    CursorThreadRoomID string `json:"cursorThreadRoomId,omitempty"`
    Limit              int    `json:"limit"`
}
type ThreadSubscriptionListResponse struct {
    Items   []ThreadListItem `json:"items"`   // ≤ limit, sorted (lastMsgAt,threadRoomId) DESC
    HasMore bool             `json:"hasMore"`
}

// Client-facing aggregator wire.
type ThreadListRequest struct {
    Cursor string `json:"cursor,omitempty"` // opaque base64; omit for first page
    Limit  int    `json:"limit,omitempty"`
}
type ThreadListResponse struct {
    Items            []ThreadListItem `json:"items"`
    NextCursor       string           `json:"nextCursor,omitempty"`
    HasNext          bool             `json:"hasNext"`
    UnavailableSites []string         `json:"unavailableSites,omitempty"`
}
```

> Verify `pkg/model` may import `pkg/model/cassandra` without a cycle (cassandra
> is a leaf package; the direction is natural). If a cycle exists, move
> `ThreadListItem` into `pkg/model/cassandra` instead — keep the wire field types
> identical.

**Verify.** `make test SERVICE=pkg/subject` (and the model package), `make lint`.
**Commit.** `feat(subject,model): thread-list subjects and wire types`.

---

## Phase 2 — history-service leaf: Mongo query

**Files:** new `history-service/internal/mongorepo/threadsubscription.go`
(+ `pipelines.go` for the pipeline builder), `threadsubscription_test.go` (unit,
pure pipeline assembly) + a `mongorepo` integration test.

**Red.** Integration test (`//go:build integration`, `testutil.MongoDB`):
seed `thread_subscriptions` + `thread_rooms` for an account across several
threads with distinct `lastMsgAt`; assert ordering DESC, the `N+1`/`HasMore`
probe, the value-cursor `$match` (page 2 excludes page-1 tail), and the
tiebreak on equal `lastMsgAt`.

**Green.** New repo `ThreadSubscriptionRepo` (owns the `thread_subscriptions`
collection; drives the query from it, `$lookup` `thread_rooms`):

```go
const threadSubscriptionsCollection = "thread_subscriptions"

type ThreadSubscriptionRepo struct {
    threadSubscriptions *mongo.Collection
}

func NewThreadSubscriptionRepo(db *mongo.Database) *ThreadSubscriptionRepo {
    return &ThreadSubscriptionRepo{threadSubscriptions: db.Collection(threadSubscriptionsCollection)}
}

// ThreadSubRow is the flat projection feeding the leaf handler's hydration.
type ThreadSubRow struct {
    ThreadRoomID    string     `bson:"_id"`            // == thread_subscriptions.threadRoomId
    RoomID          string     `bson:"roomId"`
    SiteID          string     `bson:"siteId"`
    ParentMessageID string     `bson:"parentMessageId"`
    LastMsgID       string     `bson:"lastMsgId"`
    LastMsgAt       time.Time  `bson:"lastMsgAt"`
    LastSeenAt      *time.Time `bson:"lastSeenAt"`
    HasMention      bool       `bson:"hasMention"`
}

// ListUserThreadSubscriptions returns the account's thread subs on this site,
// newest activity first, after the (lastMsgAt, threadRoomId) cursor. Fetches
// limit+1 to report hasMore.
func (r *ThreadSubscriptionRepo) ListUserThreadSubscriptions(
    ctx context.Context, account string, cursorLastMsgAt *time.Time,
    cursorThreadRoomID string, limit int,
) (rows []ThreadSubRow, hasMore bool, err error)
```

> **Index ownership moves here.** Relocate the `thread_subscriptions`
> `(threadRoomId, userAccount)` unique-index ensure out of
> `ThreadRoomRepo.EnsureIndexes` into a new `ThreadSubscriptionRepo.EnsureIndexes`,
> and add the `{userAccount: 1}` index this query needs. `ThreadRoomRepo` then
> drops its `thread_subscriptions` collection handle and only owns `thread_rooms`
> (it still `$lookup`s `thread_subscriptions` by name in `unreadThreadsPipeline` —
> that needs no Go handle). Wire `NewThreadSubscriptionRepo(...).EnsureIndexes` in
> `cmd/main.go` alongside the other repos.

Pipeline (`userThreadSubscriptionsPipeline`, in `pipelines.go`) — note the
required `// $lookup justification:` comment:

```
[
  { $match: { userAccount: account } },                 // index {userAccount:1}
  { $lookup: { from: "thread_rooms", localField: "threadRoomId",
               foreignField: "_id", as: "tr",
               pipeline: [ { $project: { lastMsgAt:1, lastMsgId:1,
                                         parentMessageId:1, roomId:1, siteId:1 } } ] } },
  { $unwind: "$tr" },
  // value cursor (omitted on first page):
  { $match: { $or: [ { "tr.lastMsgAt": { $lt: cursorTs } },
                     { "tr.lastMsgAt": cursorTs, threadRoomId: { $lt: cursorId } } ] } },
  { $sort: { "tr.lastMsgAt": -1, threadRoomId: -1 } },
  { $limit: limit + 1 },
  { $project: { _id: "$threadRoomId", roomId: "$tr.roomId", siteId: "$tr.siteId",
                parentMessageId: "$tr.parentMessageId", lastMsgId: "$tr.lastMsgId",
                lastMsgAt: "$tr.lastMsgAt", lastSeenAt: 1, hasMention: 1 } }
]
```

- Index: `ThreadSubscriptionRepo.EnsureIndexes` ensures `{userAccount: 1}` (this
  query) plus the relocated `(threadRoomId, userAccount)` unique index. Keep
  `allowDiskUse` **off** (default) so a pathological mega-follower errors cleanly
  rather than spilling (design §4.1).
- This is a plain aggregation (not `AggregatePaged`, which is offset/`$facet`);
  return the raw rows and compute `hasMore = len(rows) > limit` then trim.

**Verify.** `make test-integration SERVICE=history-service` (mongorepo), `make lint`.
**Commit.** `feat(history): ThreadSubscriptionRepo + cross-room thread list query`.

---

## Phase 3 — history-service leaf: handler + hydration + registration

**Files:** `history-service/internal/service/threads.go` (new handler),
`service.go` (interface + registration), `threads_test.go` (unit, mocked repos),
`service/integration_test.go`.

**Red.** `threads_test.go` table tests (mock `ThreadSubscriptionRepository` +
`MessageReader` + `RoomRepository`): empty result; single page < limit
(`HasMore=false`); full page (`HasMore=true`); `Unread` computed
(`lastMsgAt > lastSeenAt`, nil ⇒ unread); parent/last message hydrated and matched
by ID; `roomName`/`roomType` populated from room meta and degrade gracefully when
a room doc is missing; a missing hydrated message is skipped; access-window
exclusion (parent before `accessSince`).

**Green.** Add the interface in `service.go`:

```go
// ThreadSubscriptionRepository — NEW consumer interface (own seam, own mock);
// satisfied by *mongorepo.ThreadSubscriptionRepo.
type ThreadSubscriptionRepository interface {
    ListUserThreadSubscriptions(ctx context.Context, account string,
        cursorLastMsgAt *time.Time, cursorThreadRoomID string, limit int,
    ) ([]mongorepo.ThreadSubRow, bool, error)
}
```

`roomName`/`roomType` ride in on `ThreadSubRow` via the pipeline's post-`$limit`
`rooms` `$lookup` (Phase 2) — no `GetRoomsMeta` / `RoomRepository` change.

Wire it: add `ThreadSubscriptionRepository` to the `//go:generate mockgen` list
in `service.go`, add a `threadSubs ThreadSubscriptionRepository` field +
constructor param to `HistoryService.New`, pass it from `cmd/main.go`, and add a
compile-time check `var _ ThreadSubscriptionRepository = (*mongorepo.ThreadSubscriptionRepo)(nil)`.

Handler (mirror `GetThreadParentMessages`):

```go
// NATS: chat.server.request.thread.{siteID}.subscription.list  (server↔server)
func (s *HistoryService) ListThreadSubscriptions(
    c *natsrouter.Context, req pkgmodel.ThreadSubscriptionListRequest,
) (*pkgmodel.ThreadSubscriptionListResponse, error)
```

Steps: clamp `limit` (`defaultPageSize`/`maxPageSize` from `service/messages.go`);
convert `CursorLastMsgAt` ms→`time.Time`; call
`s.threadSubs.ListUserThreadSubscriptions(...)`; collect **distinct**
`parentMessageId` ∪ `lastMsgId` and **distinct** `roomId`; fire **one**
`GetMessagesByIDs`; build `map[id]Message`; assemble `[]ThreadListItem` in row
order — attach parent/last (reply count rides on `parent.TCount`), copy
`RoomName`/`RoomType` straight off the row, compute `Unread`, stamp
`SiteID`/`LastMsgAt` (ms). Apply
the access window per distinct `roomID` via the existing
`SubscriptionRepository.GetHistorySharedSince` (loop over the page's distinct
rooms — bounded by page size); drop items whose parent predates the window and
`redactUnavailableQuotes` the rest, exactly like the sibling. Reply with items +
`HasMore`.

Register (server-to-server, **not** the `{account}` router family — use a plain
queue-subscribe like room-service's batch RPC, or `natsrouter` with no `{account}`
param):

```go
natsrouter.Register(r, subject.ThreadSubscriptionListSubscribe(siteID), s.ListThreadSubscriptions)
```

Run `make generate SERVICE=history-service` (regenerate `mocks/mock_repository.go`
after the interface change) **before** tests.

**Verify.** `make generate SERVICE=history-service`, `make test SERVICE=history-service`,
`make test-integration SERVICE=history-service`, `make lint`.
**Commit.** `feat(history): thread-subscription list RPC (leaf)`.

---

## Phase 4 — user-service aggregator

**Files:** new `user-service/historyclient/client.go` (per-site history-service client),
`user-service/service/threads.go` (handler + merge + cursor),
`user-service/service/service.go` (interface + registration),
`threads_test.go`.

**Red.**
- `threads_test.go` (fake `HistoryClient`): k-way merge ordering across
  sites; page-boundary tiebreak on equal `lastMsgAt`; `limit` clamping; cursor
  encode/decode round-trip; **partial failure** → failed site in
  `UnavailableSites`, others still returned; all sites empty → empty page; bad
  cursor → `errcode.BadRequest`.
- merge logic factored into a pure function for exhaustive table tests.

**Green.**

1. **Per-site client** `historyclient` (copy `roomclient`):

```go
type Client struct { nc *otelnats.Conn }
func (c *Client) GetThreadList(ctx context.Context, siteID string,
    req model.ThreadSubscriptionListRequest) (model.ThreadSubscriptionListResponse, error)
// Request(subject.ThreadSubscriptionList(siteID), …, 5s) → errcode.Parse relay → decode
```

2. **Interface + DI** in `service.go`:

```go
type HistoryClient interface {
    GetThreadList(ctx context.Context, siteID string,
        req model.ThreadSubscriptionListRequest) (model.ThreadSubscriptionListResponse, error)
}
// add `history HistoryClient` to UserService + New(); add to //go:generate list
```

   The §3 fan-out set is `s.siteID` + `cfg.AllSiteIDs` (deduped, non-blank) — no
   subscription query; mirrors `publishStatus`.

3. **Handler** `service/threads.go`:

```go
func (s *UserService) ListUserThreads(c *natsrouter.Context, req models.ThreadListRequest) (*model.ThreadListResponse, error)
```

   - clamp limit (mirror history's 20/100);
   - decode composite cursor `{lastMsgAt, threadRoomId}` (base64 JSON) →
     `errcode.BadRequest` on malformed;
   - `sites := s.threadFanoutSites()` (own site + `ALL_SITE_IDS`); empty ⇒ empty response;
   - fan out with the **`enrichCrossSite` skeleton** (WaitGroup + `sem` =
     `maxSiteFanout`, `c.Err()` checks, per-site degrade → append to
     `unavailableSites`), sending each site the **same** cursor + `limit`;
   - k-way merge all returned items by `(lastMsgAt DESC, threadRoomId DESC)`,
     take top `limit`;
   - `hasNext` = (merged pool > limit) OR any site `HasMore`;
   - `nextCursor` = encode last emitted item's `(lastMsgAt, threadRoomId)`;
   - **session dedup** (design §4.2 deletion edge): drop duplicate
     `threadRoomId` while merging.

4. **Register**:

```go
natsrouter.Register(r, subject.UserThreadListPattern(s.siteID), s.ListUserThreads)
```

Run `make generate SERVICE=user-service` after the interface change.

**Verify.** `make generate SERVICE=user-service`, `make test SERVICE=user-service`,
`make test-integration SERVICE=user-service`, `make lint`.
**Commit.** `feat(user): cross-site thread-list aggregator RPC`.

---

## Phase 5 — wiring, docs, SAST

**Files:** `user-service/main.go` (construct `historyclient`, pass to `New`),
`history-service/cmd/main.go` (no new deps — handler uses existing repos),
`docs/client-api.md`.

- **history-service `cmd/main.go`**: construct `NewThreadSubscriptionRepo(db)`,
  pass it to `service.New`, and call its `EnsureIndexes` at startup (the
  relocated unique index + the new `{userAccount:1}` index).
- **user-service `main.go`**: build `historyclient.New(nc)` and inject into
  `service.New`. Confirm `EnsureIndexes` runs for the new repos at startup.
- **`docs/client-api.md`** (same PR — client-facing handler): add **"List User
  Threads"** under the `chat.user.` RPCs. Subject
  `chat.user.{account}.request.user.{siteID}.thread.list`; request table
  (`cursor`, `limit`); success table (`items` → `ThreadListItem`, `nextCursor`,
  `hasNext`, `unavailableSites`) + JSON example; error envelope ref; "Triggered
  events: None — reply only". Add `ThreadListItem` to §3.0 shared schemas,
  referencing `[Message](#message-schema)`.
- **SAST**: `make sast` (gosec/govulncheck/semgrep) before push.
- **Reviews cleanup**: if `branch_review` was run, delete `docs/reviews/*` before
  the PR (per CLAUDE.md). The design+plan docs under `docs/design/` stay.

**Verify.** `make test`, `make test-integration`, `make lint`, `make sast`.
**Commit.** `feat: wire thread-list RPC + client-api docs`. Push to
`claude/stoic-mayer-djo17o` (updates PR #371).

---

## Cross-cutting checklist

- [ ] `pkg/model` → `pkg/model/cassandra` import is cycle-free (else relocate `ThreadListItem`).
- [ ] `allowDiskUse` stays **off** on the leaf aggregation (design §4.1).
- [ ] Per-site RPC timeout 5s; overall aggregator deadline slightly larger; `maxSiteFanout=8`.
- [ ] Session dedup on `threadRoomId` (deletion edge, §4.2).
- [ ] `Unread` computed server-side; `LastMsgAt`/`LastSeenAt` emitted as UTC ms.
- [ ] Access window enforced per distinct room via `GetHistorySharedSince`.
- [ ] `make generate` run for **both** services after interface edits.
- [ ] 80% coverage floor on new code; error paths + boundaries covered.
- [ ] No client subject leaks into `chat.server.*`; leaf stays server-to-server.

## Risks / decisions deferred to implementation

- **Access-window cost**: looping `GetHistorySharedSince` over distinct rooms per
  page is fine at `limit ≤ 100`; if a page spans many rooms, add a batch
  `GetHistorySharedSinceMany`. Decide when wiring Phase 3.
- **Leaf blocking sort**: acceptable for normal users; watch for the mega-follower
  (design §4.1). No mitigation needed for v1.
- **Live head**: this RPC is backfill-only; keeping the inbox head fresh via
  existing `chat.user.{account}.event…` events is a separate client concern
  (design §4.2), out of scope here.
