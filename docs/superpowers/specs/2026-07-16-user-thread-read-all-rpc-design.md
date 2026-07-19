# Clear All Thread Unread RPC — Design

**Date:** 2026-07-16
**Status:** Approved (brainstorming)
**Branch:** `claude/thread-unread-status-rpc-61imdi`

## 1. Summary

Add a client-facing RPC that clears the unread status of **all** of a user's
threads across **all sites** in a single call — the server side of a "mark all
threads read" button. It complements the existing per-thread
[Mark Thread as Read](../../client-api.md) RPC (`message.thread.read`, one thread
in one room) and the read-only aggregators `thread.list` and
`thread.unread.summary`.

The operation clears **only the requesting user's own read state** — it
deliberately does **not** advance thread-room read floors or emit
`thread_message_read` receipt events (see §7 for rationale).

## 2. Goals / Non-goals

**Goals**

- One RPC clears every thread-unread indicator the user has, cross-site.
- Reuse the established `user-service` → per-site `room-service` fan-out pattern
  (`thread.list`, `thread.unread.summary`).
- Preserve the write-ownership boundary: `room-service` owns the authoritative
  `Subscription` / `ThreadSubscription` writes; replicas converge via the
  existing `inbox-worker` federation path.
- Degrade gracefully: a down owning site does not fail the whole call.

**Non-goals**

- No change to read-receipt semantics for *other* participants (no floor
  recompute, no `thread_message_read` fan-out).
- No new "mark all read" for top-level room messages (this is threads only).
- No batching optimization of the federation path in v1 (see §7, "Rejected
  alternatives").

## 3. RPC surface

### 3.1 Client-facing RPC (user-service)

- **Subject:** `chat.user.{account}.request.user.{siteID}.thread.read.all`
  - `{siteID}` is the **caller's own home site** — the site holding the user's
    federated thread-subscription replicas and running the aggregator.
- **Reply subject:** auto-generated `_INBOX.>` (NATS request/reply).
- **Request body:** empty object.

  ```json
  {}
  ```

  `model.ThreadReadAllRequest struct{}` — account rides the subject, no body
  fields (mirrors `ThreadUnreadSummaryRequest`).

- **Success response:** `model.ThreadReadAllResponse`

  | Field | Type | Notes |
  |---|---|---|
  | `clearedThreads` | int | Total thread-subscriptions cleared across all sites. `0` when nothing was unread. |
  | `unavailableSites` | string[] | Optional (`omitempty`). Owning sites whose bulk-clear RPC failed; their threads may remain unread. The call still succeeds. |

  ```json
  { "clearedThreads": 7 }
  ```

- **New `pkg/subject` builders** (mirror `UserThreadUnreadSummary`):
  - `UserThreadReadAll(account, siteID) string` → `chat.user.%s.request.user.%s.thread.read.all` (with `isValidAccountToken` guard).
  - `UserThreadReadAllPattern(siteID) string` → `chat.user.{account}.request.user.%s.thread.read.all` for `natsrouter` registration.

### 3.2 Internal RPC (user-service → room-service)

- **Subject:** `chat.server.request.room.{siteID}.thread.read.all` (site-scoped;
  mirrors `ThreadRoomInfoBatch` = `chat.server.request.room.%s.thread.info.batch`).
- **Queue group:** `room-service`.
- **Request body:** `model.RoomThreadReadAllRequest{ Account string }` — account
  in the body (this is a server-to-server call, not user-subject-scoped).
- **Response:** `model.RoomThreadReadAllResponse{ ClearedThreads int }`.
- **New `pkg/subject` builders:** `RoomThreadReadAll(siteID) string` +
  `RoomThreadReadAllSubscribe(siteID) string` (both return the same concrete
  subject, matching the `RoomsInfoBatch` / `RoomsInfoBatchSubscribe` pair).

## 4. Data flow

```
client
  │  chat.user.{account}.request.user.{home}.thread.read.all  {}
  ▼
user-service (home site) — ClearAllThreadUnread handler
  │  1. threadSubs.ListByAccount(account)          (home replica read)
  │  2. group threadRoomIds by owning SiteID
  │  3. fan out concurrently (bounded by maxSiteFanout):
  │        for each owning site → RoomClient.ClearAllThreadUnread(site, account)
  │  4. sum clearedThreads; collect failed sites → unavailableSites
  ▼
room-service (each owning site) — clearAllThreadRead handler
  │  a. concurrent bulk writes (errgroup):
  │       - ClearThreadSubscriptionsForAccount(account, now)
  │       - ClearSubscriptionThreadUnreadForAccount(account)
  │  b. if user home site != this site:
  │       federate one InboxThreadRead event per cleared *remote* thread
  │       (existing event, existing inbox-worker apply, $lt guard)
  │  c. (skipped) recomputeThreadFloor / thread_message_read fan-out
  ▼
outbox-worker → inbox-worker (home site) — replica converges
```

Empty thread-sub set at step 1 short-circuits to `{clearedThreads:0}` with no
fan-out. The fan-out uses the same `maxSiteFanout` semaphore and
per-site-degrade discipline as `getThreadLists` / `GetThreadUnreadSummary`; a
failed site is recorded in `unavailableSites` and never cancels its siblings.

## 5. Component changes

### 5.1 `pkg/model`

- `ThreadReadAllRequest struct{}` (`pkg/model/threadsubscription.go`).
- `ThreadReadAllResponse{ ClearedThreads int json:"clearedThreads"; UnavailableSites []string json:"unavailableSites,omitempty" }`.
- `RoomThreadReadAllRequest{ Account string json:"account" }`.
- `RoomThreadReadAllResponse{ ClearedThreads int json:"clearedThreads" }`.
- No change to `ThreadReadEvent` — the federation payload is reused verbatim.

### 5.2 `pkg/subject`

- `UserThreadReadAll` / `UserThreadReadAllPattern` (client-facing).
- `RoomThreadReadAll` / `RoomThreadReadAllSubscribe` (internal).
- Tests for each in `subject_test.go`.

### 5.3 `user-service`

- New handler `ClearAllThreadUnread(c *natsrouter.Context, _ model.ThreadReadAllRequest) (*model.ThreadReadAllResponse, error)` in `user-service/service/threadunread.go` (alongside `GetThreadUnreadSummary`; shares the grouping/fan-out helpers).
- Register `subject.UserThreadReadAllPattern(siteID)` in `service.go`'s router wiring.
- Extend the `RoomClient` interface with
  `ClearAllThreadUnread(ctx context.Context, siteID, account string) (int, error)`,
  implemented in `user-service/roomclient/client.go` (marshal
  `RoomThreadReadAllRequest`, `nc.Request` to `subject.RoomThreadReadAll(siteID)`,
  relay remote error envelopes via `errcode.Parse`, decode `clearedThreads`).
- Regenerate the `RoomClient` mock (`make generate SERVICE=user-service`).

### 5.4 `room-service`

- New handler `clearAllThreadRead(c *natsrouter.Context, req model.RoomThreadReadAllRequest) (*model.RoomThreadReadAllResponse, error)` in `handler.go`; register `subject.RoomThreadReadAllSubscribe(h.siteID)` in `routes.go`/`registerHandlers`.
- New store methods on the `Store` interface (`store.go`) + Mongo impl
  (`store_mongo.go`):
  - `ClearThreadSubscriptionsForAccount(ctx, account string, now time.Time) ([]model.ThreadSubscription, error)` — set `lastSeenAt=now`, `updatedAt=now`, `hasMention=false` for every thread-sub of `account` on this site; returns the affected rows (needed for federation `threadRoomId`/`roomId`/`parentMessageId` and the cleared count).
  - `ClearSubscriptionThreadUnreadForAccount(ctx, account string) error` — set `threadUnread=[]` and `alert=false` (per `alert = alert && len(threadUnread)>0`) for every subscription of `account` on this site that currently has a non-empty `threadUnread`.
- Federation loop reuses `federateOne(ctx, roomID, userSiteID, model.InboxThreadRead, payload, dedupID, ts)`, one call per cleared thread whose `roomID` is owned remotely relative to the user's home site (`GetUserSiteID`). `dedupID = threadRoomID + ":" + account` (identical key shape to the single-thread path, so a redelivery collapses).
- Regenerate mocks (`make generate SERVICE=room-service`).

### 5.5 No changes required

- `outbox-worker` / `inbox-worker`: the reused `InboxThreadRead` event already
  has a consumer lane (`ConcurrentEventTypes`) and an idempotent, `$lt`-guarded
  apply. **No new event type, no partition change.**
- `pkg/stream`, stream bootstrap: untouched.

## 6. Semantics & edge cases

- **Read position:** each cleared thread-sub gets `lastSeenAt = now` (server
  UTC), matching the single-thread RPC. A blanket dismiss can therefore mark a
  thread read past a reply that lands mid-operation — accepted, standard
  "mark all read" behavior.
- **Idempotent:** a second call finds nothing unread, clears nothing, returns
  `clearedThreads:0`. Re-clearing an already-read thread is a harmless
  `lastSeenAt` advance.
- **Alert flag:** `ClearSubscriptionThreadUnreadForAccount` sets `alert=false`
  only via the existing thread-unread formula. Implementation note for the
  plan: confirm `alert` has no non-thread source that this would wrongly clear;
  the single-thread path already assumes `alert` is thread-mention-driven.
- **Partial failure:** an owning site's RPC error → that site in
  `unavailableSites`; other sites still clear; the overall call returns success.
- **No self on empty:** zero thread-subs → immediate `{clearedThreads:0}`, no
  RPCs issued.

## 7. Decisions & rejected alternatives

- **Clear own state only (chosen).** A "mark all read" is a bulk *dismiss*;
  the user is clearing a badge, not asserting they read each thread. The thread
  read floor / `thread_message_read` event drives read-receipts shown to
  *senders*; advancing them from a blanket dismiss would misreport reads.
  Skipping the recompute also removes N floor recomputes + N receipt events per
  press. The floor stays stale in the safe (older) direction and self-heals via
  the existing `recomputeThreadFloor` on the next real read/reply.
  - *Rejected:* full parity (recompute floor + emit `thread_message_read` per
    thread) — heavier and semantically wrong for a dismiss.
- **Orchestrator + per-site bulk handler (chosen, Approach A).** Keeps
  authoritative writes in `room-service`; reuses fan-out + federation; scales
  as O(sites) RPCs, not O(threads).
  - *Rejected:* user-service writing home replicas directly + background
    authoritative sync (splits write ownership, divergence risk); fanning out
    over the existing single-thread RPC (O(threads) RPCs and re-triggers the
    side effects we chose to skip).
- **Reuse per-thread `thread_read` federation (chosen).** Zero new event types
  and zero `inbox-worker` changes; each remote-owned cleared thread emits the
  existing `InboxThreadRead` event under its existing `$lt` guard. Cost is N
  federation events for a cross-site user with N remote threads — acceptable for
  a rare, durable, async operation.
  - *Rejected for v1:* a batched `thread_read_all` inbox event. Cuts event
    volume to O(remote-sites) but adds a new `InboxEventType`, a
    `ConcurrentEventTypes` entry, a new payload struct, and a new `inbox-worker`
    apply handler. Revisit only if outbox volume proves a problem.

## 8. Error handling

- Handlers return typed `*errcode.Error` (Tier 1) and let `natsrouter` /
  `errnats.Reply` marshal the envelope.
- Local thread-sub read failure in `user-service` → `internal` (raw wrapped
  error collapses at the boundary). Per-site RPC failures are **not** errors —
  they degrade into `unavailableSites`.
- `room-service` bulk write failure → wrapped `fmt.Errorf(...: %w)` → `internal`
  at its boundary → relayed to `user-service`, which records the site as
  unavailable.
- `roomclient` relays remote error envelopes via `errcode.Parse`.

## 9. Documentation

- Add **"Clear All Thread Unread"** section to `docs/client-api.md` (request
  body, response field table, JSON example, error table, "no triggered events"
  note) and add it to the RPC index table.
- Regenerate/update the derived view `docs/client-api/request-reply.md` in the
  same PR (client-facing RPC — mandatory per CLAUDE.md). No `events.md` change
  (no client-visible event added).

## 10. Testing (TDD, ≥80% coverage)

- **`pkg/subject`:** builder + pattern tests for the four new subjects.
- **`user-service` (`threadunread_test.go`):** table-driven handler tests with a
  mocked `RoomClient` — multi-site fan-out and summed `clearedThreads`, grouping
  by owning site, empty thread-sub set (no RPCs), one site failing →
  `unavailableSites` while others clear, all sites failing.
- **`room-service` (`handler_test.go`):** bulk-clear happy path (both store
  bulk methods invoked, count returned); federation emitted **only** for
  remote-owned threads and **not** when the user is home-local; store error →
  `internal`; mocked store + injected publish capture (no real NATS).
- **Store integration (`//go:build integration`, testcontainers):**
  `ClearThreadSubscriptionsForAccount` and
  `ClearSubscriptionThreadUnreadForAccount` against Mongo — clears the account's
  rows, sets the expected fields, leaves other accounts untouched, returns the
  right affected set/count.
- Run `make generate` (mocks) before tests; `make lint`, `make test`, and
  `make sast` before pushing.
