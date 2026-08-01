# Remove `Subscription.ThreadUnread` (vestigial write paths + wire field)

**Date:** 2026-08-01
**Status:** Approved

## Background

`Subscription.ThreadUnread []string` (an array of unread thread parent-message IDs,
inherited from the legacy stack's `tunread[]`) was meant to be grown by the message
pipeline on every thread reply. That write path was never built — PR #245's design
(2026-05-28) recorded it as a known gap, and every later consumer was deliberately
moved to the derived model instead:

- The per-room unread-thread list drives from a `thread_subscriptions` join
  (`unreadThreadsPipeline`, 2026-04-23 redesign) — the doc explicitly marks
  `Subscription.ThreadUnread` as superseded.
- The global badge (`thread.unread.summary`, 2026-07-01) and the thread-aware
  `subscription.count` (2026-07-16) both derive unread from
  `thread_subscriptions.lastSeenAt` vs `thread_rooms.lastMsgAt`, the latter stating
  outright: "no reliance on the un-grown `subscription.threadUnread` array".

The result: no code path ever adds to the array, so it is permanently empty. Every
remaining touchpoint — two room-service store methods that shrink/clear it, the
`ThreadReadEvent` fields that federate the shrink, the inbox-worker writes that apply
it, the user-service projection that serializes it, and the client-api/frontend type
that declare it — operates on empty data. The frontend declares the field and never
reads it.

## Goal

Delete the field and all plumbing that maintains it. Thread-unread stays fully
derived. No client-observable behavior changes except one accepted change to the
room-level `alert` flag (below).

## Decisions

1. **Full removal in one PR** (model field, write paths, wire fields, projection,
   docs, frontend type). No soft-deprecation period.
2. **Thread reads stop touching room-level `alert`.** Today `message.thread.read`
   and `thread.read.all` set `alert=false` as a side effect of the empty-array
   rule; after removal only `message.read` (reading the room) clears `alert`.
   Accepted: room-level alert should reflect room-level reads. Legacy rows with
   `alert=true` (CDC-imported) still clear on the user's next room read.
3. **Clean-cut wire change** on `ThreadReadEvent` (drop `NewThreadUnread` +
   `Alert`) rather than keeping dead fields for a release cycle. Mixed-version
   federation is safe in both directions:
   - New inbox-worker + old event: unknown JSON fields are ignored.
   - Old inbox-worker + new event: decodes `NewThreadUnread=nil, Alert=false` and
     performs today's writes ($unset an already-absent field, clear alert) —
     identical to current behavior.
4. **No Mongo data migration.** Leftover `threadUnread` bson fields on old
   documents are inert: nothing projects or decodes them, and the driver ignores
   unknown fields. Not worth an ops script.

## Changes by component

### 1. `pkg/model`

- `subscription.go`: delete the `ThreadUnread` field from `Subscription`.
- `event.go`: `ThreadReadEvent` drops `NewThreadUnread` and `Alert`. Doc comment
  updated: the event advances the home-replica `ThreadSubscription` read state
  (lastSeenAt, hasMention) only.
- `model_test.go`: drop the "nil/empty ThreadUnread must be omitted" assertion and
  any fixtures seeding the field.
- **Untouched:** `ThreadUnreadRow`, `ThreadUnreadSummaryRequest/Response`,
  `ThreadReadAllRequest/Response`, `RoomThreadReadAll*` — live derived-badge
  machinery; only the name overlaps.

### 2. `room-service`

- `store.go`: remove `UpdateSubscriptionThreadRead` and
  `ClearSubscriptionThreadUnreadForAccount` from the Store interface.
- `store_mongo.go`: delete both implementations; remove `threadUnread` from
  `subscriptionReadProjection`; `UpdateSubscriptionRead` loses its `alert`
  parameter and hard-codes `alert: false` in the `$set` (reading a room always
  clears the alert).
- `handler.go`:
  - `handleMessageThreadRead`: drop the `UpdateSubscriptionThreadRead` errgroup
    leg. The `UpdateThreadSubscriptionRead` leg, the not-following no-op, the
    roomId defensive filter, the thread-floor recompute, and the federation gate
    all stay. Build the slimmed `ThreadReadEvent`.
  - `messageRead`: delete the `newAlert := sub.Alert && len(sub.ThreadUnread) > 0`
    computation (constant false); call the simplified `UpdateSubscriptionRead`.
    The federated `SubscriptionReadEvent` keeps its `Alert` field (data-migration
    CDC publishes real values through it); room-service always sends `false`.
  - `clearAllThreadRead`: drop the `ClearSubscriptionThreadUnreadForAccount`
    errgroup leg; `ClearThreadSubscriptionsForAccount` and the
    `thread_read_all` federation stay.
- Tests: `TestHandler_MessageRead_AlertStaysTrueWithThreadUnread` becomes
  "message.read always clears alert"; delete the store integration tests for the
  two removed methods; update seeds/assertions that reference `ThreadUnread`;
  `make generate` for mocks.

### 3. `inbox-worker`

- Store interface (`handler.go`): `ApplyThreadRead(ctx, threadRoomID, account,
  lastSeenAt)` — loses `roomID`, `newThreadUnread`, `alert`. `ApplyThreadReadAll`
  keeps its signature.
- `main.go`:
  - `ApplyThreadRead`: keep the `$lt`-guarded thread-subscription update
    (lastSeenAt, updatedAt, hasMention=false); delete the subscription write.
  - `ApplyThreadReadAll`: keep the guarded thread-sub bulk advance; delete the
    `subscriptions` UpdateMany.
- `handleThreadRead` passes the slimmed args; old in-flight events decode safely.
- Tests: update stub/mock signatures, drop subscription-side assertions, update
  integration tests; `make generate`.

### 4. `user-service`

- `mongorepo/subscriptions.go`: remove `"threadUnread": 1` from the
  subscription-list projection.
- Update test fixtures that seed or assert the field
  (`service/subscriptions_test.go`, `roomclient/client_integration_test.go`,
  `mongorepo/threadsubscriptions_test.go` as applicable).

### 5. `chat-frontend`

- `src/api/types.ts`: remove `threadUnread?: string[]` (declared, never read).

### 6. Docs

- `docs/client-api.md`:
  - Remove the `threadUnread` row from the Subscription field table.
  - Mark Read behaviour note: replace the alert-recomputation formula with
    "reading the room clears the alert flag".
  - `message.thread.read` behaviour notes: remove the subscription-write and
    alert-recomputation prose; the RPC updates thread-subscription read state
    only.
  - `thread.read.all` section: same trim for the subscription-side clear.
  - Sweep JSON examples for `threadUnread`.
- Derived views (`docs/client-api/request-reply.md`, `events.md`): verified — no
  `threadUnread` references; re-check during implementation.

## Error handling

Unchanged. All touched handlers keep their existing errcode/errnats patterns; the
removed store calls take their error branches with them.

## Testing

TDD per CLAUDE.md: where behavior changes (message.read alert clear, slimmed
ApplyThreadRead), adjust tests red-first, then implementation. Where code is purely
deleted, delete its tests in the same commit. Full gates: `make generate`,
`make lint`, `make test`, integration tests for room-service / inbox-worker /
user-service, `make sast` before push. Coverage must stay ≥80% on touched packages.

## Out of scope

- Any write-side thread-unread tracking (rejected in analysis; the derived model
  stays).
- Reworking `alert` semantics beyond the accepted change (who sets it true remains
  CDC-only).
- Removing the `Alert` field from `SubscriptionReadEvent` (still carries real CDC
  values).
- Mongo cleanup of legacy-seeded `threadUnread` / stale `alert` values.
