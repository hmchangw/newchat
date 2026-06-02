# Pin / Unpin Message History — Design

**Date:** 2026-05-19
**Branch:** `claude/pin-unpin-history-k8p88`
**Status:** Approved (brainstorming) — pending implementation plan

## Summary

Add the ability to **pin** and **unpin** a message in a room, and to **list a
room's pinned messages**. Pin state is stored in Cassandra
(`messages_by_id.pinned_at/pinned_by` plus a denormalized copy in
`pinned_messages_by_room`). Authorization is governed by a global Mongo
kill-switch, room subscription, and a large-room role override that mirrors
`message-gatekeeper`'s post-restriction. Pin/unpin emit new best-effort
canonical events so downstream services (broadcast-worker, search-sync-worker,
room-service) can react in later phases.

This is the **first writer** of `pinned_messages_by_room` and of the
`messages_by_id` pin columns — today only the edit/delete *mirror* code
(`history-service/internal/cassrepo/write.go`) touches them, and it only fires
when a message is already pinned.

## Context (verified against the codebase)

- **No pin wiring exists.** Exhaustive grep: there is **no**
  `MsgPin*`/`MsgUnpin*`/`MsgPinnedList*` subject builder, **no**
  `MsgCanonicalPinned/Unpinned`, **no** `EventPinned`/`EventUnpinned`, and **no**
  handler stub. `HistoryService.RegisterHandlers` (`service.go`) wires only
  history/next/surrounding/get/edit/delete/thread. Subject builders,
  `RegisterHandlers` entries, and `pkg/model` event constants are all net-new
  Phase-1 work. The **wired edit/delete handlers are the template** to mirror.
- **Schema already exists.** `pinned_messages_by_room` and the
  `messages_by_id.pinned_at/pinned_by` columns are already defined in
  `docs/cassandra_message_model.md` and the docker-local init DDL, and
  `pkg/model/cassandra/message.go` already carries `PinnedAt *time.Time` /
  `PinnedBy *Participant` with `cql:` tags. No schema change in this design.
- **Load-bearing timestamp invariant.** `editInPinnedMessagesByRoom` /
  `deleteInPinnedMessagesByRoom` (`write.go:83,97`) locate the pinned row with
  `WHERE room_id=? AND created_at=*msg.PinnedAt AND message_id=?`, guarded by
  `msg.PinnedAt != nil` (lines 119, 187). They read `msg.PinnedAt` from
  `messages_by_id`. This is correct **only if** `PinMessage` writes the *same*
  timestamp to both `messages_by_id.pinned_at` and
  `pinned_messages_by_room.created_at`. Divergence would make every later
  edit/delete silently no-op the pinned copy (stale content, no error). This is
  an explicit invariant of this design (§ Cassandra writes).
- **`messages_by_id` is keyed `(message_id, created_at)`.** All existing writes
  use `WHERE message_id=? AND created_at=msg.CreatedAt` (`write.go:69,146`).
  `PinMessage`/`UnpinMessage` do the same.
- The `isBot` regex + owner/admin bypass already exist as **two** copies
  (`message-gatekeeper/helper.go`, `room-service/helper.go`) with "keep copies
  in sync; promotion to a shared pkg is future cleanup" comments. This design
  adds a **third local copy** in history-service and does **not** promote to a
  shared package (out of scope, keeps blast radius small).

## Authorization model (pin & unpin use the identical gate)

Evaluated in order; first failure wins. Pin and unpin share the gate — pinning
is a **room-moderation** action, not authorship, so a qualifying caller may
unpin a message someone else pinned. This intentionally differs from
edit/delete (`canModify`, sender-only).

1. **Global kill-switch.** New Mongo `settings` collection, well-known doc
   `_id:"global"`, field `pinEnabled bool`, read per request via a projected
   `findOne`. **Doc absent ⇒ treated as enabled (fail-open)** — absence is not
   an outage. A Mongo *error* ⇒ `natsrouter.ErrInternal`. `pinEnabled == false`
   ⇒ `natsrouter.ErrForbidden("pinning is disabled")`. Writing this flag is
   owned by ops/another service and is **out of scope**.
2. **Subscription.** Fetch the caller's subscription by
   `{ "u.account": account, "roomId": roomID }`. Not subscribed ⇒
   `natsrouter.ErrForbidden`. By default **any subscribed member may
   pin/unpin**.
3. **Large-room override.** If `roomUserCount > LARGE_ROOM_THRESHOLD` (env,
   `envDefault:"500"`, same name/default as message-gatekeeper) **and** the
   caller is **not** bypass-eligible ⇒ `natsrouter.ErrForbidden`. Bypass =
   subscription has `model.RoleOwner` or `model.RoleAdmin`, **or** the account
   matches the bot pattern `\.bot$|^p_`.

## Handlers (new file: `history-service/internal/service/pin.go`)

`messages.go` is already large; pin logic goes in its own file. Each handler
mirrors the `EditMessage`/`DeleteMessage` shape (resolve `account`/`roomID`
from `natsrouter.Context` params, return typed response or `natsrouter.Err*`).

- **PinMessage** — pattern `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin`
- **UnpinMessage** — `…msg.unpin`
- **ListPinnedMessages** — `…msg.pinned.list`

New subject builders in `pkg/subject/subject.go`:
`MsgPinPattern`, `MsgUnpinPattern`, `MsgPinnedListPattern`,
`MsgCanonicalPinned`, `MsgCanonicalUnpinned` (each `(siteID string) string`),
each with a unit test in `subject_test.go`. All three request handlers
registered in `RegisterHandlers`.

Request/response models added to `history-service/internal/models/message.go`:
`PinMessageRequest/Response`, `UnpinMessageRequest/Response`,
`ListPinnedMessagesRequest/Response` (the list response paginated like the
existing history readers).

### Handler flow — Pin

1. Resolve `account`, `roomID`.
2. Kill-switch check (§ Authorization 1).
3. Fetch subscription (§ Authorization 2) → also yields roles + account for the
   bypass check.
4. `findMessage(roomID, messageID)` — existing helper; `ErrNotFound` for
   missing/wrong-room (leak-symmetric).
5. **`msg.Deleted` ⇒ `natsrouter.ErrNotFound`** (a deleted message is not
   user-visible; its pinned row, if any, is already tombstoned by
   `SoftDeleteMessage`). Consistent with `EditMessage`.
6. Large-room override (§ Authorization 3), using `roomUserCount`.
7. **Idempotent:** if `msg.PinnedAt != nil` ⇒ success no-op, echo the existing
   `pinnedAt`, **no** event re-publish (mirrors `DeleteMessage`
   already-deleted short-circuit; prevents duplicate `pinned_messages_by_room`
   rows and event storms).
8. Else `pinnedAt = time.Now().UTC()`; `pinnedBy = Participant` derived from
   the caller's subscription user. `msgWriter.PinMessage(ctx, msg, pinnedAt,
   pinnedBy)`.
9. Best-effort publish `EventPinned` on `subject.MsgCanonicalPinned(siteID)`
   via the existing `publishCanonicalBestEffort` helper.
10. Response `{ messageId, pinnedAt }`.

### Handler flow — Unpin

Steps 1–6 identical to Pin (kill-switch, subscription, findMessage,
`msg.Deleted ⇒ ErrNotFound`, large-room override).

7. **Idempotent:** if `msg.PinnedAt == nil` ⇒ success no-op.
8. Else `msgWriter.UnpinMessage(ctx, msg)` (uses `*msg.PinnedAt` as the
   `pinned_messages_by_room` clustering key — that row is guaranteed to exist
   by the timestamp invariant).
9. Best-effort publish `EventUnpinned` on
   `subject.MsgCanonicalUnpinned(siteID)`.
10. Response `{ messageId }`.

### Handler flow — ListPinnedMessages

Subscription check only (§ Authorization step 2). The kill-switch (step 1) and
large-room override (step 3) are **not** applied to the read — listing existing
pins stays available even when new pinning is disabled, and is not a
moderation action. Paginated read over `pinned_messages_by_room` partition
`(room_id)`, clustering `created_at DESC`, via
`msgReader.GetPinnedMessages(ctx, roomID, pageReq)` returning
`cassrepo.Page[models.Message]`. Each row's `PinnedAt` is the clustering
`created_at`; `PinnedBy` from the row's `pinned_by`.

## Cassandra writes (`internal/cassrepo/write.go`, reader in a sibling file)

**Invariant (load-bearing):**
`pinned_messages_by_room.created_at == messages_by_id.pinned_at == pinnedAt`.
`PinMessage` writes both with the *same* `pinnedAt` value. Edit/delete mirrors
depend on this; divergence silently corrupts the pinned copy.

- **`PinMessage(ctx, msg, pinnedAt, pinnedBy)`**
  - `UPDATE messages_by_id SET pinned_at = ?, pinned_by = ? WHERE message_id = ?
    AND created_at = ?` (keyed by `msg.MessageID`, `msg.CreatedAt`).
  - `INSERT INTO pinned_messages_by_room (...)` — a full denormalized copy of
    `msg` with `created_at = pinnedAt` and `pinned_by = pinnedBy`. Column set
    mirrors what the edit/delete mirror queries assume is present.
  - `messages_by_room` / `thread_messages_by_room` have **no** pin columns —
    nothing to mirror there.
- **`UnpinMessage(ctx, msg)`**
  - `UPDATE messages_by_id SET pinned_at = null, pinned_by = null WHERE
    message_id = ? AND created_at = ?`.
  - `DELETE FROM pinned_messages_by_room WHERE room_id = ? AND created_at = ?
    AND message_id = ?` using `*msg.PinnedAt` (guaranteed non-nil — handler
    short-circuits when nil).
- **`GetPinnedMessages(ctx, roomID, pageReq)`** — paginated select over
  `pinned_messages_by_room` partition `(room_id)`, `created_at DESC`, following
  the existing `cassrepo.Page`/`PageRequest` reader pattern.

**Concurrency:** no LWT (matches `UpdateMessageContent`; only delete uses CAS,
for tcount). The idempotent short-circuits cover double-submit. A concurrent
pin+unpin race is accepted last-writer-wins — there is no tcount-style
invariant at stake. Documented limitation, not a correctness bug.

**Deleted-message interaction:** unchanged. `SoftDeleteMessage` already
tombstones the pinned row when `msg.PinnedAt != nil`
(`write.go:187-191`). Pin/unpin reject `msg.Deleted` before reaching the
writer, so the two paths do not interleave on a deleted message.

## Canonical events

- `pkg/model/event.go`: add `EventPinned EventType = "pinned"` and
  `EventUnpinned EventType = "unpinned"`.
- Published **best-effort** via the existing `publishCanonicalBestEffort`
  helper (Cassandra is the source of truth; marshal/publish failure logs and is
  swallowed, exactly like edit/delete).
- Payload: a `model.MessageEvent` carrying `{ ID, RoomID, UserID,
  UserAccount, CreatedAt, PinnedAt, PinnedBy, SiteID, Timestamp }`. This is the
  contract Phases 2–4 bind to and must be stable.

## Configuration

`history-service/internal/config/config.go`: add
`LargeRoomThreshold int \`env:"LARGE_ROOM_THRESHOLD" envDefault:"500"\``
(same env name and default as message-gatekeeper). Wired through
`HistoryService` construction in `cmd/main.go`.

## Interfaces, wiring, mocks

- `SubscriptionRepository`: add `GetSubscription(ctx, account, roomID)
  (*model.Subscription, error)` (mongorepo already implements it; returns
  `nil` when not subscribed).
- `RoomRepository`: add `GetRoomUserCount(ctx, roomID) (int, error)`
  (projected `findOne` on `rooms`, mirroring message-gatekeeper's
  `GetRoomUserCount`).
- New `SettingsRepository` interface + `mongorepo.SettingsRepo`
  (`internal/mongorepo/settings.go`) with `PinEnabled(ctx) (bool, error)` over
  the `settings` collection; fail-open when the doc is absent.
- `MessageWriter`: add `PinMessage`, `UnpinMessage`. `MessageReader`: add
  `GetPinnedMessages`.
- `HistoryService` gains a `settings SettingsRepository` field + constructor
  param + `largeRoomThreshold int`; update the `//go:generate mockgen`
  directive and regenerate `internal/service/mocks/mock_repository.go`.
- `cmd/main.go`: construct `SettingsRepo`, pass threshold, register the three
  handlers.

## Documentation

- `docs/client-api.md`: document the three new client-facing RPCs
  (request/response schema, error cases, triggered canonical events). Required
  in the same PR per project guidelines (subjects begin with
  `chat.user.{account}.request.…`).
- `docs/cassandra_message_model.md`: **no change** (schema already present);
  this design only adds the first writer.

## Phasing

One spec, four independently-shippable phases.

- **Phase 1 (this vertical):** everything above — settings kill-switch, authz,
  pin/unpin/list handlers + subjects + registration, Cassandra
  writes/reader, canonical events, config, interface/mock/wiring changes,
  `docs/client-api.md`, full TDD tests (handler unit with mocked repo;
  cassrepo + settings integration with testcontainers; subject + model unit).
- **Phase 2:** broadcast-worker consumes `MsgCanonicalPinned/Unpinned` →
  per-user real-time RoomEvent push.
- **Phase 3:** search-sync-worker indexes pinned state from the same canonical
  events.
- **Phase 4:** room-service lastMessage pinned-state (old-architecture compat).

Phases 2–4 are sketched here for contract context; each gets its own
implementation plan.

## Testing (TDD, per project guidelines)

Red-Green-Refactor for every unit. Coverage ≥ 80% (target 90%+ for handlers,
store impls, shared `pkg/`).

- **Handler unit tests** (`pin_test.go`, mocked repos), table-driven, covering:
  kill-switch disabled / absent (fail-open) / Mongo error; not subscribed;
  large-room blocked vs. owner/admin/bot bypass at/over/under threshold;
  message not found / wrong room; `msg.Deleted`; idempotent re-pin and
  re-unpin; happy path with canonical-event capture; publish failure swallowed;
  list pagination and empty room.
- **Cassandra integration** (`//go:build integration`, testcontainers): pin
  then read back from both `messages_by_id` and `pinned_messages_by_room`;
  unpin clears both; the timestamp invariant (edit after pin still updates the
  pinned copy); list ordering/pagination.
- **Settings repo integration:** doc present true/false; doc absent ⇒
  fail-open true; surfaced Mongo error.
- **Unit:** new subject builders; new model request/response and event
  constants round-trip (`pkg/model/model_test.go` style).

## Non-goals (YAGNI)

Max-pins-per-room cap; pin expiry; per-room pin-permission config doc (only the
three authorization rules); a write API for the kill-switch; promoting `isBot`
to a shared package; modifying `messages_by_room`/`thread_messages_by_room`
(no pin columns there).
