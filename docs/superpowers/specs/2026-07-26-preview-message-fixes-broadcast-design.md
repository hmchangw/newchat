# Preview Message: quoted-reply fix, system-type map, and edit/delete broadcast enrichment

**Date:** 2026-07-26
**Branch:** `claude/preview-message-fixes-broadcast-4jk8qv`

## Overview

Three related changes to the room-list "preview message" feature:

1. **Fix — quoted replies are eligible previews.** `roomLastMessage` currently skips any
   message with a `QuotedParentMessage`. A quoted reply is normal room content and should be
   shown as the preview. Only system messages (and deleted messages) should be skipped.
2. **Fix — detect system messages via a lookup map, not an empty-type check.**
   `roomLastMessage` uses `m.Type != ""` to mean "system message". Replace with an explicit
   `model.IsSystemMessageType(t)` backed by a map of the known system-message types.
3. **Feature — edit/delete fan-out carries the refreshed preview.** When a message is edited
   or deleted, the room's preview may change (e.g. the last message was deleted). Today
   clients only learn the new preview by re-issuing `subscription.list`. Have `broadcast-worker`
   include a `previewMessage` field in the edit/delete fan-out events, computed by the same
   `roomLastMessage` logic used by `subscription.list`.

## Current state (as-is)

- **Preview resolution** lives entirely in `history-service`:
  `HistoryService.roomLastMessage` (`history-service/internal/service/rooms.go:71-114`) walks
  backward from the room's `lastMsgAt` and returns the latest eligible message as a
  `models.PreviewMessage` (`= pkg/model.PreviewMessage`). It is called only by the `rooms.get`
  RPC (`RoomsGet`, `rooms.go:26-66`), which `user-service` fans out per site to build each
  `subscription.list` item's `room.previewMessage`.
- **Current skip condition** (`rooms.go:100`):
  ```go
  if m.Deleted || m.Type != "" || m.QuotedParentMessage != nil {
      continue
  }
  ```
- **System-message types** are set on `Message.Type` (`pkg/model/event.go:514-530`):
  `room_created`, `members_added`, `member_removed`, `member_left`, `room_renamed`,
  `room_restricted`, `teams_meet_started`. A normal user message has `Type == ""`.
- **Edit/delete flow:** `history-service` `EditMessage`/`DeleteMessage`
  (`history-service/internal/service/messages.go:486-635`) write to Cassandra, then publish a
  `model.MessageEvent` (`EventUpdated` / `EventDeleted`) to the canonical stream via
  `publishCanonicalBestEffort`. `broadcast-worker` consumes it (`handler.go` `HandleMessage`
  → `handleUpdated` / `handleDeleted`), builds a client-facing `EditRoomEvent` /
  `DeleteRoomEvent`, and fans out via `publishMutation`.
- **Precedent for passthrough:** `MessageEvent` already carries history-service-computed
  fields (`NewTCount`, `NewThreadLastMsgAt`) that `broadcast-worker` relays. The new preview
  field follows the same pattern.

## Part 1 — Fixes (history-service + pkg/model)

### 1a. System-message type map (`pkg/model/event.go`)

Add, next to the `MessageType*` constants:

```go
// systemMessageTypes is the set of Message.Type values that denote a system/event
// message (not user-authored content). Used for fast membership checks — e.g. to
// exclude system messages from room-list previews.
var systemMessageTypes = map[string]struct{}{
    MessageTypeRoomCreated:      {},
    MessageTypeMembersAdded:     {},
    MessageTypeMemberRemoved:    {},
    MessageTypeMemberLeft:       {},
    MessageTypeRoomRenamed:      {},
    MessageTypeRoomRestricted:   {},
    MessageTypeTeamsMeetStarted: {},
}

// IsSystemMessageType reports whether t is a known system-message type.
func IsSystemMessageType(t string) bool {
    _, ok := systemMessageTypes[t]
    return ok
}
```

**Scope:** the map contains the **core 7** types only. `message_removed` (a Cassandra-only
thread-parent tombstone in `cassrepo`) and `teams_system` (a migration marker) are
intentionally excluded — they are not `pkg/model` system-message constants, and a
`message_removed` tombstone is already `Deleted` (so still skipped by the `m.Deleted` check).

**Blast radius:** only `roomLastMessage` is converted to use the map in this change. Other
`Type != ""` checks (`notification-worker.isNotifiable`, `message-worker` system handling,
migration classifiers) are **left as-is** — converting them would change their "unknown type =
system" semantics and is out of scope.

### 1b. Update the skip condition (`history-service/internal/service/rooms.go`)

```go
// System messages aren't representative room content — skip to the previous eligible
// message, same as a deleted one. Quoted replies ARE eligible (normal user content).
if m.Deleted || pkgmodel.IsSystemMessageType(m.Type) {
    continue
}
```

- Drops the `m.QuotedParentMessage != nil` clause → quoted replies now surface as previews.
- Replaces `m.Type != ""` with `pkgmodel.IsSystemMessageType(m.Type)`.
- `pkgmodel` is already imported in `rooms.go`.

No change to `toPreviewMessage` — a quoted reply's `Content` (`m.Msg`) is the reply text, which
is the correct preview content.

## Part 2 — Feature: edit/delete fan-out carries the preview

**Approach (Option A):** `history-service` computes the refreshed preview with the same
`roomLastMessage` function and embeds it on the canonical `MessageEvent`; `broadcast-worker`
relays it onto the client-facing edit/delete events. No new RPC, no new DB dependency for
`broadcast-worker`, and the preview stays byte-for-byte consistent with `subscription.list`.

### 2a. Internal event — `MessageEvent` (`pkg/model/event.go`)

Add one field:

```go
// PreviewMessage is the room's refreshed last-eligible message, computed by history-service
// after an edit/delete so broadcast-worker can relay it in the fan-out event. Uses the same
// resolution as subscription.list. Set only on EventUpdated/EventDeleted for room-visible
// messages; nil otherwise (including "no eligible message remains" — treated as cleared).
PreviewMessage *PreviewMessage `json:"previewMessage,omitempty" bson:"-"`
```

`omitempty` is fine on the internal event: `broadcast-worker` decides whether to emit the
client field based on the event *type* (`handleUpdated`/`handleDeleted`), not on the presence
of this field.

### 2b. Client-facing events — `EditRoomEvent` & `DeleteRoomEvent` (`pkg/model/event.go`)

Add to **both**:

```go
// PreviewMessage is the room's refreshed preview after this edit/delete (same resolution as
// subscription.list). Always present on edit/delete events: an object when an eligible message
// exists, or null when the room has no eligible message left (client should clear its preview).
PreviewMessage *PreviewMessage `json:"previewMessage"`
```

**No `omitempty`** — the field is always serialized on edit/delete fan-out: an object when a
preview exists, `null` when cleared. This is the "always include the field" contract: `null`
unambiguously means "clear the preview", so clients never need a separate re-fetch to learn the
last message was removed.

### 2c. history-service: compute and embed (`messages.go`)

In both `EditMessage` (after `UpdateMessageContent`) and `DeleteMessage` (after a successful
`SoftDeleteMessage`, in the `applied` branch), before publishing:

```go
// Refresh the room preview so broadcast-worker can relay it. Skip for hidden thread replies
// (TShow==false with a thread parent): they never appear in the room timeline, so the room
// preview can't have changed. Read failure / no-eligible-message → nil → clients see null.
if msg.ThreadParentID == "" || msg.TShow {
    if preview, ok := s.roomLastMessage(c, roomID, editedAt /* or deletedAt */); ok {
        canonicalEvt.PreviewMessage = &preview
    }
}
```

- `roomLastMessage` returns `models.PreviewMessage` which is a type alias of
  `pkgmodel.PreviewMessage`, so it assigns directly to `canonicalEvt.PreviewMessage`.
- Runs *after* the Cassandra write, so an edit's new content / a delete's removal is reflected.
- Best-effort: consistent with `publishCanonicalBestEffort` (Cassandra is source of truth).

### 2d. broadcast-worker: relay (`broadcast-worker/handler.go`)

In `buildEditRoomEvent` (`handler.go:701-715`) and `buildDeleteRoomEvent` (`handler.go:717-730`),
set the field from the incoming event:

```go
PreviewMessage: evt.PreviewMessage, // nil => serialized as null (preview cleared)
```

Only `handleUpdated`/`handleDeleted` (the room-level path) build these events, so the field is
emitted exactly on the room-visible edit/delete fan-out. The thread-only path
(`handleThreadUpdated`/`handleThreadDeleted`, for `TShow==false` replies) is untouched — those
emit `ThreadMetadataUpdatedEvent` badge updates and never affect the room preview.

## Data flow (edit example)

1. Client → `history-service.EditMessage`.
2. `UpdateMessageContent` writes new content to Cassandra.
3. `EditMessage` calls `roomLastMessage(roomID)` → refreshed `PreviewMessage`.
4. Publishes `MessageEvent{Event: EventUpdated, PreviewMessage: &preview, ...}` to
   `chat.msg.canonical.{siteID}.updated`.
5. `broadcast-worker.handleUpdated` builds `EditRoomEvent{..., PreviewMessage: evt.PreviewMessage}`.
6. `publishMutation` fans out to `chat.room.{roomID}.event` (channel) or per-member
   `chat.user.{account}.event.room` (DM/BotDM).
7. Clients update the room's preview from `previewMessage` (object) or clear it (`null`).

Delete is identical via `EventDeleted` → `handleDeleted` → `DeleteRoomEvent`.

## Edge cases & trade-offs

- **Deleted the last/only eligible message:** `roomLastMessage` returns `ok=false` → `nil` →
  fan-out `previewMessage: null` → client clears the room preview. This is the primary reason
  the field is always present.
- **Edit/delete of a non-preview (older) message:** `roomLastMessage` returns the same current
  preview → clients re-render identical data. Harmless; keeps the logic simple and always correct.
- **Transient read error while computing the preview:** `roomLastMessage` logs and returns
  `ok=false` → `null` → client momentarily clears the preview until the next room event or
  `subscription.list`. This is rare (the Cassandra write immediately prior just succeeded) and
  self-healing; accepted in favor of the simple always-present contract.
- **Hidden thread replies (`TShow==false` + thread parent):** preview computation is skipped in
  history-service and the thread fan-out path never carries it — no wasted walk, no room-preview
  churn.
- **Cost:** one bounded backward walk (≤ `lastMsgWalkMaxPages` × `lastMsgWalkPageSize` = 250
  messages) per room-visible edit/delete. Edit/delete are low-frequency vs. sends; acceptable.
- **Encrypted channels:** the preview mirrors whatever `subscription.list` returns for the room
  (it reads the same Cassandra rows), so behavior is consistent with the existing preview path.
  No new plaintext exposure beyond what `subscription.list` already returns.

## Testing (TDD — Red/Green/Refactor)

**`pkg/model` (new):**
- `TestIsSystemMessageType` — table-driven: each of the core 7 → true; `""`, `"message"`,
  `"message_removed"`, `"teams_system"`, unknown → false.

**`history-service/internal/service/rooms_test.go`:**
- Update/replace `TestHistoryService_RoomsGet_SkipsQuotedTail` → assert a quoted reply is now
  **returned** as the preview (was skipped).
- Keep `_SkipsSystemTail` (system messages still skipped), `_SkipsDeletedTail`,
  `_MixedTailSkipsAllIneligible` (adjust expectations: quoted no longer counts as ineligible).

**`history-service/internal/service/messages_test.go`:**
- `EditMessage` publishes a `MessageEvent` with `PreviewMessage` set to the refreshed preview.
- `DeleteMessage` of the last message publishes with `PreviewMessage == nil` (cleared).
- `DeleteMessage` of an older message publishes with `PreviewMessage` = the remaining last message.
- Hidden thread reply (`TShow==false`, thread parent) edit/delete publishes with
  `PreviewMessage == nil` and does not invoke the walk.

**`broadcast-worker/handler_test.go`:**
- `handleUpdated` copies `evt.PreviewMessage` into `EditRoomEvent.PreviewMessage`
  (object case and nil→null case).
- `handleDeleted` copies into `DeleteRoomEvent.PreviewMessage`.
- Assert the field is present (including JSON `null`) in the fanned-out payload for the
  always-include contract.

All packages must hold the ≥80% coverage floor.

## Documentation (same PR)

- `docs/client-api.md` — `EditRoomEvent` / `DeleteRoomEvent` are server→client events; add the
  `previewMessage` field (type `[PreviewMessage](#previewmessage)`, nullable) to both, with the
  null-means-cleared note and an updated JSON example.
- `docs/client-api/events.md` — mirror the same additions (derived view must not drift).
- `MessageEvent` is internal (`chat.msg.canonical.*`), not client-facing → no client-api entry.

## Out of scope

- Converting other `Type != ""` checks (notification-worker, message-worker, migrations) to the map.
- Adding preview to the **created** fan-out (`RoomEvent` already carries the full new message).
- Adding `message_removed` / `teams_system` to the system-type map.
- Any denormalized/stored preview — resolution stays read-time via `roomLastMessage`.
