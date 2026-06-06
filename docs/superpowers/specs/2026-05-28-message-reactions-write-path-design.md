# Message Reactions Write Path — Design

**Status:** Draft
**Date:** 2026-05-28
**Branch:** `claude/add-reaction-support-jd0tU`
**Related:** [`docs/specs/message-reactions.md`](../../specs/message-reactions.md) (schema + read-path)

The v3 reaction column shape (embedded `MAP<reaction_key, reactor_info>`) and the read path are already in main. This document is the design for the write path and downstream wire-up that build on top of it.

[`docs/specs/message-reactions.md`](../../specs/message-reactions.md) owns schema and field semantics. This doc owns the request/reply flow, store interface, and per-worker dispatch.

## 1. Goal

Ship end-to-end reaction toggling for client-driven UX:

1. A subscribed room member toggles a reaction via NATS request/reply (`msg.react`); add-vs-remove is server-decided.
2. The reaction lands atomically per-cell on every Cassandra mirror table the message lives in. Because Cassandra is shared cross-site, the write is immediately visible to every site's history reads.
3. Subscribers in the room see the change live; the message author gets a push notification on add. Cross-site delivery rides the same NATS gateway path that message create/edit/delete already use.

Non-goals:

- Custom emoji admin CRUD (the `custom_emojis` collection exists and is queried; population is out-of-band).
- Searching by reaction (reactions are not indexed in Elasticsearch).

## 2. Topology assumptions

- **NATS** — per-site (per-site streams; cross-site delivery via gateway interest propagation).
- **MongoDB** — per-site (OUTBOX/INBOX exists to replicate state between per-site Mongos: member events, subscription reads, thread subscriptions).
- **Cassandra** — shared across sites. A reaction write on any site is immediately readable from every site's `history-service`.

These properties match how message create/edit/delete behave today. Reactions rely on the same mechanisms.

## 3. Architecture

### 3.1 Subject taxonomy

```text
chat.user.{account}.request.room.{roomID}.{siteID}.msg.react   ← client toggle (req/reply)
chat.msg.canonical.{siteID}.reacted                             ← canonical (MESSAGES_CANONICAL stream)
chat.room.{roomID}.event                                        ← live broadcast (channel)
chat.user.{account}.event.room                                  ← live broadcast (DM, per-account)
chat.user.{account}.notification                                ← push notification (author only)
```

`MsgReactPattern(siteID)` and `MsgCanonicalReacted(siteID)` are added in `pkg/subject`. The rest already exist.

`MESSAGES_CANONICAL_<siteID>` uses the wildcard subject `chat.msg.canonical.<siteID>.>`, so `.reacted` is captured without a stream config change.

### 3.2 Producers and consumers

```text
                                                  client
                                                    │ msg.react
                                                    ▼
                                            ┌──────────────┐
                                            │history-svc   │
                                            │ ReactMessage │
                                            └──────┬───────┘
                                                   │ Add/RemoveReaction
                                                   ▼
                                            ┌──────────────┐
                                            │  Cassandra   │ (4 mirror tables, shared cross-site)
                                            └──────────────┘
                                                   │ best-effort publish
                                                   ▼
                                  chat.msg.canonical.{site}.reacted
                                      (MESSAGES_CANONICAL)
                                                   │
                              ┌────────────────────┼────────────────────┐
                              ▼                    ▼                    ▼
                       broadcast-worker     notification-w      search-sync-w (skip)
                              │                    │
                              ▼                    ▼
              chat.room.{room}.event   chat.user.{author}.notification
              chat.user.{m}.event.room  (type:"reaction", added only,
                                         actor != author)
                              │                    │
                              ▼                    ▼
                       clients on any site (via NATS gateway interest)
```

The shared Cassandra means a single write is sufficient. Cross-site delivery of the live broadcast and notification rides the NATS gateway / interest-propagation setup that already supports cross-site messaging.

### 3.3 Per-worker dispatch

| Worker | Behaviour on `EventReacted` |
|---|---|
| `broadcast-worker` | Builds a flat `ReactRoomEvent` and fans out via the same channel-vs-DM router as edit/delete. Reactions never carry message content, so the encryption branch is bypassed. |
| `notification-worker` | Publishes a `NotificationEvent{Type: "reaction"}` to the message author only, when `Action == "added"` and the actor is not the author. |
| `search-sync-worker` | Skips reaction events in `BuildAction` — reactions don't change indexed content. |

## 4. Data Model

### 4.1 Event types — `pkg/model/event.go`

```go
EventReacted EventType = "reacted"
```

```go
type ReactionDelta struct {
    Shortcode string      `json:"shortcode" bson:"shortcode"`
    Action    string      `json:"action"    bson:"action"`   // "added" | "removed"
    Actor     Participant `json:"actor"     bson:"actor"`
}

type MessageEvent struct {
    // existing fields...
    ReactionDelta *ReactionDelta `json:"reactionDelta,omitempty"` // set only when Event == EventReacted
}
```

Live wire payload mirrors the flat edit/delete pattern:

```go
type ReactRoomEvent struct {
    Type      RoomEventType `json:"type"`
    RoomID    string        `json:"roomId"`
    SiteID    string        `json:"siteId"`
    Timestamp int64         `json:"timestamp"`
    MessageID string        `json:"messageId"`
    Shortcode string        `json:"shortcode"`
    Action    string        `json:"action"`
    Actor     Participant   `json:"actor"`
    ReactedAt time.Time     `json:"reactedAt"`
    UpdatedAt time.Time     `json:"updatedAt"`
}
```

```go
RoomEventMessageReacted RoomEventType = "message_reacted"
```

### 4.2 Notification payload — `pkg/model.NotificationEvent`

```go
type NotificationEvent struct {
    Type    string  // "new_message" | "reaction"
    RoomID  string
    Message Message
    ReactionDelta *ReactionDelta `json:"reactionDelta,omitempty"` // set only when Type == "reaction"
    Timestamp int64
}
```

The delta lives on the envelope, not on `Message`, matching the canonical event design.

### 4.3 Custom emoji collection

```go
type CustomEmoji struct {
    ID        string `bson:"_id"`
    SiteID    string `bson:"siteId"`
    Shortcode string `bson:"shortcode"`
    ImageURL  string `bson:"imageUrl"`
    CreatedBy string `bson:"createdBy"`
    CreatedAt int64  `bson:"createdAt"`
}
```

Unique index on `(siteId, shortcode)`. Index ensured at history-service startup via `mongorepo.CustomEmojiRepo.EnsureIndexes`. Custom emojis live in per-site Mongo, so the lookup is site-scoped.

## 5. Handler — `history-service.ReactMessage`

### 5.1 Sequence

```text
1.  validate(messageId, shortcode)               # required-field check; cheap, no I/O
2.  shortcode, err := emoji.Validate(            # NFC-normalises and resolves;
        siteID, shortcode)                       # the normalised form replaces the input
3.  getAccessSince(account, roomID)              # subscription check
4.  msg, err := findMessage(roomID, msgID)       # Cassandra messages_by_id
5.  actor := userStore.FindUsersByAccounts([account])
6.  key := ReactionKey{Emoji: shortcode, UserAccount: actor.Account}
7.  alreadyReacted := msg.Reactions[key] exists
8.  if msg.Deleted && !alreadyReacted: return NotFound
9.  if alreadyReacted: msgWriter.RemoveReaction(msg, key, now)
    else:              msgWriter.AddReaction(msg, key, reactor)
10. publishCanonicalBestEffort(MsgCanonicalReacted,
        MessageEvent{..., ReactionDelta{Shortcode: shortcode, ...}})
11. reply ReactMessageResponse{messageId, shortcode, action, reactedAt}
```

The `shortcode` returned from step 2 is what flows through every subsequent step — the `ReactionKey.Emoji` bound into Cassandra, the `Shortcode` on the canonical event, and the `shortcode` echoed back to the client. The raw request input is not used past step 2.

### 5.2 Authorisation

Any subscribed room member may react. No sender-only restriction (differs from edit/delete which are sender-only).

### 5.3 Deleted-message gate

Adding a reaction to a soft-deleted message → `ErrNotFound`. Removing an existing reaction on a deleted message is allowed so users can clean up after the message was deleted. The check uses the in-row map state (`alreadyReacted`) — no separate query.

### 5.4 Concurrency

Per-cell writes are atomic in Cassandra. Two concurrent reactors with different `(emoji, user_account)` keys never conflict. A single user toggling fast may see "added" twice if both reads observe absent state — final state converges; the worst case is the second toggle returns the wrong action label, not a corrupted row. Acceptable for UI-driven flow.

No LWT, no read-modify-write.

### 5.4.1 Request budget

The handler issues 4–7 blocking I/O hops per request: subscription lookup (Mongo), room access (Mongo), Cassandra `findMessage`, user lookup (Mongo), custom-emoji lookup (Mongo, cache-fronted), 1–4 Cassandra writes, and the canonical-event publish (NATS). No `HandlerTimeout` middleware is wired into history-service — the codebase convention is **bound the I/O, not the handler**: `pkg/cassutil.Connect` sets `cluster.Timeout = 10s` per Cassandra query, and a `Mongo.SetTimeout` equivalent is the cross-cutting `mongoutil`-level work that would close the gap. Adding a handler-level deadline below the per-driver budget would conflict with the existing convention and would break edit / delete / get on the same pattern; that hardening belongs in its own PR across services, not here.

### 5.5 Live event ordering under JetStream redelivery

Each toggle publishes its own canonical event with a distinct `Nats-Msg-Id` (the dedup key includes actor + shortcode + action + timestamp). Under normal JetStream flow events are delivered FIFO per consumer; under redelivery (consumer stalls on event N, event N+1 is delivered and processed, then N redelivers after AckWait) `broadcast-worker` can fan out the toggles to clients in the wrong order. Cassandra is the source of truth — per-cell map writes are LWW by `updated_at` — so storage state is correct. Clients reconcile to authoritative state on REST refetch / reconnect; a brief flicker between server events is accepted.

Reactions inherit the codebase convention: live broadcast is best-effort, storage is authoritative, clients reconcile. The same risk exists today for `EventUpdated` (rapid edits redelivered out of order produce a content flicker) and `broadcast-worker.handleUpdated` performs no per-event reconciliation either. No server-side guard is introduced for reactions specifically — adding one would require introducing a Cassandra-read dependency in `broadcast-worker` (today Mongo + NATS only), which is a heavier change than the bug warrants.

## 6. Store Interface — `MessageWriter`

```go
type MessageWriter interface {
    // existing: UpdateMessageContent, SoftDeleteMessage
    AddReaction(ctx, msg *Message, key ReactionKey, reactor ReactorInfo) error
    RemoveReaction(ctx, msg *Message, key ReactionKey, updatedAt time.Time) error
}
```

Both methods route to the right mirror table based on `msg.ThreadParentID`:

- `messages_by_id` — always.
- `messages_by_room` — when `ThreadParentID == ""`.
- `thread_messages_by_thread` — when `ThreadParentID != ""`. (Mutually exclusive with the above.) Partition key is `msg.ThreadRoomID`; the store returns an error when `ThreadParentID != "" && ThreadRoomID == ""` to fail-fast on inconsistent inputs rather than write to an undefined partition.

`pinned_messages_by_room` is **not** a reactions mirror. The pinned panel does not render reactions, so writing them there is dead work; if product later wants reactions in the pinned panel, the read path can side-fetch from `messages_by_id` for the small number of pinned IDs the panel renders.

**Execution ordering.** `messages_by_id` is written first and sequentially as the source of truth. After it succeeds, the room-or-thread mirror is written. No concurrency — there is only one mirror.

**Failure semantics.** If `messages_by_id` fails, the mirror is skipped and the handler returns an error. If the mirror write fails after `messages_by_id` committed, the two tables temporarily disagree; the next reaction on the same message converges them. Acceptable trade-off — no XA, no batch.

**Remove path detail.** `RemoveReaction` issues two CQL statements per table — a per-cell `DELETE` followed by an `UPDATE … SET updated_at = ?` on the same row — because CQL does not allow combining a per-cell DELETE with a column UPDATE in one statement.

## 7. Cross-site visibility

| Concern | Mechanism |
|---|---|
| Cassandra rows (reaction state) | Shared Cassandra — same cluster, same keyspace. A write on site-a is immediately readable from site-b's history-service. |
| Live broadcast to remote subscribers | NATS gateway interest propagation. `chat.room.{roomID}.event` published on site-a's NATS reaches subscribers on site-b's NATS via the same path today's message creates use. |
| Push notification to remote-site author | Same gateway path. `chat.user.{authorAccount}.notification` published on the actor's site reaches the author wherever they're connected. |
| Custom emoji lookup | Per-site (Mongo is per-site). Custom emojis registered on site-a are not visible to site-b's history-service. Every shortcode the FE wants to surface must be registered in each site's `custom_emojis` collection. |

The application code does not federate any of this — gateway interest propagation and shared Cassandra do the work transparently.

## 8. Validation — `pkg/emoji`

```go
type Validator struct { lookup CustomEmojiLookup }
func (v *Validator) Validate(ctx, siteID, shortcode string) (normalised string, err error)
```

The validator answers two questions about an incoming shortcode and returns the canonical form for downstream use:

1. Is the input well-formed?
2. Does it identify something the server knows how to render — i.e. a registered custom emoji on this site?

The handler binds the **returned `normalised` string** into the Cassandra `ReactionKey.Emoji` and into the `ReactionDelta.Shortcode` on the canonical event. The handler must not use the raw request input past this point — see §8.1 below.

### 8.1 Pipeline

```text
        request shortcode
              │
              ▼
   ┌──────────────────────┐
   │ Length cap (256B)    │──── over ─► return "", ErrInvalidShortcode
   └──────────┬───────────┘
              ▼
   ┌──────────────────────┐
   │ NFC-normalise        │  golang.org/x/text/unicode/norm.NFC.String(s)
   └──────────┬───────────┘
              ▼
   ┌──────────────────────┐
   │ Wire-format regex?   │──── no  ──► return "", ErrInvalidShortcode
   └──────────┬───────────┘
              │ yes
              ▼
   ┌──────────────────────┐
   │ Custom emoji exists  │──── yes ──► return normalised, nil
   │   on this site?      │
   └──────────┬───────────┘
              │ no
              ▼
   return "", ErrUnknownShortcode
```

Each step is described below.

### 8.2 Length cap and NFC normalisation

A 256-byte input length cap runs before NFC so a malicious 1 MB payload doesn't allocate a 1 MB output buffer just to be rejected at the regex downstream. The cap is well above any realistic shortcode (the regex limits the post-NFC form to 32 chars) and well below pathological sizes.

After the cap, the validator applies `norm.NFC.String(shortcode)`. Cassandra map-key equality is byte-exact. The same Unicode emoji can arrive in multiple valid encodings:

- `❤️` as U+2764 + U+FE0F (combining variation selector) vs the precomposed form.
- ZWJ-joined sequences (`👨‍👩‍👧`) with different ZWJ orderings.
- NFD-decomposed combining marks vs the NFC-composed equivalents.

Without normalisation, the same user reacting "the same way" twice could land two different `(emoji, user_account)` cells in the reactions map — and an un-react sent with one form would silently fail to remove a cell stored under the other.

Every downstream use — regex check, custom emoji lookup, Cassandra map key, ReactionDelta wire field — uses the NFC form. Single chokepoint, no per-call-site normalisation drift. ASCII-only shortcodes (`thumbsup`, `acme_party`) are unchanged by NFC; the overhead is a function call. Readers can assume stored shortcodes are NFC.

### 8.3 Wire-format regex

```go
^[a-z0-9_+-]{1,32}$
```

Length 1–32; characters drawn from `[a-z0-9_+-]` in any position. Rejects uppercase, whitespace, colons, and non-ASCII. Leading `+`/`-`/`_` is admitted so the Slack/GitHub `+1`/`-1` convention (and any future symbol-led shortcode an admin registers) is reachable via the custom-emoji collection.

The regex enforces a syntactic contract so the Mongo lookup sees a bounded, well-formed input space. It is not a semantic check — passing the regex doesn't mean the shortcode means anything; that's the Mongo lookup's job.

### 8.4 Custom emoji lookup

Per-site Mongo collection (`custom_emojis`), keyed by `(siteId, shortcode)` with a unique compound index. The validator calls `CustomEmojiLookup.CustomEmojiExists(ctx, siteID, shortcode)` — projection `{_id: 1}`, no payload fetch. Missing → `ErrUnknownShortcode`; Mongo error → wrapped error → handler 500.

`CustomEmojiLookup` is an interface defined by `pkg/emoji`. Production wiring (`history-service/cmd/main.go`) injects `mongorepo.CustomEmojiRepo`; tests inject a gomock stub.

### 8.5 Failure mapping

| Input | NFC | Regex | Lookup | Result | Client sees |
|---|---|---|---|---|---|
| `"acme_party"` (registered) | unchanged | pass | found | OK | `action: added/removed` |
| `"acme_party"` (not registered) | unchanged | pass | not found | `ErrUnknownShortcode` | 400 `"invalid reaction shortcode"` |
| `":thumbsup:"` | unchanged | fail | — | `ErrInvalidShortcode` | 400 `"invalid reaction shortcode"` |
| `"+1"` (registered) | unchanged | pass | found | OK | `action: added/removed` |
| `"+1"` (not registered) | unchanged | pass | not found | `ErrUnknownShortcode` | 400 `"invalid reaction shortcode"` |
| `"❤" + VS16` (NFD) | folded to NFC `"❤️"` | fail (regex is ASCII) | — | `ErrInvalidShortcode` | 400 `"invalid reaction shortcode"` (today — see §8.6) |
| Input > 256 bytes | — | — | — | `ErrInvalidShortcode` | 400 `"invalid reaction shortcode"` |
| Mongo unreachable | — | pass | error | wrapped error | 500 internal |

### 8.6 What this design does NOT cover

- **Literal Unicode codepoints as shortcodes.** The regex is ASCII. NFC normalisation is done unconditionally, but a request carrying `"❤️"` as the shortcode currently fails the regex. If product later wants direct-emoji reactions (Slack-style: hit 👍 in the picker, the client sends `"👍"` not `"thumbsup"`), the regex needs a second branch admitting NFC-normalised emoji codepoints — but the existing NFC step is already in place to support that without storage-shape changes.
- **Custom emoji CRUD.** The collection is populated out-of-band today. See §11.

## 9. Tests

### 9.1 Unit

- `pkg/emoji/emoji_test.go` — validator with stubbed lookup; all error paths; boundary lengths; NFC normalisation contract.
- `pkg/model/model_test.go` — round-trip JSON+BSON for `CustomEmoji`, `ReactionDelta`, `MessageEvent.ReactionDelta`, `ReactRoomEvent`; `EventReacted` and `RoomEventMessageReacted` constant assertions.
- `pkg/subject/subject_test.go` — `MsgReactPattern`, `MsgCanonicalReacted` builders.
- `pkg/natsutil/canonical_dedup_test.go` — `CanonicalDedupID` for `EventReacted` includes actor/shortcode/action/timestamp.
- `history-service/internal/service/reactions_test.go` — ReactMessage handler. Covers: NotSubscribed, EmptyMessageID, EmptyShortcode, InvalidShortcodeFormat, UnknownCustomShortcode, MessageNotFound, AddOnDeleted_Blocked, RemoveOnDeleted_Allowed, Add_Success_PublishesEvent, Remove_Success, UserLookupError, UserNotFound, AddStoreError, RemoveStoreError, CustomEmojiFound_Success.
- `broadcast-worker/handler_test.go` — channel publish, DM fan-out, missing-delta error.
- `notification-worker/handler_test.go` — added notifies, removed silent, self-react silent, missing delta errors.
- `search-sync-worker/messages_test.go` — `EventReacted` produces no ES action.

### 9.2 Integration (testcontainers)

- `history-service/internal/cassrepo/reactions_integration_test.go` — AddReaction/RemoveReaction across the reaction-bearing tables (top-level, thread reply), a pinned-message test pinning that the pinned table is intentionally untouched, idempotent re-add, remove-after-add, remove on absent cell, three distinct (emoji, user) pairs.

### 9.3 Coverage

≥80% floor / ≥90% on touched code per CLAUDE.md §4. `pkg/emoji` ships at 100% statement coverage.

## 10. Files

### 10.1 New

- `pkg/emoji/{emoji.go,emoji_test.go}`
- `pkg/model/custom_emoji.go`
- `history-service/internal/mongorepo/custom_emoji.go`
- `history-service/internal/cassrepo/reactions.go`
- `history-service/internal/cassrepo/reactions_integration_test.go`
- `history-service/internal/service/reactions.go`
- `history-service/internal/service/reactions_test.go`

### 10.2 Modified

- `pkg/model/event.go` — `EventReacted`, `ReactionDelta`, `MessageEvent.ReactionDelta`, `RoomEventMessageReacted`, `ReactRoomEvent`, `NotificationEvent.ReactionDelta`.
- `pkg/model/model_test.go` — round-trips for the above.
- `pkg/subject/{subject.go,subject_test.go}` — `MsgReactPattern`, `MsgCanonicalReacted`.
- `pkg/natsutil/canonical_dedup.go` — extend `CanonicalDedupID` for `EventReacted`.
- `history-service/internal/models/message.go` — `ReactMessageRequest`, `ReactMessageResponse`, `Reactions`/`ReactionKey`/`ReactorInfo` aliases.
- `history-service/internal/service/service.go` — `UserStore` + `CustomEmojiStore` interfaces, extended `MessageWriter`, `MsgReactPattern` registration.
- `history-service/cmd/main.go` — wire `userStore` + `customEmojiRepo`.
- `history-service/internal/service/mocks/mock_repository.go` — regenerated.
- `broadcast-worker/{handler.go,handler_test.go}` — `EventReacted` branch + `ReactRoomEvent`.
- `notification-worker/{handler.go,handler_test.go}` — `EventReacted` branch.
- `search-sync-worker/{messages.go,messages_test.go}` — skip reactions in BuildAction.
- `docs/client-api.md` — "React to Message" section + reaction notification trigger.
- `docs/nats-subject-naming.md` — MESSAGES_CANONICAL section enumerating all four canonical subjects.
- `docs/specs/message-reactions.md` — added handler/validator/downstream sections.

## 11. Out of scope

- **Custom emoji admin CRUD.** The collection exists, the lookup works, the index is ensured. Population needs a follow-up — admin API or import tool.
- **Searching by reaction.** Reactions are deliberately not indexed in Elasticsearch (search-sync-worker skips `EventReacted`). Would need a dedicated index or table.
- **Custom emojis on remote sites.** Because `custom_emojis` is per-site Mongo, a custom emoji registered on site-a is not visible to a user reacting from site-b. If cross-site custom emojis are ever needed, the existing OUTBOX/INBOX pattern (for Mongo-state replication) applies.

## 12. Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Single toggle subject (`msg.react`), server-decides add/remove | One round-trip; matches edit/delete shape |
| D2 | Authorisation: any subscribed member (not sender-only) | Reactions are a social signal, not a content edit |
| D3 | Block add on deleted, allow remove | Users can clean up after the message is deleted |
| D4 | LWT-free per-cell writes | v3 schema's central property — preserving it on the write path |
| D5 | Membership decided via in-row map lookup, not a separate read | Free given v3 inline reads |
| D6 | `messages_by_id` written first; mirrors are best-effort | Source-of-truth ordering; no batch / no XA |
| D7 | `ReactionDelta` on `MessageEvent`, not on `Message` | Keep `Message` focused on document state; delta is event-level |
| D8 | Flat `ReactRoomEvent`, not nested under `RoomEvent` | Matches `EditRoomEvent` / `DeleteRoomEvent` pattern |
| D9 | Notification only on add, only to author, never on self-react | Avoid notification spam |
| D10 | No application-level federation | Cassandra is shared cross-site; NATS-gateway interest propagation handles live broadcasts and notifications |
| D11 | Custom emoji admin out of scope | Lookup wired, but no API yet — deferred to follow-up |
