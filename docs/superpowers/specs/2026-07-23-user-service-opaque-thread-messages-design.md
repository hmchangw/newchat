# Opaque `json.RawMessage` for thread.list parent/last message bodies

**Date:** 2026-07-23
**Branch:** `claude/user-service-opaque-messages-gjmv61`
**Status:** Approved design, pending review

## Problem

`ListUserThreads` in `user-service` (NATS `chat.user.{account}.request.user.{siteID}.thread.list`)
is a cross-site aggregator: it fans the leaf RPC
`chat.server.request.thread.{siteID}.subscription.list` out to every site's
`history-service`, merges the pages into one globally-ordered page, and returns it.

Each leaf item is a `pkgmodel.ThreadListItem`, a **shared** wire type carrying two
hydrated message bodies:

```go
ParentMessage *cassandra.Message `json:"parentMessage,omitempty" bson:"parentMessage,omitempty"`
LastMessage   *cassandra.Message `json:"lastMessage,omitempty"   bson:"lastMessage,omitempty"`
```

`history-service` **builds** these (typed, from Cassandra); `user-service` **decodes**
the leaf response into `ThreadSubscriptionListResponse` and **forwards** the bodies to
the client verbatim. `user-service` reads only `LastMsgAt`, `ThreadRoomID`, `RoomName`,
and `RoomType` from each item (for sort + DM/botDM enrichment) — it **never reads the
message bodies**.

`cassandra.Message` embeds:

```go
Reactions Reactions `json:"reactions,omitempty" cql:"reactions"`
// Reactions = map[ReactionKey]ReactorInfo, ReactionKey = struct{Emoji, UserAccount string}
```

`Reactions` has a custom `MarshalJSON` (emits `map<emoji, [{account, displayName}]>`)
but **no `UnmarshalJSON`**. Go's `encoding/json` cannot decode into a map with a struct
key. So when `user-service` decodes a leaf response in which any parent or last message
carries reactions, `json.Unmarshal` **fails outright** and that entire site's page is
dropped (marked `failed`/`unavailable`). This is not merely wasted work — it is a
latent correctness bug on a client-facing RPC: threads on any site holding a reacted
parent/last message silently disappear from the inbox for that page.

The fix removes the decode entirely on the path that never needs it: `user-service`
should carry the two bodies as opaque bytes and forward them untouched.

## Goal

- `user-service` carries `ParentMessage`/`LastMessage` through the `thread.list`
  aggregation as opaque `json.RawMessage`, never parsing the message body (and thus
  never touching the un-decodable `Reactions` map).
- `history-service` emits the two fields pre-marshaled.
- The client-facing wire JSON is **byte-for-byte unchanged** — still the full `Message`
  object under `parentMessage` / `lastMessage`.

## Non-goals

- No change to `Reactions` (no new `UnmarshalJSON`; the grouped-by-emoji wire form is
  lossy and cannot rebuild the struct-keyed map — irrelevant here since nothing on this
  path needs to read it back).
- No change to `RoomsGet` / `model.LastMessage` (`pkg/model/message.go`) or any other
  RPC. Scope is the `thread.list` path (`ThreadListItem`) only.
- No change to how `history-service` reads messages from Cassandra or builds the typed
  `models.Message`.

## Approach

Retype the two shared fields to `json.RawMessage`; `history-service` pre-marshals the
typed body into bytes at build time; `user-service` forwards the bytes unchanged.

### 1. `pkg/model/threadlist.go`

```go
// Hydrated message bodies, subject to the thread access window. Carried opaque:
// user-service forwards these to the client verbatim and never decodes them (avoids
// parsing cassandra.Message.Reactions, whose struct-keyed map has no JSON decoder).
// history-service emits them pre-marshaled from *cassandra.Message.
ParentMessage json.RawMessage `json:"parentMessage,omitempty" bson:"parentMessage,omitempty"`
LastMessage   json.RawMessage `json:"lastMessage,omitempty"   bson:"lastMessage,omitempty"`
```

`json.RawMessage` marshals verbatim and "decodes" by copying bytes, so:

- `history-service` marshaling the item emits the same `parentMessage`/`lastMessage`
  JSON as today (the raw bytes it stored are a `cassandra.Message` marshal).
- `user-service` decoding the leaf response stores the bytes without parsing; re-marshaling
  the merged page re-emits them verbatim.

`omitempty` on `json.RawMessage` omits nil/empty slices, preserving the current
"absent when not hydrated" behavior. `buildThreadItems` always sets both (it skips rows
missing either), so in practice both are always present.

`bson` tags are retained for struct symmetry; this type is a wire DTO and is not
persisted, so the `bson`-side behavior is immaterial.

### 2. `history-service/internal/service/threads.go` (`buildThreadItems`)

Today:

```go
item.ParentMessage = &parent
item.LastMessage = &last
```

New: marshal each typed body (`models.Message` = alias of `cassandra.Message`) to bytes
and assign. A marshal failure **skips the row** with a warning — consistent with the
existing "a row we can't fully hydrate is skipped rather than surfaced half-empty" rule
directly above (`if !hasParent || !hasLast { continue }`), so one bad body never fails the
whole page:

```go
parentJSON, err := json.Marshal(&parent)
if err != nil {
    slog.WarnContext(c, "thread-list: marshaling parent message, skipping row",
        "request_id", natsutil.RequestIDFromContext(c),
        "thread_room_id", row.ThreadRoomID, "parent_message_id", row.ParentMessageID, "error", err)
    continue
}
lastJSON, err := json.Marshal(&last)
if err != nil {
    slog.WarnContext(c, "thread-list: marshaling last message, skipping row",
        "request_id", natsutil.RequestIDFromContext(c),
        "thread_room_id", row.ThreadRoomID, "last_message_id", row.LastMsgID, "error", err)
    continue
}
item.ParentMessage = parentJSON
item.LastMessage = lastJSON
```

`history-service` uses `encoding/json` on this path (not sonic), and
`Reactions.MarshalJSON` is a valid encoder — marshal here is effectively infallible, but
the guard keeps the page resilient and satisfies the "never ignore errors" rule. The
marshal is done in the per-row loop after the `hasParent`/`hasLast` check, so the item is
only appended once both bodies are in hand.

### 3. `user-service`

**No production code change.** `historyclient.GetThreadList` already `json.Unmarshal`s the
leaf response into `model.ThreadSubscriptionListResponse`; with `json.RawMessage` fields the
two bodies are copied as bytes instead of parsed. `ListUserThreads` merges items and
re-marshals `ThreadListResponse`, re-emitting the bytes verbatim. Removing the parse is the
entire point — it is what makes a reacted parent/last message no longer fail the page.

## Wire compatibility & docs

The client-facing JSON is unchanged: `parentMessage`/`lastMessage` still serialize as the
full `Message` object (the stored bytes are a `cassandra.Message` marshal). `docs/client-api.md`
§ *List User Threads* → *ThreadListItem* describes both fields as type
`[Message](#message-schema)`, which remains accurate; the derived views
(`docs/client-api/request-reply.md`, `docs/client-api/events.md`) likewise need no change.
No doc edit is required. (This is a wire-preserving retype of a server-to-server carrier,
not a change to the client-visible schema.)

## Testing (TDD — Red first)

### `pkg/model/threadlist_test.go`

- **Rewrite `TestThreadListItemJSON_WithMessages`**: build the item with `json.RawMessage`
  bodies (a marshaled `cassandra.Message`), round-trip through `json.Marshal`/`Unmarshal`,
  and assert the raw bytes on the far side decode to a `cassandra.Message` with the expected
  `messageId`/`msg`.
- **New `TestThreadListItemJSON_MessageWithReactions_RoundTrips`** (regression for the bug):
  build a `cassandra.Message` **carrying a non-empty `Reactions` map**, marshal it, wrap the
  bytes as the item's `ParentMessage`/`LastMessage`, then `json.Marshal` the item and
  `json.Unmarshal` back into a `ThreadListItem` — this must **not error** (the old typed field
  would have failed on the reactions decode), and the re-emitted `parentMessage.reactions`
  must equal the grouped-by-emoji wire form.
- `TestThreadSubscriptionListResponseJSON` and the omit-nil cases stay green unchanged.

### `history-service/internal/service/threadlist_test.go`

- Update assertions that read the typed field (e.g. `first.ParentMessage.MessageID`,
  `*first.ParentMessage.TCount`, `first.LastMessage.MessageID`): `json.Unmarshal` the
  `json.RawMessage` into a `cassandra.Message` first, then assert on the decoded value.
- **New case**: a stubbed parent/last message that carries reactions — assert
  `buildThreadItems` still includes the row and the raw `parentMessage` bytes contain the
  grouped reactions form.
- Existing "missing parent/last ⇒ row skipped" and ordering cases stay as-is (they don't read
  the body).

### `user-service/service/threads_test.go`

- **New/confirmed case**: a mocked `HistoryClient.GetThreadList` (or a fake leaf reply the
  client decodes) returning items whose `ParentMessage`/`LastMessage` are marshaled messages
  **carrying reactions**; assert `ListUserThreads` aggregates and returns them without marking
  the site `unavailable`, and the forwarded bytes are unchanged. This is the end-to-end guard
  for the actual bug.

## Files touched

| File | Change |
|------|--------|
| `pkg/model/threadlist.go` | Retype `ParentMessage`/`LastMessage` to `json.RawMessage`; add `encoding/json` import; update field doc comment |
| `pkg/model/threadlist_test.go` | Rewrite `_WithMessages`; add reactions round-trip regression test |
| `history-service/internal/service/threads.go` | Pre-marshal both bodies in `buildThreadItems`; skip-with-warn on marshal error |
| `history-service/internal/service/threadlist_test.go` | Decode raw bytes in assertions; add reactions case |
| `user-service/service/threads_test.go` | Add reactions forward-through regression test (no production change) |

## Verification

- `make test SERVICE=user-service`, `make test SERVICE=history-service`, and the `pkg/model`
  package tests pass with `-race` (the Makefile default).
- `make lint` clean.
- No `docs/client-api.md` diff (wire-preserving); confirm by inspecting the emitted
  `parentMessage`/`lastMessage` JSON in the new tests matches the pre-change bytes.
