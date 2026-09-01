# chat-frontend: sidebar message preview

**Date:** 2026-08-14
**Status:** approved design, ready for planning
**Scope:** `chat-frontend` only. No backend change, no wire change.

## Goal

Render each sidebar room's most recent message as a snippet under the room name,
kept live as messages arrive, are edited, and are deleted.

## Background

The backend already produces this data; the frontend has never read it.

`pkg/model.PreviewMessage` (`messageId`, `sender`, `content`, `createdAt`,
`attachments`, `mentions`, `visibleTo`) reaches the client on three carriers:

| Carrier | Field | When |
|---|---|---|
| `subscription.list` reply | `sub.room.previewMessage` | Cold start |
| `message_edited` event | `evt.previewMessage` | Server-recomputed after an edit |
| `message_deleted` event | `evt.previewMessage` | Server-recomputed after a delete |

`new_message` carries no preview field — it carries the full `message`, so the
client derives the snippet itself.

Eligibility is resolved server-side: soft-deleted and system messages are
skipped, walking back to an earlier surviving message; quoted replies count as
normal content. The client never re-implements this walk.

`fetchSidebarBuckets` already stores each `subscription.list` row whole,
including its nested `room`. The bootstrap data is therefore in hand today —
`SubscriptionRoom` in `api/types.ts` simply doesn't declare the field.

### Encryption is not a factor

Two layers exist and neither blocks this feature:

- **At-rest** (`EncPayload` / `EncMeta`, `pkg/atrest`) is server-side.
  history-service decrypts before replying, so `previewMessage.content` is
  plaintext.
- **Room E2E** (`encryptedMessage`, room key) applies to broadcast events only.
  `useRoomSubscriptions.decryptAndDispatch` resolves content — or substitutes
  the existing `'[encrypted message]'` placeholder — before the reducer sees it,
  so a locally derived preview inherits whatever the message list shows.

Note for the record: broadcast-worker copies `evt.PreviewMessage` through
without encrypting it (`broadcast-worker/handler.go:737,757`), so edit and delete
previews are plaintext even in an E2E deployment. This spec neither depends on
that nor changes it.

## Decisions

| Question | Decision |
|---|---|
| Surface | Sidebar `RoomList` last-message snippet |
| Row shape | Two lines: name + badges, then `Alice: <snippet>` |
| Sender prefix | Omitted in DM rooms — both `dm` and `botDM`, whose row title already is the counterpart's name. `discussion` is multi-party and keeps the prefix. |
| Timestamp | None |
| Mention / unread indicators on the preview line | None |
| Attachment-only messages | `'Photo'` / `'Audio'` / `'Video'` / title-or-`'File'` |
| No preview available | Row keeps constant height; snippet line renders empty |
| Markup handling | Flatten to plain text |
| State location | Dedicated `state.previews` map |

## Architecture

### State

`roomEventsReducer` gains one key:

```js
previews: Record<roomId, RoomPreview>

RoomPreview = {
  messageId: string,   // guards the delete rule below
  senderName: string,  // resolved at write time
  text: string,        // flattened, capped
}
```

Three properties of this shape are deliberate:

- **Flattened at write time, not render time.** `useSidebarSections` recomputes
  its whole memo whenever `previews` changes. Flattening on read would re-flatten
  every room in the sidebar on every single message event; flattening on write is
  one call per event.
- **`senderName` is a resolved string, not a `Participant`.** The two sources
  carry the name differently — `PreviewMessage.sender.displayName` versus a live
  `Message`'s top-level `userDisplayName`. Normalizing at write time means the
  render layer never branches on provenance.
- **`createdAt` and `attachments` are not stored.** Nothing renders them. Storing
  only what renders keeps the reducer tests honest about what the feature
  consumes.

### Why a keyed map rather than a field on `RoomSummary`

`summaries` is rebuilt wholesale from `Room` records on `BUCKETS_LOADED` and
`ROOMS_LOADED`, and the `Room` type mirrors `pkg/model.Room` — which has no
preview field, because the backend hangs it off `SubscriptionRoom`. Adding one
would break the type-mirror rule. A keyed map also matches the existing shape of
`subscriptions` and `roomState`, and updates in O(1) independent of summary
rebuilds.

Rejected alternative: deriving the preview at render time from
`state.roomState[roomId].messages`. `roomState` exists only for rooms whose
history has been loaded, which at bootstrap is none of them.

### Writers

| Action | Source | Behavior |
|---|---|---|
| `BUCKETS_LOADED` | `sub.room.previewMessage` | Seed only rooms with **no** preview yet. A live message can land before `fetchSidebarBuckets` resolves (the DM subscription goes live first) and is newer than this snapshot — overwriting it would show an older message in the sidebar than the room does. |
| `MESSAGE_RECEIVED` | the incoming message | Overwrite. Applied at **both** return points — live and historical buffer modes |
| `MESSAGE_SENT_LOCAL` | the optimistic message | Overwrite; the server echo later rewrites it identically |
| `MESSAGE_EDITED_LOCAL` | the edited message | Overwrite **only** when the edited id equals the stored `messageId` |
| `MESSAGE_DELETED_LOCAL` | — | No-op. See below. |
| `ROOM_PREVIEW_UPDATED` | edit / delete events | Overwrite when `previewMessage` present; clear when absent **and** `deletedMessageId` equals the stored `messageId` |
| `SUBSCRIPTION_UPSERTED` | — | No-op. See below. |
| `ROOM_REMOVED` | — | Delete the entry so leaving a room strands nothing |
| `RESET` | — | Clear the whole map |

Every writer that derives a preview from a message applies the thread-reply guard
below — `MESSAGE_RECEIVED` and `MESSAGE_SENT_LOCAL` alike.

`MESSAGE_DELETED_LOCAL` is intentionally inert: the client cannot reproduce the
server's walk-back to an earlier eligible message, so it waits for the
authoritative event rather than guessing.

`SUBSCRIPTION_UPSERTED` is a no-op even though its `added` payload embeds a
`room` object: `docs/client-api.md` specifies `previewMessage` is *always*
omitted there. A newly added room therefore shows a blank snippet line until its
first message arrives — correct, since a just-created room has none.

`RESET` clearing the map is not optional. It fires on logout, and a stale preview
surviving into the next account's session would leak one room's message content
to a different user on the same device.

The `ROOM_PREVIEW_UPDATED` clear rule is guarded on `messageId` because an absent
`previewMessage` alongside a `deletedMessageId` that *isn't* the current preview
is contradictory input — the server would have echoed the unchanged preview.
Leaving the existing value alone is the safe read.

### Why edit and delete need their own action

Both `MESSAGE_EDITED` and `MESSAGE_DELETED` open with:

```js
const prev = state.roomState[action.roomId]
if (!prev) return state
```

They bail when the room's message buffer doesn't exist — which for a sidebar
preview is the *normal* case: the user isn't in the room, its history was never
loaded, and the preview must still update.

One layer up, `useRoomSubscriptions.handleMutationEvent` drops an edit with no
plaintext `newContent` (`if (typeof newContent !== 'string') return true`)
before dispatching at all, so in an E2E deployment the edit never reaches the
reducer even though its `previewMessage` rode along in plaintext.

Threading a preview field through two paths that each bail early for unrelated
reasons is fragile. `ROOM_PREVIEW_UPDATED` is dispatched from
`handleMutationEvent` for every edit and delete carrying preview data,
independent of whether the mutation itself dispatches.

```
ROOM_PREVIEW_UPDATED = {
  roomId: string,
  previewMessage?: PreviewMessage,
  deletedMessageId?: string,
}
```

### Read seam

`useSidebarSections`'s existing `enrich` — which already joins each summary with
its subscription's display name and `hrInfo` — gains one join against
`previews[room.id]`, and `previews` joins its memo dependency list. `RoomList`
stays a pure consumer of what `useSidebarSections` hands it.

## Components

### `lib/previewText.js`

`previewText(content, mentions) → string`. Pure: no React, no I/O. Calls the
existing `parseMessageContent(content, mentions)` and walks the node tree.

| Node | Flattens to |
|---|---|
| `text` | verbatim |
| `mention` | `@` + `node.display`; `@all` for mention-all |
| `link` | `node.text` — the visible label, never the href |
| `code`, `codeblock` | raw inner text; fences and backticks dropped |
| `strong`, `em`, `del` | recurse into children; markers dropped |

Then: collapse every whitespace run — newlines included — to a single space,
trim, and hard-cap at 140 characters.

The cap is not cosmetic. CSS ellipsis truncates visually but leaves the whole
string in the DOM; without it a 4,000-character message sits in the sidebar for
as long as it's the room's latest.

### `lib/` sender-name helper

`MessageRow`'s private `senderName(msg)` moves to `lib/` so the preview path and
the message row resolve names by one rule rather than two. `MessageRow`
delegates to it; its top-level `userDisplayName` preference is unchanged.

It is not the codebase's only such rule — `QuotedBlock.senderLabel`, and the
optimistic quote targets built in `ChatPage` and `ThreadRightBar`, each resolve
names their own way and none consult `displayName`. Those are out of scope here
and are left alone: they operate on quote snapshots, not on a `Participant`.

The extracted helper prefers `participant.displayName`, which the inline version
never consulted. That branch is **unreachable from `MessageRow`** — nothing
populates a message's nested `sender.displayName`: `broadcast-worker`'s
`buildClientMessage` sets only account/engName/chineseName, and across
history-service the field is assigned in exactly one place,
`rooms.go:216`'s `toPreviewMessage`. The branch exists for
`PreviewMessage.sender`, the one shape that carries it, where it delivers a
bot's app name.

### Attachment fallback

When `previewText` yields an empty string and the message has attachments,
classify the first via the existing `lib/attachment.attachmentKind` and store:

| Kind | Text |
|---|---|
| `image` | `Photo` |
| `audio` | `Audio` |
| `video` | `Video` |
| `file` | the attachment's `title`, or `File` when untitled |

No new classification logic — `attachmentKind` already prefers the
media-specific URL, then the `fileType` MIME family, then generic file.

### `RoomItem` rendering

A second line: `<span className="room-preview">` holding a muted sender `<span>`
(`Alice: `, omitted when `room.type === 'dm'`) followed by the text. Both lines
get `overflow: hidden; text-overflow: ellipsis; white-space: nowrap`.

Height is reserved on `.room-preview` itself — `min-height: 1.4em` against a
matching unitless `line-height: 1.4`, so an empty snippet line occupies exactly
the same box as a populated one. The row is therefore two lines tall whether or
not the join found a preview, and no row reflows as previews arrive. Colors and
spacing come from `styles/tokens.css`; no hardcoded values.

**State absence and render collapse are separate concerns and must stay
separate.** A room with no preview is absent from the `previews` map — no
sentinel entry, no empty-string record. The constant row height is a CSS
property of the row, not a consequence of the child having text.

### Type mirror fixes

Both are prerequisites — the feature doesn't type-check without them:

- `Participant` gains `displayName?: string`. Go's `model.Participant` has had
  `DisplayName` since it was written; the TS mirror omits it.
- `SubscriptionRoom` gains `previewMessage?: PreviewMessage`, and
  `PreviewMessage` is declared in `api/types.ts` mirroring the Go struct
  field-for-field.

## Edge cases

| Case | Behavior |
|---|---|
| Thread reply (any `threadParentMessageId`) | Never touches the preview. This is BROADER than the server's rule: the server excludes only *hidden* replies, and a `tshow: true` reply does get a `messages_by_room` row and can legitimately be a room's preview. Correct here only because this frontend has no `tshow` support — no thread reply reaches the room timeline. Revisit when `tshow` lands. |
| Encrypted room, live message | No special handling; `decryptAndDispatch` resolves content upstream |
| Encrypted room, undecryptable | Previews as `[encrypted message]`, matching the message list |
| Failed optimistic send (`_status: 'failed'`) | Still previews, matching the message list |
| `visibleTo` | Ignored; its server-side write path hasn't landed and the field is always empty |
| Room with only system or deleted messages | Server omits the preview; blank snippet line |
| Site enrichment degraded | Indistinguishable from "no messages" on the wire; blank snippet line |
| Message with attachments *and* text | Text wins; the attachment fallback applies only to empty text |

## Testing

TDD per the repo rule — Red confirmed before Green on each.

**`lib/previewText.test.js`** — pure, no React. Each node type; nested emphasis;
unterminated fence; mention with no matching participant; `@all`; newline
collapsing; the 140-character cap; empty and null input.

**`reducer.test.js`** — one case per writer in the table, plus the four easiest to
get wrong: an edit arriving for a room with no `roomState`; a delete whose
`deletedMessageId` doesn't match the stored preview; a hidden thread reply
leaving the preview untouched; and `RESET` clearing the map. Pure JS, no React.

**`RoomList.test.jsx`** — sender prefix present in a channel, absent in a DM;
blank line when the join finds nothing; row height constant across both; the
attachment fallback rendering.

**`RoomEventsContext.test.jsx`** — the `useSidebarSections` join, including a
room present in `summaries` but absent from `previews`.

Gates: `npm run typecheck` and `npm test` pass; `vite build` clean.

## Non-goals

Recorded so they don't creep in during implementation:

- Timestamps on the preview line
- Unread-state bolding of the snippet
- Previews in the search results pane or thread list
- URL unfurling or link preview cards
- Mention or attachment *indicators* (distinct from the text fallback above)
- Any `docs/client-api.md` change — nothing on the wire moves
