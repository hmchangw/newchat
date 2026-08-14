# chat-frontend Sidebar Message Preview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render each sidebar room's most recent message as a live-updating snippet under the room name.

**Architecture:** A dedicated `state.previews` map in `roomEventsReducer`, seeded from `subscription.list`'s existing `sub.room.previewMessage` and updated by live message, edit, and delete events. Message bodies are flattened to single-line plain text at write time by a pure `lib/` function built on the existing content tokenizer. `useSidebarSections` joins the map into each room; `RoomItem` renders a second line whose height is constant whether or not a preview exists.

**Tech Stack:** React 18, Vite, vitest + @testing-library/react, plain CSS with design tokens. `api/` is TypeScript (strict); components, contexts (except `.tsx` ones), lib, and tests are JS.

**Spec:** `docs/superpowers/specs/2026-08-14-chat-frontend-message-preview-design.md`

## Global Constraints

- All commands run from `chat-frontend/`. Never run raw `go` commands; this feature touches no Go.
- `npm run typecheck` and `npm test` must pass on **every** commit. `vite build` clean at the end.
- No hardcoded hex or px values in component CSS — every value reads a token from `src/styles/tokens.css`.
- `lib/` is pure-utility: no React, no NATS, no async I/O.
- Components never deep-import `@/api/_transport/...`; wire types come from `@/api`.
- Test files live next to source (`Foo.jsx` → `Foo.test.jsx`), same folder, `package`-local.
- TDD is mandatory: write the failing test, **run it and confirm it fails**, then implement. Never skip the Red phase.
- Never mention model identifiers, session ids, or AI provenance in commit messages or code comments.
- No change to `docs/client-api.md` — nothing on the wire moves.
- Branch: `claude/message-preview-chat-frontend-08drl2`. Commit after each task.

## File Structure

**Created:**

| File | Responsibility |
|---|---|
| `src/lib/previewText.js` | Flatten a message body + attachments to a single-line snippet |
| `src/lib/previewText.test.js` | Its tests |
| `src/lib/participantName.js` | Resolve a Participant / Message to a display name |
| `src/lib/participantName.test.js` | Its tests |

**Modified:**

| File | Change |
|---|---|
| `src/api/types.ts` | `Participant.displayName`, new `PreviewMessage`, `SubscriptionRoom.previewMessage` |
| `src/api/index.ts` | Re-export `PreviewMessage` |
| `src/context/RoomEventsContext/reducer.js` | `previews` state + six writers |
| `src/context/RoomEventsContext/reducer.test.js` | Writer tests |
| `src/context/RoomEventsContext/RoomEventsContext.tsx` | `RoomPreview` type, state field, `useSidebarSections` join |
| `src/context/RoomEventsContext/RoomEventsContext.test.jsx` | Join test |
| `src/context/RoomEventsContext/useRoomSubscriptions.js` | Dispatch `ROOM_PREVIEW_UPDATED` |
| `src/components/shared/MessageList/MessageRow/MessageRow.jsx` | Delegate `senderName` to `lib/` |
| `src/components/MainApp/Sidebar/RoomList/RoomList.jsx` | Two-line `RoomItem` |
| `src/components/MainApp/Sidebar/RoomList/style.css` | Two-line row layout |
| `src/components/MainApp/Sidebar/RoomList/RoomList.test.jsx` | Render tests |

---

### Task 1: The flattener (`lib/previewText.js`)

Pure text transformation with no dependencies on React or state. Built first because every later task consumes it.

**Files:**
- Create: `src/lib/previewText.js`
- Test: `src/lib/previewText.test.js`

**Interfaces:**
- Consumes: `parseMessageContent(content, mentions)` from `src/lib/messageContent.js`, which returns nodes shaped `{type:'text',text}`, `{type:'mention',account,all,display}`, `{type:'link',href,text}`, `{type:'code',text}`, `{type:'codeblock',text}`, `{type:'strong'|'em'|'del',children}`. Also `attachmentKind(att)` from `src/lib/attachment.js`, returning `'image'|'audio'|'video'|'file'`.
- Produces:
  - `PREVIEW_MAX_LENGTH: number` (140)
  - `previewText(content, mentions?) → string`
  - `attachmentFallbackText(attachments) → string`
  - `previewSnippet(content, mentions, attachments) → string` ← the entry point Tasks 4 and 5 call

- [ ] **Step 1: Write the failing test**

Create `src/lib/previewText.test.js`:

```js
import { describe, it, expect } from 'vitest'
import { previewText, attachmentFallbackText, previewSnippet, PREVIEW_MAX_LENGTH } from './previewText'

describe('previewText', () => {
  it('returns plain text unchanged', () => {
    expect(previewText('hello there')).toBe('hello there')
  })

  it('returns empty string for empty, null, and undefined input', () => {
    expect(previewText('')).toBe('')
    expect(previewText(null)).toBe('')
    expect(previewText(undefined)).toBe('')
  })

  it('drops markdown emphasis markers but keeps the text', () => {
    expect(previewText('**bold** and *italic* and ~~struck~~')).toBe('bold and italic and struck')
  })

  it('flattens nested emphasis', () => {
    expect(previewText('**bold with _inner italic_ inside**')).toBe('bold with inner italic inside')
  })

  it('leaves the outer markers literal when emphasis nests with the same marker', () => {
    // The tokenizer's strong pattern forbids `*` inside its captured group, so
    // `**a *b* c**` never matches as strong — the outer asterisks stay literal.
    expect(previewText('**bold with *inner italic* inside**')).toBe('**bold with inner italic inside**')
  })

  it('drops inline code backticks', () => {
    expect(previewText('run `npm test` now')).toBe('run npm test now')
  })

  it('keeps fenced code block contents and collapses their newlines', () => {
    expect(previewText('see:\n```\nline one\nline two\n```')).toBe('see: line one line two')
  })

  it('leaves an unterminated fence as literal text', () => {
    expect(previewText('oops ```never closed')).toBe('oops ```never closed')
  })

  it('renders a resolved mention as @ plus the display name', () => {
    const mentions = [{ account: 'alice', engName: 'Alice Chen' }]
    expect(previewText('hey @alice ping', mentions)).toBe('hey @Alice Chen ping')
  })

  it('renders @all as @all', () => {
    expect(previewText('@all standup now')).toBe('@all standup now')
  })

  it('leaves an unresolved mention as literal text', () => {
    expect(previewText('hey @nobody there', [])).toBe('hey @nobody there')
  })

  it('renders a link as its visible label, never a separate href', () => {
    expect(previewText('see https://example.com/x now')).toBe('see https://example.com/x now')
  })

  it('collapses newlines and runs of whitespace to single spaces, then trims', () => {
    expect(previewText('  line one\n\n  line   two \t three  ')).toBe('line one line two three')
  })

  it('caps the result at PREVIEW_MAX_LENGTH characters', () => {
    const long = 'x'.repeat(500)
    const out = previewText(long)
    expect(out).toHaveLength(PREVIEW_MAX_LENGTH)
    expect(PREVIEW_MAX_LENGTH).toBe(140)
  })
})

describe('attachmentFallbackText', () => {
  it('returns empty string for no attachments', () => {
    expect(attachmentFallbackText(undefined)).toBe('')
    expect(attachmentFallbackText([])).toBe('')
  })

  it('labels an image Photo', () => {
    expect(attachmentFallbackText([{ imageUrl: '/a.png' }])).toBe('Photo')
  })

  it('labels audio and video', () => {
    expect(attachmentFallbackText([{ audioUrl: '/a.mp3' }])).toBe('Audio')
    expect(attachmentFallbackText([{ videoUrl: '/a.mp4' }])).toBe('Video')
  })

  it('uses a generic file attachment title', () => {
    expect(attachmentFallbackText([{ title: 'report.pdf', fileType: 'application/pdf' }])).toBe('report.pdf')
  })

  it('falls back to File for an untitled file attachment', () => {
    expect(attachmentFallbackText([{ title: '', fileType: 'application/pdf' }])).toBe('File')
  })

  it('classifies by the first attachment only', () => {
    expect(attachmentFallbackText([{ imageUrl: '/a.png' }, { title: 'b.pdf' }])).toBe('Photo')
  })
})

describe('previewSnippet', () => {
  it('prefers text over the attachment fallback', () => {
    expect(previewSnippet('look at this', [], [{ imageUrl: '/a.png' }])).toBe('look at this')
  })

  it('falls back to the attachment label when the text is empty', () => {
    expect(previewSnippet('', [], [{ imageUrl: '/a.png' }])).toBe('Photo')
  })

  it('returns empty string when there is neither text nor attachments', () => {
    expect(previewSnippet('', [], [])).toBe('')
  })
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npm test -- src/lib/previewText.test.js`
Expected: FAIL — `Failed to resolve import "./previewText"`.

- [ ] **Step 3: Write the implementation**

Create `src/lib/previewText.js`:

```js
// Flatten a message body into a single-line plain-text snippet for the sidebar
// room list. Reuses the message-content tokenizer so the snippet and the
// rendered message agree on what the body says. No React, no I/O.

import { parseMessageContent } from './messageContent'
import { attachmentKind } from './attachment'

// Hard cap on the returned string. CSS ellipsis truncates visually but leaves
// the whole string in the DOM, so without this a multi-thousand-character
// message sits in the sidebar for as long as it's the room's latest.
export const PREVIEW_MAX_LENGTH = 140

const KIND_LABEL = { image: 'Photo', audio: 'Audio', video: 'Video' }

/**
 * Flatten a message body to a single line of plain text.
 *
 * @param {string | null | undefined} content
 * @param {{ account?: string, engName?: string }[]} [mentions]
 * @returns {string} '' when there is nothing to show
 */
export function previewText(content, mentions = []) {
  if (!content) return ''
  const flat = flattenNodes(parseMessageContent(content, mentions))
  const collapsed = flat.replace(/\s+/g, ' ').trim()
  return collapsed.length > PREVIEW_MAX_LENGTH ? collapsed.slice(0, PREVIEW_MAX_LENGTH) : collapsed
}

/**
 * Label for a message whose body is empty but which carries attachments.
 *
 * @param {import('../api/types').Attachment[] | null | undefined} attachments
 * @returns {string} '' when there are none
 */
export function attachmentFallbackText(attachments) {
  const first = attachments?.[0]
  if (!first) return ''
  return KIND_LABEL[attachmentKind(first)] ?? (first.title || 'File')
}

/**
 * The sidebar snippet for a message: its flattened body, or an attachment
 * label when the body is empty. Single entry point for both preview writers.
 *
 * @param {string | null | undefined} content
 * @param {{ account?: string, engName?: string }[]} [mentions]
 * @param {import('../api/types').Attachment[] | null | undefined} [attachments]
 * @returns {string}
 */
export function previewSnippet(content, mentions, attachments) {
  return previewText(content, mentions) || attachmentFallbackText(attachments)
}

function flattenNodes(nodes) {
  let out = ''
  for (const node of nodes) out += flattenNode(node)
  return out
}

function flattenNode(node) {
  switch (node.type) {
    case 'mention':
      return `@${node.display}`
    // link/code/codeblock all carry their visible text on `.text`; a link's
    // href is deliberately never emitted separately.
    case 'link':
    case 'code':
    case 'codeblock':
      return node.text
    case 'strong':
    case 'em':
    case 'del':
      return flattenNodes(node.children)
    default:
      return node.text ?? ''
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test -- src/lib/previewText.test.js`
Expected: PASS, 22 tests.

If the unterminated-fence case fails, do **not** change the expectation — check that `parseMessageContent` emitted the backticks as plain text. The tokenizer's `FENCE_RE` requires a closing fence, so an unterminated one stays literal.

- [ ] **Step 5: Commit**

```bash
git add src/lib/previewText.js src/lib/previewText.test.js
git commit -m "feat(chat-frontend): add previewText flattener for sidebar snippets"
```

---

### Task 2: Display-name resolution (`lib/participantName.js`)

`MessageRow` keeps its display-name rule private. The preview writers need the same rule for two different shapes, so it moves to `lib/` with both consumers on one implementation.

Other name-resolution sites exist (`QuotedBlock.senderLabel`, and the optimistic quote targets in `ChatPage` and `ThreadRightBar`) and are deliberately left alone — they resolve quote snapshots, not `Participant` objects.

**Files:**
- Create: `src/lib/participantName.js`
- Test: `src/lib/participantName.test.js`
- Modify: `src/components/shared/MessageList/MessageRow/MessageRow.jsx:23-31`
- Test: `src/components/shared/MessageList/MessageRow/MessageRow.test.jsx`

**Interfaces:**
- Produces:
  - `participantDisplayName(participant) → string` — for a wire `Participant` (used on `PreviewMessage.sender`)
  - `messageSenderName(msg) → string` — for a `Message` (used on live messages and by `MessageRow`)

**Behavior change to be aware of:** the current `MessageRow.senderName` reads `msg.sender.engName` and never consults `msg.sender.displayName`, even though the Go `Participant` has carried `DisplayName` all along. The extracted helper prefers `displayName`. `msg.userDisplayName` still wins ahead of both, so this only changes rendering when the top-level field is absent and the nested one is present — for bot senders, `displayName` is the app name and is the better answer.

- [ ] **Step 1: Write the failing test**

Create `src/lib/participantName.test.js`:

```js
import { describe, it, expect } from 'vitest'
import { participantDisplayName, messageSenderName } from './participantName'

describe('participantDisplayName', () => {
  it('prefers the server-composed displayName', () => {
    expect(participantDisplayName({ displayName: 'Alice Chen', engName: 'Alice', account: 'alice' }))
      .toBe('Alice Chen')
  })

  it('falls back to engName, then account, then userId', () => {
    expect(participantDisplayName({ engName: 'Alice', account: 'alice' })).toBe('Alice')
    expect(participantDisplayName({ account: 'alice', userId: 'u1' })).toBe('alice')
    expect(participantDisplayName({ userId: 'u1' })).toBe('u1')
  })

  it('returns Unknown for null, undefined, and an empty participant', () => {
    expect(participantDisplayName(null)).toBe('Unknown')
    expect(participantDisplayName(undefined)).toBe('Unknown')
    expect(participantDisplayName({})).toBe('Unknown')
  })
})

describe('messageSenderName', () => {
  it('prefers the top-level userDisplayName', () => {
    expect(messageSenderName({ userDisplayName: 'Alice Chen', sender: { engName: 'Alice' } }))
      .toBe('Alice Chen')
  })

  it('falls back to the nested sender participant', () => {
    expect(messageSenderName({ sender: { displayName: 'Deploy Bot' } })).toBe('Deploy Bot')
    expect(messageSenderName({ sender: { engName: 'Alice' } })).toBe('Alice')
  })

  it('falls back to userAccount then userId when there is no sender', () => {
    expect(messageSenderName({ userAccount: 'alice' })).toBe('alice')
    expect(messageSenderName({ userId: 'u1' })).toBe('u1')
  })

  it('returns Unknown for an empty or missing message', () => {
    expect(messageSenderName({})).toBe('Unknown')
    expect(messageSenderName(null)).toBe('Unknown')
  })
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npm test -- src/lib/participantName.test.js`
Expected: FAIL — `Failed to resolve import "./participantName"`.

- [ ] **Step 3: Write the implementation**

Create `src/lib/participantName.js`:

```js
// Display-name resolution for message senders. One rule, two shapes: a wire
// Participant (nested on a PreviewMessage) and a Message (which carries the
// server-composed name at the top level). No React, no I/O.

/**
 * Resolve a wire Participant to a render-ready name. `displayName` is composed
 * server-side (engName + chineseName + account fallback; a bot's is its app
 * name), so it wins when present.
 *
 * @param {{ displayName?: string, engName?: string, account?: string, userId?: string } | null | undefined} participant
 * @returns {string}
 */
export function participantDisplayName(participant) {
  if (!participant) return 'Unknown'
  return participant.displayName || participant.engName || participant.account || participant.userId || 'Unknown'
}

/**
 * Resolve a Message's sender name. The top-level `userDisplayName` is the
 * server's render-ready composition and is preferred over the nested sender.
 *
 * @param {{ userDisplayName?: string, sender?: object, userAccount?: string, userId?: string } | null | undefined} msg
 * @returns {string}
 */
export function messageSenderName(msg) {
  if (!msg) return 'Unknown'
  if (msg.userDisplayName) return msg.userDisplayName
  if (msg.sender) return participantDisplayName(msg.sender)
  return msg.userAccount || msg.userId || 'Unknown'
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test -- src/lib/participantName.test.js`
Expected: PASS, 9 tests.

- [ ] **Step 5: Point MessageRow at the shared helper**

In `src/components/shared/MessageList/MessageRow/MessageRow.jsx`, add to the imports:

```jsx
import { messageSenderName } from '@/lib/participantName'
```

Then delete the local `senderName` function (lines 23-31, the block starting `function senderName(msg) {` and ending with its closing brace) and replace every call to `senderName(` in that file with `messageSenderName(`. There are two call sites: inside `senderInitial` and in the component body.

- [ ] **Step 6: Run the MessageRow tests to verify nothing regressed**

Run: `npm test -- src/components/shared/MessageList/MessageRow/MessageRow.test.jsx`
Expected: PASS, all existing tests green.

- [ ] **Step 7: Add the behavior-change test**

Append to `src/components/shared/MessageList/MessageRow/MessageRow.test.jsx`, inside the `describe('MessageRow', …)` block. The file has no render helper — every test inlines `render(<MessageRow … />)` against the module-level `msg` and `room` fixtures, so this one does too:

```js
  it('renders the nested sender displayName when userDisplayName is absent', () => {
    const botMsg = {
      id: 'm-bot',
      content: 'deploy finished',
      createdAt: '2026-05-13T10:42:00Z',
      sender: { account: 'bot', displayName: 'Deploy Bot' },
    }
    render(<MessageRow message={botMsg} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />)
    expect(screen.getByText('Deploy Bot')).toBeInTheDocument()
  })
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `npm test -- src/components/shared/MessageList/MessageRow/MessageRow.test.jsx`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/lib/participantName.js src/lib/participantName.test.js \
        src/components/shared/MessageList/MessageRow/MessageRow.jsx \
        src/components/shared/MessageList/MessageRow/MessageRow.test.jsx
git commit -m "refactor(chat-frontend): extract sender display-name resolution to lib

MessageRow held the only display-name rule in the codebase, private to the
component. The sidebar preview needs the same rule for a wire Participant, so
it moves to lib/ with both consumers on one implementation.

The Participant branch now prefers the server-composed displayName, which the
old inline version ignored despite Go having carried it all along."
```

---

### Task 3: Wire type mirrors

Pure type declarations. Its test is `npm run typecheck` — there is no runtime behavior to assert. Later tasks do not compile without it.

**Files:**
- Modify: `src/api/types.ts`
- Modify: `src/api/index.ts:62-95`
- Modify: `src/context/RoomEventsContext/RoomEventsContext.tsx`

**Interfaces:**
- Produces: `PreviewMessage` (exported from `@/api`), `Participant.displayName`, `SubscriptionRoom.previewMessage`, `RoomPreview`, `RoomEventsState.previews`, `RoomSummary.preview`

- [ ] **Step 1: Add `displayName` to `Participant`**

In `src/api/types.ts`, in the `Participant` interface (around line 169), add after `siteId`:

```ts
  /** Server-composed render-ready name (engName + chineseName + account
   *  fallback; a bot sender's is its app name). Prefer it over the raw
   *  fields. Mirrors model.Participant.DisplayName. */
  displayName?: string
```

- [ ] **Step 2: Add the `PreviewMessage` interface**

In `src/api/types.ts`, immediately after the `Attachment` interface (which ends around line 217), add:

```ts
/** Mirrors model.PreviewMessage — a room's most-recent eligible message,
 *  resolved server-side at read time for room-list rendering. Eligible means
 *  not soft-deleted and not a system message; the server walks back past an
 *  ineligible tail, so the client never re-implements that rule.
 *
 *  Delivered on `subscription.list` rows (`SubscriptionRoom.previewMessage`)
 *  and refreshed on `message_edited` / `message_deleted` events. */
export interface PreviewMessage {
  messageId: string
  sender: Participant
  /** The full message body; the client truncates for display. */
  content: string
  /** RFC3339. */
  createdAt: string
  attachments?: Attachment[]
  mentions?: Participant[]
  /** Always empty today — the server-side write path has not landed.
   *  Declared for forward-compat; nothing reads it. */
  visibleTo?: string
}
```

- [ ] **Step 3: Add `previewMessage` to `SubscriptionRoom`**

In `src/api/types.ts`, in the `SubscriptionRoom` interface (around line 30), add after `keyVersion`:

```ts
  /** The room's latest eligible message. Omitted when the room has no
   *  message, that site's enrichment degraded, or the request set
   *  `includeLastMessage: false`. Never present on live
   *  `subscription.update` events — only on `subscription.list` rows. */
  previewMessage?: PreviewMessage
```

- [ ] **Step 4: Re-export from the barrel**

In `src/api/index.ts`, inside the `export type { … } from './types'` block, add `PreviewMessage` on the line after `Participant`:

```ts
  Participant,
  PreviewMessage,
```

- [ ] **Step 5: Add the context types**

In `src/context/RoomEventsContext/RoomEventsContext.tsx`, add above the `RoomSummary` interface:

```ts
/** One room's stored sidebar preview. Flattened and name-resolved at write
 *  time so the render layer never branches on whether it came from a wire
 *  PreviewMessage or a live Message. */
interface RoomPreview {
  /** The previewed message's id — guards the delete-clears rule in the
   *  reducer's ROOM_PREVIEW_UPDATED case. */
  messageId: string
  senderName: string
  /** Single-line, flattened, capped at PREVIEW_MAX_LENGTH. */
  text: string
}
```

Add to the `RoomSummary` interface, after `hrInfo`:

```ts
  /** Joined in by useSidebarSections from state.previews. Undefined when the
   *  room has no preview — the row still renders at full height with a blank
   *  snippet line. */
  preview?: RoomPreview
```

Add to the `RoomEventsState` interface, after `subscriptions`:

```ts
  /** Keyed by roomId. Absent key = no preview to show. Deliberately NOT a
   *  field on RoomSummary: summaries are rebuilt from `Room` records, which
   *  mirror pkg/model.Room — and the backend hangs previewMessage off
   *  SubscriptionRoom instead, so a summary field would break the mirror. */
  previews: Record<string, RoomPreview>
```

Finally, add `RoomPreview` to the type re-export at the bottom of the file:

```ts
export type { RoomEventsState, RoomSummary, RoomPreview, RoomBufferState, RoomEventsContextValue }
```

- [ ] **Step 6: Run typecheck to verify it passes**

Run: `npm run typecheck`
Expected: PASS, no errors. If it reports that `previews` is missing on an object literal typed `RoomEventsState`, that is Task 4's job — leave it and note it; the reducer's `initialState` is JS and unchecked, so this should not happen.

- [ ] **Step 7: Run the full suite**

Run: `npm test`
Expected: PASS — no runtime behavior changed.

- [ ] **Step 8: Commit**

```bash
git add src/api/types.ts src/api/index.ts src/context/RoomEventsContext/RoomEventsContext.tsx
git commit -m "types(chat-frontend): mirror PreviewMessage and Participant.displayName"
```

---

### Task 4: Reducer — `previews` state and the message-derived writers

Adds the state key and every writer whose source is a message the client already holds. The event-sourced writer is Task 5.

**Files:**
- Modify: `src/context/RoomEventsContext/reducer.js`
- Test: `src/context/RoomEventsContext/reducer.test.js`

**Interfaces:**
- Consumes: `previewSnippet`, `previewText` (Task 1); `participantDisplayName`, `messageSenderName` (Task 2)
- Produces: `state.previews: Record<roomId, {messageId, senderName, text}>`; module-private `previewFromMessage(msg)` and `previewFromWire(previewMessage)`, both returning the stored shape or `null`. Task 5 uses `previewFromWire`.

- [ ] **Step 1: Write the failing tests**

Append to `src/context/RoomEventsContext/reducer.test.js`. Match the file's existing import style — it already imports `roomEventsReducer` and `initialState`; add nothing new to the imports.

```js
describe('roomEventsReducer previews', () => {
  const wirePreview = (over = {}) => ({
    messageId: 'm1',
    sender: { account: 'alice', displayName: 'Alice Chen' },
    content: 'hello **there**',
    createdAt: '2026-08-14T10:00:00Z',
    ...over,
  })

  it('starts empty', () => {
    expect(initialState.previews).toEqual({})
  })

  it('BUCKETS_LOADED seeds a preview from sub.room.previewMessage', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [], appIds: [], channelDmIds: ['r1'],
      rooms: [{ id: 'r1', name: 'General', type: 'channel', siteId: 'site-A', userCount: 2 }],
      subscriptions: { r1: { roomId: 'r1', name: 'General', room: { previewMessage: wirePreview() } } },
    })
    expect(next.previews.r1).toEqual({ messageId: 'm1', senderName: 'Alice Chen', text: 'hello there' })
  })

  it('BUCKETS_LOADED leaves a room without a previewMessage absent from the map', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [], appIds: [], channelDmIds: ['r1'],
      rooms: [{ id: 'r1', name: 'General', type: 'channel', siteId: 'site-A', userCount: 2 }],
      subscriptions: { r1: { roomId: 'r1', name: 'General', room: {} } },
    })
    expect(next.previews.r1).toBeUndefined()
  })

  it('MESSAGE_RECEIVED overwrites the preview for a room with no message buffer', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: { id: 'm2', content: 'newest', userDisplayName: 'Bob Lin', createdAt: '2026-08-14T11:00:00Z' },
      },
    })
    expect(next.previews.r1).toEqual({ messageId: 'm2', senderName: 'Bob Lin', text: 'newest' })
  })

  it('MESSAGE_RECEIVED uses the attachment fallback when the body is empty', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: {
          id: 'm3', content: '', userDisplayName: 'Bob Lin',
          createdAt: '2026-08-14T11:00:00Z', attachments: [{ imageUrl: '/a.png' }],
        },
      },
    })
    expect(next.previews.r1.text).toBe('Photo')
  })

  it('MESSAGE_RECEIVED ignores a thread reply', () => {
    // roomState MUST be seeded with the parent. Without it the dispatch exits at
    // the thread block's `if (!tPrev) return state` guard and never reaches the
    // path under test — the assertion would then hold even if the preview write
    // were hoisted above the thread guard, i.e. it would not catch the regression
    // it exists to catch. Assert on the parent too, proving the thread path ran.
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm1', senderName: 'A', text: 'first' } },
      roomState: {
        r1: { ...emptyBuffer(), messages: [{ id: 'm1', content: 'first', createdAt: '2026-08-14T11:00:00Z' }] },
      },
    }
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: { id: 'm9', content: 'reply', threadParentMessageId: 'm1', createdAt: '2026-08-14T12:00:00Z' },
      },
    })
    expect(next.previews.r1.messageId).toBe('m1')
  })

  it('MESSAGE_RECEIVED keeps the previews reference on a same-content echo', () => {
    // The server echo of a message this client already stored optimistically must
    // not allocate a new previews object — a fresh reference invalidates
    // useSidebarSections' memo for every room in the sidebar.
    const msg = { id: 'm2', content: 'newest', userDisplayName: 'Bob Lin', createdAt: '2026-08-14T11:00:00Z' }
    const first = roomEventsReducer(initialState, { type: 'MESSAGE_RECEIVED', event: { roomId: 'r1', message: msg } })
    const second = roomEventsReducer(first, { type: 'MESSAGE_RECEIVED', event: { roomId: 'r1', message: msg } })
    expect(second.previews).toBe(first.previews)
  })

  it('MESSAGE_SENT_LOCAL previews the local send optimistically', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_SENT_LOCAL',
      roomId: 'r1',
      message: { id: 'm4', content: 'typed by me', userDisplayName: 'Me', createdAt: '2026-08-14T13:00:00Z', _local: true },
    })
    expect(next.previews.r1).toEqual({ messageId: 'm4', senderName: 'Me', text: 'typed by me' })
  })

  it('MESSAGE_EDITED_LOCAL rewrites the preview only when the edited message is the one on display', () => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm5', senderName: 'Me', text: 'before' } },
      roomState: { r1: { ...emptyBuffer(), messages: [{ id: 'm5', content: 'before' }] } },
    }
    const hit = roomEventsReducer(seeded, {
      type: 'MESSAGE_EDITED_LOCAL', roomId: 'r1', messageId: 'm5',
      content: 'after', editedAt: '2026-08-14T14:00:00Z',
    })
    expect(hit.previews.r1.text).toBe('after')

    const miss = roomEventsReducer(seeded, {
      type: 'MESSAGE_EDITED_LOCAL', roomId: 'r1', messageId: 'm5-other',
      content: 'irrelevant', editedAt: '2026-08-14T14:00:00Z',
    })
    expect(miss.previews.r1.text).toBe('before')
  })

  it('MESSAGE_DELETED_LOCAL leaves the preview alone', () => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm6', senderName: 'Me', text: 'doomed' } },
      roomState: { r1: { ...emptyBuffer(), messages: [{ id: 'm6', content: 'doomed' }] } },
    }
    const next = roomEventsReducer(seeded, { type: 'MESSAGE_DELETED_LOCAL', roomId: 'r1', messageId: 'm6' })
    expect(next.previews.r1.text).toBe('doomed')
  })

  it('SUBSCRIPTION_UPSERTED never seeds a preview', () => {
    // Its `added` payload embeds a `room` object, which makes it look like a
    // seeding source — but docs/client-api.md specifies previewMessage is
    // always omitted there. Regression guard against wiring it up.
    const next = roomEventsReducer(initialState, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: {
        roomId: 'r1', roomType: 'channel', siteId: 'site-A', name: 'General',
        // A previewMessage is deliberately present: the real wire never carries one
        // here, so an empty `room` would make this guard vacuously true.
        room: { previewMessage: wirePreview() },
      },
    })
    expect(next.previews).toEqual({})
  })

  it('ROOM_REMOVED drops the room preview', () => {
    const seeded = { ...initialState, previews: { r1: { messageId: 'm7', senderName: 'A', text: 'x' } } }
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'r1' })
    expect(next.previews.r1).toBeUndefined()
  })

  it('RESET clears every preview', () => {
    const seeded = { ...initialState, previews: { r1: { messageId: 'm8', senderName: 'A', text: 'x' } } }
    expect(roomEventsReducer(seeded, { type: 'RESET' }).previews).toEqual({})
  })
})
```

Add this helper near the top of the same `describe` block (the file may already have an equivalent — reuse it if so, and delete this one):

```js
function emptyBuffer() {
  return {
    messages: [], hasLoadedHistory: false, historyError: null, unreadCount: 0,
    hasMention: false, mentionAll: false, lastMsgAt: null, lastMsgId: null,
    bufferMode: 'live', pendingLiveMessages: [], focusMessageId: null,
    hasMoreOlder: true, loadingOlder: false,
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test -- src/context/RoomEventsContext/reducer.test.js -t previews`
Expected: FAIL — `expected undefined to equal {}` on the first case, and `Cannot read properties of undefined` on the rest.

- [ ] **Step 3: Add the state key and the two builders**

In `src/context/RoomEventsContext/reducer.js`, add to the imports at the top:

```js
import { previewSnippet, previewText } from '@/lib/previewText'
import { participantDisplayName, messageSenderName } from '@/lib/participantName'
```

Add to `initialState`, after the `subscriptions: {}` entry:

```js
  /**
   * Per-roomId sidebar preview: the room's most recent eligible message,
   * flattened to a single line at write time. An absent key means there is
   * nothing to show — the row still renders at full height with a blank
   * snippet line (the row's height is a CSS property, not a consequence of
   * this map).
   *
   * Shape: { [roomId]: { messageId, senderName, text } }
   */
  previews: {},
```

Add these two builders next to `toSummary` (above `mergeSubscriptionIntoSummary`):

```js
// Build a stored preview from a live message. Returns null when there is
// nothing to store, so callers leave the map untouched rather than writing an
// empty sentinel.
function previewFromMessage(msg) {
  if (!msg || !msg.id) return null
  return {
    messageId: msg.id,
    senderName: messageSenderName(msg),
    text: previewSnippet(msg.content, msg.mentions, msg.attachments),
  }
}

// Two stored previews are interchangeable when all three rendered fields match.
// Used to keep the previews reference stable on a same-content write (e.g. the
// server echo of a message this client already stored optimistically) — a fresh
// object would invalidate useSidebarSections' memo for every room in the sidebar.
function samePreview(a, b) {
  return !!a && !!b && a.messageId === b.messageId && a.senderName === b.senderName && a.text === b.text
}

// Build a stored preview from a wire PreviewMessage (subscription.list rows
// and the refreshed preview on edit/delete events).
function previewFromWire(previewMessage) {
  if (!previewMessage || !previewMessage.messageId) return null
  return {
    messageId: previewMessage.messageId,
    senderName: participantDisplayName(previewMessage.sender),
    text: previewSnippet(previewMessage.content, previewMessage.mentions, previewMessage.attachments),
  }
}
```

- [ ] **Step 4: Wire `BUCKETS_LOADED`**

In the `BUCKETS_LOADED` case, after the `summaries` if/else and before the `return`, insert:

```js
      // Seed from the room metadata user-service embeds on each list row.
      // Starts from the existing map so the partial-update path (no rooms
      // supplied) doesn't wipe previews already in hand.
      const previews = { ...state.previews }
      for (const [roomId, sub] of Object.entries(subs)) {
        const preview = previewFromWire(sub?.room?.previewMessage)
        if (preview) previews[roomId] = preview
      }
```

and add `previews,` to that case's returned object.

- [ ] **Step 5: Wire `MESSAGE_RECEIVED`**

In the `MESSAGE_RECEIVED` case, immediately after the thread-reply block closes — that is, right after `const roomId = evt.roomId` — insert:

```js
      // Thread replies returned above, so anything reaching here belongs in
      // the room timeline and is a preview candidate. Computed once and
      // applied at every return point below.
      const nextPreview = previewFromMessage(msg)
      const previews =
        !nextPreview || samePreview(state.previews[roomId], nextPreview)
          ? state.previews
          : { ...state.previews, [roomId]: nextPreview }
```

Then add `previews,` to **all three** returned objects in this case: the historical-buffer-mode return, the `existingIdx >= 0` optimistic-echo return, and the final live-append return. Missing any one of them means previews silently stop updating in that mode.

- [ ] **Step 6: Wire the local-optimistic and lifecycle cases**

In `MESSAGE_SENT_LOCAL`, after `const messages = appendBounded(prev.messages, msg)`, insert:

```js
      // A thread reply doesn't appear in the room timeline, so it isn't the
      // room's preview either — matching the server, which omits
      // previewMessage for hidden thread replies.
      const nextPreview = msg.threadParentMessageId ? null : previewFromMessage(msg)
      const previews =
        !nextPreview || samePreview(state.previews[roomId], nextPreview)
          ? state.previews
          : { ...state.previews, [roomId]: nextPreview }
```

and add `previews,` to its returned object.

In `MESSAGE_EDITED_LOCAL`, after `const messages = [...]`, insert:

```js
      // Only the message currently on display affects the preview. The
      // action carries no mentions, so mention tokens flatten literally
      // here; the server's message_edited event follows with the
      // authoritative preview a moment later.
      const cur = state.previews[action.roomId]
      const previews =
        cur && cur.messageId === action.messageId
          ? { ...state.previews, [action.roomId]: { ...cur, text: previewText(action.content) } }
          : state.previews
```

and add `previews,` to its returned object.

In `MESSAGE_DELETED_LOCAL`, add **nothing**. Leave a comment above its `return`:

```js
      // Preview deliberately untouched: the client can't reproduce the
      // server's walk-back to an earlier eligible message. The authoritative
      // message_deleted event corrects it.
```

In `ROOM_REMOVED`, after the `subscriptions` block, insert:

```js
      let previews = state.previews
      if (previews[action.roomId]) {
        const { [action.roomId]: _dropPreview, ...restPreviews } = previews
        previews = restPreviews
      }
```

and add `previews,` to its returned object.

`RESET` returns `initialState`, which now carries `previews: {}` — no edit needed, but its test stays as a regression guard.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `npm test -- src/context/RoomEventsContext/reducer.test.js`
Expected: PASS — the new `previews` block plus every pre-existing reducer test.

- [ ] **Step 8: Run the full suite and typecheck**

Run: `npm test && npm run typecheck`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/context/RoomEventsContext/reducer.js src/context/RoomEventsContext/reducer.test.js
git commit -m "feat(chat-frontend): track per-room message previews in the reducer"
```

---

### Task 5: Reducer — `ROOM_PREVIEW_UPDATED`

The event-sourced writer. Separate from Task 4 because it is the one case a reviewer might reject on its own: its clear rule is the subtlest logic in the feature.

**Files:**
- Modify: `src/context/RoomEventsContext/reducer.js`
- Test: `src/context/RoomEventsContext/reducer.test.js`

**Interfaces:**
- Consumes: `previewFromWire` (Task 4)
- Produces: the `ROOM_PREVIEW_UPDATED` action contract that Task 6 dispatches:
  `{ type: 'ROOM_PREVIEW_UPDATED', roomId: string, previewMessage?: PreviewMessage, deletedMessageId?: string }`

- [ ] **Step 1: Write the failing tests**

Append to the `describe('roomEventsReducer previews', …)` block in `src/context/RoomEventsContext/reducer.test.js`:

```js
  describe('ROOM_PREVIEW_UPDATED', () => {
    const seeded = () => ({
      ...initialState,
      previews: { r1: { messageId: 'm1', senderName: 'Alice Chen', text: 'old body' } },
    })

    it('overwrites from the event previewMessage', () => {
      const next = roomEventsReducer(seeded(), {
        type: 'ROOM_PREVIEW_UPDATED',
        roomId: 'r1',
        previewMessage: {
          messageId: 'm2',
          sender: { account: 'bob', displayName: 'Bob Lin' },
          content: 'edited body',
          createdAt: '2026-08-14T15:00:00Z',
        },
      })
      expect(next.previews.r1).toEqual({ messageId: 'm2', senderName: 'Bob Lin', text: 'edited body' })
    })

    it('updates a room that has no message buffer', () => {
      const next = roomEventsReducer(initialState, {
        type: 'ROOM_PREVIEW_UPDATED',
        roomId: 'r-unopened',
        previewMessage: {
          messageId: 'm3',
          sender: { account: 'bob', displayName: 'Bob Lin' },
          content: 'from a room I never opened',
          createdAt: '2026-08-14T15:00:00Z',
        },
      })
      expect(next.previews['r-unopened'].text).toBe('from a room I never opened')
    })

    it('clears when the deleted message is the one on display and no preview follows', () => {
      const next = roomEventsReducer(seeded(), {
        type: 'ROOM_PREVIEW_UPDATED', roomId: 'r1', deletedMessageId: 'm1',
      })
      expect(next.previews.r1).toBeUndefined()
    })

    it('does NOT clear when the deleted message is a different one', () => {
      const next = roomEventsReducer(seeded(), {
        type: 'ROOM_PREVIEW_UPDATED', roomId: 'r1', deletedMessageId: 'm-other',
      })
      expect(next.previews.r1.text).toBe('old body')
    })

    it('leaves the preview alone when neither a previewMessage nor a deletedMessageId is given', () => {
      const next = roomEventsReducer(seeded(), { type: 'ROOM_PREVIEW_UPDATED', roomId: 'r1' })
      expect(next.previews.r1.text).toBe('old body')
    })

    it('ignores an action with no roomId', () => {
      const state = seeded()
      expect(roomEventsReducer(state, { type: 'ROOM_PREVIEW_UPDATED' })).toBe(state)
    })
  })
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test -- src/context/RoomEventsContext/reducer.test.js -t ROOM_PREVIEW_UPDATED`
Expected: FAIL — the reducer falls through to its default case, so the preview never changes and the overwrite cases fail first.

- [ ] **Step 3: Write the implementation**

In `src/context/RoomEventsContext/reducer.js`, add a new case next to `MESSAGE_DELETED`:

```js
    case 'ROOM_PREVIEW_UPDATED': {
      // The room's refreshed preview after an edit or delete, computed
      // server-side. Its own action rather than a field on MESSAGE_EDITED /
      // MESSAGE_DELETED because both of those bail when the room has no
      // message buffer — which is the normal case for a sidebar row.
      const { roomId, previewMessage, deletedMessageId } = action
      if (!roomId) return state
      const next = previewFromWire(previewMessage)
      if (next) return { ...state, previews: { ...state.previews, [roomId]: next } }
      // No preview on the event. For a delete that means nothing eligible is
      // left — but only when the deleted message is the one on display. Any
      // other id is contradictory input (the server would have echoed the
      // unchanged preview), so leave what's there.
      const cur = state.previews[roomId]
      if (!deletedMessageId || !cur || cur.messageId !== deletedMessageId) return state
      const { [roomId]: _cleared, ...rest } = state.previews
      return { ...state, previews: rest }
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test -- src/context/RoomEventsContext/reducer.test.js`
Expected: PASS, including all pre-existing cases.

- [ ] **Step 5: Commit**

```bash
git add src/context/RoomEventsContext/reducer.js src/context/RoomEventsContext/reducer.test.js
git commit -m "feat(chat-frontend): apply server-refreshed previews on edit and delete"
```

---

### Task 6: Dispatch `ROOM_PREVIEW_UPDATED` and join previews into the sidebar

The context wiring: dispatch the action from live events, and expose the result through `useSidebarSections`. These ship together because the join is what makes the dispatch observable — there is no other reader of `state.previews`.

Placement matters: the dispatch must happen **before** the existing early return that drops edits with no plaintext body, or encrypted-room edits never update the sidebar.

**Files:**
- Modify: `src/context/RoomEventsContext/useRoomSubscriptions.js:196-221`
- Modify: `src/context/RoomEventsContext/RoomEventsContext.tsx` (`useSidebarSections`)
- Test: `src/context/RoomEventsContext/RoomEventsContext.test.jsx`

**Interfaces:**
- Consumes: the `ROOM_PREVIEW_UPDATED` action contract from Task 5; `state.previews` from Task 4; `RoomSummary.preview` from Task 3
- Produces: `RoomSummary.preview` populated on every room `useSidebarSections` returns — Task 7 renders it

- [ ] **Step 1: Write the failing tests**

Append to `src/context/RoomEventsContext/RoomEventsContext.test.jsx`, at the end of the `describe('RoomEventsProvider', …)` block. This uses the file's existing `mockNats` / `wrap` / `roomToSub` helpers and its established pattern of capturing subscription callbacks in a `handlers` Map.

First add the probe, next to the file's other probes (`SummariesProbe`, `EventsProbe`):

```jsx
function PreviewProbe() {
  const sections = useSidebarSections()
  const rooms = sections.flatMap((s) => s.rooms)
  return (
    <div data-testid="previews">
      {rooms
        .map((r) => `${r.id}=${r.preview ? `${r.preview.senderName}|${r.preview.text}` : ''}`)
        .join(';')}
    </div>
  )
}
```

Then the tests:

```jsx
  describe('sidebar previews', () => {
    // A channel room whose subscription.list row carries a seeded preview.
    const seededSub = () => ({
      ...roomToSub({ id: 'g1', name: 'General', type: 'channel', siteId: 'site-A', userCount: 3 }),
      room: {
        userCount: 3,
        lastMsgAt: '2026-08-14T10:00:00Z',
        previewMessage: {
          messageId: 'm1',
          sender: { account: 'alice', displayName: 'Alice Chen' },
          content: 'original body',
          createdAt: '2026-08-14T10:00:00Z',
        },
      },
    })

    function setup() {
      const request = vi.fn().mockImplementation((subject, payload) => {
        if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
          return Promise.resolve({ subscriptions: [seededSub()] })
        return Promise.resolve({ subscriptions: [] })
      })
      const handlers = new Map()
      const subscribe = vi.fn().mockImplementation((subject, cb) => {
        handlers.set(subject, cb)
        return { unsubscribe: vi.fn() }
      })
      return { nats: mockNats({ request, subscribe }), handlers, subscribe }
    }

    it('seeds the sidebar preview from subscription.list', async () => {
      const { nats, subscribe } = setup()
      render(wrap(<PreviewProbe />, nats))
      await waitFor(() => expect(subscribe).toHaveBeenCalled())
      await waitFor(() =>
        expect(screen.getByTestId('previews').textContent).toBe('g1=Alice Chen|original body')
      )
    })

    it('refreshes the preview from a message_edited event', async () => {
      const { nats, handlers, subscribe } = setup()
      render(wrap(<PreviewProbe />, nats))
      await waitFor(() => expect(subscribe).toHaveBeenCalled())
      await waitFor(() => expect(handlers.has('chat.room.g1.event')).toBe(true))

      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'message_edited',
          roomId: 'g1',
          messageId: 'm1',
          newContent: 'edited body',
          editedAt: '2026-08-14T15:00:00Z',
          previewMessage: {
            messageId: 'm1',
            sender: { account: 'alice', displayName: 'Alice Chen' },
            content: 'edited body',
            createdAt: '2026-08-14T15:00:00Z',
          },
        })
      })
      await waitFor(() =>
        expect(screen.getByTestId('previews').textContent).toBe('g1=Alice Chen|edited body')
      )
    })

    it('refreshes the preview for an encrypted edit carrying no plaintext body', async () => {
      const { nats, handlers, subscribe } = setup()
      render(wrap(<PreviewProbe />, nats))
      await waitFor(() => expect(subscribe).toHaveBeenCalled())
      await waitFor(() => expect(handlers.has('chat.room.g1.event')).toBe(true))

      // No `newContent` — the mutation itself is dropped, but the sidebar
      // snippet must still refresh from the plaintext previewMessage.
      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'message_edited',
          roomId: 'g1',
          messageId: 'm1',
          encryptedNewContent: { version: 1, nonce: 'n', ciphertext: 'c' },
          editedAt: '2026-08-14T15:00:00Z',
          previewMessage: {
            messageId: 'm1',
            sender: { account: 'alice', displayName: 'Alice Chen' },
            content: 'server-side plaintext',
            createdAt: '2026-08-14T15:00:00Z',
          },
        })
      })
      await waitFor(() =>
        expect(screen.getByTestId('previews').textContent).toBe('g1=Alice Chen|server-side plaintext')
      )
    })

    it('clears the preview when the displayed message is deleted and none follows', async () => {
      const { nats, handlers, subscribe } = setup()
      render(wrap(<PreviewProbe />, nats))
      await waitFor(() => expect(subscribe).toHaveBeenCalled())
      await waitFor(() => expect(handlers.has('chat.room.g1.event')).toBe(true))

      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'message_deleted',
          roomId: 'g1',
          messageId: 'm1',
          deletedBy: 'alice',
          deletedAt: '2026-08-14T16:00:00Z',
        })
      })
      await waitFor(() => expect(screen.getByTestId('previews').textContent).toBe('g1='))
    })

    it('keeps the preview when a different message is deleted', async () => {
      const { nats, handlers, subscribe } = setup()
      render(wrap(<PreviewProbe />, nats))
      await waitFor(() => expect(subscribe).toHaveBeenCalled())
      await waitFor(() => expect(handlers.has('chat.room.g1.event')).toBe(true))

      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'message_deleted',
          roomId: 'g1',
          messageId: 'm-someone-else',
          deletedBy: 'alice',
          deletedAt: '2026-08-14T16:00:00Z',
        })
      })
      // Give any dispatch a chance to land before asserting nothing changed.
      await act(async () => { await Promise.resolve() })
      expect(screen.getByTestId('previews').textContent).toBe('g1=Alice Chen|original body')
    })
  })
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test -- src/context/RoomEventsContext/RoomEventsContext.test.jsx -t "sidebar previews"`
Expected: FAIL — every case renders `g1=` because `useSidebarSections` doesn't join `preview` yet, and nothing dispatches `ROOM_PREVIEW_UPDATED`.

- [ ] **Step 3: Add the join**

In `src/context/RoomEventsContext/RoomEventsContext.tsx`, in `useSidebarSections`, widen the destructure and the enrich:

```ts
  const { summaries, subscriptions, chatlist, previews } = state
  return useMemo(() => {
    const enrich = (room: RoomSummary): RoomSummary => {
      const sub = subscriptions[room.id]
      const preview = previews[room.id]
      if (!sub && !preview) return room
      return {
        ...room,
        subscriptionName: sub?.name ?? room.subscriptionName,
        hrInfo: sub?.hrInfo ?? room.hrInfo,
        preview,
      }
    }
    const sections = deriveSidebarSections(summaries, subscriptions, chatlist) as SidebarSection[]
    return sections.map((s) => ({ ...s, rooms: s.rooms.map(enrich) }))
  }, [summaries, subscriptions, chatlist, previews])
```

The `if (!sub && !preview) return room` early return preserves the previous identity-stable behavior for rooms with neither. `previews` joins the dependency list — omitting it would freeze the sidebar on the first render's previews.

- [ ] **Step 4: Run the seed test to verify it passes**

Run: `npm test -- src/context/RoomEventsContext/RoomEventsContext.test.jsx -t "seeds the sidebar preview"`
Expected: PASS. The other four still fail — no dispatch yet.

- [ ] **Step 5: Write the dispatch implementation**

In `src/context/RoomEventsContext/useRoomSubscriptions.js`, in `handleMutationEvent`, change the `message_edited` branch so the preview dispatch precedes the plaintext guard:

```js
      if (evt?.type === 'message_edited' && evt.messageId) {
        const { messageId, newContent, editedAt } = evt
        // Preview first, and unconditionally: the sidebar snippet must update
        // even when the edit itself can't be applied — an encrypted body
        // returns below, and MESSAGE_EDITED bails for a room with no buffer.
        // The server omits previewMessage for hidden thread-reply edits, so
        // no client-side thread guard is needed here.
        if (evt.previewMessage) {
          safeDispatch({
            type: 'ROOM_PREVIEW_UPDATED',
            roomId: evt.roomId,
            previewMessage: evt.previewMessage,
          })
        }
        // Drop edits without a plaintext body. Encrypted channel rooms emit
        // `encryptedNewContent` instead; blanking the existing content to ''
        // would silently wipe the message until decryption is implemented.
        if (typeof newContent !== 'string') return true
```

Leave the rest of that branch unchanged.

Then change the `message_deleted` branch to dispatch the preview action first:

```js
      if (evt?.type === 'message_deleted' && evt.messageId) {
        const { messageId } = evt
        // deletedMessageId lets the reducer clear the preview only when the
        // deleted message is the one on display; an absent previewMessage
        // means nothing eligible is left in the room.
        safeDispatch({
          type: 'ROOM_PREVIEW_UPDATED',
          roomId: evt.roomId,
          previewMessage: evt.previewMessage,
          deletedMessageId: messageId,
        })
        safeDispatch({ type: 'MESSAGE_DELETED', roomId: evt.roomId, messageId })
        fanThreadMutation({ kind: 'deleted', messageId })
        return true
      }
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `npm test -- src/context/RoomEventsContext/RoomEventsContext.test.jsx`
Expected: PASS — all five preview cases plus every pre-existing test in the file.

- [ ] **Step 7: Run the full suite and typecheck**

Run: `npm test && npm run typecheck`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/context/RoomEventsContext/useRoomSubscriptions.js \
        src/context/RoomEventsContext/RoomEventsContext.tsx \
        src/context/RoomEventsContext/RoomEventsContext.test.jsx
git commit -m "feat(chat-frontend): dispatch preview updates and join them into the sidebar

The dispatch precedes the plaintext-body guard so an encrypted-room edit still
refreshes the sidebar snippet — broadcast-worker relays previewMessage in
plaintext even when it encrypts the message body."
```

---

### Task 7: Render the second line

The visible deliverable, and a pure component change — `RoomList.test.jsx` already mocks `useSidebarSections`, so this task asserts on the DOM with no context in play.

**Files:**
- Modify: `src/components/MainApp/Sidebar/RoomList/RoomList.jsx:19-43`
- Modify: `src/components/MainApp/Sidebar/RoomList/style.css:33-69`
- Test: `src/components/MainApp/Sidebar/RoomList/RoomList.test.jsx`

**Interfaces:**
- Consumes: `RoomSummary.preview` — `{ messageId, senderName, text }`, joined by `useSidebarSections` in Task 6
- Produces: the rendered sidebar row; nothing downstream depends on it

- [ ] **Step 1: Write the failing render tests**

Append to `src/components/MainApp/Sidebar/RoomList/RoomList.test.jsx`, inside the outermost `describe`. The file already has `summary(id, overrides)`, `section(key, rooms, over)`, and `setup(sections)` helpers — use them.

**Use `setup(...)`, never `useSidebarSections.mockReturnValue(...)` directly.** `setup` primes all four mocked hooks; priming only `useSidebarSections` leaves the other three returning `undefined`, and `RoomList` crashes destructuring `useChatlistActions()`. Such a test still passes in a full-file run — `vi.clearAllMocks()` clears call history but not implementations, so a previous test's `setup()` leaks through — and fails the moment it runs alone. That is exactly the order-dependence `CLAUDE.md` forbids.

```js
  const preview = { messageId: 'm1', senderName: 'Alice Chen', text: 'hey are we still on' }

  it('renders the sender prefix and snippet in a channel row', () => {
    setup([section('chats', [summary('r1', { preview })])])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText('Alice Chen:')).toBeInTheDocument()
    expect(screen.getByText('hey are we still on')).toBeInTheDocument()
  })

  it('omits the sender prefix in a DM row', () => {
    setup([section('chats', [summary('r1', { type: 'dm', preview })])])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.queryByText('Alice Chen:')).not.toBeInTheDocument()
    expect(screen.getByText('hey are we still on')).toBeInTheDocument()
  })

  it('renders the snippet line empty when the room has no preview', () => {
    setup([section('chats', [summary('r1')])])
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    const line = container.querySelector('.room-preview')
    expect(line).toBeInTheDocument()   // reserved, so the row height is stable
    // toBe, not toHaveTextContent: the latter is a substring check that every
    // string satisfies, so it would assert nothing.
    expect(line.textContent).toBe('')
  })

  it('renders the attachment label a preview carries as its text', () => {
    setup([section('chats', [summary('r1', { preview: { ...preview, text: 'Photo' } })])])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText('Photo')).toBeInTheDocument()
  })
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test -- src/components/MainApp/Sidebar/RoomList/RoomList.test.jsx -t preview`
Expected: FAIL — `Unable to find an element with the text: Alice Chen:`, and `.room-preview` is null.

- [ ] **Step 3: Restructure `RoomItem`**

In `src/components/MainApp/Sidebar/RoomList/RoomList.jsx`, replace the `RoomItem` return block. The name, badges, and counts move into a title row; the whole text column becomes one flex child so the drag handle keeps its place:

```jsx
    <div
      className={classes.join(' ')}
      onClick={() => onSelectRoom(room)}
      draggable
      onDragStart={(e) => onDragStartRoom(e, room)}
      onDragOver={(e) => e.preventDefault()}
      onDrop={(e) => {
        e.stopPropagation()
        onDropOnRoom(room)
      }}
    >
      <span className="room-drag-handle" aria-hidden="true">⋮⋮</span>
      <div className="room-item-body">
        <div className="room-item-title">
          <span className="room-name">
            {roomPrefix(room.type)}{roomDisplayName(room)}
          </span>
          {mentionBadge(room)}
          <span className="room-meta">{room.userCount}</span>
          {unread && <span className="room-badge-unread">{room.unreadCount}</span>}
        </div>
        {/* Always rendered, even with no preview — the row's height must not
            depend on whether this line has content, or the sidebar reflows
            as previews arrive during bootstrap. */}
        <div className="room-preview">
          {room.preview && room.type !== 'dm' && room.type !== 'botDM' && (
            <span className="room-preview-sender">{room.preview.senderName}: </span>
          )}
          {room.preview?.text}
        </div>
      </div>
    </div>
```

- [ ] **Step 4: Add the styles**

In `src/components/MainApp/Sidebar/RoomList/style.css`, add after the `.room-name` rule:

```css
.room-item-body {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: var(--space-xs);
}

.room-item-title {
  display: flex;
  align-items: center;
  gap: var(--space-sm);
}

/* Always occupies a line, whether or not it has content — a room with no
   preview must not render a shorter row. */
.room-preview {
  font-size: var(--text-xs);
  color: var(--text-muted);
  line-height: 1.4;
  min-height: 1.4em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.room-preview-sender {
  color: var(--text-secondary);
}
```

`.room-name` already carries `flex: 1; min-width: 0` and its own ellipsis rules — leave it untouched; it now flexes inside `.room-item-title` instead of `.room-item`.

- [ ] **Step 5: Run the render tests to verify they pass**

Run: `npm test -- src/components/MainApp/Sidebar/RoomList/RoomList.test.jsx`
Expected: PASS, including the pre-existing drag-and-drop and section tests. If a pre-existing test queried a badge as a direct child of `.room-item`, update its selector to the new nesting — that is a legitimate consequence of the restructure, not a behavior change.

- [ ] **Step 6: Run every gate**

Run: `npm test && npm run typecheck && npm run build`
Expected: all PASS, build clean.

- [ ] **Step 7: Commit**

```bash
git add src/components/MainApp/Sidebar/RoomList/RoomList.jsx \
        src/components/MainApp/Sidebar/RoomList/style.css \
        src/components/MainApp/Sidebar/RoomList/RoomList.test.jsx
git commit -m "feat(chat-frontend): render the last-message preview in the sidebar

Room rows are now two lines: name and badges above, sender plus a flattened
one-line snippet below. The snippet line is always rendered, so a room with no
preview keeps full row height rather than reflowing the list as previews land."
```

---

## Verification

After Task 7, confirm the whole feature end to end:

- [ ] `npm test` — full suite green
- [ ] `npm run typecheck` — no errors
- [ ] `npm run build` — clean production build
- [ ] `git log --oneline` shows seven feature commits on `claude/message-preview-chat-frontend-08drl2`
- [ ] `git diff main --stat` touches only the files in the File Structure table
- [ ] Confirm no `docs/client-api.md` change is present in the diff — nothing on the wire moved

## Out of scope

Do not add, even if it seems natural while working:

- Timestamps on the preview line
- Bolding the snippet for unread rooms
- Previews in the search results pane or thread list
- URL unfurling or link preview cards
- Mention or attachment *indicators* — the text fallback in Task 1 is the whole of it
- Sending `includeLastMessage: false` on `subscription.list` — the default (include) is what this feature needs
