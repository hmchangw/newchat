# Chatlist Custom Sections v2 — frontend read-model + section management

Implements the P3 client read-model from PRD #27. Wire contract per the `feat/chatlist-sections`
backend branch (docs/client-api.md + events.md + request-reply.md). Backend RPCs are UNMERGED,
so the FE ships against the documented shapes with a **dev-only mock adapter** that flips to live
once the backend lands — no rework.

## Wire contract (authoritative, from the backend branch docs)

Subscription gains two optional fields: `sectionId?: string`, `sectionOrder?: number` (fractional).
`favorite` is already a subscription boolean on the backend branch. Built-in sections are DERIVED
client-side, never stored on the sub.

RPCs (user-scoped `chat.user.{account}.request.user.{siteID}.…`):
- `chatlist.get` — no body → `ChatlistState`
- `chatlist.section.create` `{ name, sortMode? }` → ChatlistState (sortMode default `custom`)
- `chatlist.section.rename` `{ sectionId, name }`
- `chatlist.section.delete` `{ sectionId }` (members orphan → chats)
- `chatlist.section.reorder` `{ sectionOrder: string[] }` (permutation of ALL section ids)
- `chatlist.section.setsortmode` `{ sectionId, sortMode }`

Move a chat (room-scoped `chat.user.{account}.request.room.{roomID}.{siteID}.chat.move`):
- body `{ sectionId: string|null, afterRoomId?: string }` → `{ status:'ok', sectionId?, sectionOrder? }`
- built-in target rejected (`chatlist_builtin_target`); `null` removes → derived built-in.

Events:
- `chat.user.{account}.event.chatlist.update` → `ChatlistUpdateEvent { timestamp, chatlist: ChatlistState }`
  — full-state replace, LWW by `chatlist.lastUpdatedAt`.
- `subscription.update` gains `action: "section_moved"`; `subscription.sectionId/sectionOrder` carry
  the new placement (both absent = removed).

Types:
- `ChatlistState { sectionOrder: string[]; sections: ChatlistSection[]; lastUpdatedAt: number }`
- `ChatlistSection { id: string; name: string; builtIn: boolean; sortMode: 'custom'|'mostRecent' }`

Name validation (create/rename): 1–50 chars trimmed, no consecutive spaces, only
letters/digits/space/`-_./()`, unique per user. Errors: `chatlist_invalid_name`,
`chatlist_duplicate_name`, `chatlist_section_not_found`, `chatlist_builtin_immutable`,
`chatlist_invalid_order`, `chatlist_invalid_sort_mode`, `chatlist_builtin_target`.

## Client read model (PRD §"Client read model")

1. subscription.list (existing 3-bucket fetch stays) → each sub carries favorite/sectionId/sectionOrder.
2. chatlist.get once → section definitions.
3. Group each sub: favorites(favorite) · apps(bot) · teams(sectionId=='teams') · each custom
   (sectionId==id) · chats(no sectionId, not fav/bot). Orphan sectionId → chats.
4. Within a section: sortMode 'custom' → by sectionOrder; else by last message.
5. Live: subscription.update + chatlist.update each replace their own scope, timestamp LWW.

## Integration seams (verified against current code, session 2026-08-01)

- Types: `chat-frontend/src/api/types.ts` (Subscription:49, SubscriptionUpdateAction:323, add Chatlist* types).
- Subjects: `chat-frontend/src/api/_transport/subjects.ts`.
- Reducer: `chat-frontend/src/context/RoomEventsContext/reducer.js` (SUBSCRIPTION_UPSERTED, mergeSubscriptionIntoSummary, sortByLastMsgDesc, initialState).
- Grouping seam: `useSidebarSections()` in `RoomEventsContext.tsx` (hardcoded 3 sections today).
- Event wiring: `context/RoomEventsContext/useRoomSubscriptions.js` (subscribeToSubscriptionUpdates catch-all is where section_moved lands; bootstrap chatlist.get + subscribe chatlist.update).
- Render: `components/MainApp/Sidebar/RoomList/RoomList.jsx` (already section-aware — maps `{key,title,rooms}`).

## Build order (TDD, commit per layer)

1. **Types + subjects** — add fields/types + subject builders. Typecheck.
2. **API ops + mock adapter** — one folder per op (getChatlist, section CRUD ×5, moveChat, subscribeToChatlistUpdates). Mock adapter behind the barrel, toggled via runtimeConfig (`VITE_CHATLIST_MOCK` / runtime flag). op-arg→payload tests.
3. **Reducer/state** — chatlist section-def state, carry section fields onto subs+summaries, CHATLIST_LOADED/CHATLIST_UPDATED, section_moved. Pure reducer tests.
4. **Grouping read-model** — rewrite useSidebarSections to the derived model + within-section sort. Unit tests on the derivation.
5. **UI** — section headers + CRUD affordances in RoomList, create/rename/delete dialog, move-chat control, native HTML5 drag reorder (within + between sections). Component tests.
6. **Mock demo + polish** — fixture that seeds sections + membership + simulated events; `/simplify` + terse-comment pass; typecheck + test + build green; screenshots.

## Delivery
- Branch `feat/chatlist-sections-fe` off main (this clone: voldemort-frontdesk/newchat).
- Draft PR on hmchangw/newchat → pr-self-audit (internal gate) + /simplify → surface to Jacob, wait for review → ready + `ready` label.
- Screenshots to the frontdesk channel (no raw preview URL upstream).
- Docs: this is FE-only; the client-api.md contract lands with backend #134. No doc-ratchet change here (no wire struct authored by us).

## Mock adapter note
The mock keeps an in-memory ChatlistState + section membership and emulates the RPC replies +
fires chatlist.update / subscription.update(section_moved) callbacks, so the real reducer/hook
path exercises end-to-end. Flipping the flag off routes every op to the real NATS subject.
