# admin-frontend Chat Client Implementation Plan — Phase 2

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add a NATS chat client to the existing `admin-frontend/` console app so an admin (already logged in via `/admin-login`) can also chat: see rooms/DMs, read & send messages, DM a user, and manage room members — via a new "Chat" section in the app shell.

**Architecture:** No shared package exists, so the chat stack is **copied** from `chat-frontend/src` into `admin-frontend/src` (essential set + room crypto) and wired to connect over NATS in `session` mode using the login `bundle`. `AuthContext` remains the console gate + holds `authToken` for admin HTTP; a new `NatsProvider` owns the live NATS link. They coordinate only at login (feed bundle → connect) and logout (tear both down).

**Tech Stack:** adds `nats.ws@^1.30.0`, `nkeys.js@^1.1.0` (uuid already present). Pure WebCrypto for room decryption (no WASM/worker/dep). `oidc-client-ts` is **avoided** via a stub.

## Global Constraints

- Copy files **verbatim** from `chat-frontend/src/<path>` to `admin-frontend/src/<path>` unless a step says otherwise; preserve the `@/` imports (same alias in both apps). Copied code is a port of already-tested logic — do NOT rewrite it.
- chat-frontend/CLAUDE.md conventions hold. Components import from `@/api` only (never `_transport`). Errors via `formatAsyncJobError`; branch on `reason ?? code`.
- The chat `api/index.ts` barrel and admin's existing `api/index.ts` (which exports `parseHttpEnvelopeError` + admin ops) must be **MERGED** — never overwrite. The two error systems (`AsyncJobError` for NATS, `httpEnvelope` for admin HTTP) coexist.
- `useJwtRefresh` statically imports `@/api/auth/oidcClient`; in `session` mode its two functions are dead code. **Stub** `admin-frontend/src/api/auth/oidcClient.js` with throwing/no-op versions so `oidc-client-ts` is never needed.
- Provider order (gate the RoomKeys/RoomEvents/ThreadEvents stack on `nats.connected`): `ThemeProvider → DebugProvider → ErrorBoundary → NatsProvider → [connected] → RoomKeysProvider → RoomEventsProvider → ThreadEventsProvider`.
- DM targeting is **free-text account entry** (no `searchUsers` op exists) — do not invent one.
- Never log/leak the session token. `npm run build` + `npm run typecheck` + `npx vitest run` must pass on every task (run from `admin-frontend/`).
- Commit trailers per repo git rules.

---

### Task 1: NATS foundation (deps + transport + connection layer)

**Copy verbatim** (chat-frontend/src → admin-frontend/src), then wire:
- `api/_transport/subjects.ts`, `api/_transport/asyncJob.ts`, `api/_transport/normalizeMessage.ts`, `api/types.ts`
- `lib/jwtExpiry.js`, `lib/idgen.js`, `lib/messageBuffer.js`, `lib/roomFormat.js`, `lib/constants.js` (copy whichever exist; skip any not present)
- `context/DebugContext/*`, `context/ThemeContext/*`
- `context/NatsContext/{NatsContext.jsx, useJwtRefresh.js, index.jsx}`

**New/edit:**
- Add `nats.ws@^1.30.0`, `nkeys.js@^1.1.0` to `admin-frontend/package.json`; `npm install`.
- **Stub** `admin-frontend/src/api/auth/oidcClient.js`:
```js
// SSO is not used by the admin app (session-token login only). These are dead
// code in NatsContext mode:'session'; stubbed so oidc-client-ts isn't a dependency.
export async function renewSsoToken() { throw new Error('sso disabled in admin-frontend') }
export async function redirectToReloginOnTokenInvalid() {}
export function isSSOTokenInvalidError() { return false }
```
- **Merge** the chat barrel exports into `admin-frontend/src/api/index.ts` (add `requestSync`/`requestWithAsyncResult` re-exports as chat-frontend's barrel does, `AsyncJobError`, `ASYNC_JOB_ERROR_KINDS`, `formatAsyncJobError` if not already, and the domain types) WITHOUT removing the existing admin exports (`parseHttpEnvelopeError`, admin ops, `botLogin`/`changePassword`). Reconcile any duplicate symbol (both files may export `AsyncJobError`/`formatAsyncJobError` — keep ONE source of truth: the chat `asyncJob.ts` version is the fuller one; have `httpEnvelope.ts` re-use it or keep them separate under distinct names — verify no type clash at build).
- `main.jsx`: wrap the tree `ThemeProvider → DebugProvider → App` (import the two `styles/*` still first).
- `App.jsx`: wrap inside `ErrorBoundary` (already there) with `NatsProvider` around the authed subtree.

- [ ] **Step 1:** Copy the files above; add deps; `npm install`.
- [ ] **Step 2:** Stub oidcClient; merge the barrel; wire providers in main.jsx/App.jsx.
- [ ] **Step 3:** Resolve the barrel/error-type reconciliation until `npm run typecheck` is clean and `npm run build` succeeds. Fix import paths as needed (everything is `@/`-aliased, should resolve).
- [ ] **Step 4:** Write a smoke test `src/context/NatsContext/NatsContext.smoke.test.jsx`: render `<NatsProvider>` with a stubbed `nats.ws` (`vi.mock('nats.ws')`) and assert `useNats()` exposes `{connected:false, connect, disconnect, request, ...}` without throwing. Run `npx vitest run` → green (existing 116 still pass).
- [ ] **Step 5:** Commit `feat(admin-frontend): NATS transport + connection layer (chat foundation)`.

---

### Task 2: Chat API operations

**Copy verbatim** these `api/<op>/` folders (each is a thin `subjects`-only wrapper): `fetchSidebarBuckets`, `fetchMessageHistory`, `fetchSurroundingMessages`, `sendMessage`, `editMessage`, `deleteMessage`, `markRoomRead`, `getUnreadCount`, `fetchReadReceipt`, `searchRooms`, `createRoom`, `listRoomMembers`, `addMembers`, `removeMember`, `updateMemberRole`, `listOrgMembers`, `requestRoomKey`, `subscribeToRoomEvents`, `subscribeToUserRoomEvents`, `subscribeToSubscriptionUpdates`, `subscribeToRoomMetadataUpdates`, `subscribeToRoomKeyEvents`.

- [ ] **Step 1:** Copy the op folders verbatim (preserve `index.ts` per folder).
- [ ] **Step 2:** Extend `api/index.ts` to re-export each op (mirror chat-frontend's barrel entries for these ops). Keep admin ops intact.
- [ ] **Step 3:** `npm run typecheck` clean; `npm run build` clean; `npx vitest run` green. (These ops carry no new deps.)
- [ ] **Step 4:** Commit `feat(admin-frontend): chat NATS api operations`.

---

### Task 3: Room crypto + event contexts

**Copy verbatim:** `lib/roomcrypto/{index.ts,roomcrypto.ts}`; `context/RoomKeysContext/{RoomKeysContext.tsx,reducer.ts,index.tsx}`; `context/RoomEventsContext/{RoomEventsContext.tsx,reducer.js,useRoomSubscriptions.js,useUnreadCount.js,index.jsx}`; `context/ThreadEventsContext/{ThreadEventsContext.jsx,reducer.js,index.jsx}`.

- [ ] **Step 1:** Copy the three context folders + `lib/roomcrypto`. They import only `@/api`, `@/context/NatsContext`, `@/lib/*` (all present after Tasks 1–2).
- [ ] **Step 2:** `npm run typecheck` clean; `npm run build` clean. Resolve any missing transitive import by copying that file too (e.g. a `lib/*` helper) — list any additions in the report.
- [ ] **Step 3:** Copy the reducers' colocated tests (`RoomEventsContext/reducer.test.js`, `ThreadEventsContext/reducer.test.js` if they exist) verbatim — they're pure-JS and validate the copied logic. Run `npx vitest run` → green.
- [ ] **Step 4:** Commit `feat(admin-frontend): room crypto + room/thread event contexts`.

---

### Task 4: MainApp UI tree

**Copy verbatim** the essential component set + reach:
- `components/MainApp/{MainApp.jsx,index.jsx,style.css}`
- `components/MainApp/AppHeader/**` (AppHeader + `UnreadBadge/**` + `ThemeToggle/**`; skip `SearchBar`, `DebugLevelSelect`, `DebugPayloadToggle` — edit AppHeader.jsx to drop those imports/usages if present)
- `components/MainApp/Sidebar/**` (Sidebar + `RoomList/**` + `CreateRoomDialog/**`)
- `components/MainApp/ChatPage/**` (ChatPage + `RoomMessageArea/**` + `RoomMessageInput/**` + `RoomMembersBadge/**` + `ManageMembersDialog/**` incl `AddMembersForm/**`,`MemberPicker/**`,`MemberRoster/**`; skip `InRoomSearch`, `LeaveRoomButton` — trim their imports from ChatPage.jsx)
- `components/shared/MessageList/**`, `components/shared/MessageInputForm/**`, `components/shared/QuotedBlock/**`, `components/shared/TextInputDialog/**`, `components/shared/DeleteConfirmDialog/**`
- `hooks/useHoverWithDelay.js`
- Reuse admin's existing `shared/{Modal,LazyFallback,ErrorBoundary}` + `hooks/useDebouncedSearch` (do NOT re-copy).

- [ ] **Step 1:** Copy the component folders + `useHoverWithDelay`. For `MainApp.jsx`/`ChatPage.jsx`/`AppHeader.jsx`, remove references to the intentionally-skipped extras (`SearchResultsPane`, `ThreadRightBar` if you skip threads — BUT ThreadEvents provider is kept, so `ThreadRightBar` may stay; `InRoomSearch`, `SearchBar`, `LeaveRoomButton`, debug selects). Keep threads if `MainApp` hard-depends on `ThreadRightBar` — copy `ThreadRightBar/**` too rather than gut the component (simpler + lower-risk). Record what you trimmed vs kept.
- [ ] **Step 2:** `npm run typecheck` clean; `npm run build` clean. Resolve missing imports by copying the referenced file (log additions).
- [ ] **Step 3:** Copy a few high-value colocated tests that are pure (e.g. `MessageList`/`MessageRow` render gates, a reducer-ish test) to guard the copy — do NOT copy the entire chat test suite. Run `npx vitest run` → green.
- [ ] **Step 4:** Commit `feat(admin-frontend): MainApp chat UI tree`.

---

### Task 5: Wiring — connect after login + Chat section

**Edits (the genuinely new glue):**
1. `context/AuthContext/AuthContext.jsx`: expose the FULL bundle to the connect step. Keep `session={authToken,account,siteId}` for the console, but add a way to retrieve the stored full bundle (e.g. export `getStoredBundle()` or return the bundle from `login`). Do NOT widen the exposed `session`.
2. `pages/AdminLoginPage/AdminLoginPage.jsx`: after a successful `login(bundle)` (both the no-change and post-change paths), call `useNats().connect({ mode:'session', bundle })` so the NATS link comes up. Handle a connect failure by showing an error but staying logged into the console (chat degrades, console still works).
3. **Session-store reconciliation:** NatsContext persists its own `sessionStorage['chat.botSession']` and auto-resumes on mount. On admin **logout** (`AuthContext.logout`), also call `useNats().disconnect()` (clears `chat.botSession`). On mount, if `admin.session` exists, let NatsContext's own resume bring chat back up (or trigger connect from the restored bundle) — pick one and make logout tear down BOTH keys. Document the choice.
4. `components/AppShell/AppShell.jsx`: add `{key:'chat', label:'Chat'}` to `SECTIONS`; `const ChatSection = lazy(()=>import('@/components/MainApp'))`; render it when `section==='chat'`, wrapped in the `nats.connected`-gated `RoomKeysProvider → RoomEventsProvider → ThreadEventsProvider` stack (hoist these providers to wrap the whole shell, gated on `useNats().connected`, so switching sections doesn't tear down subscriptions). Show a "connecting…"/"chat unavailable" state when `!connected`.

- [ ] **Step 1: Failing wiring tests** (mock `@/api`, `nats.ws`, and the contexts as needed):
  - AdminLoginPage: successful login calls BOTH `login(bundle)` AND `nats.connect({mode:'session',bundle})`; a connect failure still leaves the console usable.
  - AppShell: a "Chat" nav item exists; selecting it renders the chat section when `connected`, and a placeholder when `!connected`; Logout calls `logout` AND `nats.disconnect`.
- [ ] **Step 2:** Run → red.
- [ ] **Step 3:** Implement the four edits above.
- [ ] **Step 4:** Run → green; `npm run typecheck` clean; `npm run build` clean.
- [ ] **Step 5:** Commit `feat(admin-frontend): connect NATS after login + Chat section in shell`.

---

### Task 6: README + final verification

- [ ] **Step 1:** Update `admin-frontend/README.md`: the app now includes chat (search/DM/rooms/messages/member management); note NATS deps + that chat connects via session mode after login; DM targeting is free-text account entry.
- [ ] **Step 2:** Full gate: `npm run build`, `npm run typecheck`, `npx vitest run` all green. Grep the whole `src/` to confirm the session token is never `console.*`-logged.
- [ ] **Step 3:** Commit `docs(admin-frontend): document chat client`.

---

## Self-Review

- **Coverage:** connection layer → T1; api ops → T2; crypto + event state → T3; UI → T4; the new wiring (connect-after-login, section, session reconciliation) → T5; docs/gate → T6.
- **Risks flagged in the manifest are addressed:** oidc stub (T1), barrel merge (T1), crypto included (T3), dual-session reconciliation (T5).
- **Copied vs new:** the bulk is verbatim ports (already tested upstream); the only genuinely new code is the T5 wiring, which is TDD'd.

## After Phase 2

Squash the ENTIRE admin-frontend (Phase 1 console + Phase 2 chat) into ONE commit on top of the admin-service backend commit, then force-push the branch.
