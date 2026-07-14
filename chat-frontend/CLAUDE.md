# chat-frontend conventions

These rules describe how this codebase is laid out and what patterns to keep using. They were established across the structural refactor that landed in PR #183 — when in doubt, mirror what's already there.

The root `/CLAUDE.md` (project-wide Go/microservices guidelines) still applies for anything cross-cutting. This file is the frontend-specific extension.

## Folder layout

```
src/
├── App.jsx                   ← top-level routing (login / oidc-callback / MainApp)
├── main.jsx                  ← Vite entry
├── api/                      ← every backend interaction
│   ├── _transport/           ← INTERNAL: subjects.ts, asyncJob.ts, normalizeMessage.ts
│   ├── <operation>/index.ts  ← one folder per RPC / pub / sub wrapper
│   ├── types.ts              ← shared wire types mirroring pkg/model
│   └── index.ts              ← public barrel — components import from here
├── components/
│   ├── MainApp/              ← folder-per-component, nested by UI tree
│   │   ├── MainApp.jsx
│   │   ├── style.css
│   │   ├── index.jsx         ← re-exports default
│   │   ├── AppHeader/
│   │   ├── Sidebar/
│   │   ├── ChatPage/
│   │   ├── ThreadRightBar/
│   │   └── SearchResultsPane/
│   └── shared/               ← used by 2+ branches of the tree
│       ├── MessageList/
│       ├── MessageInputForm/
│       ├── Modal/
│       ├── ErrorBoundary/
│       ├── LazyFallback/
│       └── …
├── context/
│   ├── <Context>/<Context>.tsx (or .jsx)
│   ├── <Context>/reducer.js          ← state machine if non-trivial
│   ├── <Context>/use<X>.js          ← extracted hooks (e.g. useRoomSubscriptions)
│   └── <Context>/index.jsx          ← re-export
├── pages/
│   ├── LoginPage/
│   └── OidcCallback/
├── hooks/                    ← React hooks reusable across components
├── lib/                      ← pure utilities only (constants, idgen, runtimeConfig, roomFormat)
└── styles/
    ├── tokens.css            ← design tokens + light/dark theme roles
    └── index.css             ← global reset, .btn family, dialog primitives, scrollbars
```

**Rules of placement:**

- **`api/`** owns every line of code that talks to NATS, JetStream, or any backend HTTP. If a function builds a NATS subject string, calls `nc.request` / `nc.publish` / `nc.subscribe`, or wraps `requestWithAsyncResult`, it lives under `api/<operation>/`.
- **`api/_transport/`** is internal. Only files inside `api/` may reach into it. Components MUST go through `api/index.ts`. (`formatAsyncJobError`, `AsyncJobError`, `ASYNC_JOB_ERROR_KINDS` are re-exported from the barrel — never deep-import them.)
- **`context/`** owns React-context providers, their reducers, and their context-specific hooks. The state machine reducer lives next to its consumer.
- **`hooks/`** is for cross-cutting hooks (`useDebouncedSearch`, `useHoverWithDelay`). Context-specific hooks stay in the context folder.
- **`lib/`** is pure-utility — no React, no NATS, no async I/O. If your file imports React or talks to a backend, it doesn't belong here.

## API layer

### One folder per operation

Every backend RPC / publish / subscription gets its own folder under `src/api/`:

```
api/createRoom/index.ts
api/sendMessage/index.ts
api/markRoomRead/index.ts
api/subscribeToRoomEvents/index.ts
…
```

Each operation is a single function with the signature:

```ts
export function operationName(nats: Nats, args: OperationArgs, opts?: OperationOpts): ReturnType
```

`nats` is always the first argument (the value from `useNats()`). Operation functions destructure what they need (`{ user, request }`, `{ user, publish }`, `{ user, requestWithAsyncResult }`, etc.) and call the underlying NATS primitive internally. **Hide the subject and the transport** — callers don't know whether their op is req/reply, JetStream publish, or two-phase async.

### Transport selection

| Operation kind | Primitive | Example |
|---|---|---|
| Sync RPC, immediate reply | `nats.request<T>(subject, payload)` | `searchRooms`, `listRoomMembers`, `getRoom` |
| Fire-and-forget event | `nats.publish(subject, payload)` | `sendMessage`, `editMessage`, `markRoomRead` |
| Two-phase async job | `nats.requestWithAsyncResult(subject, payload, opts?)` | `createRoom`, `addMembers`, `removeMember`, `updateMemberRole` |
| Subscription | `nats.subscribe(subject, cb)` | `subscribeToRoomEvents`, `subscribeToSubscriptionUpdates` |

When the op accepts `opts`, **always pass it through** — even when undefined. No `opts ? fn(s,p,opts) : fn(s,p)` ternary. Tests assert on the 3-arg shape.

### Types

- Shared wire types in `api/types.ts` mirror `pkg/model/*.go`. Re-exported from `api/index.ts`.
- Each operation declares its own `Args` and `Response` interfaces in its `index.ts` if not already covered by a shared type.
- `request<T>` is generic — every op passes its response type through. **Don't accept `Promise<any>`** at the call site.
- Discriminated subscription kinds: `Subscription` is the base (channels / botDMs / discussions); `DMSubscription extends Subscription` adds `hrInfo?: SubscriptionHRInfo` for DM rooms. State maps that hold either use `Record<string, DMSubscription>` so consumers read `.hrInfo` without narrowing.

### Error envelope (server contract)

Every backend error — NATS sync replies, JetStream async results (`AsyncJobResult`), and HTTP — comes back in one shape, owned by the backend's `pkg/errcode` package:

```ts
type ErrorEnvelope = {
  error: string                        // human-readable, user-safe message
  code: ErrorCode                      // always present — drives UX category + HTTP status
  reason?: string                      // optional domain code (when frontend must distinguish)
  metadata?: Record<string, string>    // optional structured detail
}
type ErrorCode =
  | 'bad_request' | 'unauthenticated' | 'forbidden' | 'not_found'
  | 'conflict' | 'too_many_requests' | 'unavailable' | 'internal'
```

- **Branch on `reason ?? code`** — `reason` when present (e.g. `max_room_size_reached`, `not_subscribed`, `sso_token_expired`), `code` otherwise.
- **Never branch on `error` text** — wording can change without notice; only display it.
- `code: 'internal'` always carries the message `"internal error"`. The real cause is logged server-side and never reaches the client.
- `formatAsyncJobError` and the shared transport throwers (`NatsContext.request`, `asyncJob.ts`) already throw an `AsyncJobError` with `.code` / `.reason` populated — consumers just read those fields. Don't re-parse the message.

Reasons emitted today (full catalog in [`docs/client-api.md`](../docs/client-api.md) §6):
- `max_room_size_reached`, `not_room_member`, `not_room_owner`, `last_owner_cannot_leave`, `bot_in_channel`, `bot_not_available`, `user_not_found`, `invalid_org`, `self_dm`, `last_member_cannot_remove`, `target_not_member`, `already_owner`, `cannot_demote_last_owner`, `promote_requires_individual` — room-service / room-worker
- `large_room_post_restricted`, `not_subscribed`, `outside_access_window` — message-gatekeeper / history-service
- `sso_token_expired`, `invalid_sso_token` — auth-service (drive a redirect-to-relogin)
- `invalid_request`, `invalid_nkey`, `missing_fields` — auth-service / portal-service (form-validation surface; rarely actionable by the UI today)
- `account_not_ready` — portal-service lookup (account absent from the HR directory cache, or present there but not provisioned in the users collection; show "contact your administrator" copy)

When adding a new client-facing branch in the UI, prefer matching a reason over a message substring. If the case you need isn't in the catalog, ask backend to add a `Reason` constant in `pkg/errcode/codes_<service>.go` rather than substring-matching the english text.

`formatAsyncJobError` is now reason-keyed: it looks up the thrown `AsyncJobError`'s `.reason` against an internal `REASON_COPY` map and returns the humanized english copy when present, falling back to `err.message` otherwise. Components calling `setError(formatAsyncJobError(err))` get the right UX automatically without their own per-call switch on reason. To add a new humanized line, edit `REASON_COPY` in `chat-frontend/src/api/_transport/asyncJob.ts` and add the reason to the catalog above.

**DM-exists is a SUCCESS reply, not an error.** When a client requests a DM that already exists, the reply is `{ status: 'exists', roomId: <existing> }` — open that room. The legacy error-shaped reply (`{ error: 'dm already exists', roomId: … }`) is still accepted by `isDMExistsReply` during the backend rollout window, then removed in a follow-up release. The cutover (extending the sync error decoder + `AsyncJobResult` decoder + `isDMExistsReply` to handle both shapes, plus the typecheck/test/smoke gates) is plan Chapter 19 in `docs/superpowers/plans/2026-05-28-centralized-error-codes.md`; it ships in the same release as the backend.

### What components do

```jsx
import { useNats } from '@/context/NatsContext'
import { createRoom, formatAsyncJobError } from '@/api'

const nats = useNats()
// …
try {
  const { sync } = await createRoom(nats, { name, users, orgs, channels }, { treatAsSuccess: isDMExistsReply })
  // …
} catch (err) {
  setError(formatAsyncJobError(err))
}
```

Components **never** import from `@/api/_transport/...`. If you need a transport helper that isn't re-exported from `@/api/index.ts`, add a re-export.

## Path aliases

`@/` resolves to `src/`. Use it for any import that climbs ≥2 levels. Single-up `../sibling` and same-folder `./child` stay relative — those still read fine.

Configured in `vite.config.js`, `vitest.config.js`, and `tsconfig.json`. All three must agree.

## TypeScript

- **`api/` is TypeScript.** Every operation file is `.ts`. Wire types in `api/types.ts` mirror Go structs field-by-field with the JSON tag names.
- **`allowJs: true`, `checkJs: false`** — components, contexts (except `RoomEventsContext.tsx`), lib, and tests stay `.js`/`.jsx`. Migrate incrementally; don't force the whole codebase to `.ts` in one go.
- **Strict mode is on.** When converting a file, type properly; don't use `any` as an escape hatch.
- `npm run typecheck` is the source of truth. Runs in CI on every PR.

## Components

### Folder-per-component

Every component lives in its own folder with:

```
ComponentName/
├── ComponentName.jsx        ← the component
├── ComponentName.test.jsx   ← unit tests
├── style.css                ← component-specific styles
└── index.jsx                ← `export { default } from './ComponentName'`
```

The `index.jsx` re-export lets imports stay stable: `import X from '@/components/.../X'` always works whether `X` is a single file or a folder.

**When to skip the folder:** if a component is a single `.jsx` file with no test, no style, and no children, a flat file is fine. Don't manufacture overhead.

### Lazy loading

Conditionally-rendered surfaces (dialogs, side panels, settings pages) are `React.lazy`'d:

```jsx
const ManageMembersDialog = lazy(() => import('./ManageMembersDialog/ManageMembersDialog'))

// …
{showMembers && (
  <Suspense fallback={<LazyFallback variant="dialog" />}>
    <ManageMembersDialog room={room} onClose={…} />
  </Suspense>
)}
```

Use `@/components/shared/LazyFallback` — `variant="dialog"` for modal-style, `inline` (default) for side panels. Never `fallback={null}` — a click that opens a lazy chunk shouldn't visually drop.

### Error boundary

`<ErrorBoundary>` at `App.jsx` catches render-time errors. Default fallback offers Try Again (clears state) and Reload (page reload). Render-prop API gives `{error, reset, reload}` for custom recovery UI.

The boundary does NOT catch event-handler errors, async errors outside render, or effect errors. For those, hook into `window.onerror` or `unhandledrejection` at the entry point.

## Styling

- **Design tokens in `src/styles/tokens.css`** — colors, spacing, typography, motion, radii. Light theme on `:root`, dark theme on `:root[data-theme='dark']`. Every component reads via `var(--token)`; **no hardcoded hex / px values in component CSS.**
- **`src/styles/index.css`** holds genuinely global rules: CSS reset, `body` font, focus-visible ring, `.btn` family, `.dialog` primitives (overlay, card, inputs, action row), themed scrollbars, `@keyframes flash-jump`. Keep it small.
- **Per-component `style.css`** for component-scoped rules. Import from the component: `import './style.css'`.
- **No CSS modules / styled-components / Tailwind.** Plain CSS with the design-token system is the convention.

## Context + state

### When to reducer, when to hook

- **Reducer** when (a) more than ~5 actions, (b) invariants must hold between multiple fields, or (c) you want state transitions independently testable. `roomEventsReducer` and `threadEventsReducer` qualify.
- **Plain hook** (`useState` + `useEffect`) for everything simpler. `ThemeContext` is the exemplar.

Reducers live next to their context (`context/<Name>/reducer.js`). Keep them pure — every action returns a new state object; no side effects.

### Stale-cycle protection

Long-lived async callbacks (post-login fetches, deferred dispatches) must check a generation counter at write time:

```js
const gen = currentGeneration()
const resp = await fetchSomething(nats, …)
if (currentGeneration() !== gen) return  // user reconnected; drop this dispatch
dispatch({ type: 'SOMETHING_LOADED', data: resp })
```

`useRoomSubscriptions` owns the counter; consumers read it via the returned `currentGeneration()` getter.

### Subscription state

`state.subscriptions[roomId]` is the canonical per-user-per-room record (mirrors `pkg/model.Subscription` / `DMSubscription`). Populated by:
- `BUCKETS_LOADED` (initial — three user-service RPCs in parallel via `fetchSidebarBuckets`)
- `SUBSCRIPTION_UPSERTED` (live deltas from `subscription.update` events — merges partial payloads into the prior record)

Consumers read via `useSubscription(roomId): DMSubscription | undefined`. Use it for:
- Permission gating (`sub.roles.includes('owner')`)
- Mute state (`sub.alert`)
- DM display labels (`sub.hrInfo`)

When the user opens a room, `setActiveRoom`:
1. Dispatches `SET_ACTIVE_ROOM` → reducer clears `summary.hasMention` AND `subscriptions[r].hasMention` (local optimistic)
2. Fires `markRoomRead` RPC immediately (server-side `lastSeenAt` advance)

When a message arrives in the active room, `useRoomSubscriptions.scheduleMarkActiveRead` debounces `markRoomRead` to a **trailing 500ms** so a chatty channel doesn't generate one RPC per message.

## Testing

### Framework + structure

- **vitest** with `jsdom` environment for component tests
- **@testing-library/react** for component rendering
- **@vitest/coverage** if you need coverage
- Test files live next to source: `Foo.jsx` → `Foo.test.jsx`. Same folder, not under a `__tests__/`.

### Patterns

- **Mock at the boundary you don't want to exercise.** For component tests, mock the api/ op (`vi.mock('@/api', …)`). For api tests, mock `nats.request` / `nats.publish`.
- **Lazy components need `findBy*` not `getBy*`.** A click that opens a `React.lazy` chunk requires awaiting the chunk:
  ```js
  fireEvent.click(screen.getByRole('button', { name: /Create Room/i }))
  expect(await screen.findByRole('dialog')).toBeInTheDocument()
  ```
- **Debounced behavior uses fake timers.** `vi.useFakeTimers({ shouldAdvanceTime: true })`; advance with `await vi.advanceTimersByTimeAsync(N)`; always restore with `vi.useRealTimers()` in `finally`.
- **Reducer tests are pure JS.** No React, no `renderHook`. Construct an action, run the reducer, assert on the next state. Fast (<1ms each).
- **Test fixture lengths:** when a Go type adds a required field, every JS fixture that constructs it must add the field. The TS strict typecheck catches the rest.

### What to test, what to skip

- **Always test:** action transitions in reducers; api/ op argument-to-payload mapping; component conditional render gates (own message vs others, role-based affordances).
- **Don't test:** trivial getters, pure pass-through wrappers, design-token CSS.
- **`toHaveBeenCalledWith` is exact-arity.** When `requestWithAsyncResult` is called without `opts`, the third arg is `undefined` — assert on three args including `undefined`. The api functions always pass three args.

## Smoke tests

`smoke-test.mjs` and `scripts/*.smoke.mjs` exercise live NATS / auth-service stacks. They reach directly into `api/_transport/` (subjects.ts, asyncJob.ts) because they operate at the wire layer — production code stays behind the barrel.

Run with the npm scripts:
- `npm run smoke` — full integration smoke (auth → connect → send → receive)
- `npm run smoke:asyncjob` — async-job two-phase pattern against a real NATS
- `npm run smoke:livestack` — auth + room-service + room-worker end-to-end

All three use Node's `--experimental-strip-types` to load the `.ts` modules directly (no build step). Requires Node 22+.

## Commit hygiene

- One concern per commit. Refactors and behavior changes go in separate commits.
- Commit message body explains the **why** — what was wrong, what the fix is. Two-line summary plus a fuller body for non-trivial changes.
- Reference reviewer findings inline ("Subscription reviewer's HIGH #1 — …") when the commit closes one.
- **Never** mention model identifiers, session ids, or AI provenance in commit messages, code comments, or PR bodies.
- `npm run typecheck` and `npm test` must pass on every commit. Production build (`vite build`) should be clean.

## Quick reference

| Need to… | Do… |
|---|---|
| Add a new NATS RPC | `mkdir src/api/<opName>` → write `index.ts` → re-export from `src/api/index.ts` |
| Add a new component | folder under the right parent → `.jsx` + `index.jsx` + (optional) `style.css` + `.test.jsx` |
| Add component CSS | `style.css` in the component folder, import from the component, use design tokens |
| Add a hook | cross-cutting → `src/hooks/`; context-specific → `src/context/<Name>/` |
| Add a pure utility | `src/lib/` if no React + no async I/O; else reconsider |
| Subscribe to a NATS subject | wrap in `api/subscribeTo<X>/index.ts`, then use from `useRoomSubscriptions` |
| Read this user's subscription | `useSubscription(roomId)` |
| Get the user's role in a room | `useSubscription(roomId)?.roles.includes('owner')` |
| Format a user-facing error | `formatAsyncJobError(err)` (re-exported from `@/api`) |
| Render a deferred dialog | `lazy(...)` + `<Suspense fallback={<LazyFallback variant="dialog" />}>` |
| Wrap an error-prone subtree | `<ErrorBoundary>` (custom fallback via render-prop if needed) |
