# User-Service Sidebar — Design

Reorganize the chat-frontend sidebar into three sections (Favorite, Apps, Channels and DMs) populated by three new NATS subjects served by `mock-user-service`.

## Goals

- Render the sidebar as three sections, in this fixed order: **Favorite**, **Apps**, **Channels and DMs**.
- Source bucket membership from three new user-service RPCs:
  - `chat.user.{account}.request.user.{siteID}.subscription.getCurrent` with payload `{ "favorite": true }` → Favorite section
  - `chat.user.{account}.request.user.{siteID}.subscription.getApps` → Apps section
  - `chat.user.{account}.request.user.{siteID}.subscription.getRooms` → Channels and DMs section
- Preserve every existing live behavior of the current sidebar: unread counts, mention badges, member-count badges, DM display names, and the reducer's recency ordering — applied per-section.

## Non-goals

- No backend changes. The three subjects already exist (`pkg/subject/subject.go`) and are wired in `mock-user-service`.
- No new `Favorite` field on `model.Subscription`. The `favorite: true` flag is only a request payload, not a response field.
- No client-side favoriting UI (can't toggle favorite from the sidebar).
- No persistence of section-expanded state across reloads.
- No refresh button or background refetch. The three RPCs are called exactly once per login.
- No replacement of the existing `roomsList` seed. The new RPCs are additive.

## Architecture

The change is entirely in `chat-frontend/`. Three layers are touched:

1. **Subjects (`src/lib/subjects.js`)** — add three new builders mirroring the Go definitions.
2. **State (`src/lib/roomEventsReducer.js` + `src/context/RoomEventsContext.jsx`)** — extend the reducer with three Sets: `favoriteIds` (frozen at login), `appIds` (mutated on realtime events), and `channelDmIds` (mutated on realtime events). Fire the three new RPCs in parallel with the existing `roomsList` call on login. Augment `ROOM_ADDED`/`ROOM_REMOVED` to maintain the two non-frozen Sets.
3. **Rendering (`src/components/RoomList.jsx`)** — partition `summaries` into three sections and render three collapsible blocks.

`RoomEventsContext` remains the single source of truth for live room state (unread, mention, last-message-at, name, userCount). The three new RPCs do not seed room metadata — they only decide bucket assignment.

### Bucket assignment rule

Each room is rendered in exactly one section. Favorite wins over Apps wins over Channels and DMs. All three sections iterate `summaries` in its existing recency order and filter by membership:

```
section('Favorite')         = summaries  filter (favoriteIds.has(id))
section('Apps')             = summaries  filter (appIds.has(id)   AND NOT favoriteIds.has(id))
section('Channels and DMs') = summaries  filter (channelDmIds.has(id) AND NOT favoriteIds.has(id) AND NOT appIds.has(id))
```

A room present in `summaries` but in none of the three Sets does not render in the sidebar. It can be recovered by reload, or by a matching `subscription.update added` event that triggers `ROOM_ADDED` and tags it. See "Risks and trade-offs."

### App discriminator

For realtime re-bucketing on `subscription.update added` events, an "app" is identified by `roomType === 'botDM'`. All other room types (`channel`, `dm`, `discussion`) are tagged as `channelDmIds`.

### Bootstrap flow (login)

In the existing `RoomEventsContext` `useEffect` that runs on `user` becoming non-null, in addition to the current `roomsList` call, fire the three new RPCs in parallel:

```
const [favResp, appResp, roomResp] = await Promise.all([
  request(userSubscriptionGetCurrent(user.account, user.siteId), { favorite: true }),
  request(userSubscriptionGetApps(user.account, user.siteId), {}),
  request(userSubscriptionGetRooms(user.account, user.siteId), {}),
])
dispatch({
  type: 'BUCKETS_LOADED',
  favoriteIds: favResp.subscriptions.map(s => s.roomId),
  appIds:      appResp.subscriptions.map(s => s.roomId),
  channelDmIds:    roomResp.subscriptions.map(s => s.roomId),
})
```

The three calls and the existing `roomsList` call run concurrently. Rendering tolerates partial state (sections empty until rooms load; buckets empty until `BUCKETS_LOADED`).

Each RPC failure is independent and silently leaves its Set empty (no error dispatch). A failed bucket call degrades the corresponding section (rooms that would have appeared there won't render until reload).

### Realtime updates

- `subscription.update` with `action: 'added'`, after the existing `roomsGet` enrichment, dispatch `ROOM_ADDED` (existing action). Reducer augmentation: if `room.type === 'botDM'`, add `room.id` to `appIds`; otherwise add it to `channelDmIds`. Never adds to `favoriteIds`.
- `subscription.update` with `action: 'removed'`, dispatch `ROOM_REMOVED` (existing action). Reducer augmentation: remove the roomId from all three Sets.
- `favoriteIds` is **frozen at login**. A subscription becoming favorited (or un-favorited) on the server after login is not reflected until the next reload. This is an explicit non-goal.

### Rendering rules

- Sections render in fixed order: Favorite, Apps, Channels and DMs.
- **All three sections** iterate `summaries` in its existing recency order. A new message floats the room to the top of its section, exactly as today's flat `RoomList` does.
- Sections are collapsible. Expanded state is local `useState` in `RoomList`, defaulting to expanded, not persisted across reloads.
- Empty sections (zero matching rooms) are hidden entirely — no header, no placeholder.
- Selected-room highlighting, mention badge, unread badge, and userCount badge behave exactly as today, on a per-room basis regardless of section.

## Data model details

### `chat-frontend/src/lib/subjects.js`

Add three new exports:

```js
export function userSubscriptionGetCurrent(account, siteId) {
  return `chat.user.${account}.request.user.${siteId}.subscription.getCurrent`
}
export function userSubscriptionGetApps(account, siteId) {
  return `chat.user.${account}.request.user.${siteId}.subscription.getApps`
}
export function userSubscriptionGetRooms(account, siteId) {
  return `chat.user.${account}.request.user.${siteId}.subscription.getRooms`
}
```

Mirrors the Go builders at `pkg/subject/subject.go:465`, `:493`, `:472`.

### `chat-frontend/src/lib/roomEventsReducer.js`

Extend `initialState`:

```js
favoriteIds: new Set(),  // frozen at login
appIds:      new Set(),  // mutated on realtime events
channelDmIds:    new Set(),  // mutated on realtime events
```

All three are Sets because only membership is needed (rendering iterates `summaries` and filters).

New reducer action:

- `BUCKETS_LOADED { favoriteIds: string[], appIds: string[], channelDmIds: string[] }` — replaces all three Sets with new Sets built from the supplied arrays.

Augmentations to existing actions:

- `ROOM_ADDED { room }` — in addition to existing behavior: if `room.type === 'botDM'`, return a new `appIds` with `room.id` added; otherwise return a new `channelDmIds` with `room.id` added. No-op if `room.id` is already in the target Set. Never modifies `favoriteIds`.
- `ROOM_REMOVED { roomId }` — in addition to existing behavior: if `roomId` is in any of the three Sets, return a new Set for each affected one without the roomId.
- `RESET` — also resets all three Sets to new empty instances.

The reducer must always produce new Set instances on mutation (do not mutate in place) so React detects the change.

### `chat-frontend/src/context/RoomEventsContext.jsx`

In the bootstrap `useEffect`:

- After the existing `dmSub`, `subUpdate`, `metaUpdate` subscriptions are registered, fire the three new RPCs (in parallel with the existing `roomsList` call, not sequenced after it).
- On resolution, dispatch `BUCKETS_LOADED`. Wrap with the existing `cancelledRef` / `safeDispatch` guard.
- On rejection, log and continue. No `ROOMS_FAILED`-equivalent for buckets.

Expose a new hook `useSidebarSections()` from `RoomEventsContext.jsx` that returns `[{ key, title, rooms }]` in fixed section order — Favorite, Apps, Channels and DMs. The hook does the partition in a single pass over `summaries`:

```js
for (const room of summaries) {
  if      (favoriteIds.has(room.id)) favorite.push(room)
  else if (appIds.has(room.id))      apps.push(room)
  else if (channelDmIds.has(room.id))    channelDm.push(room)
  // rooms in none of the three Sets are not rendered
}
return [
  { key: 'favorite',  title: 'Favorite',          rooms: favorite  },
  { key: 'apps',      title: 'Apps',              rooms: apps      },
  { key: 'channelDm', title: 'Channels and DMs',  rooms: channelDm },
]
```

Memoize on `[summaries, favoriteIds, appIds, channelDmIds]` so the partition only re-runs when one of the four changes.

The existing `useRoomSummaries` hook is left unchanged so unrelated callers (e.g. `ChatPage.jsx`'s `summaries.find(...)` and `summaries.some(...)`) keep working.

### `chat-frontend/src/components/RoomList.jsx`

Replace the current flat `summaries.map` with a partition + three-section render driven by `useSidebarSections`:

```jsx
const sections = useSidebarSections()
const [collapsed, setCollapsed] = useState({})  // { [sectionKey]: boolean }
// for each section: skip if rooms.length === 0; otherwise render header + room rows
```

Each section header is clickable to toggle expand/collapse. CSS class names consistent with existing `room-list-*`: add `room-list-section`, `room-list-section-header`, `room-list-section-collapsed`.

## Files touched

- `chat-frontend/src/lib/subjects.js` — three new builders.
- `chat-frontend/src/lib/subjects.test.js` — tests for the three new builders.
- `chat-frontend/src/lib/roomEventsReducer.js` — `initialState` extension, `BUCKETS_LOADED` action, `ROOM_ADDED`/`ROOM_REMOVED`/`RESET` augmentations.
- `chat-frontend/src/lib/roomEventsReducer.test.js` — tests for the new action and augmentations.
- `chat-frontend/src/context/RoomEventsContext.jsx` — fire the three new RPCs on login, expose `useSidebarSections`.
- `chat-frontend/src/context/RoomEventsContext.test.jsx` — test that bootstrap fires the three subjects and dispatches `BUCKETS_LOADED`.
- `chat-frontend/src/components/RoomList.jsx` — partition + three-section render.
- `chat-frontend/src/components/RoomList.test.jsx` — tests for section partitioning, exclusivity, collapse toggle, hide-empty, recency ordering inside sections.
- `chat-frontend/src/styles/index.css` — add `.room-list-section`, `.room-list-section-header`, `.room-list-section-collapsed` rules alongside existing `.room-list-*` rules.

No backend files are touched. `docs/client-api.md` is **not** updated as part of this PR: the subjects were introduced server-side in PR #175 (mock-user-service) and any client-API documentation belongs with that PR, not with a frontend consumer change.

## Testing

All tests are unit tests, using the existing Vitest setup.

- **`subjects.test.js`** — for each new builder, assert the exact subject string for a sample `(account, siteId)`.
- **`roomEventsReducer.test.js`** —
  - `BUCKETS_LOADED` populates all three Sets with the supplied roomIds.
  - `BUCKETS_LOADED` replaces previous content (subsequent dispatch wins).
  - `ROOM_ADDED` with `room.type === 'botDM'` adds `room.id` to `appIds`, leaves `channelDmIds` unchanged.
  - `ROOM_ADDED` with `room.type` in `{channel, dm, discussion}` adds `room.id` to `channelDmIds`, leaves `appIds` unchanged.
  - `ROOM_ADDED` never modifies `favoriteIds`.
  - `ROOM_ADDED` for a roomId already in the target Set is a no-op for the bucket.
  - `ROOM_REMOVED` removes the roomId from any of the three Sets that contains it.
  - `ROOM_REMOVED` for a roomId in none of the Sets is a no-op for the buckets.
  - `RESET` empties all three Sets.
- **`RoomEventsContext.test.jsx`** — mount the provider with a mocked `request`, assert that on login all three new subjects are requested with the documented payloads (`{ favorite: true }`, `{}`, `{}`), and that `BUCKETS_LOADED` is dispatched with the response roomIds. Failure of one RPC leaves the others' Sets populated.
- **`RoomList.test.jsx`** — with seeded `summaries`, `favoriteIds`, `appIds`, and `channelDmIds`:
  - Three sections render in the fixed order when each has at least one room.
  - A roomId in both `favoriteIds` and `appIds` appears under Favorite only.
  - A roomId in both `favoriteIds` and `channelDmIds` appears under Favorite only.
  - A roomId in both `appIds` and `channelDmIds` appears under Apps only.
  - A roomId in only `appIds` appears under Apps only.
  - A roomId in only `channelDmIds` appears under Channels and DMs only.
  - A roomId in `summaries` but in none of the three Sets does not render.
  - Empty sections are hidden (no header).
  - Clicking a section header toggles collapse; collapsed state hides items but keeps the header visible.
  - All sections preserve `summaries` recency order (verified by reordering `summaries` between renders and asserting each section reorders accordingly).
  - Existing room-item behavior (selected, unread, mention badge, userCount) renders correctly inside a section.

Coverage target: 90% for the touched files (matches the project's "core business logic" standard).

## Risks and trade-offs

- **Stale favorites:** `favoriteIds` is frozen at login. If a user favorites a room from another client, the sidebar does not reflect it until the next reload. Accepted per scoping. A future improvement would be a new `subscription.favorite.update` server event.
- **Four parallel RPCs on login:** `roomsList` + three user-service calls. All run in parallel and none block rendering, so login latency is bounded by the slowest of the four. Acceptable.
- **Rooms in `summaries` but in no bucket Set do not render.** This can happen if `roomsList` and the three user-service RPCs disagree at login (e.g., a new subscription created between the two snapshots). The room is recoverable via reload, or via a matching `subscription.update added` event that fires `ROOM_ADDED` and tags it into `appIds` or `channelDmIds`. The flat-list fallback that the previous draft used (elimination-style "default to Channels and DMs") is deliberately not in this design: per the user's instruction, Channels and DMs is sourced from `getRooms`.
- **App membership not maintained for non-botDM roomtype changes:** if a room's `roomType` could change at runtime (e.g. from `channel` to `botDM`), `appIds`/`channelDmIds` would not move it. There is no event today for "roomType changed," and the assumption is that roomType is immutable post-creation.

## Open questions

None. All clarifying points were resolved during brainstorming:

- Hybrid approach (keep `RoomEventsContext` live state, add bucket membership) — confirmed.
- Fetch-once-on-login, no refresh button — confirmed.
- Apps treated as rooms (clickable, open `MessageArea`) — confirmed.
- Bucket exclusivity, Favorite-wins — confirmed.
- App discriminator: `roomType === 'botDM'` — confirmed.
- Sections collapsible, hide-when-empty — confirmed.
- All sections use existing recency order — confirmed.
- Keep `roomsList` alongside the three new RPCs — confirmed.
- Channels and DMs sourced from `getRooms` (not derived by elimination) — confirmed.
- Client-side bucket state is in-memory only; no server writes — confirmed.
