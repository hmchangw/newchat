# User-Service Sidebar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the chat-frontend sidebar into three sections (Favorite, Apps, Channels and DMs), driven by three new user-service NATS RPCs while keeping the existing RoomEventsContext as the live source of truth.

**Architecture:** Hybrid — `roomsList` continues to seed `RoomEventsContext`. Three new RPCs (`UserSubscriptionGetCurrent` with `{favorite: true}`, `UserSubscriptionGetApps`, `UserSubscriptionGetRooms`) populate three Sets in the reducer (`favoriteIds`, `appIds`, `channelDmIds`). `RoomList` partitions `summaries` against those Sets, with Favorite > Apps > Channels-and-DMs exclusivity. All sections preserve the existing recency ordering. Realtime `subscription.update` events maintain `appIds` and `channelDmIds` membership; `favoriteIds` is frozen at login.

**Tech Stack:** React 19, Vite, Vitest, @testing-library/react. No backend changes.

**Spec:** `docs/superpowers/specs/2026-05-14-user-service-sidebar-design.md`

---

## File map

| File | Action | Responsibility |
|---|---|---|
| `chat-frontend/src/lib/subjects.js` | Modify | Add three new builders for the user-service subjects |
| `chat-frontend/src/lib/subjects.test.js` | Modify | Test the three new builders |
| `chat-frontend/src/lib/roomEventsReducer.js` | Modify | Add `favoriteIds`/`appIds`/`channelDmIds` Sets to state; add `BUCKETS_LOADED` action; augment `ROOM_ADDED`, `ROOM_REMOVED`, `RESET` |
| `chat-frontend/src/lib/roomEventsReducer.test.js` | Modify | Test the new reducer behavior |
| `chat-frontend/src/context/RoomEventsContext.jsx` | Modify | Fire three new RPCs on login; expose `useSidebarSections` |
| `chat-frontend/src/context/RoomEventsContext.test.jsx` | Modify | Test bootstrap fires the three RPCs and `useSidebarSections` partitions correctly |
| `chat-frontend/src/components/RoomList.jsx` | Modify | Replace flat list with three collapsible sections |
| `chat-frontend/src/components/RoomList.test.jsx` | Modify | Test section partitioning, exclusivity, hide-empty, collapse |
| `chat-frontend/src/styles/index.css` | Modify | Add `.room-list-section*` CSS rules |

---

## Task 1: Add the three user-service subject builders

**Files:**
- Modify: `chat-frontend/src/lib/subjects.js`
- Test: `chat-frontend/src/lib/subjects.test.js`

- [ ] **Step 1: Write the failing tests**

Add the following block to the end of the existing `describe('subjects', () => { ... })` in `chat-frontend/src/lib/subjects.test.js`, and update the import at the top of the file to include the three new names.

Update the import block (replace the existing import):

```js
import {
  userRoomEvent,
  roomEvent,
  memberAdd,
  memberRemove,
  memberRoleUpdate,
  memberList,
  searchRooms,
  searchMessages,
  msgSurrounding,
  readReceipt,
  roomCreate,
  userResponse,
  orgMembers,
  userSubscriptionGetCurrent,
  userSubscriptionGetApps,
  userSubscriptionGetRooms,
} from './subjects'
```

Add three new tests at the end of the `describe` block (just before the closing `})`):

```js
  it('userSubscriptionGetCurrent builds the user-service getCurrent subject', () => {
    expect(userSubscriptionGetCurrent('alice', 'site-A')).toBe(
      'chat.user.alice.request.user.site-A.subscription.getCurrent'
    )
  })

  it('userSubscriptionGetApps builds the user-service getApps subject', () => {
    expect(userSubscriptionGetApps('alice', 'site-A')).toBe(
      'chat.user.alice.request.user.site-A.subscription.getApps'
    )
  })

  it('userSubscriptionGetRooms builds the user-service getRooms subject', () => {
    expect(userSubscriptionGetRooms('alice', 'site-A')).toBe(
      'chat.user.alice.request.user.site-A.subscription.getRooms'
    )
  })
```

- [ ] **Step 2: Run the tests and verify they fail**

```
cd chat-frontend && npm run test -- src/lib/subjects.test.js
```

Expected: three new tests fail with `userSubscriptionGetCurrent is not a function` (or similar import error). Existing tests still pass.

- [ ] **Step 3: Add the three builders**

Append to `chat-frontend/src/lib/subjects.js` (after the `orgMembers` function):

```js
// userSubscriptionGetCurrent fetches the caller's current subscriptions, optionally
// filtered server-side. The sidebar passes `{ favorite: true }` to drive the
// Favorite section. Mirrors pkg/subject/subject.go::UserSubscriptionGetCurrent.
export function userSubscriptionGetCurrent(account, siteId) {
  return `chat.user.${account}.request.user.${siteId}.subscription.getCurrent`
}

// userSubscriptionGetApps fetches the caller's app subscriptions. Drives the
// Apps section of the sidebar. Mirrors pkg/subject/subject.go::UserSubscriptionGetApps.
export function userSubscriptionGetApps(account, siteId) {
  return `chat.user.${account}.request.user.${siteId}.subscription.getApps`
}

// userSubscriptionGetRooms fetches the caller's non-app room subscriptions
// (channels, DMs, discussions). Drives the Channels and DMs section of the
// sidebar. Mirrors pkg/subject/subject.go::UserSubscriptionGetRooms.
export function userSubscriptionGetRooms(account, siteId) {
  return `chat.user.${account}.request.user.${siteId}.subscription.getRooms`
}
```

- [ ] **Step 4: Run the tests and verify they pass**

```
cd chat-frontend && npm run test -- src/lib/subjects.test.js
```

Expected: all tests pass, including the three new ones.

- [ ] **Step 5: Commit**

```bash
cd chat-frontend && git add src/lib/subjects.js src/lib/subjects.test.js
git commit -m "feat(chat-frontend): add user-service subscription subject builders"
```

---

## Task 2: Extend roomEventsReducer with bucket Sets

**Files:**
- Modify: `chat-frontend/src/lib/roomEventsReducer.js`
- Test: `chat-frontend/src/lib/roomEventsReducer.test.js`

- [ ] **Step 1: Write the failing tests**

Append the following describe block to the end of `chat-frontend/src/lib/roomEventsReducer.test.js`. Use the existing `room()` helper at the top of the file.

```js
describe('roomEventsReducer: bucket Sets', () => {
  it('initialState has empty favoriteIds, appIds, channelDmIds Sets', () => {
    expect(initialState.favoriteIds).toBeInstanceOf(Set)
    expect(initialState.appIds).toBeInstanceOf(Set)
    expect(initialState.channelDmIds).toBeInstanceOf(Set)
    expect(initialState.favoriteIds.size).toBe(0)
    expect(initialState.appIds.size).toBe(0)
    expect(initialState.channelDmIds.size).toBe(0)
  })

  it('BUCKETS_LOADED populates all three Sets from input arrays', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1', 'f2'],
      appIds: ['a1'],
      channelDmIds: ['c1', 'c2', 'c3'],
    })
    expect([...next.favoriteIds].sort()).toEqual(['f1', 'f2'])
    expect([...next.appIds].sort()).toEqual(['a1'])
    expect([...next.channelDmIds].sort()).toEqual(['c1', 'c2', 'c3'])
  })

  it('BUCKETS_LOADED replaces previous Set contents', () => {
    const first = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: ['a1'],
      channelDmIds: ['c1'],
    })
    const second = roomEventsReducer(first, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f2'],
      appIds: [],
      channelDmIds: ['c2'],
    })
    expect([...second.favoriteIds]).toEqual(['f2'])
    expect(second.appIds.size).toBe(0)
    expect([...second.channelDmIds]).toEqual(['c2'])
  })

  it('ROOM_ADDED with botDM type adds to appIds, leaves channelDmIds unchanged', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('bot1', { type: 'botDM' }),
    })
    expect(next.appIds.has('bot1')).toBe(true)
    expect(next.channelDmIds.has('bot1')).toBe(false)
    expect(next.favoriteIds.has('bot1')).toBe(false)
  })

  it('ROOM_ADDED with channel type adds to channelDmIds, leaves appIds unchanged', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('ch1', { type: 'channel' }),
    })
    expect(next.channelDmIds.has('ch1')).toBe(true)
    expect(next.appIds.has('ch1')).toBe(false)
    expect(next.favoriteIds.has('ch1')).toBe(false)
  })

  it('ROOM_ADDED with dm type adds to channelDmIds', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('dm1', { type: 'dm' }),
    })
    expect(next.channelDmIds.has('dm1')).toBe(true)
    expect(next.appIds.has('dm1')).toBe(false)
  })

  it('ROOM_ADDED with discussion type adds to channelDmIds', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('d1', { type: 'discussion' }),
    })
    expect(next.channelDmIds.has('d1')).toBe(true)
    expect(next.appIds.has('d1')).toBe(false)
  })

  it('ROOM_ADDED never modifies favoriteIds', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: [],
      channelDmIds: [],
    })
    const next = roomEventsReducer(seeded, {
      type: 'ROOM_ADDED',
      room: room('newRoom', { type: 'botDM' }),
    })
    expect([...next.favoriteIds]).toEqual(['f1'])
  })

  it('ROOM_ADDED for a roomId already in appIds is a no-op for the bucket', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: ['bot1'],
      channelDmIds: [],
    })
    const next = roomEventsReducer(seeded, {
      type: 'ROOM_ADDED',
      room: room('bot1', { type: 'botDM' }),
    })
    expect(next.appIds.size).toBe(1)
    expect(next.appIds.has('bot1')).toBe(true)
  })

  it('ROOM_REMOVED removes the roomId from favoriteIds if present', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1', 'f2'],
      appIds: [],
      channelDmIds: [],
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'f1' })
    expect([...next.favoriteIds]).toEqual(['f2'])
  })

  it('ROOM_REMOVED removes the roomId from appIds if present', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: ['a1', 'a2'],
      channelDmIds: [],
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'a1' })
    expect([...next.appIds]).toEqual(['a2'])
  })

  it('ROOM_REMOVED removes the roomId from channelDmIds if present', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['c1', 'c2'],
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'c1' })
    expect([...next.channelDmIds]).toEqual(['c2'])
  })

  it('ROOM_REMOVED for a roomId in none of the Sets leaves them unchanged', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: ['a1'],
      channelDmIds: ['c1'],
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'unknown' })
    expect([...next.favoriteIds]).toEqual(['f1'])
    expect([...next.appIds]).toEqual(['a1'])
    expect([...next.channelDmIds]).toEqual(['c1'])
  })

  it('RESET empties all three bucket Sets', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: ['a1'],
      channelDmIds: ['c1'],
    })
    const next = roomEventsReducer(seeded, { type: 'RESET' })
    expect(next.favoriteIds.size).toBe(0)
    expect(next.appIds.size).toBe(0)
    expect(next.channelDmIds.size).toBe(0)
  })
})
```

- [ ] **Step 2: Run the tests and verify they fail**

```
cd chat-frontend && npm run test -- src/lib/roomEventsReducer.test.js
```

Expected: the new bucket-Set tests fail. Existing tests still pass.

- [ ] **Step 3: Extend `initialState` with the three Sets**

In `chat-frontend/src/lib/roomEventsReducer.js`, replace the existing `initialState` block:

```js
export const initialState = {
  summaries: [],
  roomState: {},
  activeRoomId: null,
  roomsError: null,
  favoriteIds: new Set(),
  appIds: new Set(),
  channelDmIds: new Set(),
}
```

- [ ] **Step 4: Add the `BUCKETS_LOADED` case**

In `chat-frontend/src/lib/roomEventsReducer.js`, add a new case to the `switch` in `roomEventsReducer`. Place it immediately before the `case 'ROOMS_FAILED'` block:

```js
    case 'BUCKETS_LOADED': {
      return {
        ...state,
        favoriteIds: new Set(action.favoriteIds),
        appIds: new Set(action.appIds),
        channelDmIds: new Set(action.channelDmIds),
      }
    }
```

- [ ] **Step 5: Augment `ROOM_ADDED` to bucket the room by roomType**

Replace the existing `case 'ROOM_ADDED'` block in `chat-frontend/src/lib/roomEventsReducer.js` with:

```js
    case 'ROOM_ADDED': {
      if (state.summaries.some((r) => r.id === action.room.id)) return state
      const summaries = sortByLastMsgDesc([...state.summaries, toSummary(action.room)])
      const roomId = action.room.id
      let appIds = state.appIds
      let channelDmIds = state.channelDmIds
      if (action.room.type === 'botDM') {
        if (!appIds.has(roomId)) {
          appIds = new Set(appIds)
          appIds.add(roomId)
        }
      } else if (!channelDmIds.has(roomId)) {
        channelDmIds = new Set(channelDmIds)
        channelDmIds.add(roomId)
      }
      return { ...state, summaries, appIds, channelDmIds }
    }
```

- [ ] **Step 6: Augment `ROOM_REMOVED` to evict from all three Sets**

Replace the existing `case 'ROOM_REMOVED'` block with:

```js
    case 'ROOM_REMOVED': {
      const summaries = state.summaries.filter((r) => r.id !== action.roomId)
      const { [action.roomId]: _removed, ...rest } = state.roomState
      let favoriteIds = state.favoriteIds
      let appIds = state.appIds
      let channelDmIds = state.channelDmIds
      if (favoriteIds.has(action.roomId)) {
        favoriteIds = new Set(favoriteIds)
        favoriteIds.delete(action.roomId)
      }
      if (appIds.has(action.roomId)) {
        appIds = new Set(appIds)
        appIds.delete(action.roomId)
      }
      if (channelDmIds.has(action.roomId)) {
        channelDmIds = new Set(channelDmIds)
        channelDmIds.delete(action.roomId)
      }
      return { ...state, summaries, roomState: rest, favoriteIds, appIds, channelDmIds }
    }
```

- [ ] **Step 7: `RESET` already returns `initialState`**

`case 'RESET'` already returns `initialState`, which now has empty Sets, so no change is needed there. Confirm by reading line 300 of `chat-frontend/src/lib/roomEventsReducer.js`.

- [ ] **Step 8: Run the tests and verify they pass**

```
cd chat-frontend && npm run test -- src/lib/roomEventsReducer.test.js
```

Expected: all tests pass, including the new bucket-Set suite.

- [ ] **Step 9: Commit**

```bash
cd chat-frontend && git add src/lib/roomEventsReducer.js src/lib/roomEventsReducer.test.js
git commit -m "feat(chat-frontend): track favorite/app/channelDm bucket Sets in reducer"
```

---

## Task 3: Wire bootstrap RPC calls in RoomEventsContext

**Files:**
- Modify: `chat-frontend/src/context/RoomEventsContext.jsx`
- Test: `chat-frontend/src/context/RoomEventsContext.test.jsx`

- [ ] **Step 1: Write the failing tests**

Append the following `describe` block to the end of `chat-frontend/src/context/RoomEventsContext.test.jsx`. The existing `mockNats`, `wrap`, and `SummariesProbe` helpers are reused.

```js
describe('RoomEventsProvider sidebar buckets bootstrap', () => {
  beforeEach(() => vi.clearAllMocks())

  it('fires the three user-service subjects on login with the documented payloads', async () => {
    const calls = []
    const request = vi.fn().mockImplementation((subject, payload) => {
      calls.push({ subject, payload })
      if (subject === 'chat.user.alice.request.rooms.list') return Promise.resolve({ rooms: [] })
      if (subject.endsWith('.subscription.getCurrent'))
        return Promise.resolve({ subscriptions: [{ roomId: 'f1' }] })
      if (subject.endsWith('.subscription.getApps'))
        return Promise.resolve({ subscriptions: [{ roomId: 'a1' }] })
      if (subject.endsWith('.subscription.getRooms'))
        return Promise.resolve({ subscriptions: [{ roomId: 'c1' }] })
      throw new Error('unexpected subject: ' + subject)
    })
    const nats = mockNats({ request })

    render(wrap(<SummariesProbe />, nats))
    await waitFor(() => expect(request).toHaveBeenCalledTimes(4))

    const getCurrent = calls.find((c) => c.subject.endsWith('.subscription.getCurrent'))
    const getApps = calls.find((c) => c.subject.endsWith('.subscription.getApps'))
    const getRooms = calls.find((c) => c.subject.endsWith('.subscription.getRooms'))

    expect(getCurrent.subject).toBe(
      'chat.user.alice.request.user.site-A.subscription.getCurrent'
    )
    expect(getCurrent.payload).toEqual({ favorite: true })
    expect(getApps.subject).toBe('chat.user.alice.request.user.site-A.subscription.getApps')
    expect(getApps.payload).toEqual({})
    expect(getRooms.subject).toBe('chat.user.alice.request.user.site-A.subscription.getRooms')
    expect(getRooms.payload).toEqual({})
  })

  it('does not block rendering when one bucket RPC fails', async () => {
    const request = vi.fn().mockImplementation((subject) => {
      if (subject === 'chat.user.alice.request.rooms.list') return Promise.resolve({ rooms: [] })
      if (subject.endsWith('.subscription.getCurrent'))
        return Promise.reject(new Error('boom'))
      if (subject.endsWith('.subscription.getApps'))
        return Promise.resolve({ subscriptions: [{ roomId: 'a1' }] })
      if (subject.endsWith('.subscription.getRooms'))
        return Promise.resolve({ subscriptions: [{ roomId: 'c1' }] })
      throw new Error('unexpected subject: ' + subject)
    })
    const nats = mockNats({ request })

    render(wrap(<SummariesProbe />, nats))
    await waitFor(() => expect(request).toHaveBeenCalledTimes(4))
    // SummariesProbe renders without throwing — that's the assertion.
    expect(screen.getByTestId('count').textContent).toBe('0')
  })
})
```

- [ ] **Step 2: Run the tests and verify they fail**

```
cd chat-frontend && npm run test -- src/context/RoomEventsContext.test.jsx
```

Expected: the two new tests fail (request only called once for `roomsList`).

- [ ] **Step 3: Add the new subject imports to `RoomEventsContext.jsx`**

In `chat-frontend/src/context/RoomEventsContext.jsx`, replace the existing import block from `'../lib/subjects'`:

```js
import {
  msgHistory,
  msgSurrounding,
  roomEvent,
  roomsGet,
  roomsList,
  subscriptionUpdate,
  roomMetadataUpdate,
  userRoomEvent,
  userSubscriptionGetCurrent,
  userSubscriptionGetApps,
  userSubscriptionGetRooms,
} from '../lib/subjects'
```

- [ ] **Step 4: Fire the three RPCs in the bootstrap useEffect**

In `chat-frontend/src/context/RoomEventsContext.jsx`, locate the existing `request(roomsList(user.account), {})` block (around line 100). Immediately after the `.catch(...)` for `roomsList`, add the bucket bootstrap. The full added block:

```js
    Promise.all([
      request(userSubscriptionGetCurrent(user.account, user.siteId), { favorite: true }),
      request(userSubscriptionGetApps(user.account, user.siteId), {}),
      request(userSubscriptionGetRooms(user.account, user.siteId), {}),
    ])
      .then(([favResp, appResp, roomResp]) => {
        if (cancelledRef.current) return
        safeDispatch({
          type: 'BUCKETS_LOADED',
          favoriteIds: (favResp?.subscriptions ?? []).map((s) => s.roomId),
          appIds: (appResp?.subscriptions ?? []).map((s) => s.roomId),
          channelDmIds: (roomResp?.subscriptions ?? []).map((s) => s.roomId),
        })
      })
      .catch((err) => {
        // Bucket RPC failure is non-fatal — sidebar grouping degrades but rooms
        // still render via the existing roomsList path. Log and move on.
        console.warn('sidebar bucket bootstrap failed:', err.message)
      })
```

Insert this block right before the existing `return () => { ... }` cleanup, in the same `useEffect` body. Place it after the existing `request(roomsList(...))...catch(...)` chain so both fire concurrently.

- [ ] **Step 5: Run the tests and verify they pass**

```
cd chat-frontend && npm run test -- src/context/RoomEventsContext.test.jsx
```

Expected: all tests pass, including the two new ones. Existing tests still pass.

- [ ] **Step 6: Commit**

```bash
cd chat-frontend && git add src/context/RoomEventsContext.jsx src/context/RoomEventsContext.test.jsx
git commit -m "feat(chat-frontend): fetch sidebar bucket subscriptions on login"
```

---

## Task 4: Add useSidebarSections hook

**Files:**
- Modify: `chat-frontend/src/context/RoomEventsContext.jsx`
- Test: `chat-frontend/src/context/RoomEventsContext.test.jsx`

- [ ] **Step 1: Write the failing tests**

Append the following `describe` block to the end of `chat-frontend/src/context/RoomEventsContext.test.jsx`. Add `useSidebarSections` to the existing `RoomEventsContext` import at the top of the file:

```js
import { RoomEventsProvider, useRoomEvents, useRoomSummaries, useSidebarSections } from './RoomEventsContext'
```

Then append the describe block:

```js
describe('useSidebarSections', () => {
  beforeEach(() => vi.clearAllMocks())

  function bootstrapNats({ buckets }) {
    const rooms = [
      { id: 'f1', name: 'fav-channel', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: '2026-04-17T10:00:00Z' },
      { id: 'a1', name: 'app-bot',     type: 'botDM',   siteId: 'site-A', userCount: 1, lastMsgAt: '2026-04-17T11:00:00Z' },
      { id: 'c1', name: 'general',     type: 'channel', siteId: 'site-A', userCount: 5, lastMsgAt: '2026-04-17T12:00:00Z' },
      { id: 'u1', name: 'unbucketed',  type: 'channel', siteId: 'site-A', userCount: 1, lastMsgAt: '2026-04-17T09:00:00Z' },
    ]
    return mockNats({
      request: vi.fn().mockImplementation((subject) => {
        if (subject === 'chat.user.alice.request.rooms.list') return Promise.resolve({ rooms })
        if (subject.endsWith('.subscription.getCurrent'))
          return Promise.resolve({ subscriptions: buckets.favoriteIds.map((id) => ({ roomId: id })) })
        if (subject.endsWith('.subscription.getApps'))
          return Promise.resolve({ subscriptions: buckets.appIds.map((id) => ({ roomId: id })) })
        if (subject.endsWith('.subscription.getRooms'))
          return Promise.resolve({ subscriptions: buckets.channelDmIds.map((id) => ({ roomId: id })) })
        throw new Error('unexpected subject: ' + subject)
      }),
    })
  }

  function SectionsProbe() {
    const sections = useSidebarSections()
    return (
      <ul>
        {sections.map((s) => (
          <li key={s.key} data-testid={`section-${s.key}`}>
            {s.title}: {s.rooms.map((r) => r.id).join(',')}
          </li>
        ))}
      </ul>
    )
  }

  it('returns three sections in fixed order', async () => {
    const nats = bootstrapNats({ buckets: { favoriteIds: ['f1'], appIds: ['a1'], channelDmIds: ['c1'] } })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() =>
      expect(screen.getByTestId('section-channelDm').textContent).toContain('c1')
    )
    const items = screen.getAllByRole('listitem').map((li) => li.getAttribute('data-testid'))
    expect(items).toEqual(['section-favorite', 'section-apps', 'section-channelDm'])
  })

  it('puts favorited rooms only in Favorite (favorite > apps > channelDm exclusivity)', async () => {
    const nats = bootstrapNats({
      buckets: { favoriteIds: ['f1', 'a1'], appIds: ['a1'], channelDmIds: ['c1', 'a1'] },
    })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() =>
      expect(screen.getByTestId('section-favorite').textContent).toContain('a1')
    )
    expect(screen.getByTestId('section-apps').textContent).not.toContain('a1')
    expect(screen.getByTestId('section-channelDm').textContent).not.toContain('a1')
  })

  it('puts apps in Apps (apps > channelDm exclusivity)', async () => {
    const nats = bootstrapNats({
      buckets: { favoriteIds: [], appIds: ['a1'], channelDmIds: ['a1', 'c1'] },
    })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() => expect(screen.getByTestId('section-apps').textContent).toContain('a1'))
    expect(screen.getByTestId('section-channelDm').textContent).not.toContain('a1')
    expect(screen.getByTestId('section-channelDm').textContent).toContain('c1')
  })

  it('does not render rooms that are in summaries but in no bucket Set', async () => {
    const nats = bootstrapNats({
      buckets: { favoriteIds: ['f1'], appIds: ['a1'], channelDmIds: ['c1'] },
    })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() =>
      expect(screen.getByTestId('section-channelDm').textContent).toContain('c1')
    )
    expect(screen.getByTestId('section-favorite').textContent).not.toContain('u1')
    expect(screen.getByTestId('section-apps').textContent).not.toContain('u1')
    expect(screen.getByTestId('section-channelDm').textContent).not.toContain('u1')
  })

  it('preserves summaries recency order within each section', async () => {
    // bootstrapNats returns rooms in this lastMsgAt order:
    // c1 (12:00) > a1 (11:00) > f1 (10:00) > u1 (09:00)
    // So for buckets that include multiple, we expect sorted-by-recency.
    const nats = bootstrapNats({
      buckets: { favoriteIds: ['f1', 'c1'], appIds: ['a1'], channelDmIds: [] },
    })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() =>
      expect(screen.getByTestId('section-favorite').textContent).toContain('f1')
    )
    // Favorite should list c1 first (most recent), then f1.
    expect(screen.getByTestId('section-favorite').textContent).toMatch(/c1.*f1/)
  })
})
```

- [ ] **Step 2: Run the tests and verify they fail**

```
cd chat-frontend && npm run test -- src/context/RoomEventsContext.test.jsx
```

Expected: the new useSidebarSections tests fail (`useSidebarSections is not a function`).

- [ ] **Step 3: Add the `useSidebarSections` hook**

Append the following to `chat-frontend/src/context/RoomEventsContext.jsx`, after the `useRoomSummaries` function:

```js
export function useSidebarSections() {
  const { state } = useRoomEventsInternal()
  const { summaries, favoriteIds, appIds, channelDmIds } = state
  return useMemo(() => {
    const favorite = []
    const apps = []
    const channelDm = []
    for (const room of summaries) {
      if (favoriteIds.has(room.id)) favorite.push(room)
      else if (appIds.has(room.id)) apps.push(room)
      else if (channelDmIds.has(room.id)) channelDm.push(room)
      // rooms in none of the three Sets are not rendered
    }
    return [
      { key: 'favorite',  title: 'Favorite',          rooms: favorite },
      { key: 'apps',      title: 'Apps',              rooms: apps },
      { key: 'channelDm', title: 'Channels and DMs',  rooms: channelDm },
    ]
  }, [summaries, favoriteIds, appIds, channelDmIds])
}
```

- [ ] **Step 4: Run the tests and verify they pass**

```
cd chat-frontend && npm run test -- src/context/RoomEventsContext.test.jsx
```

Expected: all tests pass, including the new `useSidebarSections` suite.

- [ ] **Step 5: Commit**

```bash
cd chat-frontend && git add src/context/RoomEventsContext.jsx src/context/RoomEventsContext.test.jsx
git commit -m "feat(chat-frontend): add useSidebarSections hook for bucket partition"
```

---

## Task 5: Refactor RoomList to render three collapsible sections

**Files:**
- Modify: `chat-frontend/src/components/RoomList.jsx`
- Test: `chat-frontend/src/components/RoomList.test.jsx`

- [ ] **Step 1: Replace the existing RoomList tests**

Replace the entire contents of `chat-frontend/src/components/RoomList.test.jsx` with:

```jsx
import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import RoomList from './RoomList'

vi.mock('../context/RoomEventsContext', () => ({
  useRoomSummaries: vi.fn(),
  useSidebarSections: vi.fn(),
}))

import { useRoomSummaries, useSidebarSections } from '../context/RoomEventsContext'

function summary(id, overrides = {}) {
  return {
    id,
    name: id,
    type: 'channel',
    siteId: 'site-A',
    userCount: 2,
    lastMsgAt: '2026-04-17T10:00:00Z',
    unreadCount: 0,
    hasMention: false,
    mentionAll: false,
    ...overrides,
  }
}

function setupSections({ favorite = [], apps = [], channelDm = [], error = null } = {}) {
  useRoomSummaries.mockReturnValue({
    summaries: [...favorite, ...apps, ...channelDm],
    setActiveRoom: vi.fn(),
    error,
  })
  useSidebarSections.mockReturnValue([
    { key: 'favorite',  title: 'Favorite',         rooms: favorite },
    { key: 'apps',      title: 'Apps',             rooms: apps },
    { key: 'channelDm', title: 'Channels and DMs', rooms: channelDm },
  ])
}

describe('RoomList: three-section render', () => {
  it('renders all three section headers when each section has rooms', () => {
    setupSections({
      favorite:  [summary('f1', { name: 'fav' })],
      apps:      [summary('a1', { name: 'app',  type: 'botDM' })],
      channelDm: [summary('c1', { name: 'gen' })],
    })
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText('Favorite')).toBeInTheDocument()
    expect(screen.getByText('Apps')).toBeInTheDocument()
    expect(screen.getByText('Channels and DMs')).toBeInTheDocument()
  })

  it('renders sections in fixed order: Favorite, Apps, Channels and DMs', () => {
    setupSections({
      favorite:  [summary('f1')],
      apps:      [summary('a1', { type: 'botDM' })],
      channelDm: [summary('c1')],
    })
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    const headers = Array.from(container.querySelectorAll('.room-list-section-header')).map(
      (el) => el.textContent
    )
    expect(headers).toEqual(['Favorite', 'Apps', 'Channels and DMs'])
  })

  it('hides empty sections (no header rendered)', () => {
    setupSections({ favorite: [], apps: [], channelDm: [summary('c1')] })
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.queryByText('Favorite')).not.toBeInTheDocument()
    expect(screen.queryByText('Apps')).not.toBeInTheDocument()
    expect(screen.getByText('Channels and DMs')).toBeInTheDocument()
  })

  it('toggles section collapse on header click', () => {
    setupSections({
      favorite: [],
      apps: [],
      channelDm: [summary('c1', { name: 'general' })],
    })
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText(/# general/)).toBeInTheDocument()
    fireEvent.click(screen.getByText('Channels and DMs'))
    expect(screen.queryByText(/# general/)).not.toBeInTheDocument()
    fireEvent.click(screen.getByText('Channels and DMs'))
    expect(screen.getByText(/# general/)).toBeInTheDocument()
  })

  it('preserves room item rendering: prefix, mention badge, unread badge, userCount', () => {
    setupSections({
      favorite: [],
      apps: [],
      channelDm: [
        summary('c1', { name: 'general', unreadCount: 3, hasMention: true }),
      ],
    })
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText(/# general/)).toBeInTheDocument()
    expect(container.querySelector('.room-badge-mention')).toBeInTheDocument()
    expect(container.querySelector('.room-badge-unread').textContent).toBe('3')
    expect(container.querySelector('.room-meta').textContent).toBe('2')
  })

  it('calls onSelectRoom when a room item is clicked', () => {
    const onSelectRoom = vi.fn()
    setupSections({ favorite: [summary('f1', { name: 'fav' })], apps: [], channelDm: [] })
    render(<RoomList selectedRoomId={null} onSelectRoom={onSelectRoom} />)
    fireEvent.click(screen.getByText(/# fav/))
    expect(onSelectRoom).toHaveBeenCalledWith(expect.objectContaining({ id: 'f1' }))
  })

  it('still surfaces the rooms-load error', () => {
    setupSections({ error: 'oh no' })
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText('oh no')).toBeInTheDocument()
  })
})
```

- [ ] **Step 2: Run the tests and verify they fail**

```
cd chat-frontend && npm run test -- src/components/RoomList.test.jsx
```

Expected: tests fail because `RoomList` still renders the flat list.

- [ ] **Step 3: Replace `RoomList.jsx` with the three-section render**

Replace the entire contents of `chat-frontend/src/components/RoomList.jsx` with:

```jsx
import { useState } from 'react'
import { useRoomSummaries, useSidebarSections } from '../context/RoomEventsContext'
import { roomPrefix, roomDisplayName } from '../lib/roomFormat'

function mentionBadge(summary) {
  if (summary.mentionAll) return <span className="room-badge-mention-all">!</span>
  if (summary.hasMention) return <span className="room-badge-mention">@</span>
  return null
}

function RoomItem({ room, isSelected, onSelectRoom }) {
  const unread = room.unreadCount > 0
  const classes = ['room-item']
  if (isSelected) classes.push('room-item-selected')
  if (unread) classes.push('room-item-unread')
  return (
    <div className={classes.join(' ')} onClick={() => onSelectRoom(room)}>
      <span className="room-name">
        {roomPrefix(room.type)}{roomDisplayName(room)}
      </span>
      {mentionBadge(room)}
      <span className="room-meta">{room.userCount}</span>
      {unread && <span className="room-badge-unread">{room.unreadCount}</span>}
    </div>
  )
}

export default function RoomList({ selectedRoomId, onSelectRoom }) {
  const { error } = useRoomSummaries()
  const sections = useSidebarSections()
  const [collapsed, setCollapsed] = useState({})

  const toggle = (key) => setCollapsed((c) => ({ ...c, [key]: !c[key] }))

  return (
    <div className="room-list">
      <div className="room-list-header">Rooms</div>
      {error && <div className="room-list-error">{error}</div>}
      <div className="room-list-items">
        {sections.map((section) => {
          if (section.rooms.length === 0) return null
          const isCollapsed = !!collapsed[section.key]
          const sectionClasses = ['room-list-section']
          if (isCollapsed) sectionClasses.push('room-list-section-collapsed')
          return (
            <div key={section.key} className={sectionClasses.join(' ')}>
              <div
                className="room-list-section-header"
                onClick={() => toggle(section.key)}
              >
                {section.title}
              </div>
              {!isCollapsed &&
                section.rooms.map((room) => (
                  <RoomItem
                    key={room.id}
                    room={room}
                    isSelected={room.id === selectedRoomId}
                    onSelectRoom={onSelectRoom}
                  />
                ))}
            </div>
          )
        })}
      </div>
    </div>
  )
}
```

- [ ] **Step 4: Run the tests and verify they pass**

```
cd chat-frontend && npm run test -- src/components/RoomList.test.jsx
```

Expected: all tests pass.

- [ ] **Step 5: Run the full test suite to make sure nothing else broke**

```
cd chat-frontend && npm run test
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
cd chat-frontend && git add src/components/RoomList.jsx src/components/RoomList.test.jsx
git commit -m "feat(chat-frontend): render sidebar as three collapsible sections"
```

---

## Task 6: Add CSS for sections

**Files:**
- Modify: `chat-frontend/src/styles/index.css`

- [ ] **Step 1: Append the new CSS rules**

Add the following block to `chat-frontend/src/styles/index.css` immediately after the `.room-list-error` rule (around line 167):

```css
.room-list-section {
  margin-bottom: 0.5rem;
}

.room-list-section-header {
  padding: 0.5rem 1rem;
  font-weight: 600;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--text-muted);
  cursor: pointer;
  user-select: none;
}

.room-list-section-header:hover {
  color: var(--text-secondary);
}

.room-list-section-collapsed .room-list-section-header {
  opacity: 0.7;
}
```

- [ ] **Step 2: Visually verify the sidebar in the browser**

Start the dev server and confirm the three sections render with the new styling.

```
cd chat-frontend && npm run dev
```

Open the printed URL, log in, and verify:
- Three section headers appear: Favorite, Apps, Channels and DMs (any non-empty section).
- Clicking a header toggles its room list visibility.
- Empty sections do not render their header.
- Existing room item appearance (selected highlight, unread badge, mention badge, member count) still works.
- The mock-user-service mock returns Subscriptions for all three RPCs, so under normal local-dev all three sections will be populated as long as `roomsList` and the mock agree on roomIds. If they disagree (different roomIds), some rooms may not render — this is the documented edge case and is acceptable for local dev.

Stop the dev server with Ctrl-C when done.

- [ ] **Step 3: Commit**

```bash
cd chat-frontend && git add src/styles/index.css
git commit -m "feat(chat-frontend): style three-section sidebar headers"
```

---

## Task 7: Final verification

**Files:** none (commands only)

- [ ] **Step 1: Run the full chat-frontend test suite**

```
cd chat-frontend && npm run test
```

Expected: all tests pass. No regressions in any of the existing test files.

- [ ] **Step 2: Build the chat-frontend bundle to catch any unused-import or syntax issues**

```
cd chat-frontend && npm run build
```

Expected: build completes without errors.

- [ ] **Step 3: Push the branch**

```bash
git push -u origin claude/add-user-service-sidebar-qBTdf
```

Expected: push succeeds.
