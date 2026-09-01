# chat-frontend: remember the user's subscriptions

**Date:** 2026-08-12
**Scope:** `chat-frontend` only. No backend service, NATS subject, or `pkg/model` type changes.

## Problem

Every login and every page reload starts the sidebar from nothing. `useRoomSubscriptions`
fires `fetchSidebarBuckets` once per login cycle; until those three paginated
`subscription.list` RPCs drain, `state.subscriptions`, `state.summaries`, and the three
bucket ID sets are empty and the sidebar renders blank.

The blank sidebar is also *sticky on failure*. `fetchSidebarBuckets` wraps the three bucket
fetches in `Promise.allSettled` and degrades a failed bucket to `[]`, and `fetchAllPages`
returns the pages it already has when a later page fails. The caller cannot distinguish
"this user has no rooms" from "the fetch failed", so it dispatches `BUCKETS_LOADED`
regardless — and that action replaces `state.subscriptions` wholesale and rebuilds
`summaries` from `action.rooms`. A degraded fetch therefore overwrites a populated sidebar
with an empty one.

## Goals

1. **Warm start.** The sidebar's first paint after reload shows the user's rooms, grouped
   into their real chatlist sections, with no empty-then-populate flash.
2. **Failures are never destructive.** Neither a degraded `subscription.list` bootstrap nor a
   failed per-room `fetchMessageHistory` may remove rooms from the sidebar. Only a complete,
   successful fetch is allowed to delete anything.

## Non-goals

Caching message bodies; an offline send queue; restoring `activeRoomId` across reloads;
persisting room E2E keys; rendering the sidebar before the NATS connection is established
(`App.jsx` gates `RoomEventsProvider` on `connected`, and that gate is unchanged).

## Approach

localStorage, read synchronously from the reducer's lazy initializer.

IndexedDB was considered and rejected: its async API cannot run during the provider's first
render, so hydration would have to arrive via an effect-dispatched action — reintroducing
exactly the empty first frame this design removes. Its extra quota is not needed; at roughly
300 bytes per trimmed subscription, 1,000 rooms is about 300KB, comfortably inside the ~5MB
localStorage budget.

## Component 1 — `src/lib/subscriptionCache.js`

A synchronous storage helper. No React, no NATS, no async I/O, so it belongs in `lib/`.

```js
save(user, { subscriptions, favoriteIds, appIds, channelDmIds, chatlist })
load(user) -> payload | null
clear()
```

**Storage key.** One key, `chat.subscriptionCache.v1`, with `account` and `siteId` stamped
inside the record. `load` returns `null` unless both the schema version and the identity match
the passed `user`.

A single key means a second account on the same machine overwrites the first rather than
accumulating one blob per user, which bounds disk usage. Accepted trade-off: after account B
logs in, account A's next login is a cold start. The cache is deliberately *not* cleared on
logout — re-login by the same account stays warm.

**Trim rule.** Deep-copy each subscription minus `room.privateKey`. One rule, not a field
allowlist, so the cache does not silently drop fields as the wire type grows. Room E2E keys
stay memory-only, matching `RoomKeysContext`'s current policy.

**Failure handling.** Every read and write is `try/catch`-wrapped — disabled or private-mode
storage must never break the app. On `QuotaExceededError`, retry once with the 300
subscriptions having the most recent `room.lastMsgAt`; if that also fails, `clear()` and give
up. The cache is only a paint accelerator, so degrading it is always safe.

## Component 2 — hydration in `RoomEventsProvider`

`state.summaries` is **not** persisted. Each subscription already carries its room metadata
inline under `sub.room` — the reason the cold-start path needs no separate `rooms.list` RPC —
so summaries are derived on hydration through the existing mappers:

```js
const [state, dispatch] = useReducer(roomEventsReducer, initialState, () => {
  const cached = load(user)
  if (!cached) return initialState
  const rooms = Object.values(cached.subscriptions).map((s) => subToRoom(s, user.siteId))
  const s = roomEventsReducer(initialState, { type: 'BUCKETS_LOADED', ...cached, rooms })
  return roomEventsReducer(s, { type: 'CHATLIST_LOADED', chatlist: cached.chatlist })
})
```

Hydration replays the cached data through the same actions the network path uses, so cached
state and fetched state are identical in shape by construction — there is no parallel
hydration code to drift. Two details follow for free: `toSummary` zero-inits `unreadCount`
(a session-local counter that no fetch ever corrects, so a persisted value would be
permanently stale), and `hasMention` is merged from the subscription record, which is
server-canonical.

Running inside the lazy initializer means this executes during the provider's first render,
so the first paint already has rooms.

**Consequence.** Cached subscriptions carry no `privateKey`, so an encrypted room warm-paints
in the sidebar but its messages show the decryption placeholder until the fetch lands and
re-seeds keys via `seedKeys`. Sidebar rows are unaffected.

## Component 3 — write-through

An effect in `RoomEventsProvider` keyed by reference on the five persisted slices
(`subscriptions`, `favoriteIds`, `appIds`, `channelDmIds`, `chatlist`). Ordinary message
traffic churns `roomState`, `summaries`, and `msgRecvSeq` but never these five, so the effect
stays quiet; a 1s trailing debounce absorbs `SUBSCRIPTION_UPSERTED` bursts.

**Teardown guard.** Skip the write when `state.subscriptions === initialState.subscriptions`.
`RESET` returns the `initialState` object itself, so that identity check is an exact
"we're torn down" test — it stops logout and StrictMode remounts from blanking the cache.

## Component 4 — degradation-aware bootstrap

### `fetchSidebarBuckets` reports what failed

Add one field to `SidebarBuckets`:

```ts
failures: ('favorites' | 'apps' | 'rooms')[]
```

populated from the existing `allSettled` unwrap. A partial-page failure inside `fetchAllPages`
(page 3 of 5 fails, pages 1–2 kept) also marks its bucket degraded — a truncated bucket must
not be allowed to delete rooms.

### `useRoomSubscriptions` branches on it

| `failures.length` | Action |
|---|---|
| 0 | `BUCKETS_LOADED` as today — full replace, so rooms left while away disappear |
| 1–2 | `BUCKETS_LOADED` with `merge: true`, **omitting the failed buckets' id arrays** |
| 3 | No `BUCKETS_LOADED`; dispatch `ROOMS_FAILED` — cached sidebar stays, `useRoomSummaries().error` surfaces it |

### `BUCKETS_LOADED` gains `action.merge`

Replace mode (the default) is unchanged. Merge mode:

- `subscriptions = { ...state.subscriptions, ...subs }` — upsert only, never remove.
- Each bucket set: `action.favoriteIds ? new Set(action.favoriteIds) : state.favoriteIds`, and
  likewise for `appIds` / `channelDmIds`. Because the hook *omits* failed buckets rather than
  sending empty arrays, presence alone marks a bucket authoritative — no extra flag needed.
- `summaries`: upsert `action.rooms` into `state.summaries`, keep unlisted rows, re-sort.

### Per-room history failures

`HISTORY_FAILED` already touches only that room's buffer in `roomState`, leaving `summaries`
and `subscriptions` alone. This is a don't-regress requirement, covered by a test rather than
a code change.

## Component 5 — live subscriptions for warm-painted channels

`openChannelSub` is driven by the fetch's `buckets.rooms` loop, so without this a warm-painted
channel would render but receive no live messages until the fetch lands.

The effect in `useRoomSubscriptions` opens channel subs for hydrated rooms in
`stateRef.current.summaries` immediately after the four base subscriptions are set up.
`openChannelSub` is already idempotent per `(roomId, crossSite)`, so the later fetch loop
no-ops for rooms already open and re-opens any whose `crossSite` the fetch corrects.

## Data flow

Cold start (no cache):

```
mount → load() → null → initialState (sidebar empty)
      → fetchSidebarBuckets → BUCKETS_LOADED (replace) → sidebar paints
      → write-through persists
```

Warm start (cache present, fetch succeeds):

```
mount → load() → payload → BUCKETS_LOADED + CHATLIST_LOADED in lazy init
      → FIRST PAINT has rooms + sections
      → openChannelSub for cached channels
      → fetchSidebarBuckets → BUCKETS_LOADED (replace) → reconciled
      → write-through persists
```

Warm start, bootstrap fully fails:

```
mount → hydrated first paint
      → fetchSidebarBuckets → failures = 3 → ROOMS_FAILED only
      → sidebar keeps showing cached rooms; error surfaces via useRoomSummaries().error
      → write-through does not fire (no persisted slice changed)
```

## Testing

TDD, red first, per the root guidelines.

**`lib/subscriptionCache.test.js`** — save/load round trip; `privateKey` stripped; version
mismatch → `null`; account or siteId mismatch → `null`; malformed JSON → `null`; quota error →
trim-and-retry then clear; storage throwing on access → no throw.

**`reducer.test.js`** — merge mode adds new subs, keeps unlisted ones, keeps bucket sets for
omitted buckets, replaces sets for present ones; replace mode still removes departed rooms;
`HISTORY_FAILED` leaves `summaries` and `subscriptions` untouched.

**`fetchSidebarBuckets/index.test.ts`** — `failures` populated per bucket, including the
mid-pagination case.

**`RoomEventsContext.test.jsx`** — a hydrated provider has rooms on first render (asserted
without awaiting); an all-three-bucket failure leaves hydrated state intact and sets
`roomsError`; a partial failure keeps the failed bucket's cached rooms; channel subs are
opened for cached channels.

## Documentation

`chat-frontend/CLAUDE.md`'s "Subscription state" section gains cache hydration as the third
population source alongside `BUCKETS_LOADED` and `SUBSCRIPTION_UPSERTED`, plus the
merge-vs-replace rule.

`docs/client-api.md` is untouched — no client-facing handler, subject, or `pkg/model` type
changes.
