# Read-Receipt Recipient Count Fix

## Problem

`MessageActionMenu` renders "Read by X of Y" for an own message. Y is sourced
from `room.userCount - 1`. After logout/login, Y collapses to 0 even when the
room has multiple members.

Reproduction: Alice in a 5-member channel sends a message. Initial menu shows
"Read by 0 of 4". Bob reads → "Read by 1 of 4". Alice logs out and back in.
Reopens the menu → "Read by 1 of 0". Carol reads → "Read by 2 of 0". X is
correct; Y is wrong.

## Root cause

Cold-start sidebar hydration goes through `fetchSidebarBuckets`, which derives
each `Room` summary from a `Subscription` record:

```ts
// chat-frontend/src/api/fetchSidebarBuckets/index.ts:131
userCount: sub.userCount ?? 0,
```

`model.Subscription` (Go) has no `UserCount` field, and `mock-user-service`
does not populate one on its subscription replies. So `sub.userCount` is
`undefined`, the summary's `userCount` becomes `0`, and `MessageActionMenu`
computes `Y = max(0, 0 - 1) = 0`.

In-session room joins escape the bug only because `useRoomSubscriptions`
follows each `subscription.update added` event with a `getRoom` RPC, and
`model.Room` does carry `UserCount`. Cold-start skips that path.

## Fix

Make `MessageActionMenu` fetch the authoritative recipient count itself when
the kebab opens, alongside the existing read-receipt RPC. Mirror the pattern
`RoomMembersBadge` already uses (`listRoomMembers` on every room open) — same
RPC, same data source.

### Changes

`src/components/shared/MessageList/MessageRow/MessageActions/MessageActionMenu/MessageActionMenu.jsx`:

1. Import `listRoomMembers` from `@/api`.
2. Add a `recipientCount` state, initialized to `null`. Reset to `null` in
   `close()` so re-opens always refetch.
3. In `handleKebabClick`, fire `fetchReadReceipt` and `listRoomMembers` in
   parallel via `Promise.all`. On resolve, set `readers` and
   `recipientCount = max(0, members.length - 1)`.
4. Treat `listRoomMembers` failure as non-blocking: catch it locally and
   leave `recipientCount` at `null`. `fetchReadReceipt` failure still routes
   to the existing error path (Y is meaningless if X failed).
5. Compute `Y = recipientCount ?? max(0, (room?.userCount ?? 1) - 1)`. The
   fallback preserves today's behavior when the new RPC is unavailable.

### Why `members.length - 1`

The kebab only renders for the current user's own message (`isOwnMessage`
check at line 74). The user counts themselves in `members` but is not a
recipient of their own message, so subtract 1. Matches today's formula's
intent (`userCount - 1`).

## Tests

`MessageActionMenu.test.jsx`:

- Update the existing `request` mock pattern: route by subject substring so
  the new `member.list` call resolves separately from `read-receipt`.
- Add a regression test: `room = { id: 'r1', siteId: 'siteA', userCount: 0 }`,
  mock `member.list` to return 5 members, mock `read-receipt` to return 2
  readers → expect "Read by 2 of 4".
- Add a fallback test: `room.userCount = 4` but `member.list` rejects → Y
  falls back to `userCount - 1 = 3`.
- Update existing tests that assert on `request.toHaveBeenCalledTimes(2)`
  (the "refetches on reopen" test) to account for the doubled call count
  (2 RPCs per open).

## Out of scope

- Backend changes to `model.Subscription` or `mock-user-service` to embed
  `userCount` on subscription replies. The frontend self-healing fix handles
  the symptom regardless of backend behavior.
- Backfilling `room.userCount` for other consumers (sidebar count, header
  badge). `RoomMembersBadge` already self-fetches on room open, so no other
  surface is affected by the stale value.
