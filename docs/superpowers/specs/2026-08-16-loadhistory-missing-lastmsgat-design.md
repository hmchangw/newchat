# history-service: loadHistory empty results when room doc has no `lastMsgAt`

**Date:** 2026-08-16
**Status:** Approved

## Problem

For rooms whose MongoDB doc has no `lastMsgAt` field, Load History always
returns an empty page even though the room has messages in Cassandra. Reported
from production.

## Root cause

1. `mongorepo.GetRoomTimes` (`history-service/internal/mongorepo/room.go`)
   correctly returns `lastMsgAt` = zero time when the field is unset. The field
   is only written by `message-worker` when it persists a message
   (`message-worker/store_mongo.go`), so a room doc can lack it while the room
   has messages: legacy/migrated docs, or a failed/raced update.
2. `resolveRoomTimes` (`history-service/internal/service/room_times.go`) ends
   with a consistency collapse: when `createdAt > lastMsgAt` it sets
   `lastMsgAt = createdAt`, on the assumption the room is genuinely empty. A
   zero `lastMsgAt` always trips this (any real `createdAt` is after zero), so
   **"lastMsgAt unknown" is silently rewritten to "lastMsgAt = createdAt"**.
3. `LoadHistory` (`history-service/internal/service/messages.go`) sees a
   non-zero `lastMsgAt` and applies its dead-room optimization, capping
   `before` at `lastMsgAt + 1ms` = `createdAt + 1ms`.
4. All the room's messages were sent after creation, so both walk paths scan
   only pre-creation time and return nothing — empty page, `hasNext=false`,
   every time.

Blast radius: the same collapsed value feeds `walkBounds`, whose ceiling
becomes `createdAt`, so `msg.next` (LoadNextMessages) and `msg.surrounding`
(LoadSurroundingMessages) are broken identically for these rooms.
`GetThreadMessages` is unaffected (thread rooms always set `LastMsgAt` at
creation).

## Decision

Code-only fix (chosen over a Mongo backfill, and over backfill-only): make
`resolveRoomTimes` preserve zero `lastMsgAt` as "unknown". Every consumer
already handles zero correctly — `LoadHistory` skips its `before`-cap
(`!lastMsgAt.IsZero()`), `walkBounds` substitutes `now + clockSkewTolerance`
as the ceiling — the bug is purely that the resolver destroys the zero before
callers can see it.

## Change

One guard in the final collapse of `resolveRoomTimes`
(`room_times.go`):

```go
// Still inverted with a real lastMsgAt — corrupt pair; collapse the range.
// A zero lastMsgAt stays zero: "not recorded" means unknown, not "empty
// room" — the room may hold messages (legacy docs, failed lastMsgAt
// update); callers treat zero as unknown (LoadHistory skips its cap,
// walkBounds ceilings at now+skew).
if !last.IsZero() && created.After(*last) {
    last = created
}
```

Unchanged on purpose:

- The hint-consistency **refetch** above the collapse (a hint-supplied pair
  that disagrees still re-reads Mongo). For a no-`lastMsgAt` room queried with
  a `createdAt` hint this costs one extra Mongo read — pre-existing behavior.
- The collapse for a **non-zero** inverted pair (`createdAt > lastMsgAt`, both
  present — genuine data corruption) keeps today's normalization.
- `mongorepo.GetRoomTimes` — already correct.

Accepted trade-off: a room with genuinely no messages and no `lastMsgAt`
loses the 1-bucket dead-room shortcut and instead walks its floor-clamped,
`maxBuckets`-bounded empty range. New rooms have `createdAt ≈ now`, so that is
~1 bucket in practice.

## Testing (TDD — red first)

1. **Resolver unit tests** (`room_times_test.go`): Mongo returns
   `(zero, createdAt)`, no hints → want `(zero, createdAt)` (currently
   `(createdAt, createdAt)` — red). Hint-involved variant: `createdAt` hint +
   Mongo zero `lastMsgAt` → refetch runs (2 Mongo calls), `lastMsgAt` still
   zero. These are standalone cases with their own mock returns (the existing
   table fixes one Mongo return for all rows).
2. **Service-level regression** (the user-visible repro): `GetRoomTimes` →
   `(zero, realCreatedAt)`; the mocked reader requires `before` **after**
   `createdAt` (proving the cap was not applied) and returns messages;
   `LoadHistory` must return them. Lives in a `package service` test file —
   the shared `service_test` fixture pre-stubs `GetRoomTimes` with an
   unbounded-max expectation that cannot be overridden per-test.
3. **`walkBounds` unit test** — none exists today (verified); add one covering
   the zero-`lastMsgAt` ceiling (`now + clockSkewTolerance`) and the normal
   non-zero ceiling.
4. Existing rows of `TestResolveRoomTimes` (including
   "createdAt > lastMsgAt → mongo refetch") must stay green.

## Out of scope

- Mongo backfill of `lastMsgAt` for legacy docs.
- Changing corrupt-pair (both-present, inverted) semantics.
- Wire/schema changes — none; `docs/client-api.md` needs no edit.
