# Design: Timestamp pivot for Load Surrounding Messages

**Date:** 2026-07-25
**Service:** history-service
**Status:** Approved (brainstorming)

## Problem

`LoadSurroundingMessages` (`chat.user.{account}.request.room.{roomID}.{siteID}.msg.surrounding`)
centers a window of messages on a `messageId`. Clients now want to center the window on a
**Unix-millis timestamp** instead — the primary use case is "resume at last-read position": the
client sends the user's subscription `lastSeenAt` and wants the read context just before it plus
the first unread messages just after it, in one round-trip.

## Key insight

The handler already pivots on a `time.Time`, not on the ID itself. `messageId` is only used to
(1) derive that pivot via a `messages_by_id` lookup, (2) run the access-window check, and
(3) splice the central message into the middle of the result. A timestamp **is** the pivot
directly, so timestamp mode is one Cassandra read *cheaper* (no by-ID lookup) and reuses the
existing before/after bucket-walk machinery unchanged.

## Approach

Extend the existing RPC (Approach A) rather than add a new subject. The request accepts **either**
`messageId` **or** `timestamp`, exactly-one-of. The two modes diverge on one axis only — the pivot
and whether a central message exists — so a shared assembler serves both.

### 1. Wire schema (`internal/models/message.go`)

```go
type LoadSurroundingMessagesRequest struct {
    MessageID string    `json:"messageId,omitempty"` // pivot by message; exactly one of messageId/timestamp
    Timestamp *int64    `json:"timestamp,omitempty"` // UTC millis pivot; exactly one of messageId/timestamp
    Limit     int       `json:"limit"`
    Meta      *RoomMeta `json:"meta,omitempty"`
}
```

Response struct (`LoadSurroundingMessagesResponse`) is unchanged. In timestamp mode there is **no
distinct central message**: `messages` is `read context (createdAt <= timestamp)` followed by
`unread (createdAt > timestamp)`, oldest-first. `moreBefore`/`moreAfter` keep their meaning.

### 2. Validation (entry handler)

- Exactly-one-of: both set → `BadRequest("provide either messageId or timestamp, not both")`;
  neither set → `BadRequest("messageId or timestamp is required")`.
- `timestamp` must be `> 0` when present → `BadRequest("timestamp must be positive")`.

### 3. Boundary semantics — before-inclusive (timestamp mode only)

`lastSeenAt` is usually the last-read message's `created_at`. To anchor that message in the result,
the **before** read is inclusive (`created_at <= pivot`) and the **after** read stays strict
(`created_at > pivot`). The at-pivot message therefore appears as the newest row of the read group
and cannot be duplicated on the after side. If several messages share that exact millisecond, all
land in the read group (correct: all are "at or before" the watermark).

**Critical constraint:** inclusivity is scoped to timestamp mode only. The **messageId path keeps
strict `<`** and its manual central splice unchanged — making its before-read inclusive would return
the central message in the before group *and* splice it, duplicating it.

### 4. Repository — reuse the strict reads via a `+1ms` pivot shift

Option (a) chosen: no new repository methods. The timestamp before-read calls the existing strict
`GetMessagesBefore` / `GetMessagesBetweenDesc` with the pivot shifted up one millisecond
(`beforeUpper = pivot + 1ms`). At millisecond precision, `created_at < pivot+1ms` selects exactly
the same rows as `created_at <= pivot`, so the shift is an exact inclusive upper bound.

**Bucket-boundary correctness.** `pivot+1ms` crosses into the next bucket only when `pivot` is the
last millisecond of its bucket window (`pivot ≡ windowMs-1`). In that single case `pivot` is also
the bucket's maximum representable time, so "all of that bucket" still equals "`created_at <=
pivot`" — the strict walk (first-bucket `< pivot+1ms` on the empty next bucket, then the pivot
bucket read unbounded) returns exactly the right rows. No leak, no new method needed.

The `MessageReader` interface is unchanged.

### 5. Handler (`internal/service/messages.go`)

Refactor `LoadSurroundingMessages` into:

- **Entry** — validate exactly-one-of, clamp `limit`, dispatch.
- **`loadSurroundingByMessageID`** — the current body verbatim (access check → `findMessage` →
  room-times → strict before/after → central splice). No behavior change.
- **`loadSurroundingByTimestamp`** — resolve access + room-times concurrently via
  `checkAccessAndRoomTimes` (no `findMessage` dependency, so both Mongo reads parallelise); access
  check `pivot.Before(*accessSince)` → `Forbidden(..., WithReason(MessageOutsideAccessWindow))`
  (same reason as messageId mode for consistent frontend branching); split `limit` into
  `beforeCount = (limit+1)/2` / `afterCount = limit/2` (before gets the larger half, no slot
  reserved for a central); before-read uses the strict method with `pivot+1ms` (inclusive of the
  at-pivot message), after-read strict at `pivot`; `central = nil`.
- **`assembleSurrounding`** (shared) — runs the errgroup (before, after, read-floor), assembles
  `reverse(before) + [central?] + after`, redacts inaccessible quotes, decodes attachments, and
  builds the response with `MoreBefore`/`MoreAfter`/`MinUserLastSeenAt`.

Zero-count guard: in timestamp mode `afterCount == 0` only when `limit == 1`; that read is skipped
(returns an empty page) rather than calling `parsePageRequest(0)`, which would balloon the page size
to `defaultCassPageSize (50)`.

## Split-count comparison

| limit | messageId mode (central reserves 1) | timestamp mode (no central) |
|---|---|---|
| 1 | central only (special-cased) | 1 before (`<= ts`), 0 after |
| 6 | 3 before + central + 2 after | 3 before (`<= ts`) + 3 after |

## Access-window edge cases (timestamp mode)

- `pivot == accessSince` → allowed (not `Before`); before-read `> since AND <= pivot` is empty, so
  only unread context returns. Acceptable at the exact boundary.
- `pivot` far future → before returns recent messages, after empty.
- `pivot` far past with `accessSince == nil` → before floor clamps to `historyFloor`.

## Testing (TDD)

- **Unit** (`internal/service/messages_test.go`, mocked reader):
  - timestamp happy path (no HSS): `GetMessagesBefore` called with `pivot+1ms` + `GetMessagesAfter`
    with `pivot`; result has no central; oldest-first assembly.
  - timestamp with HSS: `GetMessagesBetweenDesc` used for before with upper bound `pivot+1ms`.
  - `pivot.Before(accessSince)` → 403 `MessageOutsideAccessWindow`.
  - validation: both set / neither set / non-positive timestamp → `BadRequest`.
  - `limit == 1` → 1 before, after read skipped.
  - `moreBefore`/`moreAfter` propagation; before/after store errors wrapped.
  - regression: all existing messageId-mode tests still pass unchanged.
- **Integration** (`internal/cassrepo/messages_by_room_integration_test.go`):
  - `GetMessagesBefore(pivot+1ms)` includes the exact-pivot row and same-ms siblings; excludes `> pivot`.
  - bucket-boundary case (`pivot` = last ms of its window): the next-bucket message must not leak.

## Docs (mandatory — `chat.user.` handler + client-facing request struct)

- `docs/client-api.md` §Load Surrounding Messages: document `messageId`/`timestamp` as exactly-one-of,
  the no-central timestamp semantics, and the new error cases.
- `docs/client-api/request-reply.md` §Load Surrounding Messages: mirror the request-field change.
- (No event view change — request/reply only.)

## Out of scope (flagged, not fixed)

Pre-existing: in messageId mode with `limit == 2`, `afterCount == 0` reaches
`parsePageRequest(0)` → page size 50, an over-fetch on the after side. Left as-is to keep this
change focused; the timestamp path avoids it via the zero-count guard above.
