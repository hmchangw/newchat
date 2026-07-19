# Thread-Aware Unread Count: Rooms with Unread Threads Count as Unread

**Date:** 2026-07-16
**Status:** Approved

## Summary

`user-service`'s `subscription.count` (unread=true) counts unread *rooms*, comparing each
room's `lastMsgAt` to the subscription's `lastSeenAt`, and ignores threads. This extends
`countUnread` so a room also counts as unread when it has ≥1 unread thread — **at most +1
per room** (existence, not count). Read-side only: no new persisted field, no write-side
fan-out, no reliance on the un-grown `subscription.threadUnread` array. Muted rooms stay
excluded; the `{count}` response shape is unchanged. Thread state is derived at read time
and pruned using the existence-only property (skip already-unread rooms; stop at the first
unread thread per room).

## Design

### 1. `pkg/model` (`threadsubscription.go`)

Add the parent room ID to the badge read projection so a thread can be attributed to its room:

```go
type ThreadUnreadRow struct {
	ThreadRoomID string     `json:"threadRoomId" bson:"threadRoomId"`
	RoomID       string     `json:"roomId"       bson:"roomId"`
	SiteID       string     `json:"siteId"       bson:"siteId"`
	RoomType     RoomType   `json:"roomType"     bson:"roomType"`
	LastSeenAt   *time.Time `json:"lastSeenAt"   bson:"lastSeenAt"`
	HasMention   bool       `json:"hasMention"   bson:"hasMention"`
}
```

Internal projection, not a client payload — no `client-api.md` schema change.

### 2. `user-service/mongorepo` (`threadsubscriptions.go`)

`ListByAccount`'s final `$project` gains `"roomId": 1`. The source document already carries
`roomId` (the `$lookup` join key), so the pipeline is otherwise unchanged.

### 3. `user-service/service` (`subscriptions.go`, `countUnread`)

After the existing room-level pass (local join + cross-site `GetRoomsInfo`):

1. Build `pendingRooms` (`roomID → siteID`): fetched active rooms that came out **read** at
   the message level. Already-unread rooms are excluded — they are already +1.
2. If `pendingRooms` is empty, return the room-level count unchanged (no thread work).
3. `threadSubs.ListByAccount` (access-gated, capped at 500); keep only rows whose
   `RoomID ∈ pendingRooms`. This confines bumps to the `maxSubs` page and drops muted rooms.
4. Group survivors by `siteId`; resolve each thread's `lastMsgAt` via
   `rooms.GetThreadRoomInfoBatch` — chunked at 500, one goroutine per site, per-site
   degradation (the `GetThreadUnreadSummary` pattern).
5. For each resolved thread: if `unread(lastSeenAt, lastMsgAt)` and its room is still in
   `pendingRooms`, `count++` and remove the room from `pendingRooms` — so its remaining
   threads no longer match and are skipped.

Final count is `roomLevelUnread + threadOnlyUnread`, each room at most 1.

### 4. Error handling

A failed `GetThreadRoomInfoBatch` logs WARN with `request_id` and leaves that site's rooms
un-bumped — the count under-reports rather than erroring, matching today's cross-site room
path. Context cancellation returns the count so far.

### 5. Bounds

Room page bounded by `maxSubs`; thread read bounded by 500; thread bumps confined to the
room page. No unbounded fan-out.

## Testing (TDD, table-driven)

`user-service/service/subscriptions_test.go` (mocked `threadSubs` + `rooms`), red first:

- Room-level unread only — unchanged from today.
- Read room + one unread thread → +1.
- Already-unread room + unread thread → still +1 (thread never resolved).
- Room with three unread threads → +1.
- Thread in a muted room → excluded.
- Cross-site thread resolution → bumps correctly.
- Per-site thread-batch failure → rooms un-bumped, degrades, no error.
- Thread whose parent room is beyond the `maxSubs` cap → ignored.
- Empty thread list and `total == 0` → short-circuit.

Plus: `pkg/model/model_test.go` round-trips the new `roomId`; a `mongorepo` integration
test asserts `ListByAccount` emits `roomId`.

## Documentation

`docs/client-api.md` `subscription.count` section (~line 4620): prose note that a room
counts as unread when it has an unread thread (at most +1, muted excluded, best-effort under
cross-site degradation). Schema tables unchanged.

## Out of scope

- Growing `subscription.threadUnread` on thread reply (write-side fan-out).
- Changes to `thread.unread.summary`, `subscription.list`, or the `{count}` shape.
- Exact per-room unread thread counts.
- Counting threads for rooms outside the `maxSubs` page.

## Touched files

- `pkg/model/threadsubscription.go` — add `RoomID` to `ThreadUnreadRow`.
- `user-service/mongorepo/threadsubscriptions.go` — project `roomId`.
- `user-service/service/subscriptions.go` — extend `countUnread`.
- Tests: `user-service/service/subscriptions_test.go`, `pkg/model/model_test.go`,
  `user-service/mongorepo` integration test.
- `docs/client-api.md` — prose note.
