# Forwarded-Message Room Enrichment — Design

> **⚠️ SUPERSEDED (2026-08-10).** The read-time room-enrichment approach described
> below was implemented and then removed. Only `roomId` + `roomType` (plus the
> source's `tshow`/`threadRoomId`) are needed, and all of those are immutable —
> so they are now captured once at forward time into the `ForwardedMessage`
> snapshot by `message-gatekeeper`, and there is no read-time lookup at all.
> Kept for the decision record; do not implement from this document.

**Date:** 2026-08-06
**Base:** `claude/message-forwarding-impl-deq7za` (message-forwarding implementation)
**Branch:** `claude/message-room-enrichment-krp5m3`

## Problem

The `ForwardedMessage` snapshot embedded on a forwarded message carries only the
source room's `roomId`. Clients rendering a forward ("Forwarded from #prj-alpha")
need the source room's name and type, and today they would have to issue a
separate lookup per forward. History reads should return the room object
inline, reusing the existing `MessageRoom` enrichment object introduced for
search.

## Scope

**In scope:** every history-service RPC that returns `Message` bodies —
`msg.history`, `msg.next`, `msg.surrounding`, `msg.get`, `msg.get.ids`,
`msg.pinned.list`, thread messages (replies + `parentMessage`), thread parent
list.

**Out of scope (explicit decisions):**
- `msg.send` async reply (gatekeeper echo) — not enriched.
- Broadcast events (real-time delivery) — not enriched.
- Search results (`SearchMessage.forwardedMessage`) — not enriched.
- `rooms.get` previews — `PreviewMessage` has no forward field yet (TODO #106).

## Decisions

1. **Read-time enrichment by roomID** in history-service (not write-time
   snapshotting): names stay fresh across renames; no Cassandra schema change,
   no migration, no DDL/doc mirrors.
2. **dm/botDM sources return `{id, type}` only** — no `name`, `hrInfo`, or
   `appInfo`. Avoids leaking the DM counterpart's identity to readers who are
   not members of the source room, and keeps the lookup to a single Mongo
   query. Channel/discussion sources return `{id, name, type}`.
3. **Best-effort:** a lookup miss (room deleted) or Mongo error logs one
   `slog.Warn` and omits `room`; a read is never failed by enrichment (same
   posture as search enrichment).
4. **Site-local lookup only:** the gatekeeper fetches a forward source via
   `msg.get` on the same `siteID` as the send, so the source room is always on
   the same site as the destination room — the local `rooms` collection is
   authoritative. No cross-site RPC, no fan-out.

## Model changes (`pkg/model` / `pkg/model/cassandra`)

`pkg/model` imports `pkg/model/cassandra` (one-way), so the cassandra UDT
struct cannot reference `model.MessageRoom`. To reuse the existing object
without an import cycle:

- **Move** `MessageRoom`, `MessageHRInfo`, `MessageAppInfo` (json-tag-only
  carriers) from `pkg/model/search.go` into a new
  `pkg/model/cassandra/enrich.go`, and move `type RoomType string` there.
- **Alias** in `pkg/model`: `type MessageRoom = cassandra.MessageRoom`,
  `type MessageHRInfo = cassandra.MessageHRInfo`,
  `type MessageAppInfo = cassandra.MessageAppInfo`,
  `type RoomType = cassandra.RoomType`. The `RoomTypeChannel/DM/BotDM/Discussion`
  constants stay in `pkg/model/room.go`. All existing consumers
  (search-service, room-service, user-service, …) compile unchanged; JSON wire
  shape is byte-identical.
- **New transient field** on `cassandra.ForwardedMessage`:

  ```go
  // Room is read-time enrichment of RoomID (name/type from the local rooms
  // collection); transient (cql:"-"), never persisted into the UDT.
  Room *MessageRoom `json:"room,omitempty" cql:"-"`
  ```

  Precedent: `QuotedParentMessage.TShow` is an existing `cql:"-"` transient.

### Wire shape

```json
"forwardedMessage": {
  "messageId": "…", "roomId": "…", "sender": {"…": "…"}, "createdAt": "…",
  "msg": "…",
  "room": { "id": "…", "name": "prj-alpha", "type": "channel" }
}
```

dm/botDM source: `"room": {"id": "…", "type": "dm"}`. Lookup miss/error: no
`room` key.

## history-service changes

### Repo layer (`internal/mongorepo`)

```go
// room.go
type RoomNameType struct {
    Name string
    Type model.RoomType
}

// GetRoomsNameType returns name/type for the given room IDs; absent IDs are
// simply missing from the map.
func (r *RoomRepo) GetRoomsNameType(ctx context.Context, roomIDs []string) (map[string]RoomNameType, error)
```

One `find` on `rooms` with `_id $in` and explicit projection
`{name: 1, type: 1}` (always-project rule).

### Service layer (`internal/service`)

- `RoomRepository` interface (consumer-side) gains `GetRoomsNameType`;
  `make generate` refreshes mocks.
- New unexported helper called at the end of each in-scope read path:

  ```go
  func (s *HistoryService) enrichForwardedRooms(ctx context.Context, msgs ...[]models.Message)
  ```

  Collects distinct `ForwardedMessage.RoomID`s across the slices (messages
  without a forward are skipped — the common case costs zero lookups), issues
  the single batched lookup, and stamps `Room` on each snapshot:
  channel/discussion → `{ID, Name, Type}`; dm/botDM → `{ID, Type}`.
  Variadic slices let thread reads pass replies and the parent in one call.

### Call sites

`LoadHistory`, `LoadNextMessages`, `LoadSurroundingMessages`,
`GetMessageByID`, `GetMessagesByIDs`, `ListPinnedMessages`,
`GetThreadMessages` (replies + `parentMessage`), `GetThreadParentMessages`.

Not touched: `RoomsGet`, edit/delete/pin/unpin/react responses (ID-only
echoes), migration handlers.

Note: `msg.get` also serves the gatekeeper's forward-source fetch; its
projection ignores the new `room` key, so forward-send behavior is unchanged
(one extra Mongo read only when the fetched source is itself a forward —
accepted).

## Error handling

- Enrichment never fails a read: Mongo error or missing room doc → one
  `slog.Warn` with the request context, `room` omitted.
- No new client-facing error cases.

## Testing (TDD — red first)

- **Unit (service, mocked `RoomRepository`)**, table-driven: forward
  present/absent; channel vs dm vs botDM; room-doc miss; Mongo error
  (response still succeeds, room omitted); multiple forwards deduped into one
  lookup call; thread read enriches both replies and `parentMessage`; no
  lookup issued when no forwards are present.
- **Integration (`mongorepo`)**: `GetRoomsNameType` found/missing IDs, exact
  projection.
- **Model round-trip**: new `Room` field on `ForwardedMessage`
  marshals/unmarshals; omitted when nil.
- Coverage ≥80% on touched packages.

## Docs (same PR)

- `docs/client-api.md`: `ForwardedMessage` schema table gains `room`
  (`MessageRoom`, optional): best-effort, dm/botDM carry id+type only; JSON
  examples on the history read paths updated.
- `docs/client-api/request-reply.md` / `docs/client-api/events.md`: matching
  updates wherever `ForwardedMessage` appears (must not drift).
