# Message Partition Bucketing Design

**Status:** approved
**Date:** 2026-05-05
**Branch:** `claude/fix-partition-size-limit-kESFY`

## Problem

Cassandra message tables are partitioned solely by `room_id`. A busy room writes to a single partition unboundedly, eventually exceeding the operational soft limit (~10 MB per partition is the project's stated target). Once partitions grow large, read latency, GC pressure, repair cost, and compaction overhead all degrade.

This spec changes the partition key to `(room_id, bucket)`, where `bucket` is a fixed-width time window derived from the row's `created_at`. The change keeps 99% of partitions under 10 MB even for the busiest realistic rooms, with no side tables, no extra writes, and no per-room state.

## Goals

- Bound Cassandra partition size for chat history so that 99% of partitions stay under 10 MB.
- Keep the implementation simple: deterministic bucket math, no side tables, no pointers.
- Preserve current API semantics for history reads (paginated time-range queries) and edits/deletes.
- Make the bucket window operator-tunable via configuration.

## Non-Goals

- **No backfill.** Existing data is discarded; the project is in initial stage and historical chat data is not retained.
- **No per-room adaptive bucket sizing.** Single global window only.
- **No safety valve for the busiest 1%.** A broadcast room running at 50k messages/day with daily buckets will produce ~100 MB partitions; this is accepted under the 99th-percentile target.
- **No new metrics or observability** beyond existing logging.
- **Pinned messages and `messages_by_id` are not bucketed.** Their growth profiles are already bounded.
- **Federation, outbox, and inbox unchanged.** Bucket is a local Cassandra storage detail; it does not appear on NATS payloads, in `pkg/model.Message`, or in cross-site events.
- **No client changes mandated.** Optional client hints are additive; clients that don't supply them continue to work via a MongoDB fallback.

## Scope

**Tables changing in place (existing data dropped):**
- `messages_by_room`
- `thread_messages_by_room`

**Tables unchanged:**
- `messages_by_id` — already bounded (one row per message + `created_at`).
- `pinned_messages_by_room` — manual pins, low growth.

**Services touched:**
- `message-worker` — write path (`SaveMessage`, `SaveThreadMessage`, `incrementParentTcount`, `UpdateParentMessageThreadRoomID`).
- `history-service` — read path (`cassrepo` queries) and edit/delete path (`cassrepo/write.go`).
- New shared package `pkg/msgbucket` consumed by both.

**Documentation updates that move with the code change (per `CLAUDE.md` Section 6):**
- `docs/cassandra_message_model.md` — schema source of truth.
- `pkg/model/cassandra/message.go` — Go row structs with `cql` tags.
- `docker-local/cassandra/init/*.cql` — local DDL.
- `CLAUDE.md` Section 6 — invariant note about `MESSAGE_BUCKET_HOURS`.

## Design

### Bucket scheme

`bucket` is a `BIGINT` storing the start-of-window value in unix milliseconds:

```
bucket = (created_at_unix_ms / windowMs) * windowMs
```

- Deterministic: always derivable from `created_at`. No lookups, no side state.
- `BIGINT` (not `TIMESTAMP`) makes the integer-math nature explicit and avoids any driver-side `time.Time` precision rounding.
- Default window: **24 hours**. Configurable via env var `MESSAGE_BUCKET_HOURS` (positive integer, hours).
- The bucket value is a storage-only detail; it is never exposed on `pkg/model.Message`, NATS payloads, or HTTP responses.

### Schema (DDL)

`messages_by_room`:

```cql
CREATE TABLE IF NOT EXISTS messages_by_room(
  room_id TEXT,
  bucket BIGINT,                              -- NEW
  created_at TIMESTAMP,
  message_id TEXT,
  thread_room_id TEXT,
  sender FROZEN<"Participant">,
  target_user FROZEN<"Participant">,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  file FROZEN<"File">,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  tshow BOOLEAN,
  tcount INT,
  thread_parent_id TEXT,
  thread_parent_created_at TIMESTAMP,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to TEXT,
  reactions MAP<TEXT,FROZEN<SET<FROZEN<"Participant">>>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  updated_at TIMESTAMP,
  PRIMARY KEY((room_id, bucket), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```

`thread_messages_by_room`:

```cql
CREATE TABLE IF NOT EXISTS thread_messages_by_room(
  room_id TEXT,
  bucket BIGINT,                              -- NEW
  thread_room_id TEXT,
  created_at TIMESTAMP,
  message_id TEXT,
  thread_parent_id TEXT,
  sender FROZEN<"Participant">,
  target_user FROZEN<"Participant">,
  msg TEXT,
  mentions SET<FROZEN<"Participant">>,
  attachments LIST<BLOB>,
  file FROZEN<"File">,
  card FROZEN<"Card">,
  card_action FROZEN<"CardAction">,
  quoted_parent_message FROZEN<"QuotedParentMessage">,
  visible_to TEXT,
  reactions MAP<TEXT,FROZEN<SET<FROZEN<"Participant">>>>,
  deleted BOOLEAN,
  type TEXT,
  sys_msg_data BLOB,
  site_id TEXT,
  edited_at TIMESTAMP,
  updated_at TIMESTAMP,
  PRIMARY KEY((room_id, bucket), thread_room_id, created_at, message_id)
) WITH CLUSTERING ORDER BY (thread_room_id DESC, created_at DESC, message_id DESC);
```

`pinned_messages_by_room` and `messages_by_id` are unchanged.

### Go row structs

`pkg/model/cassandra/message.go` `MessageRow` (and the thread row equivalent) gain:

```go
Bucket int64 `cql:"bucket"`
```

### Shared package: `pkg/msgbucket`

```go
package msgbucket

import "time"

// Sizer computes time-bucket boundaries for Cassandra message partitions.
// Bucket value is the start-of-window in unix milliseconds.
type Sizer struct {
    windowMs int64
}

func New(window time.Duration) Sizer {
    return Sizer{windowMs: window.Milliseconds()}
}

// Of returns the bucket value containing t.
func (s Sizer) Of(t time.Time) int64 {
    return (t.UnixMilli() / s.windowMs) * s.windowMs
}

// Prev returns the bucket immediately before b.
func (s Sizer) Prev(b int64) int64 { return b - s.windowMs }

// Next returns the bucket immediately after b.
func (s Sizer) Next(b int64) int64 { return b + s.windowMs }

// WindowMs exposes the configured window for cursor encoding.
func (s Sizer) WindowMs() int64 { return s.windowMs }
```

### Configuration

Each service that touches the bucketed tables adds these config fields:

```go
// All services that read or write the bucketed tables.
MessageBucketHours int `env:"MESSAGE_BUCKET_HOURS" envDefault:"24"`

// Read-side only (history-service).
MessageReadMaxBuckets   int `env:"MESSAGE_READ_MAX_BUCKETS"   envDefault:"365"`
MessageHistoryFloorDays int `env:"MESSAGE_HISTORY_FLOOR_DAYS" envDefault:"730"`
```

| Var | Used by | Purpose |
|---|---|---|
| `MESSAGE_BUCKET_HOURS` | all services | Bucket window size, in hours. Must match across services. |
| `MESSAGE_READ_MAX_BUCKETS` | history-service | Max buckets the read walk traverses before returning a non-terminal cursor. Bounds worst-case latency on cold/dead rooms. |
| `MESSAGE_HISTORY_FLOOR_DAYS` | history-service | Fallback floor for `GetAllMessagesAsc` when room `createdAt` is unavailable. |

Startup validation in `main.go` (fail fast):

```go
if cfg.MessageBucketHours < 1 {
    return fmt.Errorf("MESSAGE_BUCKET_HOURS must be >= 1, got %d", cfg.MessageBucketHours)
}
sizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
```

The configured value is logged on startup so operators can spot drift in log scans.

**Cross-service invariant:** `MESSAGE_BUCKET_HOURS` MUST match across all services that read or write `messages_by_room` or `thread_messages_by_room`. A mismatch causes writes and reads to target different partitions and silently lose data. This invariant is documented in `CLAUDE.md` Section 6.

### Write path

Bucket is computed once per write at the call site from the row's `created_at`. It is never stored on the domain model and never crosses a NATS boundary.

`message-worker/store_cassandra.go` `CassandraStore` gains a `Sizer` field:

```go
type CassandraStore struct {
    cassSession *gocql.Session
    bucket      msgbucket.Sizer
}

func NewCassandraStore(session *gocql.Session, bucket msgbucket.Sizer) *CassandraStore {
    return &CassandraStore{cassSession: session, bucket: bucket}
}
```

**`SaveMessage`** — INSERT into `messages_by_room` includes `bucket`:

```go
b := s.bucket.Of(msg.CreatedAt)
err := s.cassSession.Query(
    `INSERT INTO messages_by_room
       (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at,
        mentions, type, sys_msg_data, tshow, quoted_parent_message)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    msg.RoomID, b, msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt,
    toMentionSet(msg.Mentions), msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
).WithContext(ctx).Exec()
```

The `messages_by_id` insert is unchanged.

**`SaveThreadMessage`** — same treatment for the `thread_messages_by_room` insert (`bucket` derived from `msg.CreatedAt`).

**Parent-row updates** (`incrementParentTcount`, `UpdateParentMessageThreadRoomID`, and the corresponding read-tcount SELECTs and CAS UPDATEs) — the parent's bucket is derived from `msg.ThreadParentMessageCreatedAt`, which is already on the message:

```go
parentBucket := s.bucket.Of(*msg.ThreadParentMessageCreatedAt)

// before:  WHERE room_id = ? AND created_at = ? AND message_id = ?
// after:   WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?
```

**`history-service/internal/cassrepo/write.go`** — edit and soft-delete paths get the same predicate change. The repository struct gains a `Sizer`. Affected helpers:

- `editInMessagesByRoom`, `editInThreadMessagesByRoom`
- `deleteInMessagesByRoom`, `deleteInThreadMessagesByRoom`
- The CAS decrement queries against `messages_by_room` inside `decrementParentTcount` (parent's `bucket = bucket.Of(parentCreatedAt)`).

Edits and deletes always operate on rows we just looked up — `created_at` is in hand — so the bucket is always derivable without a separate lookup.

The pinned-message helpers (`editInPinnedMessagesByRoom`, `deleteInPinnedMessagesByRoom`) and all `messages_by_id` paths are unchanged.

### Read path

A single Cassandra query cannot span partitions, so the repository walks buckets sequentially.

**Walk algorithm (per public read function):**

```
fillPage(direction, startBucket, predicate, pageSize, maxBuckets):
    out = []
    bucket = startBucket
    walked = 0
    while len(out) < pageSize and walked < maxBuckets:
        rows = single-bucket query at (room_id, bucket) with predicate, LIMIT pageSize - len(out)
        out.append(rows)
        bucket = next bucket in direction (Prev for DESC, Next for ASC)
        walked++
    return out, hasMore
```

- The per-call `created_at` predicate is applied **only in the first bucket walked**. Later buckets are entirely on one side of the boundary, so the predicate is dropped.
- `maxBuckets` is bounded by config (default 365 — about a year of daily buckets). If reached without filling the page, return what we have plus a non-terminal cursor.
- For DESC walks bounded by a `since`, walk terminates when `bucket < bucket.Of(since)`.

**Public function semantics (signatures unchanged):**

| Function | Direction | Start bucket | First-bucket predicate | Floor |
|---|---|---|---|---|
| `GetMessagesBefore(roomID, before, page)` | DESC, walk Prev | `bucket.Of(before)` | `created_at < before` | `bucket.Of(createdAt)` |
| `GetMessagesBetweenDesc(roomID, since, before, page)` | DESC, walk Prev | `bucket.Of(before)` | `created_at > since AND created_at < before` | `bucket.Of(since)` |
| `GetMessagesAfter(roomID, after, page)` | ASC, walk Next | `bucket.Of(after)` | `created_at > after` | bounded by `maxBuckets` |
| `GetAllMessagesAsc(roomID, page)` | ASC, walk Next | `bucket.Of(createdAt)` | none | bounded by `maxBuckets` |

`GetAllMessagesAsc` previously had no time anchor. It now starts from `bucket.Of(room.createdAt)` (resolved by the service layer — see "Service layer wiring" below). If `createdAt` is unavailable, it falls back to a configured floor `MESSAGE_HISTORY_FLOOR_DAYS` (default 730 — two years back from "now").

**Cursor redesign:**

Today's cursor is a raw Cassandra `PageState` blob (one partition's worth of state). The new cursor encodes:

```
struct cursor {
    bucket    int64    // current bucket
    pageState []byte   // gocql page state inside this bucket; empty = fresh
}
```

Wire format (then base64-encoded for transport): `[bucket:8 bytes BE][pageStateLen:2 bytes BE][pageState:N bytes]`. The existing `maxCursorBytes = 512` cap remains.

Cursor lifecycle within `fillPage`:
- **Resume:** if request cursor has `pageState`, the first bucket query continues from it; otherwise it's a fresh query in the start bucket.
- **Within a bucket:** drain rows; if gocql returns a non-empty page state after collecting the last needed row, save that state into the next cursor (we'll resume the same bucket).
- **Crossing buckets:** when a bucket is exhausted (gocql returns empty page state), advance bucket; cursor stores the new bucket and empty page state.

**Stop conditions:**
- Page filled → return cursor at current bucket + remaining `pageState` (if any).
- Walked `maxBuckets` without filling → return cursor at next bucket, `pageState` empty.
- Crossed the lower/upper bound for `Between`, or crossed the floor for ASC walks → terminal cursor (`HasNext=false`).

`thread_messages_by_room` read functions follow the same walk pattern. WHERE clauses gain `bucket = ?`, and any function that paginates by `created_at` walks buckets the same way. Functions that filter by `thread_room_id` within a single partition still walk buckets (since `thread_room_id` is a clustering column, not part of the partition key).

### Service-layer wiring: room hints + Mongo fallback

The service layer (`history-service/internal/service/`) is responsible for resolving `lastMessageAt` and `createdAt` for each request, then passing them down to `cassrepo`. The `cassrepo` package stays Mongo-free (clean layering).

**Room document changes:**
- `pkg/model/Room` (and the corresponding MongoDB document) gains `LastMessageAt time.Time \`json:"lastMessageAt" bson:"lastMessageAt"\``.
- `message-worker` already updates the room document on each message persist (for `updatedAt`); it now also `$set`s `lastMessageAt = msg.CreatedAt` in the same write — no extra round-trip.
- `history-service/internal/mongorepo/room.go` gains `GetRoomTimes(ctx, roomID) (lastMessageAt, createdAt time.Time, err error)`.

**Optional client hints (additive request fields):**

```go
type RoomHints struct {
    LastMessageAt *time.Time `json:"lastMessageAt,omitempty"`
    CreatedAt     *time.Time `json:"createdAt,omitempty"`
}

type GetMessagesRequest struct {
    RoomID   string
    Before   time.Time
    Cursor   string
    PageSize int
    Hints    *RoomHints `json:"hints,omitempty"`
}
```

`RoomHints` is reused across all read endpoints that need either field (`GetMessages`, `GetThreadMessages`, etc.).

**Trust model:** these hints affect only the requester's own view. A wrong `lastMessageAt` cannot hide messages from other users — at worst it produces an under- or over-walked result for that single request. Server applies light sanity bounds and skips the Mongo lookup when valid hints are present.

**Resolution helper (service layer):**

```go
func resolveRoomTimes(ctx context.Context, roomID string, hints *RoomHints) (lastMessageAt, createdAt time.Time, err error) {
    var last, created *time.Time
    if hints != nil {
        last = hints.LastMessageAt
        created = hints.CreatedAt
    }
    now := time.Now().UTC()

    // Sanity-check hints; ignore obviously bad ones.
    if last != nil && (last.IsZero() || last.After(now.Add(time.Hour))) {
        last = nil
    }
    if created != nil && (created.IsZero() || created.After(now)) {
        created = nil
    }

    // Single Mongo round-trip covers any missing field.
    if last == nil || created == nil {
        l, c, err := s.mongo.GetRoomTimes(ctx, roomID)
        if err != nil {
            return time.Time{}, time.Time{}, fmt.Errorf("resolve room times: %w", err)
        }
        if last == nil {
            last = &l
        }
        if created == nil {
            created = &c
        }
    }

    return *last, *created, nil
}
```

**How resolved values are used:**
- `lastMessageAt` caps the starting clock for DESC walks: `GetMessagesBefore` starts from `bucket.Of(min(before, lastMessageAt))`. This makes a year-dead room's first page a single-bucket read.
- `createdAt` provides the floor for ASC walks (`GetMessagesAfter`, `GetAllMessagesAsc`) and for `GetMessagesBefore`'s walk-back termination.

**Sanity bounds (tunable):**
- `LastMessageAt > now + 1h` → ignore (clock skew tolerance is generous).
- `LastMessageAt.IsZero()` → ignore.
- `CreatedAt > now` → ignore.
- `CreatedAt.IsZero()` → ignore.

These bounds prevent a client from forcing a 365-bucket walk by supplying `MAX_TIME`.

**Net effect:**
- Cache-hot client request: zero MongoDB reads added for bucketing.
- Cache-cold client request: one MongoDB read for room times.
- Old clients without hints: behave correctly via the Mongo fallback.

## Operational Considerations

**99th-percentile target.** With daily buckets and ~2 KB average row size, partition sizes scale linearly with daily message volume:

| Daily volume | Daily partition size |
|---|---|
| 1k msg/day | ~2 MB |
| 5k msg/day | ~10 MB (at the soft cap) |
| 10k msg/day | ~20 MB (over) |
| 50k msg/day | ~100 MB (well over) |

If the busiest 1% of rooms run at ~10k msg/day, daily buckets will produce ~20 MB partitions for them. This is accepted under the 99% target. If observed traffic shows the 99th percentile higher, operators can drop `MESSAGE_BUCKET_HOURS` to `12` or `6` to gain headroom — no schema change.

**Year-dead rooms.** With `lastMessageAt` resolution (via hint or Mongo), the first page of a year-dead room is a single-bucket read (~5 ms). Without it (e.g., `lastMessageAt` not yet populated for a freshly-imported room), the walk would scan up to `maxBuckets` empty buckets — bounded but slow. This is acceptable for the initial-stage rollout.

**Federation.** Each site computes buckets independently from `created_at`. Cross-site events arrive on INBOX with the original `created_at`, so the receiving site computes the same bucket the sending site did (provided `MESSAGE_BUCKET_HOURS` matches across sites — operators must align this when federating).

## Testing Strategy

Per `CLAUDE.md` Section 4 — TDD, 80% minimum coverage, table-driven tests preferred.

**Unit tests:**

`pkg/msgbucket/bucket_test.go`:
- `Of` — table-driven over windows (`24h`, `12h`, `1h`), boundary cases (exactly on window edge, ±1ms, epoch zero).
- `Prev`/`Next` round-trip property.
- `WindowMs` getter.

`message-worker/store_cassandra_test.go` — extend to assert that mocked `Query` calls bind the expected `bucket` value derived from `msg.CreatedAt`. Cover `SaveMessage`, `SaveThreadMessage`, `incrementParentTcount`, `UpdateParentMessageThreadRoomID`.

`history-service/internal/service/...` — table-driven tests for `resolveRoomTimes`:
- Both hints valid → no Mongo call.
- One hint missing → one Mongo call.
- No hints → one Mongo call.
- `lastMessageAt > now + 1h` → ignored, fallback.
- `createdAt > now` → ignored, fallback.
- Zero values → ignored.

`history-service/internal/cassrepo` — pure-logic cursor codec tests:
- Round-trip: `decode(encode(x)) == x`.
- Length cap enforced.
- Empty cursor → fresh-walk semantics.

**Integration tests** (`//go:build integration`, testcontainers):

`history-service/internal/cassrepo/messages_by_room_integration_test.go`:
- Single-bucket page (write 100 in one bucket, page through with `pageSize=50`).
- Cross-bucket walk DESC (5 buckets, walk backward via cursor).
- Cross-bucket walk ASC.
- Sparse walk (gaps of empty buckets, walk skips up to `maxBuckets`).
- Cap reached without filling (returns non-terminal cursor at next bucket).
- Bounded `GetMessagesBetweenDesc` terminates at floor bucket.
- Edit and delete using the new `bucket = ?` predicate.
- `incrementParentTcount` cross-bucket (parent in bucket A, reply in bucket B).

`message-worker/integration_test.go` — extend save-flow tests to verify rows land in expected `(room_id, bucket)` partitions.

**Configuration tests:**
- `message-worker` and `history-service` config-parsing assert `MESSAGE_BUCKET_HOURS` defaults to `24`, accepts other positive integers, and rejects `0` / negative at startup.
- Startup-log assertion that the configured bucket size is logged.

## Documentation Updates (in same PR)

- `docs/cassandra_message_model.md` — updated DDL with `bucket` and brief note on bucket semantics.
- `pkg/model/cassandra/message.go` — `Bucket` field added to row structs.
- `docker-local/cassandra/init/10-table-messages_by_room.cql` — updated DDL.
- `docker-local/cassandra/init/11-table-thread_messages_by_room.cql` — updated DDL.
- `CLAUDE.md` Section 6 — invariant note: "`MESSAGE_BUCKET_HOURS` MUST match across all services that read or write `messages_by_room`/`thread_messages_by_room`."
