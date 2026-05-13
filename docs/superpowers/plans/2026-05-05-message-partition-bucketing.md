# Message Partition Bucketing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change `messages_by_room` and `thread_messages_by_room` from a single `room_id` partition key to a composite `(room_id, bucket)` partition key so that 99% of partitions stay under 10 MB.

**Architecture:** A new `pkg/msgbucket.Sizer` deterministically derives a `BIGINT` bucket value from `created_at`. Writes bind the bucket; reads walk buckets sequentially via a redesigned cursor `(bucket, pageState)`. Existing data is dropped (initial-stage project). The service layer fetches `room.LastMsgAt` and `room.CreatedAt` from MongoDB (with optional client-supplied hints) so dead-room reads stay fast.

**Tech Stack:** Go 1.25, gocql v1, mongo-driver/v2, gin not used here (NATS request/reply), caarlos0/env/v11, stretchr/testify, go.uber.org/mock, testcontainers-go.

**Spec:** `docs/superpowers/specs/2026-05-05-message-partition-bucketing-design.md`

---

## File Structure

**Create:**
- `pkg/msgbucket/bucket.go` — `Sizer` struct, `Of`/`Prev`/`Next`/`WindowMs`.
- `pkg/msgbucket/bucket_test.go` — unit tests.
- `history-service/internal/mongorepo/room.go` — `RoomRepo` with `GetRoomTimes`.
- `history-service/internal/mongorepo/room_test.go` — integration test.
- `history-service/internal/cassrepo/walker.go` — `fillPage` walk algorithm + new cursor codec.
- `history-service/internal/cassrepo/walker_test.go` — pure-logic cursor codec tests.

**Modify:**
- `docker-local/cassandra/init/10-table-messages_by_room.cql` — add `bucket BIGINT` to schema.
- `docker-local/cassandra/init/11-table-thread_messages_by_room.cql` — add `bucket BIGINT` to schema.
- `docs/cassandra_message_model.md` — schema source of truth.
- `pkg/model/cassandra/message.go` — `Bucket int64` field on row structs.
- `pkg/model/cassandra/message_test.go` — extend round-trip tests to include `Bucket`.
- `pkg/model/event.go` — `RoomHints` type added.
- `message-worker/main.go` — config field + `Sizer` wiring.
- `message-worker/store_cassandra.go` — bind `bucket` in all writes.
- `message-worker/store_cassandra_test.go` — extend to assert bucket binding.
- `message-worker/integration_test.go` — verify rows land in expected partition.
- `history-service/internal/config/config.go` — three new env knobs.
- `history-service/cmd/main.go` — wire `Sizer` and pass to repos.
- `history-service/internal/cassrepo/repository.go` — `Repository` gains a `Sizer` and `MaxBuckets`.
- `history-service/internal/cassrepo/messages_by_room.go` — switch each read function to `fillPage`.
- `history-service/internal/cassrepo/thread_messages.go` — same walk pattern.
- `history-service/internal/cassrepo/write.go` — add `bucket = ?` to edit/delete WHERE clauses.
- `history-service/internal/cassrepo/messages_by_room_integration_test.go` — extended cross-bucket tests.
- `history-service/internal/cassrepo/thread_messages_integration_test.go` — extended cross-bucket tests.
- `history-service/internal/cassrepo/write_integration_test.go` — confirm edit/delete still work.
- `history-service/internal/cassrepo/utils.go` — keep base64 framing helpers; cursor type moves to walker.go.
- `history-service/internal/models/message.go` — add `Bucket int64` (cql tag) to scan struct.
- `history-service/internal/models/event.go` (or `message.go`) — `RoomHints` accepted on request types.
- `history-service/internal/service/service.go` — `HistoryService` gains a `roomRepo` dep and `resolveRoomTimes` helper.
- `history-service/internal/service/messages.go` — call `resolveRoomTimes`, pass through to cassrepo.
- `history-service/internal/service/threads.go` — same.
- `history-service/internal/service/messages_test.go`, `threads_test.go` — extend with hint cases.
- `CLAUDE.md` Section 6 — invariant note about `MESSAGE_BUCKET_HOURS`.

**Unchanged:** `messages_by_id` schema/queries; `pinned_messages_by_room` schema/queries; federation (outbox/inbox); NATS subjects; `pkg/model.Message` (no bucket field).

---

## Task 1: Create `pkg/msgbucket` package

**Files:**
- Create: `pkg/msgbucket/bucket.go`
- Create: `pkg/msgbucket/bucket_test.go`

- [ ] **Step 1: Write the failing test**

`pkg/msgbucket/bucket_test.go`:

```go
package msgbucket_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestSizer_Of(t *testing.T) {
	tests := []struct {
		name   string
		window time.Duration
		t      time.Time
		want   int64
	}{
		{
			name:   "epoch in 24h window",
			window: 24 * time.Hour,
			t:      time.UnixMilli(0).UTC(),
			want:   0,
		},
		{
			name:   "1ms after start of bucket lands in same bucket",
			window: 24 * time.Hour,
			t:      time.UnixMilli(1).UTC(),
			want:   0,
		},
		{
			name:   "1ms before window edge lands in same bucket",
			window: 24 * time.Hour,
			t:      time.UnixMilli(86_400_000 - 1).UTC(),
			want:   0,
		},
		{
			name:   "exactly on window edge advances",
			window: 24 * time.Hour,
			t:      time.UnixMilli(86_400_000).UTC(),
			want:   86_400_000,
		},
		{
			name:   "1h window",
			window: time.Hour,
			t:      time.UnixMilli(3_600_000 + 123).UTC(),
			want:   3_600_000,
		},
		{
			name:   "12h window",
			window: 12 * time.Hour,
			t:      time.UnixMilli(13 * 3_600_000).UTC(),
			want:   12 * 3_600_000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := msgbucket.New(tc.window)
			assert.Equal(t, tc.want, s.Of(tc.t))
		})
	}
}

func TestSizer_PrevNextRoundTrip(t *testing.T) {
	s := msgbucket.New(24 * time.Hour)
	now := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)
	b := s.Of(now)
	assert.Equal(t, b, s.Next(s.Prev(b)))
	assert.Equal(t, b, s.Prev(s.Next(b)))
}

func TestSizer_PrevAdvancesBy_Window(t *testing.T) {
	s := msgbucket.New(6 * time.Hour)
	b := s.Of(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	assert.Equal(t, b-int64(6*time.Hour/time.Millisecond), s.Prev(b))
	assert.Equal(t, b+int64(6*time.Hour/time.Millisecond), s.Next(b))
}

func TestSizer_WindowMs(t *testing.T) {
	s := msgbucket.New(24 * time.Hour)
	assert.Equal(t, int64(86_400_000), s.WindowMs())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/msgbucket/...`
Expected: FAIL with "package github.com/hmchangw/chat/pkg/msgbucket: cannot find package" or similar.

- [ ] **Step 3: Write the implementation**

`pkg/msgbucket/bucket.go`:

```go
// Package msgbucket computes time-bucket boundaries for Cassandra message
// partitions. Bucket values are start-of-window unix milliseconds derived
// deterministically from a row's created_at; no shared state is required.
package msgbucket

import "time"

// Sizer computes the bucket value containing a timestamp.
type Sizer struct {
	windowMs int64
}

// New returns a Sizer for the given fixed window. Window must be positive;
// callers are expected to validate at startup (see service main.go).
func New(window time.Duration) Sizer {
	return Sizer{windowMs: window.Milliseconds()}
}

// Of returns the bucket (start-of-window unix millis) containing t.
func (s Sizer) Of(t time.Time) int64 {
	return (t.UnixMilli() / s.windowMs) * s.windowMs
}

// Prev returns the bucket immediately before b.
func (s Sizer) Prev(b int64) int64 { return b - s.windowMs }

// Next returns the bucket immediately after b.
func (s Sizer) Next(b int64) int64 { return b + s.windowMs }

// WindowMs returns the configured window in milliseconds.
func (s Sizer) WindowMs() int64 { return s.windowMs }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/user/chat && make test SERVICE=msgbucket` (the Makefile's per-service helper accepts package directories under pkg/) — or fall back to `go test ./pkg/msgbucket/...`.
Expected: all tests PASS, no race warnings.

- [ ] **Step 5: Lint**

Run: `cd /home/user/chat && make lint`
Expected: no findings in `pkg/msgbucket/`.

- [ ] **Step 6: Commit**

```bash
git -C /home/user/chat add pkg/msgbucket/
git -C /home/user/chat commit -m "feat(msgbucket): add bucket sizer for cassandra message partitions"
```

---

## Task 2: Update Cassandra DDL and `MessageRow` struct

This task changes the schema source-of-truth in three places (per `CLAUDE.md` Section 6: doc → Go struct → init DDL must move together). Existing data is dropped, so no migration logic is needed.

**Files:**
- Modify: `docker-local/cassandra/init/10-table-messages_by_room.cql`
- Modify: `docker-local/cassandra/init/11-table-thread_messages_by_room.cql`
- Modify: `docs/cassandra_message_model.md`
- Modify: `pkg/model/cassandra/message.go`
- Modify: `pkg/model/cassandra/message_test.go`

- [ ] **Step 1: Update `messages_by_room` DDL**

`docker-local/cassandra/init/10-table-messages_by_room.cql` — add `bucket BIGINT` and include it in the partition key:

```cql
CREATE TABLE IF NOT EXISTS chat.messages_by_room (
  room_id                  TEXT,
  bucket                   BIGINT,
  created_at               TIMESTAMP,
  message_id               TEXT,
  thread_room_id           TEXT,
  sender                   FROZEN<"Participant">,
  target_user              FROZEN<"Participant">,
  msg                      TEXT,
  mentions                 SET<FROZEN<"Participant">>,
  attachments              LIST<BLOB>,
  file                     FROZEN<"File">,
  card                     FROZEN<"Card">,
  card_action              FROZEN<"CardAction">,
  tshow                    BOOLEAN,
  tcount                   INT,
  thread_parent_id         TEXT,
  thread_parent_created_at TIMESTAMP,
  quoted_parent_message    FROZEN<"QuotedParentMessage">,
  visible_to               TEXT,
  reactions                MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>,
  deleted                  BOOLEAN,
  type                     TEXT,
  sys_msg_data             BLOB,
  site_id                  TEXT,
  edited_at                TIMESTAMP,
  updated_at               TIMESTAMP,
  PRIMARY KEY ((room_id, bucket), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```

- [ ] **Step 2: Update `thread_messages_by_room` DDL**

`docker-local/cassandra/init/11-table-thread_messages_by_room.cql`:

```cql
CREATE TABLE IF NOT EXISTS chat.thread_messages_by_room (
  room_id                  TEXT,
  bucket                   BIGINT,
  thread_room_id           TEXT,
  created_at               TIMESTAMP,
  message_id               TEXT,
  thread_parent_id         TEXT,
  sender                   FROZEN<"Participant">,
  target_user              FROZEN<"Participant">,
  msg                      TEXT,
  mentions                 SET<FROZEN<"Participant">>,
  attachments              LIST<BLOB>,
  file                     FROZEN<"File">,
  card                     FROZEN<"Card">,
  card_action              FROZEN<"CardAction">,
  quoted_parent_message    FROZEN<"QuotedParentMessage">,
  visible_to               TEXT,
  reactions                MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>,
  deleted                  BOOLEAN,
  type                     TEXT,
  sys_msg_data             BLOB,
  site_id                  TEXT,
  edited_at                TIMESTAMP,
  updated_at               TIMESTAMP,
  PRIMARY KEY ((room_id, bucket), thread_room_id, created_at, message_id)
) WITH CLUSTERING ORDER BY (thread_room_id DESC, created_at DESC, message_id DESC);
```

- [ ] **Step 3: Update doc `docs/cassandra_message_model.md`**

In the `messages_by_room` block, replace the body with the DDL from Step 1. In the `thread_messages_by_room` block, replace the body with the DDL from Step 2. Add this paragraph above the `messages_by_room` heading:

```markdown
### Partition Bucketing

`messages_by_room` and `thread_messages_by_room` use a composite partition key
`(room_id, bucket)`. `bucket` is the start-of-window in unix milliseconds derived
deterministically from `created_at` via `pkg/msgbucket.Sizer`. The window size
is configured per service via `MESSAGE_BUCKET_HOURS` (default 24); all services
that read or write these tables MUST be configured with the same window.
```

- [ ] **Step 4: Add `Bucket` field to `Message` row struct**

In `pkg/model/cassandra/message.go`, insert after the `RoomID` field (line 76) so partition-key fields are grouped:

```go
RoomID    string `json:"roomId"           cql:"room_id"`
Bucket    int64  `json:"bucket,omitempty" cql:"bucket"`
CreatedAt time.Time `json:"createdAt"     cql:"created_at"`
```

(Keep the rest of the struct unchanged. `omitempty` is set so `messages_by_id` and `pinned_messages_by_room` rows — which have no `bucket` column — round-trip through JSON without spurious zero values.)

- [ ] **Step 5: Extend round-trip test to include `Bucket`**

Find the existing `Message` round-trip test in `pkg/model/cassandra/message_test.go` and add `Bucket: 86_400_000` (or any non-zero int64) to the populated fixture. Confirm the round-trip still passes.

- [ ] **Step 6: Verify**

Run: `make lint && make test SERVICE=cassandra`
Expected: lint clean; `pkg/model/cassandra` tests pass.

- [ ] **Step 7: Commit**

```bash
git -C /home/user/chat add docker-local/cassandra/init/ docs/cassandra_message_model.md pkg/model/cassandra/
git -C /home/user/chat commit -m "feat(cassandra): add bucket to messages_by_room and thread_messages_by_room partition key"
```

---

## Task 3: Wire `Sizer` into `message-worker`

This task touches the constructor of `CassandraStore` and the config struct. Many integration tests call `NewCassandraStore(cassSession)` — they all need updating in the same commit so the build stays green.

**Files:**
- Modify: `message-worker/main.go`
- Modify: `message-worker/store_cassandra.go`
- Modify: `message-worker/integration_test.go`

- [ ] **Step 1: Add config field**

In `message-worker/main.go` `type config struct`, add (alphabetically-grouped near `MaxWorkers`):

```go
MessageBucketHours int `env:"MESSAGE_BUCKET_HOURS" envDefault:"24"`
```

- [ ] **Step 2: Validate at startup**

After `cfg, err := env.ParseAs[config]()` and before `nc, err := natsutil.Connect(...)`, add:

```go
if cfg.MessageBucketHours < 1 {
    slog.Error("invalid config", "MESSAGE_BUCKET_HOURS", cfg.MessageBucketHours)
    os.Exit(1)
}
slog.Info("message bucket configured", "hours", cfg.MessageBucketHours)

bucketSizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
```

Add the import:

```go
"github.com/hmchangw/chat/pkg/msgbucket"
```

- [ ] **Step 3: Update `NewCassandraStore` signature**

In `message-worker/store_cassandra.go`:

```go
type CassandraStore struct {
    cassSession *gocql.Session
    bucket      msgbucket.Sizer
}

func NewCassandraStore(session *gocql.Session, bucket msgbucket.Sizer) *CassandraStore {
    return &CassandraStore{cassSession: session, bucket: bucket}
}
```

Add the import: `"github.com/hmchangw/chat/pkg/msgbucket"`.

- [ ] **Step 4: Update the `main.go` call site**

In `message-worker/main.go`, replace:

```go
store := NewCassandraStore(cassSession)
```

with:

```go
store := NewCassandraStore(cassSession, bucketSizer)
```

- [ ] **Step 5: Update integration test call sites**

In `message-worker/integration_test.go`, find every `NewCassandraStore(cassSession)` call (there are ~10) and replace with:

```go
NewCassandraStore(cassSession, msgbucket.New(24*time.Hour))
```

Add the import to the test file:

```go
"github.com/hmchangw/chat/pkg/msgbucket"
```

(Using a fixed 24-hour Sizer in tests keeps test data deterministic and matches the production default.)

- [ ] **Step 6: Verify the project still builds**

Run: `go build ./message-worker/...`
Expected: clean build (writes still bind without `bucket` — that comes in Task 4 and will fail the integration tests, but unit compilation succeeds).

Run: `make lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git -C /home/user/chat add message-worker/
git -C /home/user/chat commit -m "refactor(message-worker): inject msgbucket.Sizer into CassandraStore"
```

---

## Task 4: Bind `bucket` in `SaveMessage`

**Files:**
- Modify: `message-worker/store_cassandra.go` (`SaveMessage`)
- Modify: `message-worker/integration_test.go` (existing SaveMessage test, plus a new partition-key assertion)

- [ ] **Step 1: Write the failing test**

Add a new test to `message-worker/integration_test.go` (next to the existing SaveMessage tests):

```go
func TestSaveMessage_BindsBucket(t *testing.T) {
    cassSession := setupCassandra(t)
    bucket := msgbucket.New(24 * time.Hour)
    store := NewCassandraStore(cassSession, bucket)

    msg := &model.Message{
        ID:        "msg-bucket-1",
        RoomID:    "room-bucket-1",
        Content:   "hello",
        CreatedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
        Type:      "text",
    }
    sender := &cassParticipant{ID: "u1", Account: "alice"}

    require.NoError(t, store.SaveMessage(context.Background(), msg, sender, "site-A"))

    expectedBucket := bucket.Of(msg.CreatedAt)
    var gotBucket int64
    require.NoError(t, cassSession.Query(
        `SELECT bucket FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
        msg.RoomID, expectedBucket, msg.CreatedAt, msg.ID,
    ).Scan(&gotBucket))
    assert.Equal(t, expectedBucket, gotBucket)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && make test-integration SERVICE=message-worker`
Expected: `TestSaveMessage_BindsBucket` FAILS — the INSERT in `SaveMessage` does not yet bind `bucket`, and Cassandra rejects a write whose partition-key column is missing (or the row is found in a different partition than expected).

- [ ] **Step 3: Implement bucket binding in `SaveMessage`**

In `message-worker/store_cassandra.go`, change the `messages_by_room` INSERT inside `SaveMessage`:

```go
b := s.bucket.Of(msg.CreatedAt)
if err := s.cassSession.Query(
    `INSERT INTO messages_by_room
       (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at,
        mentions, type, sys_msg_data, tshow, quoted_parent_message)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    msg.RoomID, b, msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt,
    toMentionSet(msg.Mentions), msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
).WithContext(ctx).Exec(); err != nil {
    return fmt.Errorf("insert messages_by_room %s: %w", msg.ID, err)
}
```

The `messages_by_id` INSERT below is unchanged.

- [ ] **Step 4: Run integration tests to verify**

Run: `cd /home/user/chat && make test-integration SERVICE=message-worker`
Expected: `TestSaveMessage_BindsBucket` PASSES; existing SaveMessage tests still PASS.

- [ ] **Step 5: Commit**

```bash
git -C /home/user/chat add message-worker/
git -C /home/user/chat commit -m "feat(message-worker): bind bucket in SaveMessage"
```

---

## Task 5: Bind `bucket` in `SaveThreadMessage`

**Files:**
- Modify: `message-worker/store_cassandra.go` (`SaveThreadMessage`)
- Modify: `message-worker/integration_test.go`

- [ ] **Step 1: Write the failing test**

Add to `message-worker/integration_test.go`:

```go
func TestSaveThreadMessage_BindsBucket(t *testing.T) {
    cassSession := setupCassandra(t)
    bucket := msgbucket.New(24 * time.Hour)
    store := NewCassandraStore(cassSession, bucket)
    ctx := context.Background()

    parentCreatedAt := time.Date(2026, 4, 30, 9, 0, 0, 0, time.UTC)
    parentID := "parent-1"

    // seed parent in messages_by_id and messages_by_room (parent's bucket is parentCreatedAt's bucket)
    parentBucket := bucket.Of(parentCreatedAt)
    sender := &cassParticipant{ID: "u1", Account: "alice"}
    require.NoError(t, store.SaveMessage(ctx, &model.Message{
        ID: parentID, RoomID: "room-thread-1", Content: "parent",
        CreatedAt: parentCreatedAt, Type: "text",
    }, sender, "site-A"))

    replyCreatedAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
    reply := &model.Message{
        ID:                           "reply-1",
        RoomID:                       "room-thread-1",
        Content:                      "reply",
        CreatedAt:                    replyCreatedAt,
        Type:                         "text",
        ThreadParentMessageID:        parentID,
        ThreadParentMessageCreatedAt: &parentCreatedAt,
    }
    require.NoError(t, store.SaveThreadMessage(ctx, reply, sender, "site-A", "thread-room-1"))

    expectedBucket := bucket.Of(replyCreatedAt)
    var gotBucket int64
    require.NoError(t, cassSession.Query(
        `SELECT bucket FROM thread_messages_by_room
         WHERE room_id = ? AND bucket = ? AND thread_room_id = ? AND created_at = ? AND message_id = ?`,
        reply.RoomID, expectedBucket, "thread-room-1", reply.CreatedAt, reply.ID,
    ).Scan(&gotBucket))
    assert.Equal(t, expectedBucket, gotBucket)

    // parent's tcount in messages_by_room should also be readable via parent bucket
    var tcount int
    require.NoError(t, cassSession.Query(
        `SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
        reply.RoomID, parentBucket, parentCreatedAt, parentID,
    ).Scan(&tcount))
    assert.Equal(t, 1, tcount)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=message-worker`
Expected: `TestSaveThreadMessage_BindsBucket` FAILS — both because `thread_messages_by_room` INSERT lacks `bucket`, and because the parent-row CAS in `incrementParentTcount` queries `messages_by_room` without `bucket`.

- [ ] **Step 3: Implement bucket binding in `SaveThreadMessage`**

In `message-worker/store_cassandra.go`, change the `thread_messages_by_room` INSERT:

```go
b := s.bucket.Of(msg.CreatedAt)
if err := s.cassSession.Query(
    `INSERT INTO thread_messages_by_room
     (room_id, bucket, thread_room_id, created_at, message_id, thread_parent_id, sender, msg,
      site_id, updated_at, mentions, type, sys_msg_data, quoted_parent_message)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    msg.RoomID, b, threadRoomID, msg.CreatedAt, msg.ID, msg.ThreadParentMessageID,
    sender, msg.Content, siteID, msg.CreatedAt, toMentionSet(msg.Mentions),
    msg.Type, msg.SysMsgData, msg.QuotedParentMessage,
).WithContext(ctx).Exec(); err != nil {
    return fmt.Errorf("insert thread_messages_by_room %s: %w", msg.ID, err)
}
```

The `messages_by_id` INSERT in this method is unchanged. (The `incrementParentTcount` call below stays put — its bucket binding is implemented in Task 6.)

- [ ] **Step 4: Run tests**

Run: `make test-integration SERVICE=message-worker`
Expected: `TestSaveThreadMessage_BindsBucket` reaches the parent-tcount assertion but fails on it (still missing in `incrementParentTcount`). That sub-failure is fixed in Task 6.

For now, narrow the test to just the `thread_messages_by_room` row check by commenting out the parent-tcount assertion temporarily, OR proceed straight to Task 6 in the same branch — choose whichever is cleaner. The test fully passes after Task 6.

- [ ] **Step 5: Commit**

```bash
git -C /home/user/chat add message-worker/
git -C /home/user/chat commit -m "feat(message-worker): bind bucket in SaveThreadMessage"
```

---

## Task 6: Bind `bucket` on parent-row updates

These are the queries inside `incrementParentTcount` and `UpdateParentMessageThreadRoomID` that update the parent's row in `messages_by_room`. The parent's bucket is derived from `msg.ThreadParentMessageCreatedAt` (already on the message domain object).

**Files:**
- Modify: `message-worker/store_cassandra.go` (`incrementParentTcount`, `UpdateParentMessageThreadRoomID`)

- [ ] **Step 1: Update `incrementParentTcount`**

In `message-worker/store_cassandra.go`, replace the body of `incrementParentTcount` so the `messages_by_room` SELECT and CAS UPDATE include `bucket`:

```go
func (s *CassandraStore) incrementParentTcount(ctx context.Context, msg *model.Message) error {
    if msg.ThreadParentMessageCreatedAt == nil {
        return nil
    }
    parentID := msg.ThreadParentMessageID
    parentCreatedAt := *msg.ThreadParentMessageCreatedAt
    parentBucket := s.bucket.Of(parentCreatedAt)

    // CAS increment on messages_by_id (no bucket — table unchanged).
    var tcount *int
    if err := s.cassSession.Query(
        `SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
        parentID, parentCreatedAt,
    ).WithContext(ctx).Scan(&tcount); err != nil {
        if errors.Is(err, gocql.ErrNotFound) {
            return nil
        }
        return fmt.Errorf("read tcount for parent message %s: %w", parentID, err)
    }
    if err := casIncrement(casMaxRetries, tcount, func(newVal int, expected *int) (bool, *int, error) {
        var current *int
        applied, err := s.cassSession.Query(
            `UPDATE messages_by_id SET tcount = ? WHERE message_id = ? AND created_at = ? IF tcount = ?`,
            newVal, parentID, parentCreatedAt, expected,
        ).WithContext(ctx).ScanCAS(&current)
        return applied, current, err
    }); err != nil {
        return fmt.Errorf("cas tcount in messages_by_id for parent %s: %w", parentID, err)
    }

    // CAS increment on messages_by_room (now includes bucket in the WHERE).
    if err := s.cassSession.Query(
        `SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
        msg.RoomID, parentBucket, parentCreatedAt, parentID,
    ).WithContext(ctx).Scan(&tcount); err != nil {
        if errors.Is(err, gocql.ErrNotFound) {
            return nil
        }
        return fmt.Errorf("read tcount in messages_by_room for parent %s: %w", parentID, err)
    }
    if err := casIncrement(casMaxRetries, tcount, func(newVal int, expected *int) (bool, *int, error) {
        var current *int
        applied, err := s.cassSession.Query(
            `UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ? IF tcount = ?`,
            newVal, msg.RoomID, parentBucket, parentCreatedAt, parentID, expected,
        ).WithContext(ctx).ScanCAS(&current)
        return applied, current, err
    }); err != nil {
        return fmt.Errorf("cas tcount in messages_by_room for parent %s: %w", parentID, err)
    }

    return nil
}
```

- [ ] **Step 2: Update `UpdateParentMessageThreadRoomID`**

In the same file, change the `messages_by_room` UPDATE inside `UpdateParentMessageThreadRoomID` to include `bucket`:

```go
parentBucket := s.bucket.Of(parentCreatedAt)

applied, err = s.cassSession.Query(
    `UPDATE messages_by_room SET thread_room_id = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ? IF EXISTS`,
    threadRoomID, roomID, parentBucket, parentCreatedAt, parentMessageID,
).WithContext(ctx).ScanCAS()
if err != nil {
    return fmt.Errorf("set thread_room_id on parent %s in messages_by_room: %w", parentMessageID, err)
}
if !applied {
    slog.Warn("parent row absent in messages_by_room; thread_room_id not stamped", "messageID", parentMessageID, "roomID", roomID)
}
```

The `messages_by_id` UPDATE above is unchanged.

- [ ] **Step 3: Re-enable the parent-tcount assertion in `TestSaveThreadMessage_BindsBucket`** (if you commented it out in Task 5).

- [ ] **Step 4: Run integration tests**

Run: `make test-integration SERVICE=message-worker`
Expected: all PASS, including the parent-tcount assertion in `TestSaveThreadMessage_BindsBucket` and any existing tests covering `UpdateParentMessageThreadRoomID`.

- [ ] **Step 5: Commit**

```bash
git -C /home/user/chat add message-worker/
git -C /home/user/chat commit -m "feat(message-worker): bind bucket on parent-row updates"
```

---

## Task 7: Add bucket config to `history-service`

**Files:**
- Modify: `history-service/internal/config/config.go`
- Modify: `history-service/cmd/main.go`

- [ ] **Step 1: Add config fields**

In `history-service/internal/config/config.go`, append a new struct and add it to the top-level Config:

```go
// MessageReadConfig holds message-bucketing knobs for the read/edit path.
// MESSAGE_BUCKET_HOURS MUST match the value used by message-worker.
type MessageReadConfig struct {
    BucketHours       int `env:"BUCKET_HOURS"        envDefault:"24"`
    MaxBuckets        int `env:"READ_MAX_BUCKETS"    envDefault:"365"`
    HistoryFloorDays  int `env:"HISTORY_FLOOR_DAYS"  envDefault:"730"`
}
```

Wait — `envPrefix` works only at the parent level. The cleanest way to namespace these under `MESSAGE_*` is to declare them on `Config` directly with explicit env names:

```go
// Config is the top-level configuration for history-service.
type Config struct {
    SiteID                  string          `env:"SITE_ID" envDefault:"site-local"`
    Cassandra               CassandraConfig `envPrefix:"CASSANDRA_"`
    Mongo                   MongoConfig     `envPrefix:"MONGO_"`
    NATS                    NATSConfig      `envPrefix:"NATS_"`
    Valkey                  ValkeyConfig    `envPrefix:"VALKEY_"`
    MessageBucketHours      int             `env:"MESSAGE_BUCKET_HOURS"       envDefault:"24"`
    MessageReadMaxBuckets   int             `env:"MESSAGE_READ_MAX_BUCKETS"   envDefault:"365"`
    MessageHistoryFloorDays int             `env:"MESSAGE_HISTORY_FLOOR_DAYS" envDefault:"730"`
}
```

(Discard the nested struct sketch above — keep flat.)

- [ ] **Step 2: Validate at startup**

In `history-service/cmd/main.go`, after `cfg, err := config.Load()` and before any repo construction, add:

```go
if cfg.MessageBucketHours < 1 {
    slog.Error("invalid config", "MESSAGE_BUCKET_HOURS", cfg.MessageBucketHours)
    os.Exit(1)
}
if cfg.MessageReadMaxBuckets < 1 {
    slog.Error("invalid config", "MESSAGE_READ_MAX_BUCKETS", cfg.MessageReadMaxBuckets)
    os.Exit(1)
}
if cfg.MessageHistoryFloorDays < 1 {
    slog.Error("invalid config", "MESSAGE_HISTORY_FLOOR_DAYS", cfg.MessageHistoryFloorDays)
    os.Exit(1)
}
slog.Info("message bucket configured",
    "hours", cfg.MessageBucketHours,
    "maxBuckets", cfg.MessageReadMaxBuckets,
    "historyFloorDays", cfg.MessageHistoryFloorDays,
)

bucketSizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
historyFloor := time.Duration(cfg.MessageHistoryFloorDays) * 24 * time.Hour
```

Add the import:

```go
"github.com/hmchangw/chat/pkg/msgbucket"
```

(`time` is likely already imported.)

- [ ] **Step 3: Verify build and lint**

Run: `go build ./history-service/...`
Run: `make lint`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git -C /home/user/chat add history-service/
git -C /home/user/chat commit -m "feat(history-service): add MESSAGE_BUCKET_HOURS and read knobs to config"
```

---

## Task 8: Inject `Sizer` and `MaxBuckets` into `cassrepo.Repository`

**Files:**
- Modify: `history-service/internal/cassrepo/repository.go`
- Modify: `history-service/cmd/main.go` (call site)
- Modify: every `NewRepository(session)` call site in tests:
  - `history-service/internal/cassrepo/messages_by_id_integration_test.go`
  - `history-service/internal/cassrepo/write_integration_test.go`
  - `history-service/internal/cassrepo/messages_by_room_integration_test.go`
  - `history-service/internal/cassrepo/thread_messages_integration_test.go`
  - `history-service/internal/cassrepo/integration_test.go`

- [ ] **Step 1: Update `Repository` and constructor**

Replace `history-service/internal/cassrepo/repository.go`:

```go
package cassrepo

import (
    "github.com/gocql/gocql"

    "github.com/hmchangw/chat/pkg/msgbucket"
)

// Repository wraps a Cassandra session with the bucket sizer and read-walk
// configuration shared by all queries against bucketed message tables.
type Repository struct {
    session    *gocql.Session
    bucket     msgbucket.Sizer
    maxBuckets int
}

// NewRepository wires a session, bucket sizer, and max-walk depth.
// maxBuckets caps how far a paginated read walks through empty buckets before
// returning a non-terminal cursor.
func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets int) *Repository {
    return &Repository{
        session:    session,
        bucket:     bucket,
        maxBuckets: maxBuckets,
    }
}
```

- [ ] **Step 2: Update `cmd/main.go` call site**

Replace:

```go
cassRepo := cassrepo.NewRepository(cassSession)
```

with:

```go
cassRepo := cassrepo.NewRepository(cassSession, bucketSizer, cfg.MessageReadMaxBuckets)
```

- [ ] **Step 3: Update test call sites**

In each integration test file listed above, replace every `NewRepository(session)` with:

```go
NewRepository(session, msgbucket.New(24*time.Hour), 365)
```

Add the import to each test file:

```go
"time"
"github.com/hmchangw/chat/pkg/msgbucket"
```

(`time` may already be imported.)

- [ ] **Step 4: Verify build**

Run: `go build ./history-service/...`
Expected: build succeeds (existing read functions don't yet use `repo.bucket` — that's Task 11+; build cleanliness only).

Run: `make lint`
Expected: clean. The unused-field linter may complain about `bucket`/`maxBuckets`; if so, this is resolved as soon as Task 11 references them. To avoid noise during the gap, add `_ = r.bucket; _ = r.maxBuckets` in a small `init`-style helper or simply press through to Task 11 in the same series of subagent commits.

- [ ] **Step 5: Commit**

```bash
git -C /home/user/chat add history-service/
git -C /home/user/chat commit -m "refactor(history-service): inject msgbucket.Sizer and maxBuckets into Repository"
```

---

## Task 9: Add `RoomRepo.GetRoomTimes` to mongorepo

The room document already has `LastMsgAt *time.Time` and `CreatedAt time.Time` (see `pkg/model/room.go`). `broadcast-worker.FetchAndUpdateRoom` populates `LastMsgAt` on every message, so no message-worker changes are needed for this. We only add a read accessor.

**Files:**
- Create: `history-service/internal/mongorepo/room.go`
- Create: `history-service/internal/mongorepo/room_test.go`
- Modify: `history-service/cmd/main.go` (construct + pass to service)

- [ ] **Step 1: Write the failing integration test**

`history-service/internal/mongorepo/room_test.go`:

```go
//go:build integration

package mongorepo

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.mongodb.org/mongo-driver/v2/bson"

    "github.com/hmchangw/chat/pkg/model"
)

func TestRoomRepo_GetRoomTimes(t *testing.T) {
    db := setupMongo(t)
    repo := NewRoomRepo(db)

    createdAt := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
    lastMsgAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
    room := model.Room{
        ID:        "room-times-1",
        SiteID:    "site-A",
        Type:      model.RoomTypeChannel,
        CreatedAt: createdAt,
        LastMsgAt: &lastMsgAt,
    }
    _, err := db.Collection("rooms").InsertOne(context.Background(), room)
    require.NoError(t, err)

    gotLast, gotCreated, err := repo.GetRoomTimes(context.Background(), "room-times-1")
    require.NoError(t, err)
    assert.Equal(t, lastMsgAt.UTC(), gotLast.UTC())
    assert.Equal(t, createdAt.UTC(), gotCreated.UTC())
}

func TestRoomRepo_GetRoomTimes_NoLastMsg(t *testing.T) {
    db := setupMongo(t)
    repo := NewRoomRepo(db)

    createdAt := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
    _, err := db.Collection("rooms").InsertOne(context.Background(), bson.M{
        "_id":       "room-no-lastmsg",
        "siteId":    "site-A",
        "type":      "channel",
        "createdAt": createdAt,
    })
    require.NoError(t, err)

    gotLast, gotCreated, err := repo.GetRoomTimes(context.Background(), "room-no-lastmsg")
    require.NoError(t, err)
    assert.True(t, gotLast.IsZero(), "lastMsgAt absent → zero time")
    assert.Equal(t, createdAt.UTC(), gotCreated.UTC())
}

func TestRoomRepo_GetRoomTimes_NotFound(t *testing.T) {
    db := setupMongo(t)
    repo := NewRoomRepo(db)

    _, _, err := repo.GetRoomTimes(context.Background(), "no-such-room")
    require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=history-service`
Expected: compile errors — `NewRoomRepo` undefined.

- [ ] **Step 3: Write the implementation**

`history-service/internal/mongorepo/room.go`:

```go
package mongorepo

import (
    "context"
    "fmt"
    "time"

    "go.mongodb.org/mongo-driver/v2/bson"
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
)

const roomsCollection = "rooms"

type RoomRepo struct {
    rooms *mongo.Collection
}

func NewRoomRepo(db *mongo.Database) *RoomRepo {
    return &RoomRepo{rooms: db.Collection(roomsCollection)}
}

// roomTimes is a projection-only struct used by GetRoomTimes.
type roomTimes struct {
    LastMsgAt *time.Time `bson:"lastMsgAt"`
    CreatedAt time.Time  `bson:"createdAt"`
}

// GetRoomTimes returns lastMsgAt (zero time when unset) and createdAt for the given room.
// Returns mongo.ErrNoDocuments wrapped when the room does not exist.
func (r *RoomRepo) GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error) {
    opts := options.FindOne().SetProjection(bson.M{"lastMsgAt": 1, "createdAt": 1, "_id": 0})
    var rt roomTimes
    if err := r.rooms.FindOne(ctx, bson.M{"_id": roomID}, opts).Decode(&rt); err != nil {
        return time.Time{}, time.Time{}, fmt.Errorf("get room times for %s: %w", roomID, err)
    }
    if rt.LastMsgAt != nil {
        lastMsgAt = *rt.LastMsgAt
    }
    return lastMsgAt, rt.CreatedAt, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=history-service`
Expected: the three new tests in `room_test.go` PASS.

- [ ] **Step 5: Wire into `cmd/main.go`**

In `history-service/cmd/main.go`, where the other mongorepo constructors are called, add:

```go
roomRepo := mongorepo.NewRoomRepo(mongoDB)
```

Pass `roomRepo` into `service.New(...)` (the constructor will gain a parameter in Task 14 — for now, just construct it; if `service.New` doesn't accept it yet, hold off on the call-site change until Task 14 and commit only the `room.go` + `room_test.go` files here).

- [ ] **Step 6: Commit**

```bash
git -C /home/user/chat add history-service/internal/mongorepo/room.go history-service/internal/mongorepo/room_test.go
git -C /home/user/chat commit -m "feat(history-service/mongorepo): add RoomRepo.GetRoomTimes"
```

---

## Task 10: Bucket-aware cursor codec (`walker.go`)

The current cursor is a raw Cassandra `PageState` blob (one partition's worth of state). After bucketing, a cursor must carry both a bucket value and an in-bucket page state. The new cursor type lives in `walker.go` alongside the `fillPage` helper added in Task 11.

**Files:**
- Create: `history-service/internal/cassrepo/walker.go` (cursor codec only in this task; `fillPage` added in Task 11)
- Create: `history-service/internal/cassrepo/walker_test.go`
- Modify: `history-service/internal/cassrepo/utils.go` (only the `Cursor` definition — see Step 3)

- [ ] **Step 1: Write the failing test**

`history-service/internal/cassrepo/walker_test.go`:

```go
package cassrepo

import (
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestBucketCursor_RoundTrip(t *testing.T) {
    tests := []struct {
        name      string
        bucket    int64
        pageState []byte
    }{
        {name: "empty page state", bucket: 86_400_000, pageState: nil},
        {name: "small page state", bucket: 0, pageState: []byte{0x01, 0x02, 0x03}},
        {name: "negative bucket allowed (pre-epoch test data)", bucket: -86_400_000, pageState: []byte{0xff}},
        {name: "long page state", bucket: 1_700_000_000_000, pageState: make([]byte, 200)},
    }
    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            encoded := encodeBucketCursor(tc.bucket, tc.pageState)
            require.NotEmpty(t, encoded, "encoded cursor must not be empty")

            gotBucket, gotPageState, err := decodeBucketCursor(encoded)
            require.NoError(t, err)
            assert.Equal(t, tc.bucket, gotBucket)
            assert.Equal(t, tc.pageState, gotPageState)
        })
    }
}

func TestBucketCursor_EmptyEncoded_IsFreshWalk(t *testing.T) {
    bucket, pageState, err := decodeBucketCursor("")
    require.NoError(t, err)
    assert.Equal(t, int64(0), bucket)
    assert.Nil(t, pageState)
}

func TestBucketCursor_RejectsOversize(t *testing.T) {
    big := make([]byte, maxCursorBytes+1)
    encoded := encodeBucketCursor(0, big)
    _, _, err := decodeBucketCursor(encoded)
    require.Error(t, err)
}

func TestBucketCursor_RejectsCorruptBase64(t *testing.T) {
    _, _, err := decodeBucketCursor("not-valid-base64!@#")
    require.Error(t, err)
}

func TestBucketCursor_RejectsTruncatedFraming(t *testing.T) {
    // Valid base64 but only 4 bytes (< 8-byte bucket header).
    encoded := encodeBucketCursor(0, nil)[:6]
    _, _, err := decodeBucketCursor(encoded)
    require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./history-service/internal/cassrepo/ -run BucketCursor -v`
Expected: FAIL — `encodeBucketCursor`/`decodeBucketCursor` undefined.

- [ ] **Step 3: Write the implementation**

Create `history-service/internal/cassrepo/walker.go`:

```go
package cassrepo

import (
    "encoding/base64"
    "encoding/binary"
    "fmt"
)

// Bucket cursor wire format (then base64-encoded for transport):
//   [bucket: 8 bytes BE int64][pageStateLen: 2 bytes BE uint16][pageState: N bytes]
//
// Empty input string decodes to (bucket=0, pageState=nil), interpreted by the
// walker as "start from caller-supplied startBucket with a fresh in-bucket
// query". The bucket=0 sentinel is unambiguous because the walker passes its
// own startBucket when the cursor is empty.

const bucketCursorHeaderBytes = 8 + 2

func encodeBucketCursor(bucket int64, pageState []byte) string {
    buf := make([]byte, bucketCursorHeaderBytes+len(pageState))
    binary.BigEndian.PutUint64(buf[0:8], uint64(bucket))
    binary.BigEndian.PutUint16(buf[8:10], uint16(len(pageState)))
    copy(buf[bucketCursorHeaderBytes:], pageState)
    return base64.StdEncoding.EncodeToString(buf)
}

func decodeBucketCursor(encoded string) (int64, []byte, error) {
    if encoded == "" {
        return 0, nil, nil
    }
    if len(encoded) > base64.StdEncoding.EncodedLen(maxCursorBytes) {
        return 0, nil, fmt.Errorf("decode bucket cursor: encoded length %d exceeds maximum", len(encoded))
    }
    raw, err := base64.StdEncoding.DecodeString(encoded)
    if err != nil {
        return 0, nil, fmt.Errorf("decode bucket cursor: %w", err)
    }
    if len(raw) > maxCursorBytes {
        return 0, nil, fmt.Errorf("decode bucket cursor: decoded length %d exceeds maximum %d", len(raw), maxCursorBytes)
    }
    if len(raw) < bucketCursorHeaderBytes {
        return 0, nil, fmt.Errorf("decode bucket cursor: truncated framing (%d bytes)", len(raw))
    }
    bucket := int64(binary.BigEndian.Uint64(raw[0:8]))
    psLen := int(binary.BigEndian.Uint16(raw[8:10]))
    if bucketCursorHeaderBytes+psLen > len(raw) {
        return 0, nil, fmt.Errorf("decode bucket cursor: pageState length %d exceeds available %d", psLen, len(raw)-bucketCursorHeaderBytes)
    }
    var pageState []byte
    if psLen > 0 {
        pageState = make([]byte, psLen)
        copy(pageState, raw[bucketCursorHeaderBytes:bucketCursorHeaderBytes+psLen])
    }
    return bucket, pageState, nil
}
```

`maxCursorBytes` is defined in `utils.go` and reused; do not duplicate it.

- [ ] **Step 4: Verify**

Run: `make test SERVICE=history-service`
Expected: cursor codec tests PASS.

Run: `make lint`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git -C /home/user/chat add history-service/internal/cassrepo/walker.go history-service/internal/cassrepo/walker_test.go
git -C /home/user/chat commit -m "feat(cassrepo): add bucket-aware cursor codec"
```

---

## Task 11: Add `fillPage` walk helper

**Files:**
- Modify: `history-service/internal/cassrepo/walker.go` (append `fillPage` and types)

The walker is a single internal helper used by all four messages_by_room read functions and the thread reads. It is parameterized by:
- a query factory `func(bucket int64) *gocql.Query` — produces a single-bucket query
- direction (DESC walks Prev, ASC walks Next)
- starting bucket
- floor bucket (inclusive lower bound for DESC, inclusive upper bound for ASC walks via `floorBucket+1` semantics)
- max-walk depth

- [ ] **Step 1: Write the test plan in comment form, then implement**

(There are no pure-unit tests for `fillPage` — its behavior is exercised end-to-end through integration tests in Task 12+. Keeping it pure-internal reduces test maintenance against gocql internals.)

- [ ] **Step 2: Append to `walker.go`**

```go
// walkDirection controls bucket traversal in fillPage.
type walkDirection int

const (
    walkDesc walkDirection = -1 // Prev — newest to oldest
    walkAsc  walkDirection = +1 // Next — oldest to newest
)

// pageResult is fillPage's output. NextCursor is the empty string when the walk
// has reached a terminal state (floor crossed, or both page filled and no more
// rows in current bucket).
type pageResult[T any] struct {
    Rows       []T
    NextCursor string
    HasNext    bool
}

// bucketQueryFn returns a freshly-prepared gocql.Query bound to the given
// bucket value. Implementations are produced by each public read function
// (e.g. GetMessagesBefore creates a factory that interpolates bucket and
// the per-call predicate into the SELECT statement).
//
// firstBucket is true on the first invocation only; the factory may use this
// to apply a per-call predicate (e.g. created_at < before) only on the first
// bucket walked. Later buckets are entirely on one side of the boundary and
// do not need the predicate.
type bucketQueryFn func(bucket int64, firstBucket bool) *gocql.Query

// fillPage walks buckets in the given direction starting at startBucket,
// issuing one query per bucket and accumulating rows into out until pageSize
// is reached or maxBuckets is exhausted. The first bucket may resume from a
// caller-supplied gocql page state; later buckets always start fresh.
//
// scan must consume all rows from iter that fit in the remaining capacity and
// return the number consumed; it is responsible for appending to out.
//
// floorBucket bounds the walk: DESC stops when bucket < floorBucket; ASC stops
// when bucket > floorBucket. Set floorBucket = math.MinInt64 (DESC) or
// math.MaxInt64 (ASC) to disable floor-based termination.
func fillPage[T any](
    ctx context.Context,
    sizer msgbucket.Sizer,
    direction walkDirection,
    startBucket int64,
    floorBucket int64,
    maxBuckets int,
    pageSize int,
    initialPageState []byte,
    queryFn bucketQueryFn,
    scan func(iter *gocql.Iter, remaining int) []T,
) (pageResult[T], error) {
    out := make([]T, 0, pageSize)
    bucket := startBucket
    pageState := initialPageState
    walked := 0

    advance := func() {
        if direction == walkDesc {
            bucket = sizer.Prev(bucket)
        } else {
            bucket = sizer.Next(bucket)
        }
    }

    floorCrossed := func(b int64) bool {
        if direction == walkDesc {
            return b < floorBucket
        }
        return b > floorBucket
    }

    for len(out) < pageSize && walked < maxBuckets {
        if floorCrossed(bucket) {
            return pageResult[T]{Rows: out, NextCursor: "", HasNext: false}, nil
        }

        q := queryFn(bucket, walked == 0).WithContext(ctx)
        q = q.PageSize(pageSize - len(out))
        if pageState != nil {
            q = q.PageState(pageState)
        }

        iter := q.Iter()
        rows := scan(iter, pageSize-len(out))
        out = append(out, rows...)
        nextPageState := iter.PageState()
        if err := iter.Close(); err != nil {
            return pageResult[T]{}, fmt.Errorf("scan bucket %d: %w", bucket, err)
        }

        if len(nextPageState) > 0 && len(out) < pageSize {
            // Bucket has more rows but page wasn't filled yet — continue draining same bucket.
            pageState = nextPageState
            continue
        }
        if len(nextPageState) > 0 && len(out) >= pageSize {
            // Page filled mid-bucket — return cursor pointing at this bucket so caller resumes here.
            return pageResult[T]{
                Rows:       out,
                NextCursor: encodeBucketCursor(bucket, nextPageState),
                HasNext:    true,
            }, nil
        }

        // Bucket exhausted; advance.
        pageState = nil
        advance()
        walked++
    }

    if floorCrossed(bucket) {
        return pageResult[T]{Rows: out, NextCursor: "", HasNext: false}, nil
    }
    // maxBuckets reached or pageSize reached at bucket boundary — non-terminal cursor at next bucket.
    return pageResult[T]{
        Rows:       out,
        NextCursor: encodeBucketCursor(bucket, nil),
        HasNext:    true,
    }, nil
}
```

Add the imports at the top of `walker.go`:

```go
import (
    "context"
    "encoding/base64"
    "encoding/binary"
    "fmt"

    "github.com/gocql/gocql"

    "github.com/hmchangw/chat/pkg/msgbucket"
)
```

- [ ] **Step 3: Verify build**

Run: `go build ./history-service/...`
Expected: clean.

Run: `make lint`
Expected: clean (the helper is unused at this point but exported-only types are not — generics on unexported types should be fine. If lint flags `fillPage` as dead, leave it — it gets first use in Task 12 in the same series).

- [ ] **Step 4: Commit**

```bash
git -C /home/user/chat add history-service/internal/cassrepo/walker.go
git -C /home/user/chat commit -m "feat(cassrepo): add fillPage walk helper"
```

---

## Task 12: Convert `messages_by_room.go` reads to bucket walks

This task rewrites all four read functions in `messages_by_room.go` to use `fillPage`. Their public signatures change minimally: each now requires the caller to supply a `lastMessageAt` (DESC walks) and/or `floor time.Time` (ASC walks and DESC walk-back terminus). The `LoadHistory` style API is unchanged at the service layer — Task 14 introduces resolver wiring to compute these.

**Files:**
- Modify: `history-service/internal/cassrepo/messages_by_room.go`
- Modify: `history-service/internal/cassrepo/messages_by_room_integration_test.go`

- [ ] **Step 1: Write the failing integration test**

Append to `messages_by_room_integration_test.go`:

```go
func TestGetMessagesBefore_CrossBucketWalkDESC(t *testing.T) {
    session := setupCassandra(t)
    repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
    ctx := context.Background()

    roomID := "room-walk-desc"
    base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
    // 5 messages spread one per day across 5 distinct buckets.
    for i := 0; i < 5; i++ {
        seedMessage(t, session, roomID, fmt.Sprintf("m%d", i), base.AddDate(0, 0, -i))
    }

    // Floor far in the past so walk completes; lastMessageAt at the newest message's time.
    lastMsgAt := base
    floor := base.AddDate(0, 0, -10)

    page, err := repo.GetMessagesBefore(ctx, roomID, lastMsgAt.Add(time.Second), floor, PageRequest{PageSize: 3})
    require.NoError(t, err)
    require.Len(t, page.Data, 3)
    // Expect newest first.
    assert.Equal(t, "m0", page.Data[0].MessageID)
    assert.Equal(t, "m1", page.Data[1].MessageID)
    assert.Equal(t, "m2", page.Data[2].MessageID)
    assert.True(t, page.HasNext)

    // Continue with cursor.
    cursor, err := NewCursor(page.NextCursor)
    require.NoError(t, err)
    page2, err := repo.GetMessagesBefore(ctx, roomID, lastMsgAt.Add(time.Second), floor, PageRequest{Cursor: cursor, PageSize: 3})
    require.NoError(t, err)
    require.Len(t, page2.Data, 2)
    assert.Equal(t, "m3", page2.Data[0].MessageID)
    assert.Equal(t, "m4", page2.Data[1].MessageID)
    assert.False(t, page2.HasNext)
}

func TestGetMessagesBefore_FloorTerminates(t *testing.T) {
    session := setupCassandra(t)
    repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
    ctx := context.Background()

    roomID := "room-floor"
    base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
    for i := 0; i < 5; i++ {
        seedMessage(t, session, roomID, fmt.Sprintf("m%d", i), base.AddDate(0, 0, -i))
    }
    floor := base.AddDate(0, 0, -2) // only m0, m1, m2 survive the floor.

    page, err := repo.GetMessagesBefore(ctx, roomID, base.Add(time.Second), floor, PageRequest{PageSize: 10})
    require.NoError(t, err)
    require.Len(t, page.Data, 3)
    assert.False(t, page.HasNext)
}

func TestGetMessagesBefore_MaxBucketsCap(t *testing.T) {
    session := setupCassandra(t)
    repo := NewRepository(session, msgbucket.New(24*time.Hour), 2) // cap at 2 buckets
    ctx := context.Background()

    roomID := "room-cap"
    base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
    // 5 buckets of empties — no messages — walk hits cap immediately.
    floor := base.AddDate(0, 0, -10)

    page, err := repo.GetMessagesBefore(ctx, roomID, base, floor, PageRequest{PageSize: 50})
    require.NoError(t, err)
    assert.Empty(t, page.Data)
    assert.True(t, page.HasNext, "non-terminal cursor when cap reached")
}
```

`seedMessage` is a small helper inserting into `messages_by_room` (one definition per test file, near the top):

```go
func seedMessage(t *testing.T, session *gocql.Session, roomID, messageID string, createdAt time.Time) {
    t.Helper()
    sizer := msgbucket.New(24 * time.Hour)
    bucket := sizer.Of(createdAt)
    err := session.Query(
        `INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at, type)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        roomID, bucket, createdAt, messageID,
        map[string]interface{}{"id": "u1", "account": "alice"},
        "m", "site-A", createdAt, "text",
    ).Exec()
    require.NoError(t, err)
}
```

(Verify with the existing test file's helper conventions before duplicating.)

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=history-service`
Expected: compile error — `GetMessagesBefore` signature does not yet accept `floor time.Time`. (And once that compiles, the new tests would fail before the walk logic is in.)

- [ ] **Step 3: Update read function signatures**

Add a `floor time.Time` parameter to each of the four read functions in `messages_by_room.go`:

```go
func (r *Repository) GetMessagesBefore(ctx context.Context, roomID string, before time.Time, floor time.Time, pageReq PageRequest) (Page[models.Message], error)
func (r *Repository) GetMessagesBetweenDesc(ctx context.Context, roomID string, since, before time.Time, pageReq PageRequest) (Page[models.Message], error) // since IS the floor — no extra param
func (r *Repository) GetMessagesAfter(ctx context.Context, roomID string, after time.Time, ceiling time.Time, pageReq PageRequest) (Page[models.Message], error)
func (r *Repository) GetAllMessagesAsc(ctx context.Context, roomID string, floor time.Time, ceiling time.Time, pageReq PageRequest) (Page[models.Message], error)
```

Service-layer callers passing through to these functions also need updating; that change is part of Task 14.

- [ ] **Step 4: Implement `GetMessagesBefore` using `fillPage`**

```go
func (r *Repository) GetMessagesBefore(ctx context.Context, roomID string, before time.Time, floor time.Time, pageReq PageRequest) (Page[models.Message], error) {
    startBucket, initialPageState, err := startBucketFromCursor(pageReq, r.bucket.Of(before))
    if err != nil {
        return Page[models.Message]{}, err
    }
    floorBucket := r.bucket.Of(floor)

    queryFn := func(bucket int64, firstBucket bool) *gocql.Query {
        if firstBucket {
            return r.session.Query(
                messageByRoomQuery+` WHERE room_id = ? AND bucket = ? AND created_at < ? ORDER BY created_at DESC`,
                roomID, bucket, before,
            )
        }
        return r.session.Query(
            messageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at DESC`,
            roomID, bucket,
        )
    }

    res, err := fillPage[models.Message](
        ctx, r.bucket, walkDesc, startBucket, floorBucket, r.maxBuckets,
        pageReq.PageSize, initialPageState, queryFn, scanMessagesUpTo,
    )
    if err != nil {
        return Page[models.Message]{}, fmt.Errorf("get messages before: %w", err)
    }
    return Page[models.Message]{Data: res.Rows, NextCursor: res.NextCursor, HasNext: res.HasNext}, nil
}
```

Add helpers `startBucketFromCursor` and `scanMessagesUpTo` in `messages_by_room.go`:

```go
// startBucketFromCursor returns the bucket to start the walk at, plus an
// initial in-bucket pageState if the request carried a non-empty cursor.
// When the cursor is empty, defaultBucket is used.
func startBucketFromCursor(pageReq PageRequest, defaultBucket int64) (int64, []byte, error) {
    if pageReq.Cursor == nil {
        return defaultBucket, nil, nil
    }
    encoded := pageReq.Cursor.Encode()
    if encoded == "" {
        return defaultBucket, nil, nil
    }
    bucket, pageState, err := decodeBucketCursor(encoded)
    if err != nil {
        return 0, nil, fmt.Errorf("start bucket from cursor: %w", err)
    }
    return bucket, pageState, nil
}

// scanMessagesUpTo consumes up to remaining rows from iter via structScan.
func scanMessagesUpTo(iter *gocql.Iter, remaining int) []models.Message {
    out := make([]models.Message, 0, remaining)
    for len(out) < remaining {
        var m models.Message
        if !structScan(iter, &m) {
            break
        }
        out = append(out, m)
    }
    return out
}
```

- [ ] **Step 5: Implement `GetMessagesBetweenDesc`, `GetMessagesAfter`, `GetAllMessagesAsc`**

Each follows the same pattern as `GetMessagesBefore`. Differences:

- `GetMessagesBetweenDesc`: floor is `since`. First-bucket predicate is `created_at > since AND created_at < before`. Subsequent-bucket predicate drops the `< before` part but retains nothing else (each subsequent bucket is entirely older than the first; the `since` bound is enforced by `floorCrossed` in `fillPage`).
- `GetMessagesAfter`: direction is `walkAsc`. First-bucket predicate is `created_at > after`. Subsequent buckets drop the predicate. Ceiling = `r.bucket.Of(ceiling)`.
- `GetAllMessagesAsc`: direction is `walkAsc`. Start bucket = `r.bucket.Of(floor)`. Ceiling = `r.bucket.Of(ceiling)`. No first-bucket predicate (floor is enforced by walk start).

Code each function explicitly — DRY through `fillPage` only, not by sharing predicate logic across functions (the variants are small enough to be readable inline).

- [ ] **Step 6: Run integration tests**

Run: `make test-integration SERVICE=history-service`
Expected: the new cross-bucket / floor / cap tests PASS, plus the existing tests for these functions (which call them with the previously single-arg signature need to be updated to pass a floor — usually `time.Time{}` for "epoch" or a far-back time).

- [ ] **Step 7: Commit**

```bash
git -C /home/user/chat add history-service/internal/cassrepo/messages_by_room.go history-service/internal/cassrepo/messages_by_room_integration_test.go
git -C /home/user/chat commit -m "feat(cassrepo): walk buckets in messages_by_room reads"
```

---

## Task 13: Convert `thread_messages.go` reads

`GetThreadMessages` queries with both `room_id` and `thread_room_id` clustering key. After bucketing, it must walk buckets like `messages_by_room`. The `thread_room_id` filter stays on every bucket query.

**Files:**
- Modify: `history-service/internal/cassrepo/thread_messages.go`
- Modify: `history-service/internal/cassrepo/thread_messages_integration_test.go`

- [ ] **Step 1: Write the failing test**

Append to `thread_messages_integration_test.go`:

```go
func TestGetThreadMessages_CrossBucketWalk(t *testing.T) {
    session := setupCassandra(t)
    repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
    ctx := context.Background()

    roomID := "room-thread-walk"
    threadRoomID := "thread-1"
    base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
    for i := 0; i < 4; i++ {
        seedThreadMessage(t, session, roomID, threadRoomID, fmt.Sprintf("t%d", i), base.AddDate(0, 0, -i))
    }
    floor := base.AddDate(0, 0, -10)

    page, err := repo.GetThreadMessages(ctx, roomID, threadRoomID, base.Add(time.Second), floor, PageRequest{PageSize: 10})
    require.NoError(t, err)
    require.Len(t, page.Data, 4)
    assert.Equal(t, "t0", page.Data[0].MessageID)
    assert.False(t, page.HasNext)
}
```

`seedThreadMessage` is the analogous helper inserting into `thread_messages_by_room`. Match the existing helper style in the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=history-service`
Expected: compile failure (signature change) followed by behavior failures.

- [ ] **Step 3: Update `GetThreadMessages`**

Add `before`, `floor` params and walk buckets:

```go
func (r *Repository) GetThreadMessages(
    ctx context.Context, roomID, threadRoomID string,
    before time.Time, floor time.Time,
    pageReq PageRequest,
) (Page[models.Message], error) {
    startBucket, initialPageState, err := startBucketFromCursor(pageReq, r.bucket.Of(before))
    if err != nil {
        return Page[models.Message]{}, err
    }
    floorBucket := r.bucket.Of(floor)

    queryFn := func(bucket int64, firstBucket bool) *gocql.Query {
        if firstBucket {
            return r.session.Query(
                "SELECT "+threadMessageColumns+
                    ` FROM thread_messages_by_room WHERE room_id = ? AND bucket = ? AND thread_room_id = ? AND created_at < ? ORDER BY created_at DESC`,
                roomID, bucket, threadRoomID, before,
            )
        }
        return r.session.Query(
            "SELECT "+threadMessageColumns+
                ` FROM thread_messages_by_room WHERE room_id = ? AND bucket = ? AND thread_room_id = ? ORDER BY created_at DESC`,
            roomID, bucket, threadRoomID,
        )
    }

    res, err := fillPage[models.Message](
        ctx, r.bucket, walkDesc, startBucket, floorBucket, r.maxBuckets,
        pageReq.PageSize, initialPageState, queryFn, scanMessagesUpTo,
    )
    if err != nil {
        return Page[models.Message]{}, fmt.Errorf("get thread messages: %w", err)
    }
    return Page[models.Message]{Data: res.Rows, NextCursor: res.NextCursor, HasNext: res.HasNext}, nil
}
```

Service-layer callers pass `before` and `floor` derived from request + room metadata.

- [ ] **Step 4: Run integration tests**

Run: `make test-integration SERVICE=history-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /home/user/chat add history-service/internal/cassrepo/thread_messages.go history-service/internal/cassrepo/thread_messages_integration_test.go
git -C /home/user/chat commit -m "feat(cassrepo): walk buckets in GetThreadMessages"
```

---

## Task 14: Add `bucket = ?` to edit/delete WHERE clauses in `cassrepo/write.go`

The `Repository` already has `bucket msgbucket.Sizer` after Task 8. Every UPDATE against `messages_by_room` or `thread_messages_by_room` now needs `bucket` in its WHERE clause. The bucket is derived from each row's `created_at` (which is in hand on edit/delete since the caller has already loaded the message).

**Files:**
- Modify: `history-service/internal/cassrepo/write.go`
- Modify: `history-service/internal/cassrepo/write_integration_test.go`

- [ ] **Step 1: Update query constants and helper signatures**

Replace the bucketed query strings in `write.go`:

```go
const (
    editMsgByID   = `UPDATE messages_by_id SET msg = ?, edited_at = ?, updated_at = ? WHERE message_id = ? AND created_at = ?`
    editMsgByRoom = `UPDATE messages_by_room SET msg = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
    editThreadMsg = `UPDATE thread_messages_by_room SET msg = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND thread_room_id = ? AND created_at = ? AND message_id = ?`
    editPinnedMsg = `UPDATE pinned_messages_by_room SET msg = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND created_at = ? AND message_id = ?`

    deleteMsgByIDCAS = `UPDATE messages_by_id SET deleted = true, updated_at = ? WHERE message_id = ? AND created_at = ? IF deleted != true`
    deleteMsgByRoom  = `UPDATE messages_by_room SET deleted = true, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
    deleteThreadMsg  = `UPDATE thread_messages_by_room SET deleted = true, updated_at = ? WHERE room_id = ? AND bucket = ? AND thread_room_id = ? AND created_at = ? AND message_id = ?`
    deletePinnedMsg  = `UPDATE pinned_messages_by_room SET deleted = true, updated_at = ? WHERE room_id = ? AND created_at = ? AND message_id = ?`
)

// Thread-parent delete queries — same as the regular delete queries plus type = MessageTypeRemoved.
const (
    deleteThreadParentMsgByIDCAS = "UPDATE messages_by_id SET deleted = true, type = '" + MessageTypeRemoved + "', updated_at = ? WHERE message_id = ? AND created_at = ? IF deleted != true"
    deleteThreadParentMsgByRoom  = "UPDATE messages_by_room SET deleted = true, type = '" + MessageTypeRemoved + "', updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?"
    deleteThreadParentThreadMsg  = "UPDATE thread_messages_by_room SET deleted = true, type = '" + MessageTypeRemoved + "', updated_at = ? WHERE room_id = ? AND bucket = ? AND thread_room_id = ? AND created_at = ? AND message_id = ?"
    deleteThreadParentPinnedMsg  = "UPDATE pinned_messages_by_room SET deleted = true, type = '" + MessageTypeRemoved + "', updated_at = ? WHERE room_id = ? AND created_at = ? AND message_id = ?"
)
```

- [ ] **Step 2: Pass bucket through helpers**

Replace the helpers that bind `messages_by_room` and `thread_messages_by_room` parameters:

```go
func (r *Repository) editInMessagesByRoom(ctx context.Context, msg *models.Message, newMsg string, editedAt time.Time) error {
    b := r.bucket.Of(msg.CreatedAt)
    return r.session.Query(editMsgByRoom, newMsg, editedAt, editedAt, msg.RoomID, b, msg.CreatedAt, msg.MessageID).WithContext(ctx).Exec()
}

func (r *Repository) editInThreadMessagesByRoom(ctx context.Context, msg *models.Message, newMsg string, editedAt time.Time) error {
    b := r.bucket.Of(msg.CreatedAt)
    return r.session.Query(editThreadMsg, newMsg, editedAt, editedAt, msg.RoomID, b, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID).WithContext(ctx).Exec()
}

func (r *Repository) deleteInMessagesByRoom(ctx context.Context, q string, msg *models.Message, deletedAt time.Time) error {
    b := r.bucket.Of(msg.CreatedAt)
    return r.session.Query(q, deletedAt, msg.RoomID, b, msg.CreatedAt, msg.MessageID).WithContext(ctx).Exec()
}

func (r *Repository) deleteInThreadMessagesByRoom(ctx context.Context, q string, msg *models.Message, deletedAt time.Time) error {
    b := r.bucket.Of(msg.CreatedAt)
    return r.session.Query(q, deletedAt, msg.RoomID, b, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID).WithContext(ctx).Exec()
}
```

The pinned-message helpers (`editInPinnedMessagesByRoom`, `deleteInPinnedMessagesByRoom`) and the `messages_by_id` helpers are unchanged.

- [ ] **Step 3: Update `decrementParentTcount`**

The two CAS sequences against `messages_by_room` (read tcount, then CAS update) get `bucket = ?` in their WHERE clauses. Replace the `messages_by_room` portion of `decrementParentTcount`:

```go
parentBucket := r.bucket.Of(parentCreatedAt)

if err := r.session.Query(
    `SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
    msg.RoomID, parentBucket, parentCreatedAt, parentID,
).WithContext(ctx).Scan(&tcount); err != nil {
    if errors.Is(err, gocql.ErrNotFound) {
        return nil
    }
    return fmt.Errorf("read tcount for parent %s in messages_by_room: %w", parentID, err)
}
if err := casDecrement(casMaxRetries, tcount, func(newVal int, expected *int) (bool, *int, error) {
    var current *int
    applied, err := r.session.Query(
        `UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ? IF tcount = ?`,
        newVal, msg.RoomID, parentBucket, parentCreatedAt, parentID, expected,
    ).WithContext(ctx).ScanCAS(&current)
    return applied, current, err
}); err != nil {
    return fmt.Errorf("cas tcount decrement in messages_by_room for parent %s: %w", parentID, err)
}
```

The `messages_by_id` portion is unchanged.

- [ ] **Step 4: Run integration tests**

Run: `make test-integration SERVICE=history-service`
Expected: existing edit/delete tests continue to PASS now that they correctly include `bucket` in the WHERE clauses. (After Task 8 — when `bucket` was added to the schema but not the predicates — these tests would have failed with "no rows updated" or similar; if you ran them between then and Task 14, that's why.)

- [ ] **Step 5: Commit**

```bash
git -C /home/user/chat add history-service/internal/cassrepo/write.go history-service/internal/cassrepo/write_integration_test.go
git -C /home/user/chat commit -m "feat(cassrepo): include bucket in edit/delete WHERE predicates"
```

---

## Task 15: Update `MessageReader` interface and regenerate mocks

**Files:**
- Modify: `history-service/internal/service/service.go` (interface)
- Run: `make generate SERVICE=history-service`

- [ ] **Step 1: Update interface signatures**

In `history-service/internal/service/service.go`, replace `MessageReader`:

```go
type MessageReader interface {
    GetMessagesBefore(ctx context.Context, roomID string, before time.Time, floor time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
    GetMessagesBetweenDesc(ctx context.Context, roomID string, since, before time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
    GetMessagesAfter(ctx context.Context, roomID string, after time.Time, ceiling time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
    GetAllMessagesAsc(ctx context.Context, roomID string, floor, ceiling time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
    GetMessageByID(ctx context.Context, messageID string) (*models.Message, error)
    GetThreadMessages(ctx context.Context, roomID, threadRoomID string, before, floor time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
    GetMessagesByIDs(ctx context.Context, messageIDs []string) ([]models.Message, error)
}
```

`GetMessageByID` and `GetMessagesByIDs` go through `messages_by_id` (not bucketed) and keep their old signatures.

- [ ] **Step 2: Regenerate mocks**

Run: `make generate SERVICE=history-service`
Expected: `history-service/internal/service/mocks/mock_repository.go` rewritten in place. Do not edit by hand.

- [ ] **Step 3: Verify build**

Run: `go build ./history-service/...`
Expected: callers (service/messages.go, service/threads.go) will fail to compile because they pass the old argument list. That's expected; they're fixed in Task 17. To unblock the commit, comment out the bodies of the affected service methods temporarily, OR proceed straight to Task 17 in the same commit.

- [ ] **Step 4: Commit**

```bash
git -C /home/user/chat add history-service/internal/service/service.go history-service/internal/service/mocks/
git -C /home/user/chat commit -m "refactor(history-service): extend MessageReader signatures with bucket bounds"
```

---

## Task 16: Add `RoomHints` to request models

**Files:**
- Modify: `history-service/internal/models/message.go`

- [ ] **Step 1: Add `RoomHints` and embed on relevant request types**

```go
// RoomHints carry client-cached room metadata so the server can skip a Mongo
// lookup. Both fields are optional and individually validated server-side
// (LastMsgAt > now+1h and CreatedAt > now are ignored, falling back to Mongo).
type RoomHints struct {
    LastMsgAt *int64 `json:"lastMsgAt,omitempty"` // UTC millis
    CreatedAt *int64 `json:"createdAt,omitempty"` // UTC millis
}

type LoadHistoryRequest struct {
    Before *int64     `json:"before,omitempty"`
    Limit  int        `json:"limit"`
    Hints  *RoomHints `json:"hints,omitempty"`
}

type LoadNextMessagesRequest struct {
    After  *int64     `json:"after,omitempty"`
    Limit  int        `json:"limit"`
    Cursor string     `json:"cursor"`
    Hints  *RoomHints `json:"hints,omitempty"`
}

type LoadSurroundingMessagesRequest struct {
    MessageID string     `json:"messageId"`
    Limit     int        `json:"limit"`
    Hints     *RoomHints `json:"hints,omitempty"`
}

type GetThreadMessagesRequest struct {
    ThreadMessageID string     `json:"threadMessageId"`
    Cursor          string     `json:"cursor,omitempty"`
    Limit           int        `json:"limit"`
    Hints           *RoomHints `json:"hints,omitempty"`
}
```

(Edit/Delete/GetByID/etc. do not paginate over a room — leave them alone.)

- [ ] **Step 2: Extend round-trip tests**

In `history-service/internal/models/message_test.go`, add cases that JSON-marshal/unmarshal the new types with `Hints` populated and with `Hints == nil`. Confirm round-trip equality.

- [ ] **Step 3: Verify**

Run: `make test SERVICE=history-service`
Expected: model tests PASS.

- [ ] **Step 4: Commit**

```bash
git -C /home/user/chat add history-service/internal/models/
git -C /home/user/chat commit -m "feat(history-service/models): add optional RoomHints to history requests"
```

---

## Task 17: Add `RoomTimeResolver` and wire `resolveRoomTimes` into service handlers

**Files:**
- Modify: `history-service/internal/service/service.go` (interface + struct + constructor)
- Create: `history-service/internal/service/room_times.go` (resolver helper)
- Create: `history-service/internal/service/room_times_test.go` (table-driven unit tests)
- Modify: `history-service/internal/service/messages.go` (LoadHistory, LoadNextMessages, LoadSurroundingMessages)
- Modify: `history-service/internal/service/threads.go`
- Modify: `history-service/cmd/main.go` (pass `roomRepo` to `service.New`)

- [ ] **Step 1: Add `RoomTimeResolver` interface**

In `history-service/internal/service/service.go`, add:

```go
type RoomTimeResolver interface {
    GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error)
}
```

Update the `//go:generate` directive to include `RoomTimeResolver`:

```go
//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . MessageReader,MessageWriter,MessageRepository,SubscriptionRepository,EventPublisher,ThreadRoomRepository,RoomKeyProvider,RoomTimeResolver
```

Add the field and constructor parameter:

```go
type HistoryService struct {
    msgReader     MessageReader
    msgWriter     MessageWriter
    subscriptions SubscriptionRepository
    publisher     EventPublisher
    threadRooms   ThreadRoomRepository
    keyProvider   RoomKeyProvider
    roomTimes     RoomTimeResolver
    historyFloor  time.Duration // from MESSAGE_HISTORY_FLOOR_DAYS
}

func New(
    msgs MessageRepository,
    subs SubscriptionRepository,
    pub EventPublisher,
    threadRooms ThreadRoomRepository,
    keyProvider RoomKeyProvider,
    roomTimes RoomTimeResolver,
    historyFloor time.Duration,
) *HistoryService {
    return &HistoryService{
        msgReader:     msgs,
        msgWriter:     msgs,
        subscriptions: subs,
        publisher:     pub,
        threadRooms:   threadRooms,
        keyProvider:   keyProvider,
        roomTimes:     roomTimes,
        historyFloor:  historyFloor,
    }
}
```

- [ ] **Step 2: Add `cmd/main.go` wiring**

In `history-service/cmd/main.go`, replace the existing `service.New(...)` call with:

```go
roomRepo := mongorepo.NewRoomRepo(mongoDB)
historyFloor := time.Duration(cfg.MessageHistoryFloorDays) * 24 * time.Hour

svc := service.New(cassRepo, subRepo, publisherImpl, threadRoomRepo, keyProvider, roomRepo, historyFloor)
```

- [ ] **Step 3: Write the failing test for `resolveRoomTimes`**

`history-service/internal/service/room_times_test.go`:

```go
package service

import (
    "context"
    "testing"
    "time"

    "github.com/golang/mock/gomock"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "github.com/hmchangw/chat/history-service/internal/models"
    "github.com/hmchangw/chat/history-service/internal/service/mocks"
)

func ts(t time.Time) *int64 {
    ms := t.UnixMilli()
    return &ms
}

func TestResolveRoomTimes(t *testing.T) {
    now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
    last := time.Date(2026, 4, 30, 8, 0, 0, 0, time.UTC)
    created := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
    future := now.Add(2 * time.Hour) // beyond +1h skew tolerance

    tests := []struct {
        name        string
        hints       *models.RoomHints
        mongoCalls  int
        wantLast    time.Time
        wantCreated time.Time
    }{
        {name: "no hints → mongo fallback", hints: nil, mongoCalls: 1, wantLast: last, wantCreated: created},
        {name: "both hints valid → no mongo", hints: &models.RoomHints{LastMsgAt: ts(last), CreatedAt: ts(created)}, mongoCalls: 0, wantLast: last, wantCreated: created},
        {name: "lastMsgAt missing → mongo fallback for both", hints: &models.RoomHints{CreatedAt: ts(created)}, mongoCalls: 1, wantLast: last, wantCreated: created},
        {name: "createdAt missing → mongo fallback for both", hints: &models.RoomHints{LastMsgAt: ts(last)}, mongoCalls: 1, wantLast: last, wantCreated: created},
        {name: "lastMsgAt too far in future → ignored", hints: &models.RoomHints{LastMsgAt: ts(future), CreatedAt: ts(created)}, mongoCalls: 1, wantLast: last, wantCreated: created},
        {name: "createdAt in future → ignored", hints: &models.RoomHints{LastMsgAt: ts(last), CreatedAt: ts(future)}, mongoCalls: 1, wantLast: last, wantCreated: created},
        {name: "implausibly old values (pre-2020) → ignored", hints: &models.RoomHints{
            LastMsgAt: func() *int64 { v := int64(0); return &v }(), // 1970-01-01 epoch
            CreatedAt: func() *int64 { v := int64(0); return &v }(),
        }, mongoCalls: 1, wantLast: last, wantCreated: created},
    }
    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            ctrl := gomock.NewController(t)
            defer ctrl.Finish()
            mockResolver := mocks.NewMockRoomTimeResolver(ctrl)
            mockResolver.EXPECT().
                GetRoomTimes(gomock.Any(), "room-1").
                Return(last, created, nil).
                Times(tc.mongoCalls)

            s := &HistoryService{roomTimes: mockResolver}
            gotLast, gotCreated, err := s.resolveRoomTimes(context.Background(), "room-1", tc.hints, now)
            require.NoError(t, err)
            assert.Equal(t, tc.wantLast.UTC(), gotLast.UTC())
            assert.Equal(t, tc.wantCreated.UTC(), gotCreated.UTC())
        })
    }
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `make test SERVICE=history-service -run TestResolveRoomTimes`
Expected: FAIL — `resolveRoomTimes` undefined.

- [ ] **Step 5: Implement `resolveRoomTimes`**

Create `history-service/internal/service/room_times.go`:

```go
package service

import (
    "context"
    "fmt"
    "time"

    "github.com/hmchangw/chat/history-service/internal/models"
)

// clockSkewTolerance allows clients with mildly out-of-sync clocks to still
// have their LastMsgAt hint accepted. Anything further out is treated as
// suspicious and triggers a Mongo fallback.
const clockSkewTolerance = time.Hour

// resolveRoomTimes returns lastMsgAt and createdAt for roomID. Client-supplied
// hints are trusted after sanity checks; missing or invalid hints fall back to
// Mongo via the RoomTimeResolver. now is injected for deterministic testing.
func (s *HistoryService) resolveRoomTimes(
    ctx context.Context,
    roomID string,
    hints *models.RoomHints,
    now time.Time,
) (lastMsgAt, createdAt time.Time, err error) {
    var last, created *time.Time
    if hints != nil {
        last = sanitizeLastMsgAt(hints.LastMsgAt, now)
        created = sanitizeCreatedAt(hints.CreatedAt, now)
    }

    if last == nil || created == nil {
        l, c, gerr := s.roomTimes.GetRoomTimes(ctx, roomID)
        if gerr != nil {
            return time.Time{}, time.Time{}, fmt.Errorf("resolve room times for %s: %w", roomID, gerr)
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

// minPlausibleEpoch rejects clearly-bogus millis (e.g. *ms == 0 → 1970-01-01)
// without imposing tight bounds on real-world clock skew. `time.Time{}.IsZero()`
// does NOT match `time.UnixMilli(0)` — the latter is unix epoch, a real time.
var minPlausibleEpoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

func sanitizeLastMsgAt(ms *int64, now time.Time) *time.Time {
    if ms == nil {
        return nil
    }
    t := time.UnixMilli(*ms).UTC()
    if t.Before(minPlausibleEpoch) {
        return nil
    }
    if t.After(now.Add(clockSkewTolerance)) {
        return nil
    }
    return &t
}

func sanitizeCreatedAt(ms *int64, now time.Time) *time.Time {
    if ms == nil {
        return nil
    }
    t := time.UnixMilli(*ms).UTC()
    if t.Before(minPlausibleEpoch) {
        return nil
    }
    if t.After(now) {
        return nil
    }
    return &t
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `make test SERVICE=history-service`
Expected: `TestResolveRoomTimes` and all sibling unit tests PASS.

- [ ] **Step 7: Wire into `LoadHistory`**

In `history-service/internal/service/messages.go`, change `LoadHistory` to call `resolveRoomTimes` and pass derived bounds through to cassrepo:

```go
func (s *HistoryService) LoadHistory(c *natsrouter.Context, req models.LoadHistoryRequest) (*models.LoadHistoryResponse, error) {
    account := c.Param("account")
    roomID := c.Param("roomID")

    accessSince, err := s.getAccessSince(c, account, roomID)
    if err != nil {
        return nil, err
    }

    now := time.Now().UTC()
    lastMsgAt, createdAt, err := s.resolveRoomTimes(c, roomID, req.Hints, now)
    if err != nil {
        slog.Error("resolve room times", "error", err, "roomID", roomID)
        return nil, natsrouter.ErrInternal("failed to resolve room metadata")
    }

    before := millisToTime(req.Before)
    if before.IsZero() {
        before = now
    }
    // Cap before by lastMsgAt+1ms so the walk starts from the actual last
    // message bucket, not from "now". Year-dead rooms become 1-bucket reads.
    if !lastMsgAt.IsZero() && before.After(lastMsgAt) {
        before = lastMsgAt.Add(time.Millisecond)
    }

    // Floor: room.createdAt for full-access subscribers; max(createdAt, accessSince) for restricted.
    floor := createdAt
    if accessSince != nil && accessSince.After(floor) {
        floor = *accessSince
    }
    if floor.IsZero() {
        // Fallback floor when room.createdAt missing — historyFloor days back from now.
        floor = now.Add(-s.historyFloor)
    }

    limit := req.Limit
    if limit <= 0 {
        limit = defaultPageSize
    }
    if limit > maxPageSize {
        limit = maxPageSize
    }
    pageReq, err := parsePageRequest("", limit)
    if err != nil {
        return nil, err
    }

    var page cassrepo.Page[models.Message]
    if accessSince == nil {
        page, err = s.msgReader.GetMessagesBefore(c, roomID, before, floor, pageReq)
    } else {
        page, err = s.msgReader.GetMessagesBetweenDesc(c, roomID, *accessSince, before, pageReq)
    }
    if err != nil {
        slog.Error("loading history", "error", err, "roomID", roomID)
        return nil, natsrouter.ErrInternal("failed to load message history")
    }

    redactUnavailableQuotes(page.Data, accessSince)
    return &models.LoadHistoryResponse{Messages: page.Data}, nil
}
```

(The `_ = createdAt` line in the existing flow is replaced by explicit floor derivation above.)

- [ ] **Step 8: Wire into `LoadNextMessages`, `LoadSurroundingMessages`, `GetThreadMessages`**

Apply the same pattern: call `resolveRoomTimes`, derive `before`/`after`/`ceiling`/`floor` bounds, and pass them through. Specific notes:
- `LoadNextMessages` (ASC walk): `after = max(req.After, accessSince)`. `ceiling = lastMsgAt` (or `now` if lastMsgAt zero).
- `LoadSurroundingMessages`: builds an asc + desc sub-walk around the central message; both walks need the resolved floor and ceiling.
- `GetThreadMessages` (DESC walk on threads): `before = lastMsgAt+1ms` (capped), `floor = max(createdAt, accessSince)`.

For each handler, write a quick subtest under the existing `*_test.go` file that supplies hints and asserts the mock cassrepo is called with the expected `before`, `floor`, `ceiling` bounds. (Add the assertions to existing happy-path tests — don't duplicate them.)

- [ ] **Step 9: Run tests**

Run: `make test SERVICE=history-service && make test-integration SERVICE=history-service`
Expected: all PASS.

- [ ] **Step 10: Commit**

```bash
git -C /home/user/chat add history-service/
git -C /home/user/chat commit -m "feat(history-service): wire RoomTimeResolver and bucket bounds into handlers"
```

---

## Task 18: Update `CLAUDE.md` Section 6 invariant

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Append the invariant note**

In `CLAUDE.md`, locate Section 6 ("Cassandra"). Append a bullet under the existing list:

```markdown
- **Bucketed message tables.** `messages_by_room` and `thread_messages_by_room` use a composite partition key `(room_id, bucket)`. The bucket is `floor(created_at_unix_ms / windowMs) * windowMs`. The window is configured per service via `MESSAGE_BUCKET_HOURS` (default 24). All services that read or write these tables MUST be configured with the same `MESSAGE_BUCKET_HOURS`; mismatches will cause writes and reads to target different partitions and silently lose data. Bucket math lives in `pkg/msgbucket`.
```

- [ ] **Step 2: Commit**

```bash
git -C /home/user/chat add CLAUDE.md
git -C /home/user/chat commit -m "docs(claude): note MESSAGE_BUCKET_HOURS invariant in section 6"
```

---

## Task 19: End-to-end verification

- [ ] **Step 1: Lint everything**

Run: `cd /home/user/chat && make lint`
Expected: clean.

- [ ] **Step 2: Unit tests**

Run: `cd /home/user/chat && make test`
Expected: all green, no race warnings, coverage ≥ 80% on changed packages.

- [ ] **Step 3: Integration tests (Docker required)**

Run: `cd /home/user/chat && make test-integration`
Expected: all green.

- [ ] **Step 4: Coverage spot-check**

Run:

```bash
cd /home/user/chat
go test -coverprofile=/tmp/cov-msgbucket.out ./pkg/msgbucket/...
go tool cover -func=/tmp/cov-msgbucket.out

go test -coverprofile=/tmp/cov-cassrepo.out ./history-service/internal/cassrepo/...
go tool cover -func=/tmp/cov-cassrepo.out
```

Expected: `pkg/msgbucket` ≥ 90%; `cassrepo` ≥ 80%.

- [ ] **Step 5: Push the branch**

```bash
git -C /home/user/chat push -u origin claude/fix-partition-size-limit-kESFY
```

---
