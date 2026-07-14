# tcount: COUNT-Based Approach Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CAS increment/decrement approach for `tcount` with a COUNT of non-deleted rows from `thread_messages_by_thread`, eliminating the crash window and simplifying the code in both `message-worker` and `history-service`.

**Architecture:** Each time a thread reply is added or deleted, derive the authoritative `tcount` by iterating the `thread_messages_by_thread` partition (one partition = one thread) and counting non-deleted rows in Go. Then blind-SET that value on the parent row in both `messages_by_id` and `messages_by_room`. Because the count is re-derived from the source of truth on every write — including JetStream redeliveries — there is no crash window and no need for CAS loops, sentinel columns, or schema changes.

**Tech Stack:** Go 1.25, gocql, testify, testcontainers-go (via `pkg/testutil`). All tests run via `make test` (unit) and `make test-integration SERVICE=<name>` (integration).

---

## Background

### Why tcount is broken today

`SaveThreadMessage` (message-worker) does three things:
1. LWT INSERT reply into `messages_by_id` — idempotency gate (`IF NOT EXISTS`)
2. Plain INSERT into `thread_messages_by_thread` — idempotent
3. If `applied=true` → `incrementParentTcount` (CAS increment on parent in both tables)
   If `applied=false` → `readParentTcount` (read-only, no increment)

**Crash window:** process crashes after step 1 but before step 3. On JetStream redelivery, `applied=false` → step 3 is skipped permanently → `tcount` stays at 0 forever.

`decrementParentTcount` in history-service has the same pattern for deletes.

### Fix

After the INSERT into `thread_messages_by_thread`, always derive the count from that table and SET it on the parent — regardless of `applied`. The partition contains every reply for the thread. Counting non-deleted rows is always correct and always repeatable.

---

## Files Changed

| File | Action |
|------|--------|
| `message-worker/store_cassandra.go` | Remove `casMaxRetries`, `casIncrement`, `readParentTcount`, `incrementParentTcount`. Add `countThreadReplies`, `setParentTcount`, `countAndSetParentTcount`. Simplify `SaveThreadMessage` and `saveThreadMessageEncrypted`. |
| `message-worker/integration_test.go` | Add `TestCassandraStore_SaveThreadMessage_CountBasedTcount` (the new failing test). Existing tests stay; they pass because the observable behavior is unchanged. |
| `history-service/internal/cassrepo/write.go` | Remove `casMaxRetries`, `casDecrement`, `decrementParentTcount`. Add `countThreadReplies`, `setParentTcount`, `countAndSetParentTcount`. Update `SoftDeleteMessage` to call `countAndSetParentTcount`. |
| `history-service/internal/cassrepo/cas_test.go` | Delete — tests `casDecrement` which is being removed. |
| `history-service/internal/cassrepo/write_integration_test.go` | Rewrite `TestRepository_SoftDeleteMessage_DecrementsParentTcount` to seed `thread_messages_by_thread` rows (not a hard-coded `tcount=3` in the parent) and verify the COUNT-derived result. |

---

## Task 1 — message-worker: Write the failing integration test

**Files:**
- Modify: `message-worker/integration_test.go`

The new test pre-seeds 2 reply rows directly into `thread_messages_by_thread` before calling `SaveThreadMessage` for a third reply. With the old CAS approach the parent starts at `tcount=null` → increments to 1. With the new COUNT approach it reads 3 rows in the partition → sets tcount=3.

- [ ] **Step 1: Write the failing test**

Add this function at the end of `message-worker/integration_test.go`, before the final `}`:

```go
func TestCassandraStore_SaveThreadMessage_CountBasedTcount(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := msgbucket.New(24 * time.Hour).Of(parentCreatedAt)

	parentSender := &cassParticipant{ID: "u-cnt-parent", Account: "alice", EngName: "Alice"}
	parentMsg := &model.Message{
		ID:        "cnt-parent",
		RoomID:    "cnt-room",
		UserID:    "u-cnt-parent",
		CreatedAt: parentCreatedAt,
		Content:   "parent message",
	}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, parentSender, "site-a"))

	// Pre-seed two existing replies directly in thread_messages_by_thread.
	// This simulates replies that were already processed before a crash —
	// the kind of state that the CAS approach can't recover from but COUNT can.
	threadRoomID := "tr-cnt-1"
	t1 := parentCreatedAt.Add(1 * time.Minute)
	t2 := parentCreatedAt.Add(2 * time.Minute)
	for _, row := range []struct {
		msgID     string
		createdAt time.Time
	}{
		{"cnt-reply-pre-1", t1},
		{"cnt-reply-pre-2", t2},
	} {
		require.NoError(t, cassSession.Query(
			`INSERT INTO thread_messages_by_thread
			 (thread_room_id, created_at, message_id, room_id, thread_parent_id, deleted)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			threadRoomID, row.createdAt, row.msgID, "cnt-room", "cnt-parent", false,
		).Exec())
	}

	// Now process a third reply via SaveThreadMessage.
	t3 := parentCreatedAt.Add(3 * time.Minute)
	replySender := &cassParticipant{ID: "u-cnt-replier", Account: "bob", EngName: "Bob"}
	replyMsg := &model.Message{
		ID:                           "cnt-reply-3",
		RoomID:                       "cnt-room",
		UserID:                       "u-cnt-replier",
		Content:                      "third reply",
		CreatedAt:                    t3,
		ThreadParentMessageID:        "cnt-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	newTcount, err := store.SaveThreadMessage(ctx, replyMsg, replySender, "site-a", threadRoomID)
	require.NoError(t, err)

	// COUNT of non-deleted rows in the partition = 3 (2 pre-seeded + 1 just inserted).
	// The old CAS approach would give 1 (increments from null).
	require.NotNil(t, newTcount, "newTcount must not be nil — parent tcount must be updated")
	assert.Equal(t, 3, *newTcount, "tcount must equal the COUNT of non-deleted replies in the partition")

	t.Run("tcount=3 written to messages_by_id", func(t *testing.T) {
		var tcount int
		require.NoError(t, cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"cnt-parent", parentCreatedAt,
		).Scan(&tcount))
		assert.Equal(t, 3, tcount)
	})

	t.Run("tcount=3 written to messages_by_room", func(t *testing.T) {
		var tcount int
		require.NoError(t, cassSession.Query(
			`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"cnt-room", parentBucket, parentCreatedAt, "cnt-parent",
		).Scan(&tcount))
		assert.Equal(t, 3, tcount)
	})
}
```

- [ ] **Step 2: Run the test to verify it FAILS**

```bash
make test-integration SERVICE=message-worker
```

Expected: `TestCassandraStore_SaveThreadMessage_CountBasedTcount` FAILS with something like `assert.Equal: expected 3, got 1`.

---

## Task 2 — message-worker: Implement COUNT-based approach

**Files:**
- Modify: `message-worker/store_cassandra.go`

- [ ] **Step 1: Add the three new functions after the `buildCassandraMessage` function (around line 330)**

Insert the following block after `buildCassandraMessage` and before the `casMaxRetries` constant:

```go
// countThreadReplies returns the number of non-deleted replies in the thread
// by iterating the thread_messages_by_thread partition and counting in Go.
// Using Go-side filtering handles the null/false/true ambiguity: message-worker
// never writes deleted on INSERT (null), history-service writes true on delete.
// Both null and false are treated as "active".
func (s *CassandraStore) countThreadReplies(ctx context.Context, threadRoomID string) (int, error) {
	iter := s.cassSession.Query(
		`SELECT deleted FROM thread_messages_by_thread WHERE thread_room_id = ?`,
		threadRoomID,
	).WithContext(ctx).Iter()
	var deleted *bool
	n := 0
	for iter.Scan(&deleted) {
		if deleted == nil || !*deleted {
			n++
		}
	}
	if err := iter.Close(); err != nil {
		return 0, fmt.Errorf("count thread replies for thread %s: %w", threadRoomID, err)
	}
	return n, nil
}

// setParentTcount blindly overwrites tcount on the parent message row in both
// messages_by_id and messages_by_room. No IF guard — idempotent on redelivery
// because the source value is always re-derived from countThreadReplies.
func (s *CassandraStore) setParentTcount(ctx context.Context, msg *model.Message, n int) error {
	parentID := msg.ThreadParentMessageID
	parentCreatedAt := *msg.ThreadParentMessageCreatedAt
	parentBucket := s.bucket.Of(parentCreatedAt)

	if err := s.cassSession.Query(
		`UPDATE messages_by_id SET tcount = ? WHERE message_id = ? AND created_at = ?`,
		n, parentID, parentCreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount on parent %s in messages_by_id: %w", parentID, err)
	}
	if err := s.cassSession.Query(
		`UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		n, msg.RoomID, parentBucket, parentCreatedAt, parentID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount on parent %s in messages_by_room: %w", parentID, err)
	}
	return nil
}

// countAndSetParentTcount counts non-deleted replies in the thread and writes
// the result to the parent row in both tables. Safe to call on any delivery
// (first or redelivery) — both the count and the SET are idempotent.
// Returns (nil, nil) when ThreadParentMessageCreatedAt is unset (same semantics
// as the old incrementParentTcount).
func (s *CassandraStore) countAndSetParentTcount(ctx context.Context, msg *model.Message, threadRoomID string) (*int, error) {
	if msg.ThreadParentMessageCreatedAt == nil {
		return nil, nil
	}
	n, err := s.countThreadReplies(ctx, threadRoomID)
	if err != nil {
		return nil, fmt.Errorf("count thread replies: %w", err)
	}
	if err := s.setParentTcount(ctx, msg, n); err != nil {
		return nil, err
	}
	return &n, nil
}
```

- [ ] **Step 2: Simplify `SaveThreadMessage` — replace the applied branch**

In `SaveThreadMessage` (around line 217), replace:
```go
	if !applied {
		return s.readParentTcount(ctx, msg)
	}
	return s.incrementParentTcount(ctx, msg)
```
with:
```go
	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
```

Also change `applied, err :=` to `_, err :=` on the `MapScanCAS` call (line 189) since `applied` is no longer used:
```go
	_, err := s.cassSession.Query(
```

- [ ] **Step 3: Simplify `saveThreadMessageEncrypted` — same replacement**

In `saveThreadMessageEncrypted` (around line 275), replace:
```go
	if !applied {
		return s.readParentTcount(ctx, msg)
	}
	return s.incrementParentTcount(ctx, msg)
```
with:
```go
	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
```

Also change `applied, err :=` to `_, err :=` on that function's `MapScanCAS` call (line 245).

- [ ] **Step 4: Remove the four dead functions and constant**

Delete the following from `store_cassandra.go`:
- `const casMaxRetries = 16` (line 336)
- The entire `readParentTcount` function (lines 288–304)
- The entire `casIncrement` function (lines 343–360)
- The entire `incrementParentTcount` function (lines 374–427)

Also remove the comment block above `casMaxRetries` (lines 332–336).

- [ ] **Step 5: Run integration tests**

```bash
make test-integration SERVICE=message-worker
```

Expected: ALL tests pass, including `TestCassandraStore_SaveThreadMessage_CountBasedTcount`.

- [ ] **Step 6: Run unit tests and lint**

```bash
make test SERVICE=message-worker && make lint
```

Expected: PASS with no errors.

- [ ] **Step 7: Commit**

```bash
git add message-worker/store_cassandra.go message-worker/integration_test.go
git commit -m "message-worker: replace tcount CAS increment with COUNT-based approach

tcount is now derived from thread_messages_by_thread on every write
(add or redelivery) and blind-SET on the parent row. Eliminates the
crash window between LWT INSERT and the old incrementParentTcount.

Removes casMaxRetries, casIncrement, readParentTcount, and
incrementParentTcount. Adds countThreadReplies, setParentTcount,
and countAndSetParentTcount."
```

---

## Task 3 — history-service: Write the failing integration test

**Files:**
- Modify: `history-service/internal/cassrepo/write_integration_test.go`

The existing `TestRepository_SoftDeleteMessage_DecrementsParentTcount` seeds `tcount=3` directly in the parent tables so the CAS decrement has something to work from. Rewrite it to instead seed 3 rows in `thread_messages_by_thread` and verify the result is the COUNT (2 remaining after delete), not the CAS decrement.

- [ ] **Step 1: Rewrite the test**

Find `TestRepository_SoftDeleteMessage_DecrementsParentTcount` (around line 396) and replace the entire function with:

```go
func TestRepository_SoftDeleteMessage_DecrementsParentTcount(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-tcount"
	threadRoomID := "thread-tcount"
	parentID := "m-tcount-parent"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := msgbucket.New(24 * time.Hour).Of(parentCreatedAt)
	replyID := "m-tcount-reply"
	replyCreatedAt := parentCreatedAt.Add(10 * time.Second)

	// Seed parent WITHOUT a pre-existing tcount — the COUNT approach derives
	// the authoritative value from thread_messages_by_thread, not from a
	// materialized counter in the parent tables.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?)`,
		parentID, roomID, parentCreatedAt, sender, "parent", false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, parentBucket, parentCreatedAt, parentID, sender, "parent", false,
	).Exec())

	// Seed three reply rows in thread_messages_by_thread: two survivors and one
	// being deleted. This is the source of truth for the COUNT.
	for _, row := range []struct {
		id  string
		off time.Duration
	}{
		{"m-tcount-survivor-1", 1 * time.Second},
		{"m-tcount-survivor-2", 2 * time.Second},
		{replyID, 10 * time.Second},
	} {
		require.NoError(t, session.Query(
			`INSERT INTO thread_messages_by_thread
			 (thread_room_id, created_at, message_id, room_id, thread_parent_id, deleted)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			threadRoomID, parentCreatedAt.Add(row.off), row.id, roomID, parentID, false,
		).Exec())
	}

	// Seed the reply being deleted in messages_by_id.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id
		 (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id, deleted)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		replyID, roomID, replyCreatedAt, sender, "reply", parentID, parentCreatedAt, threadRoomID, false,
	).Exec())

	parentCreatedAtPtr := parentCreatedAt
	msg := &models.Message{
		MessageID:             replyID,
		RoomID:                roomID,
		CreatedAt:             replyCreatedAt,
		Sender:                sender,
		ThreadParentID:        parentID,
		ThreadParentCreatedAt: &parentCreatedAtPtr,
		ThreadRoomID:          threadRoomID,
	}
	_, applied, newTcount, err := repo.SoftDeleteMessage(ctx, msg, replyCreatedAt.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, applied, "first delete should apply")
	require.NotNil(t, newTcount, "newTcount must be non-nil after a successful thread-reply delete")
	assert.Equal(t, 2, *newTcount, "tcount must equal COUNT of non-deleted rows: 3 seeded - 1 deleted = 2")

	var gotTcount int
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).Scan(&gotTcount))
	assert.Equal(t, 2, gotTcount, "messages_by_id.tcount must be set to COUNT result")

	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, parentBucket, parentCreatedAt, parentID,
	).Scan(&gotTcount))
	assert.Equal(t, 2, gotTcount, "messages_by_room.tcount must be set to COUNT result")
}
```

- [ ] **Step 2: Run the test to verify it FAILS**

```bash
make test-integration SERVICE=history-service
```

Expected: `TestRepository_SoftDeleteMessage_DecrementsParentTcount` FAILS — `assert.Equal: expected 2, got <nil>` (casDecrement returns nil for nil initial since no tcount was seeded in the parent).

---

## Task 4 — history-service: Implement COUNT-based approach

**Files:**
- Modify: `history-service/internal/cassrepo/write.go`
- Delete: `history-service/internal/cassrepo/cas_test.go`

- [ ] **Step 1: Add the three new functions to `write.go`**

Insert after `ErrMessageNotFound` (around line 60), before the `casMaxRetries` constant:

```go
// countThreadReplies returns the number of non-deleted replies in the thread
// by iterating the thread_messages_by_thread partition and counting in Go.
// null and false are both treated as "active" — message-worker never writes
// deleted on INSERT (null), history-service writes true on delete.
func (r *Repository) countThreadReplies(ctx context.Context, threadRoomID string) (int, error) {
	iter := r.session.Query(
		`SELECT deleted FROM thread_messages_by_thread WHERE thread_room_id = ?`,
		threadRoomID,
	).WithContext(ctx).Iter()
	var deleted *bool
	n := 0
	for iter.Scan(&deleted) {
		if deleted == nil || !*deleted {
			n++
		}
	}
	if err := iter.Close(); err != nil {
		return 0, fmt.Errorf("count thread replies for thread %s: %w", threadRoomID, err)
	}
	return n, nil
}

// setParentTcount blindly overwrites tcount on the parent message row in both
// messages_by_id and messages_by_room. No IF guard — idempotent because the
// value is always re-derived from countThreadReplies.
func (r *Repository) setParentTcount(ctx context.Context, msg *models.Message, n int) error {
	parentID := msg.ThreadParentID
	parentCreatedAt := *msg.ThreadParentCreatedAt
	parentBucket := r.bucket.Of(parentCreatedAt)

	if err := r.session.Query(
		`UPDATE messages_by_id SET tcount = ? WHERE message_id = ? AND created_at = ?`,
		n, parentID, parentCreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount on parent %s in messages_by_id: %w", parentID, err)
	}
	if err := r.session.Query(
		`UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		n, msg.RoomID, parentBucket, parentCreatedAt, parentID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount on parent %s in messages_by_room: %w", parentID, err)
	}
	return nil
}

// countAndSetParentTcount counts non-deleted replies for the thread and
// writes the result to the parent row in both tables. Safe on any delivery.
// Returns (nil, nil) when ThreadParentCreatedAt is unset.
func (r *Repository) countAndSetParentTcount(ctx context.Context, msg *models.Message) (*int, error) {
	if msg.ThreadParentCreatedAt == nil {
		return nil, nil
	}
	n, err := r.countThreadReplies(ctx, msg.ThreadRoomID)
	if err != nil {
		return nil, fmt.Errorf("count thread replies: %w", err)
	}
	if err := r.setParentTcount(ctx, msg, n); err != nil {
		return nil, err
	}
	return &n, nil
}
```

- [ ] **Step 2: Replace `decrementParentTcount` call in `SoftDeleteMessage`**

In `SoftDeleteMessage` (around line 362), replace:
```go
	newTcount, err := r.decrementParentTcount(ctx, msg)
	if err != nil {
		// The LWT delete already committed — return applied=true so callers correctly
		// identify this as a decrement failure rather than a concurrent-winner race.
		return deletedAt, true, nil, fmt.Errorf("decrement parent tcount for message %s: %w", msg.MessageID, err)
	}
	return deletedAt, true, newTcount, nil
```
with:
```go
	newTcount, err := r.countAndSetParentTcount(ctx, msg)
	if err != nil {
		return deletedAt, true, nil, fmt.Errorf("set parent tcount for message %s: %w", msg.MessageID, err)
	}
	return deletedAt, true, newTcount, nil
```

- [ ] **Step 3: Remove dead code from `write.go`**

Delete the following from `write.go`:
- `const casMaxRetries = 16` (line 25) and its comment block (lines 17–25)
- The entire `casDecrement` function (lines 76–104) and its comment
- The entire `decrementParentTcount` function (lines 371–438) and its comment

- [ ] **Step 4: Delete `cas_test.go`**

```bash
rm /home/user/chat/history-service/internal/cassrepo/cas_test.go
```

This file only tests `casDecrement`, which is now removed.

- [ ] **Step 5: Run integration tests**

```bash
make test-integration SERVICE=history-service
```

Expected: ALL tests pass, including the rewritten `TestRepository_SoftDeleteMessage_DecrementsParentTcount`.

- [ ] **Step 6: Run unit tests and lint**

```bash
make test SERVICE=history-service && make lint
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add history-service/internal/cassrepo/write.go \
        history-service/internal/cassrepo/write_integration_test.go
git rm history-service/internal/cassrepo/cas_test.go
git commit -m "history-service: replace tcount CAS decrement with COUNT-based approach

tcount on delete is now derived from thread_messages_by_thread COUNT
and blind-SET on the parent row, matching the message-worker add path.
Eliminates the crash window between the LWT soft-delete and the old
decrementParentTcount.

Removes casMaxRetries, casDecrement, decrementParentTcount, and
cas_test.go. Adds countThreadReplies, setParentTcount, and
countAndSetParentTcount."
```

---

## Task 5 — Final verification

- [ ] **Step 1: Run all unit tests**

```bash
make test
```

Expected: PASS.

- [ ] **Step 2: Run all integration tests for both services**

```bash
make test-integration SERVICE=message-worker
make test-integration SERVICE=history-service
```

Expected: PASS.

- [ ] **Step 3: Run lint across the repo**

```bash
make lint
```

Expected: PASS with no errors or warnings.

- [ ] **Step 4: Push**

```bash
git push -u origin claude/gallant-galileo-ice0C
```

---

## Self-Review

**Spec coverage:**
- ✅ crash window on add path (message-worker) — fixed by `countAndSetParentTcount`
- ✅ crash window on delete path (history-service) — fixed by `countAndSetParentTcount`
- ✅ no schema change — confirmed, no DDL touched
- ✅ redelivery safe — COUNT + blind SET is idempotent
- ✅ existing behavior preserved — tcount still updated in both `messages_by_id` and `messages_by_room`
- ✅ nil `ThreadParentMessageCreatedAt` returns `(nil, nil)` — preserved

**Placeholder scan:** None found.

**Type consistency:**
- `countAndSetParentTcount` in message-worker: `(ctx, *model.Message, threadRoomID string) (*int, error)` — consistent across Task 2 steps
- `countAndSetParentTcount` in history-service: `(ctx, *models.Message) (*int, error)` — consistent across Task 4 steps (ThreadRoomID is a field on `models.Message`, so it's not a separate arg)

**One known gap:** The `casRow` map in `SaveThreadMessage` and `saveThreadMessageEncrypted` is allocated but its contents are never read after the change. It is still needed because `MapScanCAS` must absorb all existing columns when the LWT is not applied — switching to `ScanCAS` with no destinations would cause the "not enough columns to scan" panic that was already fixed once. Keep `casRow` allocated and passed to `MapScanCAS`; it is a required absorber even though its values are unused.

---

## Known Trade-offs and Future Work

### O(N) partition scan in `countThreadReplies`

**Current behavior:** `countThreadReplies` streams every row in the `thread_messages_by_thread` partition for the thread and counts non-deleted rows in Go. For a thread with *N* replies this is O(N) on every add or delete event.

**Why it was designed this way:** The O(N) scan is the minimum-complexity design that achieves full crash-safety. The alternative — a stored CAS counter (increment/decrement) — has a 2PC crash window: a crash between the Cassandra write succeeding and the counter update leaves the counter permanently wrong. COUNT gives the ground truth at the moment of the SET, so any JetStream redelivery converges to the correct value regardless of how many times the handler ran.

**Planned improvement (target: follow-up PR):**

Replace the Go-side row scan with a dedicated Cassandra COUNTER table:

```sql
CREATE TABLE thread_reply_counts (
    thread_room_id text PRIMARY KEY,
    count          counter
);
```

- **Add-path:** `UPDATE thread_reply_counts SET count = count + 1 WHERE thread_room_id = ?` after the LWT INSERT succeeds.
- **Delete-path:** `UPDATE thread_reply_counts SET count = count - 1 WHERE thread_room_id = ?` after the soft-delete.
- **Crash-safety:** a periodic reconciliation job (scheduled, low-frequency) overwrites the COUNTER with the true `SELECT COUNT(*)` scan. This bounds drift to the reconciliation interval — O(N) scan becomes a maintenance operation, not a hot path.
- **Read-path:** `tcount` value comes from this COUNTER table instead of the live scan, making it O(1).

Until the COUNTER table ships, the current O(N) scan is correct behavior. Threads with fewer than ~500 replies see sub-millisecond scan latency on a well-partitioned Cassandra cluster; the trade-off is acceptable for the initial rollout.

**Resolution (superseded):** The COUNTER-table + reconciliation-job approach was
not pursued. A Cassandra `counter` increment is not idempotent under JetStream
redelivery and would re-introduce the drift this design eliminated, requiring a
new scheduled reconciliation job. Instead the per-write scan was bounded by a
display cap (`pkg/threadcount.Cap = 99`), keeping the idempotent, stateless
recompute-then-blind-SET model with no new table or job. See
`docs/superpowers/specs/2026-06-21-bounded-thread-reply-count-design.md`.
