# Bounded Thread Reply Count (tcount) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the per-write `tcount` recount cost to a constant by replacing the unbounded `thread_messages_by_thread` partition scan with an early-terminating bounded count, shared by both authoritative writers.

**Architecture:** Extract the bounded count into a new `pkg/threadcount` (single source of truth for the count logic and the display cap). `message-worker` (reply-add path) delegates `countThreadReplies` to `threadcount.Count`; `history-service` (reply-delete path) delegates to `threadcount.CountAndLatest`, which also recomputes the thread-last-message timestamp (tlm) from the same bounded scan, since a delete may remove the newest reply. The blind-`SET` of `tcount` (and tlm) on the parent row is unchanged, so the idempotent-under-redelivery and soft-delete-aware invariants are preserved. No schema change, no second source of truth.

**Tech Stack:** Go 1.25, gocql (Cassandra), testcontainers via `pkg/testutil`, testify.

**Spec:** `docs/superpowers/specs/2026-06-21-bounded-thread-reply-count-design.md`

---

## File Structure

- **Create** `pkg/threadcount/count.go` — `package threadcount`; `const Cap = 99` and `func Count(ctx, *gocql.Session, threadRoomID) (int, error)`. The only place the count algorithm and cap live.
- **Create** `pkg/threadcount/main_test.go` — `TestMain` driving testutil cleanup (build tag `integration`).
- **Create** `pkg/threadcount/integration_test.go` — Cassandra-backed tests for `Count` (build tag `integration`).
- **Modify** `message-worker/store_cassandra.go:331-347` — `countThreadReplies` delegates to `threadcount.Count`; add import.
- **Modify** `message-worker/integration_test.go` — add a capping integration test; add `threadcount` import.
- **Modify** `history-service/internal/cassrepo/write.go:351-367` — `countThreadReplies` delegates to `threadcount.CountAndLatest` (delete path also recomputes tlm); add import.
- **Modify** `history-service/internal/cassrepo/write_integration_test.go` — add a capping integration test; add `threadcount` import.
- **Modify** `docs/cassandra_message_model.md` — `tcount` column comments (both tables).
- **Modify** `docs/superpowers/plans/2026-06-04-tcount-count-based.md` — point the COUNTER-table future-work item at this design as the chosen resolution.
- **Modify** `docs/client-api.md` — note the cap on the `tcount` and `newTcount` field rows.

---

## Task 1: Create `pkg/threadcount` with bounded `Count` + `Cap`

**Files:**
- Create: `pkg/threadcount/count.go`
- Create: `pkg/threadcount/main_test.go`
- Test: `pkg/threadcount/integration_test.go`

- [ ] **Step 1: Write the failing integration test**

Create `pkg/threadcount/main_test.go`:

```go
//go:build integration

package threadcount

import (
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }
```

Create `pkg/threadcount/integration_test.go`:

```go
//go:build integration

package threadcount

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// setupThreadTable creates an isolated keyspace with a minimal
// thread_messages_by_thread table (only the columns Count reads) and returns a
// keyspace-bound session.
func setupThreadTable(t *testing.T) *gocql.Session {
	t.Helper()
	keyspace, admin, host := testutil.CassandraKeyspace(t, "threadcount_test")
	require.NoError(t, admin.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_thread (
		thread_room_id text,
		created_at     timestamp,
		message_id     text,
		deleted        boolean,
		PRIMARY KEY ((thread_room_id), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`, keyspace)).Exec())

	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	sess, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(sess.Close)
	return sess
}

// seedReplies inserts count rows for threadRoomID. message_id is prefixed so two
// calls in the same thread never collide on the (created_at, message_id) key.
// deleted may be nil to mimic message-worker's INSERT, which never writes it.
func seedReplies(t *testing.T, sess *gocql.Session, threadRoomID, idPrefix string, count int, deleted *bool) {
	t.Helper()
	base := time.Now().UTC()
	for i := 0; i < count; i++ {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, deleted) VALUES (?, ?, ?, ?)`,
			threadRoomID, base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("%s-%d", idPrefix, i), deleted,
		).Exec())
	}
}

func TestCount_UnderCap_Exact(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 5, nil)

	n, err := Count(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, 5, n)
}

func TestCount_OverCap_ReturnsCap(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", Cap+10, nil)

	n, err := Count(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, Cap, n)
}

func TestCount_ExcludesDeleted(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	deleted := true
	seedReplies(t, sess, "thread-1", "live", 10, nil)
	seedReplies(t, sess, "thread-1", "del", 5, &deleted)

	n, err := Count(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, 10, n)
}

func TestCount_EmptyPartition_Zero(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)

	n, err := Count(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-integration SERVICE=pkg/threadcount`
Expected: FAIL — compile error `undefined: Count` / `undefined: Cap`.

- [ ] **Step 3: Write the minimal implementation**

Create `pkg/threadcount/count.go`:

```go
// Package threadcount derives the thread reply-count badge (tcount) from the
// Cassandra thread_messages_by_thread partition. The count is bounded by Cap so
// the per-write cost stays constant regardless of thread size. Both
// message-worker (reply-add path) and history-service (reply-delete path) call
// Count, so the two authoritative writers of tcount can never compute different
// values.
package threadcount

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

// Cap is the maximum value Count returns. A thread with more than Cap
// non-deleted replies reports exactly Cap; the frontend renders tcount >= Cap as
// "Cap+". 99 keeps virtually all real threads exact while bounding the per-write
// partition read to ~Cap rows.
const Cap = 99

// Count returns min(non-deleted replies in threadRoomID's partition, Cap).
//
// It scans thread_messages_by_thread in clustering order and stops as soon as
// the non-deleted tally reaches Cap, so the read materializes at most ~Cap rows
// (plus any soft-deleted rows interspersed before them). No CQL LIMIT is used:
// soft-deleted rows (deleted = true) live in the partition, so a hard LIMIT
// could return Cap rows of which some are deleted and undercount the live total.
//
// The deleted column may be NULL (message-worker does not write it on INSERT),
// so NULL is treated as not-deleted. session must have its Keyspace set.
func Count(ctx context.Context, session *gocql.Session, threadRoomID string) (int, error) {
	iter := session.Query(
		`SELECT deleted FROM thread_messages_by_thread WHERE thread_room_id = ?`,
		threadRoomID,
	).WithContext(ctx).PageSize(Cap).Iter()

	var deleted *bool
	n := 0
	for n < Cap && iter.Scan(&deleted) {
		if deleted == nil || !*deleted {
			n++
		}
	}
	if err := iter.Close(); err != nil {
		return 0, fmt.Errorf("count thread replies for thread %s: %w", threadRoomID, err)
	}
	return n, nil
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test-integration SERVICE=pkg/threadcount`
Expected: PASS (all four tests).

- [ ] **Step 5: Lint**

Run: `make lint`
Expected: 0 issues.

- [ ] **Step 6: Commit**

```bash
git add pkg/threadcount/
git commit -m "feat(threadcount): bounded thread reply count helper"
```

---

## Task 2: Delegate `message-worker` count to `threadcount`

**Files:**
- Modify: `message-worker/store_cassandra.go:331-347`
- Test: `message-worker/integration_test.go`

- [ ] **Step 1: Write the failing test**

Add to `message-worker/integration_test.go` (append a new test function). It seeds `Cap+10` thread rows and asserts the store's `countThreadReplies` caps the result:

```go
func TestCassandraStore_countThreadReplies_CapsAtThreadcountCap(t *testing.T) {
	ctx := context.Background()
	keyspace, admin, host := testutil.CassandraKeyspace(t, "message_worker_tcount_cap")
	require.NoError(t, admin.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_thread (
		thread_room_id text,
		created_at     timestamp,
		message_id     text,
		deleted        boolean,
		PRIMARY KEY ((thread_room_id), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`, keyspace)).Exec())

	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	cassSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(cassSession.Close)

	base := time.Now().UTC()
	for i := 0; i < threadcount.Cap+10; i++ {
		require.NoError(t, cassSession.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id) VALUES (?, ?, ?)`,
			"thread-1", base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("reply-%d", i),
		).WithContext(ctx).Exec())
	}

	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	n, err := store.countThreadReplies(ctx, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, threadcount.Cap, n)
}
```

Add `"github.com/hmchangw/chat/pkg/threadcount"` to the import block of `message-worker/integration_test.go` (the file already imports `gocql`, `testutil`, `msgbucket`, `context`, `fmt`, `time`, `assert`, `require`).

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-integration SERVICE=message-worker`
Expected: FAIL — `countThreadReplies` currently scans the whole partition and returns `Cap+10` (109), so `assert.Equal(t, 99, n)` fails with `expected: 99, actual: 109`.

- [ ] **Step 3: Replace the implementation with a delegation**

In `message-worker/store_cassandra.go`, replace the entire `countThreadReplies` method (currently lines 327-347, including its doc comment) with:

```go
// countThreadReplies returns the bounded, soft-delete-aware reply count for the
// thread. It delegates to pkg/threadcount so this add-path writer and the
// history-service delete-path writer compute an identical, identically-capped
// value (see pkg/threadcount.Cap).
func (s *CassandraStore) countThreadReplies(ctx context.Context, threadRoomID string) (int, error) {
	return threadcount.Count(ctx, s.cassSession, threadRoomID)
}
```

Add `"github.com/hmchangw/chat/pkg/threadcount"` to the import block of `message-worker/store_cassandra.go`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test-integration SERVICE=message-worker`
Expected: PASS — the new test plus all existing `message-worker` integration tests (the small-thread idempotency tests still see counts of 1, unchanged).

- [ ] **Step 5: Lint**

Run: `make lint`
Expected: 0 issues (confirms no unused imports remain after removing the old loop body).

- [ ] **Step 6: Commit**

```bash
git add message-worker/store_cassandra.go message-worker/integration_test.go
git commit -m "refactor(message-worker): bound tcount via pkg/threadcount"
```

---

## Task 3: Delegate `history-service` count to `threadcount`

**Files:**
- Modify: `history-service/internal/cassrepo/write.go:351-367`
- Test: `history-service/internal/cassrepo/write_integration_test.go`

- [ ] **Step 1: Write the failing test**

Add to `history-service/internal/cassrepo/write_integration_test.go` (append a new test function):

```go
func TestRepository_countThreadReplies_CapsAtThreadcountCap(t *testing.T) {
	ctx := context.Background()
	keyspace, admin, host := testutil.CassandraKeyspace(t, "history_tcount_cap")
	require.NoError(t, admin.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_thread (
		thread_room_id text,
		created_at     timestamp,
		message_id     text,
		deleted        boolean,
		PRIMARY KEY ((thread_room_id), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`, keyspace)).Exec())

	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(session.Close)

	base := time.Now().UTC()
	for i := 0; i < threadcount.Cap+10; i++ {
		require.NoError(t, session.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id) VALUES (?, ?, ?)`,
			"thread-1", base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("reply-%d", i),
		).WithContext(ctx).Exec())
	}

	repo := NewRepository(session, msgbucket.New(24*time.Hour), 10, nil)
	n, err := repo.countThreadReplies(ctx, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, threadcount.Cap, n)
}
```

Add these imports to `history-service/internal/cassrepo/write_integration_test.go` (the file already imports `context`, `testing`, `time`, `gocql`, `assert`, `require`, `msgbucket`):
- `"fmt"`
- `"github.com/hmchangw/chat/pkg/testutil"`
- `"github.com/hmchangw/chat/pkg/threadcount"`

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-integration SERVICE=history-service`
Expected: FAIL — `countThreadReplies` returns `109`, so `assert.Equal(t, 99, n)` fails with `expected: 99, actual: 109`.

- [ ] **Step 3: Replace the implementation with a delegation**

In `history-service/internal/cassrepo/write.go`, replace the entire `countThreadReplies` method (currently lines 348-367, including its doc comment) with:

```go
// countThreadReplies returns the bounded, soft-delete-aware reply count and the
// latest surviving reply's created_at (tlm) for the thread. It delegates to
// pkg/threadcount.CountAndLatest so this delete-path writer and the
// message-worker add-path writer compute an identical, identically-capped count
// (see pkg/threadcount.Cap), while the delete path also recomputes tlm.
func (r *Repository) countThreadReplies(ctx context.Context, threadRoomID string) (int, *time.Time, error) {
	return threadcount.CountAndLatest(ctx, r.session, threadRoomID)
}
```

Add `"github.com/hmchangw/chat/pkg/threadcount"` to the import block of `history-service/internal/cassrepo/write.go`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test-integration SERVICE=history-service`
Expected: PASS — the new test plus all existing `cassrepo` integration tests (existing delete tests use small threads, counts unchanged).

- [ ] **Step 5: Lint**

Run: `make lint`
Expected: 0 issues.

- [ ] **Step 6: Commit**

```bash
git add history-service/internal/cassrepo/write.go history-service/internal/cassrepo/write_integration_test.go
git commit -m "refactor(history-service): bound tcount via pkg/threadcount"
```

---

## Task 4: Documentation

**Files:**
- Modify: `docs/cassandra_message_model.md`
- Modify: `docs/superpowers/plans/2026-06-04-tcount-count-based.md`
- Modify: `docs/client-api.md`

- [ ] **Step 1: Update the Cassandra schema doc**

In `docs/cassandra_message_model.md`, the `tcount` column appears in both `messages_by_room` and `messages_by_id`. Replace each occurrence of:

```
  tcount INT, // message reply thread count
```

with:

```
  tcount INT, // bounded non-deleted thread reply count, capped at 99 (pkg/threadcount.Cap); FE renders >= 99 as "99+"
```

- [ ] **Step 2: Update the #245 future-work item**

In `docs/superpowers/plans/2026-06-04-tcount-count-based.md`, at the end of the "### O(N) partition scan in `countThreadReplies`" section (after the line beginning "Until the COUNTER table ships..."), append:

```markdown

**Resolution (superseded):** The COUNTER-table + reconciliation-job approach was
not pursued. A Cassandra `counter` increment is not idempotent under JetStream
redelivery and would re-introduce the drift this design eliminated, requiring a
new scheduled reconciliation job. Instead the per-write scan was bounded by a
display cap (`pkg/threadcount.Cap = 99`), keeping the idempotent, stateless
recompute-then-blind-SET model with no new table or job. See
`docs/superpowers/specs/2026-06-21-bounded-thread-reply-count-design.md`.
```

- [ ] **Step 3: Update the client API doc**

In `docs/client-api.md`:

Replace the `tcount` field row (line ~2110):

```
| `tcount` | number | Optional. Number of replies on a thread parent. |
```

with:

```
| `tcount` | number | Optional. Number of non-deleted replies on a thread parent, capped at 99; a value of 99 means "99 or more". |
```

Replace the `newTcount` field row (line ~4496):

```
| `newTcount` | number | Authoritative post-CAS reply count for the parent message. Replaces any locally-computed count — do not delta. |
```

with:

```
| `newTcount` | number | Authoritative reply count for the parent message, capped at 99 (99 means "99 or more"). Replaces any locally-computed count — do not delta. |
```

- [ ] **Step 4: Commit**

```bash
git add docs/cassandra_message_model.md docs/superpowers/plans/2026-06-04-tcount-count-based.md docs/client-api.md
git commit -m "docs: note tcount 99 cap; mark COUNTER-table plan superseded"
```

---

## Task 5: Final verification

- [ ] **Step 1: Full unit suite**

Run: `make test`
Expected: PASS (no unit suites depend on the removed loop bodies; delegation is behavior-preserving for small threads).

- [ ] **Step 2: Integration suites for all three touched packages**

Run, in turn:
- `make test-integration SERVICE=pkg/threadcount`
- `make test-integration SERVICE=message-worker`
- `make test-integration SERVICE=history-service`

Expected: PASS for each.

- [ ] **Step 3: SAST gate**

Run: `make sast`
Expected: no medium+ findings (no new `InsecureSkipVerify`, conversions, or shell-outs introduced).

- [ ] **Step 4: Confirm the working tree is clean and the branch is ready**

Run: `git status`
Expected: clean tree on `claude/message-gateway-bottleneck-is4kqg`, all work committed.

---

## Self-Review Notes

- **Spec coverage:** bounded count → Task 1; both write sites delegate to the shared helper → Tasks 2 & 3; cap = 99 + FE contract → Task 1 (`Cap`) and Task 4 (docs); supersede COUNTER plan → Task 4 Step 2; preserve idempotency/soft-delete (blind-SET untouched, still reads `deleted`) → Tasks 2 & 3 leave `countAndSetParentTcount`/`setParentTcount` unchanged; testing at helper + both sites → Tasks 1-3, 5. Out-of-scope items (#286 ghost-row, `tcount==0`) are intentionally untouched.
- **No second source of truth introduced;** no DDL/migration (Task list contains no schema change).
- **Type consistency:** `threadcount.Count(ctx, *gocql.Session, string) (int, error)` and `const Cap` are used identically in Tasks 1, 2, 3; both delegating methods keep their original `(ctx, threadRoomID string) (int, error)` signatures, so `countAndSetParentTcount` call sites are untouched.
