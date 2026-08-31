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

// testParent is the parent coordinate every test stamps through.
var testParent = Parent{MessageID: "parent-1", RoomID: "room-1", Bucket: 0}

// setupThreadTable creates an isolated keyspace holding the three things
// pkg/threadcount touches: the thread partition it counts, and the authority +
// mirror rows it stamps. testing.TB so the benchmarks share it.
func setupThreadTable(t testing.TB) *gocql.Session {
	t.Helper()
	keyspace, admin, host := testutil.CassandraKeyspace(t, "threadcount_test")
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_thread (
			thread_room_id text,
			created_at     timestamp,
			message_id     text,
			deleted        boolean,
			PRIMARY KEY ((thread_room_id), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`,
		`CREATE TABLE IF NOT EXISTS %s.messages_by_id (
			message_id         text PRIMARY KEY,
			tcount             int,
			thread_last_msg_at timestamp
		)`,
		`CREATE TABLE IF NOT EXISTS %s.messages_by_room (
			room_id            text,
			bucket             bigint,
			created_at         timestamp,
			message_id         text,
			tcount             int,
			thread_last_msg_at timestamp,
			PRIMARY KEY ((room_id, bucket), created_at, message_id)
		)`,
	} {
		require.NoError(t, admin.Query(fmt.Sprintf(ddl, keyspace)).Exec())
	}

	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	sess, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(sess.Close)
	return sess
}

// seedReplies inserts count rows for threadRoomID. message_id is prefixed so two
// calls in the same thread never collide on the (created_at, message_id) key.
// deleted may be nil to mimic the write path, which never writes the column.
func seedReplies(t testing.TB, sess *gocql.Session, threadRoomID, idPrefix string, count int, deleted *bool) {
	t.Helper()
	const chunk = 100
	base := time.Now().UTC()
	for start := 0; start < count; start += chunk {
		batch := sess.NewBatch(gocql.UnloggedBatch)
		for i := start; i < start+chunk && i < count; i++ {
			batch.Query(
				`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, deleted) VALUES (?, ?, ?, ?)`,
				threadRoomID, base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("%s-%d", idPrefix, i), deleted,
			)
		}
		require.NoError(t, sess.ExecuteBatch(batch))
	}
}

func seedStamp(t testing.TB, sess *gocql.Session, parentID string, tcount int, tlm *time.Time) {
	t.Helper()
	require.NoError(t, sess.Query(
		`UPDATE messages_by_id SET tcount = ?, thread_last_msg_at = ? WHERE message_id = ?`,
		tcount, tlm, parentID,
	).Exec())
}

// readStamped returns the authority row's tcount/tlm, nil for null columns.
func readStamped(t testing.TB, sess *gocql.Session, parentID string) (*int, *time.Time) {
	t.Helper()
	var (
		tcount *int
		tlm    *time.Time
	)
	require.NoError(t, sess.Query(
		`SELECT tcount, thread_last_msg_at FROM messages_by_id WHERE message_id = ?`, parentID,
	).Scan(&tcount, &tlm))
	return tcount, tlm
}

// readMirror returns the messages_by_room row's tcount, proving both rows are
// stamped together.
func readMirror(t testing.TB, sess *gocql.Session, p Parent) *int {
	t.Helper()
	var tcount *int
	require.NoError(t, sess.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		p.RoomID, p.Bucket, p.CreatedAt, p.MessageID,
	).Scan(&tcount))
	return tcount
}

func TestCountAndLatest_CountsSurvivorsNewestFirst(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 8, nil)

	n, latest, complete, err := countAndLatest(ctx, sess, "thread-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	require.NotNil(t, latest)
	assert.True(t, complete, "the partition ended inside the cap")
}

// The cap bounds rows READ, not replies counted, so a truncated read reports
// the survivors among the newest rows — a lower bound on the true total — and
// still resolves the newest survivor.
func TestCountAndLatest_Truncated(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	deleted := true
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 15; i++ {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id) VALUES (?, ?, ?)`,
			"thread-1", base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("old-%d", i),
		).Exec())
	}
	newer := base.Add(time.Second)
	for i := 0; i < 10; i++ {
		var del *bool
		if i%3 == 0 { // rows 0,3,6,9 deleted
			del = &deleted
		}
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, deleted) VALUES (?, ?, ?, ?)`,
			"thread-1", newer.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("new-%d", i), del,
		).Exec())
	}

	n, latest, complete, err := countAndLatest(ctx, sess, "thread-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	require.NotNil(t, latest)
	assert.False(t, complete, "rows remain past the cap")
	// new-9 is deleted, so the newest survivor is new-8.
	assert.Equal(t, newer.Add(8*time.Millisecond).UnixMilli(), latest.UnixMilli())
}

// Deleted rows spend the read budget, so a deleted-heavy head leaves no
// survivor to point at — the caller must read nil as "unresolved", not "empty".
func TestCountAndLatest_DeletedRowsConsumeTheCap(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	deleted := true
	// Explicit disjoint timestamps: the deleted rows must sort NEWER than the
	// live ones, which deriving both from time.Now() would leave to chance.
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 5; i++ {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id) VALUES (?, ?, ?)`,
			"thread-1", base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("live-%d", i),
		).Exec())
	}
	newer := base.Add(time.Hour)
	for i := 0; i < 10; i++ {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, deleted) VALUES (?, ?, ?, ?)`,
			"thread-1", newer.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("del-%d", i), &deleted,
		).Exec())
	}

	n, latest, complete, err := countAndLatest(ctx, sess, "thread-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Nil(t, latest)
	assert.False(t, complete, "the live rows are past the cap, so 0 is not an answer")
}

// An uncapped read pages to the end of the partition and still excludes
// soft-deleted rows — a bare CQL LIMIT would spend rows on the dead ones.
func TestCountAndLatest_Uncapped_PagesAndExcludesDeleted(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	deleted := true
	live := cassPageSize + 5
	seedReplies(t, sess, "thread-1", "del", 200, &deleted)
	seedReplies(t, sess, "thread-1", "live", live, nil)

	n, latest, complete, err := countAndLatest(ctx, sess, "thread-1", 0)
	require.NoError(t, err)
	assert.Equal(t, live, n)
	require.NotNil(t, latest)
	assert.True(t, complete)
}

// The internal scanTimeout must not mask a caller's own cancellation, and an
// aborted read must surface an error rather than leak a partial count.
func TestCountAndLatest_CanceledContext(t *testing.T) {
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 10, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n, latest, complete, err := countAndLatest(ctx, sess, "thread-1", 100)
	require.Error(t, err)
	assert.Equal(t, 0, n)
	assert.Nil(t, latest)
	assert.False(t, complete, "an aborted read has proved nothing")
}

func TestMaintain_UnderLimit_RecountsExactlyAndStampsBothRows(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 4, nil)
	// A wrong stamp (but still under the limit, which is what selects the
	// path) must not survive an exact recount.
	seedStamp(t, sess, "parent-1", 99, nil)
	replyAt := time.Now().UTC().Truncate(time.Millisecond)

	res, err := Maintain(ctx, sess, "thread-1", testParent, Policy{ScanLimit: 100, ReanchorBudget: 0}, +1, &replyAt, false)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Count)

	gotN, gotTLM := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.Equal(t, 4, *gotN)
	require.NotNil(t, gotTLM)
	assert.Equal(t, replyAt.UnixMilli(), gotTLM.UnixMilli(), "the new reply is the newest, so tlm is its own time")

	mirror := readMirror(t, sess, testParent)
	require.NotNil(t, mirror)
	assert.Equal(t, 4, *mirror, "authority and mirror are stamped together")
}

// The exact path merges the reply's own time with the scan's newest survivor
// rather than trusting either alone: a reply processed out of order must not
// drag thread_last_msg_at backwards, and a scan served by a replica that has
// not yet seen this reply must not lose it.
func TestMaintain_UnderLimit_TLMNeverRegresses(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	newest := base.Add(time.Hour)
	for _, at := range []time.Time{base, newest} {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id) VALUES (?, ?, ?)`,
			"thread-1", at, fmt.Sprintf("live-%d", at.UnixMilli()),
		).Exec())
	}
	pol := Policy{ScanLimit: 100, ReanchorBudget: 0}

	// An older reply arriving after the newest one must not pull tlm back.
	older := base.Add(time.Minute)
	res, err := Maintain(ctx, sess, "thread-1", testParent, pol, +1, &older, false)
	require.NoError(t, err)
	require.NotNil(t, res.TLM)
	assert.Equal(t, newest.UnixMilli(), res.TLM.UnixMilli())

	// A reply newer than anything the scan saw still wins.
	future := newest.Add(time.Hour)
	res, err = Maintain(ctx, sess, "thread-1", testParent, pol, +1, &future, false)
	require.NoError(t, err)
	require.NotNil(t, res.TLM)
	assert.Equal(t, future.UnixMilli(), res.TLM.UnixMilli())
}

// Past the limit the partition is never read: the stamped value alone drives
// the result, so a stamp far from the real row count proves the scan was skipped.
func TestMaintain_PastLimit_AdjustsWithoutScanning(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 3, nil)
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	seedStamp(t, sess, "parent-1", 900, &t0)
	replyAt := t0.Add(time.Minute)

	res, err := Maintain(ctx, sess, "thread-1", testParent, Policy{ScanLimit: 100, ReanchorBudget: 0}, +1, &replyAt, false)
	require.NoError(t, err)
	assert.Equal(t, 901, res.Count)

	gotN, gotTLM := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.Equal(t, 901, *gotN)
	require.NotNil(t, gotTLM)
	assert.Equal(t, replyAt.UnixMilli(), gotTLM.UnixMilli())
}

// A redelivery past the limit may already be counted, so it must not count
// again — but it must still re-stamp. stampParent batches two partitions
// unlogged, so the delivery that failed can have landed on the authority row
// and not the mirror, and nothing else would repair that before the ack.
func TestMaintain_PastLimit_RedeliveryRepairsTheMirror(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	// Authority only: exactly what a half-applied batch leaves behind.
	seedStamp(t, sess, "parent-1", 900, &t0)
	replyAt := t0.Add(time.Minute)

	res, err := Maintain(ctx, sess, "thread-1", testParent, Policy{ScanLimit: 100, ReanchorBudget: 0}, +1, &replyAt, true)
	require.NoError(t, err)
	assert.Equal(t, 900, res.Count, "a retry must not add the same reply twice")

	gotN, gotTLM := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.Equal(t, 900, *gotN)
	require.NotNil(t, gotTLM)
	assert.Equal(t, replyAt.UnixMilli(), gotTLM.UnixMilli(),
		"the reply is already persisted, so its time belongs on the parent even though the count does not move")

	mirror := readMirror(t, sess, testParent)
	require.NotNil(t, mirror, "the mirror the failed batch skipped must be repaired before the retry is acked")
	assert.Equal(t, 900, *mirror)
}

// A retry burst is the worst moment to add partition scans, so a redelivery
// takes the stamped value even when the sample would otherwise fire — here the
// budget makes re-anchoring certain and the stamp is far from the real count.
func TestMaintain_PastLimit_RedeliveryNeverReanchors(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 3, nil)
	seedStamp(t, sess, "parent-1", 900, nil)
	replyAt := time.Now().UTC().Truncate(time.Millisecond)

	res, err := Maintain(ctx, sess, "thread-1", testParent,
		Policy{ScanLimit: 100, ReanchorBudget: 1_000_000}, +1, &replyAt, true)
	require.NoError(t, err)
	assert.Equal(t, 900, res.Count, "a re-anchor would have found 3")
}

// tlm only ever moves forward, so a retry carrying an older time cannot drag it back.
func TestMaintain_PastLimit_TLMNeverRegresses(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	tNew := time.Now().UTC().Truncate(time.Millisecond)
	seedStamp(t, sess, "parent-1", 900, &tNew)
	older := tNew.Add(-time.Hour)

	_, err := Maintain(ctx, sess, "thread-1", testParent, Policy{ScanLimit: 100, ReanchorBudget: 0}, +1, &older, false)
	require.NoError(t, err)

	_, gotTLM := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotTLM)
	assert.Equal(t, tNew.UnixMilli(), gotTLM.UnixMilli())
}

func TestMaintain_Delete_UnderLimit_UsesNewestSurvivor(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 3; i++ {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id) VALUES (?, ?, ?)`,
			"thread-1", base.Add(time.Duration(i)*time.Second), fmt.Sprintf("live-%d", i),
		).Exec())
	}
	seedStamp(t, sess, "parent-1", 3, nil)

	res, err := Maintain(ctx, sess, "thread-1", testParent, Policy{ScanLimit: 100, ReanchorBudget: 0}, -1, nil, false)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Count, "the count comes from the survivors, not from the delta")
	require.NotNil(t, res.TLM)
	assert.Equal(t, base.Add(2*time.Second).UnixMilli(), res.TLM.UnixMilli())
}

// A delete past the limit leaves tlm alone: the removed reply may or may not
// have been the newest, and resolving that needs the skipped scan.
func TestMaintain_Delete_PastLimit_DecrementsAndLeavesTLM(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	seedStamp(t, sess, "parent-1", 900, &t0)

	res, err := Maintain(ctx, sess, "thread-1", testParent, Policy{ScanLimit: 100, ReanchorBudget: 0}, -1, nil, false)
	require.NoError(t, err)
	assert.Equal(t, 899, res.Count)

	gotN, gotTLM := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.Equal(t, 899, *gotN)
	require.NotNil(t, gotTLM)
	assert.Equal(t, t0.UnixMilli(), gotTLM.UnixMilli(), "an unresolved tlm must never clear the column")
}

func TestMaintain_Delete_NeverGoesNegative(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedStamp(t, sess, "parent-1", 100, nil)

	res, err := Maintain(ctx, sess, "thread-1", testParent, Policy{ScanLimit: 1, ReanchorBudget: 0}, -1, nil, false)
	require.NoError(t, err)
	assert.Equal(t, 99, res.Count)

	seedStamp(t, sess, "parent-1", 0, nil)
	res, err = Maintain(ctx, sess, "thread-1", testParent, Policy{ScanLimit: 0, ReanchorBudget: 0}, -1, nil, false)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Count)
}

// A budget above the stamped count forces the re-anchor, which replaces the
// estimate with the truth — the drift the ±1 path cannot walk back on its own.
func TestMaintain_ReanchorRepairsDriftInBothDirections(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 40, nil)
	replyAt := time.Now().UTC().Truncate(time.Millisecond)
	always := Policy{ScanLimit: 10, ReanchorBudget: 1_000_000}

	for _, stamped := range []int{9999, 11} { // drifted high, then low
		seedStamp(t, sess, "parent-1", stamped, nil)
		res, err := Maintain(ctx, sess, "thread-1", testParent, always, +1, &replyAt, false)
		require.NoError(t, err)
		assert.Equalf(t, 40, res.Count, "re-anchor must correct a count stamped at %d", stamped)

		gotN, _ := readStamped(t, sess, "parent-1")
		require.NotNil(t, gotN)
		assert.Equal(t, 40, *gotN)
	}
}

func TestReconcile_AllDeleted_ClearsCountAndTLM(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	deleted := true
	base := time.Now().UTC().Truncate(time.Millisecond)
	seedReplies(t, sess, "thread-1", "del", 3, &deleted)
	seedStamp(t, sess, "parent-1", 3, &base)

	res, err := Reconcile(ctx, sess, "thread-1", testParent, Policy{})
	require.NoError(t, err)
	assert.Equal(t, 0, res.Count)
	assert.Nil(t, res.TLM)

	gotN, gotTLM := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.Equal(t, 0, *gotN)
	assert.Nil(t, gotTLM, "an exact scan proves there is no survivor, so clearing is correct")
}

func TestReanchorIfDue(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 6, nil)
	seedStamp(t, sess, "parent-1", 500, nil)

	done, err := ReanchorIfDue(ctx, sess, "thread-1", testParent, Policy{ReanchorBudget: 0})
	require.NoError(t, err)
	assert.False(t, done, "a zero budget never re-anchors")
	gotN, _ := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.Equal(t, 500, *gotN)

	done, err = ReanchorIfDue(ctx, sess, "thread-1", testParent, Policy{ReanchorBudget: 1_000_000})
	require.NoError(t, err)
	assert.True(t, done)
	gotN, _ = readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.Equal(t, 6, *gotN)
}

// The scan cap bounds rows READ, and soft-deleted replies hold rows, so a
// deleted-heavy head can fill the whole cap while live replies survive just
// past it. Stamping that read as exact is what would write tcount=0 over a live
// thread and clear its last-reply time, so a truncated read may only raise the
// count and advance the timestamp.
func TestMaintain_TruncatedScan_NeverStampsAnExactCount(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	deleted := true
	base := time.Now().UTC().Truncate(time.Millisecond)
	// Three survivors, then five deleted rows that are newer than all of them —
	// enough to fill a cap of five on their own.
	for i := 0; i < 3; i++ {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id) VALUES (?, ?, ?)`,
			"thread-1", base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("live-%d", i),
		).Exec())
	}
	newer := base.Add(time.Hour)
	for i := 0; i < 5; i++ {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, deleted) VALUES (?, ?, ?, ?)`,
			"thread-1", newer.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("del-%d", i), &deleted,
		).Exec())
	}
	pol := Policy{ScanLimit: 5, ReanchorBudget: 0}

	t.Run("add keeps the stamped count moving forward", func(t *testing.T) {
		t0 := base.Add(-time.Hour)
		seedStamp(t, sess, "parent-1", 4, &t0)
		replyAt := newer.Add(time.Hour)

		res, err := Maintain(ctx, sess, "thread-1", testParent, pol, +1, &replyAt, false)
		require.NoError(t, err)
		assert.Equal(t, 5, res.Count, "the truncated scan saw 0 survivors; the stamped count must win")

		gotN, gotTLM := readStamped(t, sess, "parent-1")
		require.NotNil(t, gotN)
		assert.Equal(t, 5, *gotN)
		require.NotNil(t, gotTLM)
		assert.Equal(t, replyAt.UnixMilli(), gotTLM.UnixMilli())
	})

	t.Run("delete never clears an unresolved timestamp", func(t *testing.T) {
		t0 := base.Add(-time.Hour)
		seedStamp(t, sess, "parent-1", 4, &t0)

		res, err := Maintain(ctx, sess, "thread-1", testParent, pol, -1, nil, false)
		require.NoError(t, err)
		assert.Equal(t, 3, res.Count)

		gotN, gotTLM := readStamped(t, sess, "parent-1")
		require.NotNil(t, gotN)
		assert.Equal(t, 3, *gotN)
		require.NotNil(t, gotTLM, "a truncated read has not proved the thread is empty")
		assert.Equal(t, t0.UnixMilli(), gotTLM.UnixMilli())
	})

	t.Run("an unstamped parent is not zeroed", func(t *testing.T) {
		require.NoError(t, sess.Query(
			`DELETE FROM messages_by_id WHERE message_id = ?`, "parent-1").Exec())
		replyAt := newer.Add(time.Hour)

		res, err := Maintain(ctx, sess, "thread-1", testParent, pol, +1, &replyAt, false)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Count, "the reply itself is the floor, not the truncated scan's 0")
		require.NotNil(t, res.TLM)
	})
}

// A re-anchor prices itself on the live count but reads physical rows, so a
// partition kept deep by soft deletes would charge far more than the budget.
// Past the row limit it refuses rather than stamp a count it cannot verify, and
// the caller keeps its approximate value.
func TestReconcile_PartitionDeeperThanTheRowLimit_DoesNotStamp(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 12, nil)
	seedStamp(t, sess, "parent-1", 900, nil)

	_, err := Reconcile(ctx, sess, "thread-1", testParent, Policy{ReconcileRowLimit: 5})
	require.Error(t, err)

	gotN, _ := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.Equal(t, 900, *gotN, "an unverifiable count must be left alone, not replaced by a partial one")

	// The same partition inside the limit reconciles normally.
	res, err := Reconcile(ctx, sess, "thread-1", testParent, Policy{ReconcileRowLimit: 100})
	require.NoError(t, err)
	assert.Equal(t, 12, res.Count)
}

// Maintain must not fail a reply because a re-anchor could not be afforded: the
// row limit makes Reconcile error on a deep partition, and the adjustment still
// has to land.
func TestMaintain_ReanchorTooExpensive_FallsBackToTheAdjustment(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 12, nil)
	seedStamp(t, sess, "parent-1", 900, nil)
	replyAt := time.Now().UTC().Truncate(time.Millisecond)

	res, err := Maintain(ctx, sess, "thread-1", testParent,
		Policy{ScanLimit: 100, ReanchorBudget: 1_000_000, ReconcileRowLimit: 5}, +1, &replyAt, false)
	require.NoError(t, err)
	assert.Equal(t, 901, res.Count)
}

// Concurrent adjustments race on read-modify-write and can lose one — the
// accepted cost of dropping the CAS. It is bounded (never an overcount) and a
// re-anchor erases it.
func TestMaintain_ConcurrentAdjustmentsMayLoseUpdates_ReanchorRepairs(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 40, nil)
	seedStamp(t, sess, "parent-1", 100, nil)
	replyAt := time.Now().UTC().Truncate(time.Millisecond)
	pol := Policy{ScanLimit: 10, ReanchorBudget: 0}

	const writers = 16
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func() {
			_, err := Maintain(ctx, sess, "thread-1", testParent, pol, +1, &replyAt, false)
			errs <- err
		}()
	}
	for i := 0; i < writers; i++ {
		require.NoError(t, <-errs)
	}

	gotN, _ := readStamped(t, sess, "parent-1")
	require.NotNil(t, gotN)
	assert.LessOrEqual(t, *gotN, 100+writers, "lost updates undercount; they never overcount")
	assert.Greater(t, *gotN, 100)

	res, err := Reconcile(ctx, sess, "thread-1", testParent, Policy{})
	require.NoError(t, err)
	assert.Equal(t, 40, res.Count)
}
