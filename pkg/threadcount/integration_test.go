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
// Chunked UnloggedBatch: every row lands in the one partition, so this is a
// single-partition write, and the paging tests seed thousands of rows.
func seedReplies(t *testing.T, sess *gocql.Session, threadRoomID, idPrefix string, count int, deleted *bool) {
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

func TestCount_ShortThread_Exact(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 5, nil)

	n, err := Count(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, 5, n)
}

// A thread longer than one driver page is still counted exactly: the scan pages
// to the end of the partition rather than stopping at any ceiling.
func TestCount_LongerThanOnePage_ExactCount(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	total := cassPageSize + 25
	seedReplies(t, sess, "thread-1", "live", total, nil)

	n, err := Count(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, total, n)
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

// Locks in the no-CQL-LIMIT guarantee: with soft-deleted rows interspersed
// across more than one page, the scan must walk past them and still return the
// exact live total. A hard LIMIT would undercount by spending the limit on
// deleted rows.
func TestCount_ExcludesDeleted_MultiPage(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	deleted := true
	live := cassPageSize + 5
	seedReplies(t, sess, "thread-1", "del", 200, &deleted)
	seedReplies(t, sess, "thread-1", "live", live, nil)

	n, err := Count(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, live, n)
}

func TestCount_EmptyPartition_Zero(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)

	n, err := Count(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestCountAndLatest_ReturnsCountAndLatestSurvivor(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 3; i++ {
		require.NoError(t, sess.Query(
			`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id) VALUES (?, ?, ?)`,
			"thread-1", base.Add(time.Duration(i)*time.Second), fmt.Sprintf("live-%d", i),
		).Exec())
	}

	n, latest, err := CountAndLatest(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	require.NotNil(t, latest)
	assert.Equal(t, base.Add(2*time.Second).UnixMilli(), latest.UnixMilli())
}

// TestCountAndLatest_ExcludesDeletedFromLatest confirms tlm skips a soft-deleted
// newest row and reports the latest *surviving* reply instead.
func TestCountAndLatest_ExcludesDeletedFromLatest(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	deleted := true
	require.NoError(t, sess.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, deleted) VALUES (?, ?, ?, ?)`,
		"thread-1", base, "live-0", nil,
	).Exec())
	require.NoError(t, sess.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, deleted) VALUES (?, ?, ?, ?)`,
		"thread-1", base.Add(time.Second), "del-0", &deleted,
	).Exec())

	n, latest, err := CountAndLatest(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, latest)
	assert.Equal(t, base.UnixMilli(), latest.UnixMilli())
}

func TestCountAndLatest_AllDeleted_NilLatest(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	deleted := true
	seedReplies(t, sess, "thread-1", "del", 3, &deleted)

	n, latest, err := CountAndLatest(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Nil(t, latest)
}

func TestCountAndLatest_LongThread_ExactCount(t *testing.T) {
	ctx := context.Background()
	sess := setupThreadTable(t)
	const replies = 150
	seedReplies(t, sess, "thread-1", "live", replies, nil)

	n, latest, err := CountAndLatest(ctx, sess, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, replies, n)
	require.NotNil(t, latest)
}

// The internal scanTimeout must not mask a caller's own cancellation, and an
// aborted scan must surface an error rather than leak the rows counted so far.
func TestCountAndLatest_CanceledContext_NoPartialCount(t *testing.T) {
	sess := setupThreadTable(t)
	seedReplies(t, sess, "thread-1", "live", 10, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n, latest, err := CountAndLatest(ctx, sess, "thread-1")
	require.Error(t, err)
	assert.Equal(t, 0, n)
	assert.Nil(t, latest)
}
