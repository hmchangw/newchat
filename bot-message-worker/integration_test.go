//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/testutil"
)

// setupCassandra creates an isolated keyspace holding the three message tables
// the bot thread-reply write path touches, and returns a keyspace-bound session.
func setupCassandra(t *testing.T) *gocql.Session {
	t.Helper()
	keyspace, admin, host := testutil.CassandraKeyspace(t, "bot_message_worker_test")
	stmts := []string{
		fmt.Sprintf(`CREATE TYPE IF NOT EXISTS %s."Participant" (
			id           TEXT,
			eng_name     TEXT,
			company_name TEXT,
			app_id       TEXT,
			app_name     TEXT,
			is_bot       BOOLEAN,
			account      TEXT
		)`, keyspace),
		fmt.Sprintf(`CREATE TYPE IF NOT EXISTS %s."Card" (
			template TEXT,
			data     BLOB
		)`, keyspace),
		fmt.Sprintf(`CREATE TYPE IF NOT EXISTS %s."CardAction" (
			verb          TEXT,
			text          TEXT,
			card_id       TEXT,
			display_text  TEXT,
			hide_exec_log BOOLEAN,
			card_tmid     TEXT,
			data          BLOB,
			bot_username  TEXT
		)`, keyspace),
		fmt.Sprintf(`CREATE TYPE IF NOT EXISTS %s."QuotedParentMessage" (
			message_id               TEXT,
			room_id                  TEXT,
			sender                   FROZEN<"Participant">,
			created_at               TIMESTAMP,
			msg                      TEXT,
			mentions                 SET<FROZEN<"Participant">>,
			attachments              LIST<BLOB>,
			message_link             TEXT,
			thread_parent_id         TEXT,
			thread_parent_created_at TIMESTAMP
		)`, keyspace),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.messages_by_room (
			room_id                  TEXT,
			bucket                   BIGINT,
			created_at               TIMESTAMP,
			message_id               TEXT,
			sender                   FROZEN<"Participant">,
			msg                      TEXT,
			site_id                  TEXT,
			updated_at               TIMESTAMP,
			mentions                 SET<FROZEN<"Participant">>,
			attachments              LIST<BLOB>,
			card                     FROZEN<"Card">,
			card_action              FROZEN<"CardAction">,
			thread_room_id           TEXT,
			thread_parent_id         TEXT,
			thread_parent_created_at TIMESTAMP,
			tcount                   INT,
			thread_last_msg_at       TIMESTAMP,
			tshow                    BOOLEAN,
			type                     TEXT,
			sys_msg_data             BLOB,
			quoted_parent_message    FROZEN<"QuotedParentMessage">,
			PRIMARY KEY ((room_id, bucket), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`, keyspace),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.messages_by_id (
			message_id               TEXT,
			created_at               TIMESTAMP,
			room_id                  TEXT,
			sender                   FROZEN<"Participant">,
			msg                      TEXT,
			site_id                  TEXT,
			updated_at               TIMESTAMP,
			mentions                 SET<FROZEN<"Participant">>,
			attachments              LIST<BLOB>,
			card                     FROZEN<"Card">,
			card_action              FROZEN<"CardAction">,
			thread_room_id           TEXT,
			thread_parent_id         TEXT,
			thread_parent_created_at TIMESTAMP,
			tcount                   INT,
			thread_last_msg_at       TIMESTAMP,
			tshow                    BOOLEAN,
			type                     TEXT,
			sys_msg_data             BLOB,
			quoted_parent_message    FROZEN<"QuotedParentMessage">,
			PRIMARY KEY (message_id)
		)`, keyspace),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_thread (
			thread_room_id        TEXT,
			created_at            TIMESTAMP,
			message_id            TEXT,
			room_id               TEXT,
			thread_parent_id      TEXT,
			sender                FROZEN<"Participant">,
			msg                   TEXT,
			site_id               TEXT,
			updated_at            TIMESTAMP,
			mentions              SET<FROZEN<"Participant">>,
			attachments           LIST<BLOB>,
			card                  FROZEN<"Card">,
			card_action           FROZEN<"CardAction">,
			deleted               BOOLEAN,
			tshow                 BOOLEAN,
			type                  TEXT,
			sys_msg_data          BLOB,
			quoted_parent_message FROZEN<"QuotedParentMessage">,
			PRIMARY KEY ((thread_room_id), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`, keyspace),
	}
	for _, stmt := range stmts {
		require.NoError(t, admin.Query(stmt).Exec())
	}

	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	sess, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(sess.Close)
	return sess
}

// readParentTcount returns the parent's stamped tcount and tlm from the table
// named by table (messages_by_id or messages_by_room).
func readParentTcountByID(t *testing.T, sess *gocql.Session, parentID string) (int, time.Time) {
	t.Helper()
	var (
		tcount int
		tlm    time.Time
	)
	require.NoError(t, sess.Query(
		`SELECT tcount, thread_last_msg_at FROM messages_by_id WHERE message_id = ?`, parentID,
	).Scan(&tcount, &tlm))
	return tcount, tlm
}

func readParentTcountByRoom(t *testing.T, sess *gocql.Session, roomID string, bucket int64, parentCreatedAt time.Time, parentID string) (int, time.Time) {
	t.Helper()
	var (
		tcount int
		tlm    time.Time
	)
	require.NoError(t, sess.Query(
		`SELECT tcount, thread_last_msg_at FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, bucket, parentCreatedAt, parentID,
	).Scan(&tcount, &tlm))
	return tcount, tlm
}

func TestCassandraStore_SaveThreadMessage_StampsParentTcount(t *testing.T) {
	ctx := context.Background()
	sess := setupCassandra(t)
	sizer := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(sess, sizer, nil)

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := sizer.Of(parentCreatedAt)
	require.NoError(t, sess.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, msg) VALUES (?, ?, ?, ?)`,
		"bot-parent", "bot-room", parentCreatedAt, "parent",
	).Exec())
	require.NoError(t, sess.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, msg) VALUES (?, ?, ?, ?, ?)`,
		"bot-room", parentBucket, parentCreatedAt, "bot-parent", "parent",
	).Exec())

	var lastReplyAt time.Time
	for i := 1; i <= 2; i++ {
		lastReplyAt = parentCreatedAt.Add(time.Duration(i) * time.Minute)
		reply := &model.Message{
			ID:                           fmt.Sprintf("bot-reply-%d", i),
			RoomID:                       "bot-room",
			UserID:                       "bot-1",
			UserAccount:                  "bot",
			Content:                      "bot reply",
			CreatedAt:                    lastReplyAt,
			ThreadParentMessageID:        "bot-parent",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
		}
		require.NoError(t, store.SaveThreadMessage(ctx, reply, "site-a", "tr-bot"))
	}

	gotN, gotTLM := readParentTcountByID(t, sess, "bot-parent")
	assert.Equal(t, 2, gotN)
	assert.Equal(t, lastReplyAt.UnixMilli(), gotTLM.UTC().UnixMilli())

	gotN, gotTLM = readParentTcountByRoom(t, sess, "bot-room", parentBucket, parentCreatedAt, "bot-parent")
	assert.Equal(t, 2, gotN)
	assert.Equal(t, lastReplyAt.UnixMilli(), gotTLM.UTC().UnixMilli())
}

// Past the scan limit the bot writer must switch to the O(1) incremental CAS
// path and keep advancing the stamped count in both parent tables.
func TestCassandraStore_SaveThreadMessage_PastScanLimit_IncrementalTcount(t *testing.T) {
	ctx := context.Background()
	sess := setupCassandra(t)
	sizer := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(sess, sizer, nil)
	store.threadPolicy.ScanLimit = 3

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := sizer.Of(parentCreatedAt)
	require.NoError(t, sess.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, msg) VALUES (?, ?, ?, ?)`,
		"bot-mega-parent", "bot-mega-room", parentCreatedAt, "parent",
	).Exec())
	require.NoError(t, sess.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, msg) VALUES (?, ?, ?, ?, ?)`,
		"bot-mega-room", parentBucket, parentCreatedAt, "bot-mega-parent", "parent",
	).Exec())

	var lastReplyAt time.Time
	for i := 1; i <= 5; i++ {
		lastReplyAt = parentCreatedAt.Add(time.Duration(i) * time.Minute)
		reply := &model.Message{
			ID:                           fmt.Sprintf("bot-mega-reply-%d", i),
			RoomID:                       "bot-mega-room",
			UserID:                       "bot-1",
			UserAccount:                  "bot",
			Content:                      "bot reply",
			CreatedAt:                    lastReplyAt,
			ThreadParentMessageID:        "bot-mega-parent",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
		}
		require.NoError(t, store.SaveThreadMessage(ctx, reply, "site-a", "tr-bot-mega"))
	}

	gotN, gotTLM := readParentTcountByID(t, sess, "bot-mega-parent")
	assert.Equal(t, 5, gotN, "count keeps advancing across the exact→incremental switch")
	assert.Equal(t, lastReplyAt.UnixMilli(), gotTLM.UTC().UnixMilli())

	gotN, gotTLM = readParentTcountByRoom(t, sess, "bot-mega-room", parentBucket, parentCreatedAt, "bot-mega-parent")
	assert.Equal(t, 5, gotN)
	assert.Equal(t, lastReplyAt.UnixMilli(), gotTLM.UTC().UnixMilli())
}

// A legacy reply carrying no parent createdAt cannot address the
// messages_by_room mirror row, so tcount maintenance is skipped entirely.
func TestCassandraStore_SaveThreadMessage_NilParentCreatedAt_SkipsTcount(t *testing.T) {
	ctx := context.Background()
	sess := setupCassandra(t)
	store := NewCassandraStore(sess, msgbucket.New(24*time.Hour), nil)

	createdAt := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, sess.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, msg) VALUES (?, ?, ?, ?)`,
		"bot-legacy-parent", "bot-legacy-room", createdAt, "parent",
	).Exec())

	reply := &model.Message{
		ID:                    "bot-legacy-reply",
		RoomID:                "bot-legacy-room",
		UserID:                "bot-1",
		UserAccount:           "bot",
		Content:               "bot reply",
		CreatedAt:             createdAt.Add(time.Minute),
		ThreadParentMessageID: "bot-legacy-parent",
	}
	require.NoError(t, store.SaveThreadMessage(ctx, reply, "site-a", "tr-bot-legacy"))

	var tcount *int
	require.NoError(t, sess.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ?`, "bot-legacy-parent",
	).Scan(&tcount))
	assert.Nil(t, tcount, "tcount must stay unstamped when the parent createdAt is unknown")
}

// A JetStream redelivery re-runs SaveThreadMessage for a reply already
// persisted. Past the scan limit the incremental path must neither count it a
// second time nor leave a lost increment behind: it reconciles from an exact
// scan. Redelivery is signalled by the consumer through the delivery context,
// not re-derived by the store with a per-reply database probe.
func TestCassandraStore_SaveThreadMessage_PastScanLimit_RedeliveryReconciles(t *testing.T) {
	ctx := context.Background()
	sess := setupCassandra(t)
	sizer := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(sess, sizer, nil)
	store.threadPolicy.ScanLimit = 3

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, sess.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, msg) VALUES (?, ?, ?, ?)`,
		"bot-redel-parent", "bot-redel-room", parentCreatedAt, "parent",
	).Exec())
	require.NoError(t, sess.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, msg) VALUES (?, ?, ?, ?, ?)`,
		"bot-redel-room", sizer.Of(parentCreatedAt), parentCreatedAt, "bot-redel-parent", "parent",
	).Exec())

	newReply := func(i int) *model.Message {
		return &model.Message{
			ID:                           fmt.Sprintf("bot-redel-reply-%d", i),
			RoomID:                       "bot-redel-room",
			UserID:                       "bot-1",
			UserAccount:                  "bot",
			Content:                      "bot reply",
			CreatedAt:                    parentCreatedAt.Add(time.Duration(i) * time.Minute),
			ThreadParentMessageID:        "bot-redel-parent",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
		}
	}
	for i := 1; i <= 5; i++ {
		require.NoError(t, store.SaveThreadMessage(ctx, newReply(i), "site-a", "tr-bot-redel"))
	}

	require.NoError(t, store.SaveThreadMessage(natsutil.WithRedelivery(ctx), newReply(5), "site-a", "tr-bot-redel"))

	gotN, _ := readParentTcountByID(t, sess, "bot-redel-parent")
	assert.Equal(t, 5, gotN, "a redelivered bot reply must not be counted twice")
}
