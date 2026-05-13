//go:build integration

package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/testutil"
)

// setupCassandra provisions an isolated Cassandra keyspace with the message
// tables plus the required UDTs. Mirrors the helper in cassrepo/integration_test.go.
func setupCassandra(t *testing.T) *gocql.Session {
	t.Helper()
	keyspace, adminSession, host := testutil.CassandraKeyspace(t, "history_service_test")
	cql := func(format string) string { return fmt.Sprintf(format, keyspace) }

	for _, stmt := range []string{
		cql(`CREATE TYPE IF NOT EXISTS %s."Participant" (id TEXT, eng_name TEXT, company_name TEXT, app_id TEXT, app_name TEXT, is_bot BOOLEAN, account TEXT)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."File" (id TEXT, name TEXT, type TEXT)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."Card" (template TEXT, data BLOB)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."CardAction" (verb TEXT, text TEXT, card_id TEXT, display_text TEXT, hide_exec_log BOOLEAN, card_tmid TEXT, data BLOB)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."QuotedParentMessage" (message_id TEXT, room_id TEXT, sender FROZEN<"Participant">, created_at TIMESTAMP, msg TEXT, mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>, message_link TEXT, thread_parent_id TEXT, thread_parent_created_at TIMESTAMP)`),
	} {
		require.NoError(t, adminSession.Query(stmt).Exec())
	}

	// messages_by_room
	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_room (
		room_id TEXT, bucket BIGINT, created_at TIMESTAMP, message_id TEXT, thread_room_id TEXT,
		sender FROZEN<"Participant">, target_user FROZEN<"Participant">, msg TEXT,
		mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
		file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
		tshow BOOLEAN, tcount INT, thread_parent_id TEXT, thread_parent_created_at TIMESTAMP,
		quoted_parent_message FROZEN<"QuotedParentMessage">, visible_to TEXT,
		reactions MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>, deleted BOOLEAN,
		type TEXT, sys_msg_data BLOB, site_id TEXT, edited_at TIMESTAMP, updated_at TIMESTAMP,
		PRIMARY KEY ((room_id, bucket), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`)).Exec())

	// messages_by_id
	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_id (
		message_id TEXT, room_id TEXT, thread_room_id TEXT,
		sender FROZEN<"Participant">, target_user FROZEN<"Participant">, msg TEXT,
		mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
		file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
		tshow BOOLEAN, tcount INT, thread_parent_id TEXT, thread_parent_created_at TIMESTAMP,
		quoted_parent_message FROZEN<"QuotedParentMessage">, visible_to TEXT,
		reactions MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>, deleted BOOLEAN,
		type TEXT, sys_msg_data BLOB, site_id TEXT, edited_at TIMESTAMP, created_at TIMESTAMP,
		updated_at TIMESTAMP, pinned_at TIMESTAMP, pinned_by FROZEN<"Participant">,
		PRIMARY KEY (message_id, created_at)
	) WITH CLUSTERING ORDER BY (created_at DESC)`)).Exec())

	// thread_messages_by_room — needed by TestDeleteMessage_ParentWithReplies_NoCascade
	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_room (
		room_id TEXT, bucket BIGINT, thread_room_id TEXT, created_at TIMESTAMP, message_id TEXT,
		sender FROZEN<"Participant">, target_user FROZEN<"Participant">, msg TEXT,
		mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
		file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
		thread_parent_id TEXT,
		quoted_parent_message FROZEN<"QuotedParentMessage">, visible_to TEXT,
		reactions MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>, deleted BOOLEAN,
		type TEXT, sys_msg_data BLOB, site_id TEXT, edited_at TIMESTAMP, updated_at TIMESTAMP,
		PRIMARY KEY ((room_id, bucket), thread_room_id, created_at, message_id)
	) WITH CLUSTERING ORDER BY (thread_room_id DESC, created_at DESC, message_id DESC)`)).Exec())

	// pinned_messages_by_room isn't needed for the flows exercised here; the
	// cassrepo integration tests cover that branch directly. Keeping the setup
	// minimal reduces container-start time.

	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.One
	cluster.Keyspace = keyspace
	ksSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(func() { ksSession.Close() })
	return ksSession
}

// recordingPublisher captures every Publish call for assertions.
type recordingPublisher struct {
	mu   sync.Mutex
	sent []recordedMessage
}

type recordedMessage struct {
	Subject string
	Data    []byte
}

func (p *recordingPublisher) Publish(_ context.Context, subj string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	p.sent = append(p.sent, recordedMessage{Subject: subj, Data: cp})
	return nil
}

// alwaysSubscribedRepo stubs SubscriptionRepository so the subscription gate passes.
type alwaysSubscribedRepo struct{}

func (alwaysSubscribedRepo) GetHistorySharedSince(_ context.Context, _, _ string) (*time.Time, bool, error) {
	return nil, true, nil
}

// stubRoomRepo returns sensible defaults so the edit/delete integration tests
// don't need a Mongo container: MinUserLastSeenAt is absent (no read floor),
// and GetRoomTimes returns a wide enough range to never clip fixtures.
type stubRoomRepo struct{}

func (stubRoomRepo) GetMinUserLastSeenAt(_ context.Context, _ string) (*time.Time, error) {
	return nil, nil
}

func (stubRoomRepo) GetRoomTimes(_ context.Context, _ string) (lastMsgAt, createdAt time.Time, err error) {
	now := time.Now().UTC()
	return now, now.AddDate(-1, 0, 0), nil
}

func TestEditMessage_Integration(t *testing.T) {
	session := setupCassandra(t)
	repo := cassrepo.NewRepository(session, msgbucket.New(24*time.Hour), 365)
	pub := &recordingPublisher{}
	svc := service.New(repo, alwaysSubscribedRepo{}, stubRoomRepo{}, pub, nil, nil, 730*24*time.Hour, false)

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "r-integ"
	msgID := "m-integ"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed the message directly via CQL (bypassing message-worker).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "original", "",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "original", "",
	).Exec())

	// Call the handler directly with a prepared natsrouter.Context.
	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": roomID})
	resp, err := svc.EditMessage(c, models.EditMessageRequest{
		MessageID: msgID,
		NewMsg:    "edited via integration test",
	})
	require.NoError(t, err)
	assert.Equal(t, msgID, resp.MessageID)
	assert.NotZero(t, resp.EditedAt)

	// Cassandra: both tables should reflect the edit.
	var gotMsg string
	require.NoError(t, session.Query(
		`SELECT msg FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotMsg))
	assert.Equal(t, "edited via integration test", gotMsg)

	require.NoError(t, session.Query(
		`SELECT msg FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID,
	).Scan(&gotMsg))
	assert.Equal(t, "edited via integration test", gotMsg)

	// Publisher: exactly one event captured, on the right subject, with the right payload.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.sent, 1)
	assert.Equal(t, "chat.room."+roomID+".event", pub.sent[0].Subject)

	var evt models.MessageEditedEvent
	require.NoError(t, json.Unmarshal(pub.sent[0].Data, &evt))
	assert.Equal(t, "message_edited", evt.Type)
	assert.Equal(t, roomID, evt.RoomID)
	assert.Equal(t, msgID, evt.MessageID)
	assert.Equal(t, "edited via integration test", evt.NewMsg)
	assert.Equal(t, "alice", evt.EditedBy)
	assert.NotZero(t, evt.Timestamp)
	assert.NotZero(t, evt.EditedAt)
}

func TestDeleteMessage_Integration(t *testing.T) {
	session := setupCassandra(t)
	repo := cassrepo.NewRepository(session, msgbucket.New(24*time.Hour), 365)
	pub := &recordingPublisher{}
	svc := service.New(repo, alwaysSubscribedRepo{}, stubRoomRepo{}, pub, nil, nil, 730*24*time.Hour, false)

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "r-del-integ"
	msgID := "m-del-integ"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed a top-level message directly via CQL.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "content", "", false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "content", "", false,
	).Exec())

	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": roomID})
	resp, err := svc.DeleteMessage(c, models.DeleteMessageRequest{MessageID: msgID})
	require.NoError(t, err)
	assert.Equal(t, msgID, resp.MessageID)
	assert.NotZero(t, resp.DeletedAt)

	// Cassandra: both tables flipped to deleted = true; msg content preserved.
	var gotDeleted bool
	var gotMsg string
	require.NoError(t, session.Query(
		`SELECT deleted, msg FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotDeleted, &gotMsg))
	assert.True(t, gotDeleted)
	assert.Equal(t, "content", gotMsg, "msg content must be retained on soft-delete")

	require.NoError(t, session.Query(
		`SELECT deleted, msg FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID,
	).Scan(&gotDeleted, &gotMsg))
	assert.True(t, gotDeleted)
	assert.Equal(t, "content", gotMsg)

	// Publisher: exactly one message_deleted event on the room subject.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.sent, 1)
	assert.Equal(t, "chat.room."+roomID+".event", pub.sent[0].Subject)

	var evt models.MessageDeletedEvent
	require.NoError(t, json.Unmarshal(pub.sent[0].Data, &evt))
	assert.Equal(t, "message_deleted", evt.Type)
	assert.Equal(t, roomID, evt.RoomID)
	assert.Equal(t, msgID, evt.MessageID)
	assert.Equal(t, "alice", evt.DeletedBy)
	assert.NotZero(t, evt.Timestamp)
	assert.NotZero(t, evt.DeletedAt)
}

func TestDeleteMessage_ParentWithReplies_NoCascade(t *testing.T) {
	session := setupCassandra(t)
	repo := cassrepo.NewRepository(session, msgbucket.New(24*time.Hour), 365)
	pub := &recordingPublisher{}
	svc := service.New(repo, alwaysSubscribedRepo{}, stubRoomRepo{}, pub, nil, nil, 730*24*time.Hour, false)

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "r-parent-cascade"
	threadRoomID := "thread-parent-cascade"
	parentID := "m-parent-casc"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyID := "m-reply-survives"
	replyCreatedAt := parentCreatedAt.Add(10 * time.Second)

	// Parent top-level message with tcount = 1 reflecting the one existing reply.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		parentID, roomID, parentCreatedAt, sender, "parent question", "", 1, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID, sender, "parent question", "", 1, false,
	).Exec())

	// Reply authored by someone else — the cascade question is specifically
	// about other users' content being preserved.
	otherSender := models.Participant{ID: "u2", Account: "bob"}
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		replyID, roomID, replyCreatedAt, otherSender, "bob's reply", parentID, parentCreatedAt, threadRoomID, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_room (room_id, bucket, thread_room_id, created_at, message_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(replyCreatedAt), threadRoomID, replyCreatedAt, replyID, otherSender, "bob's reply", parentID, false,
	).Exec())

	// Alice (the parent's sender) deletes the parent.
	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": roomID})
	_, err := svc.DeleteMessage(c, models.DeleteMessageRequest{MessageID: parentID})
	require.NoError(t, err)

	// Parent is soft-deleted.
	var gotDeleted bool
	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted, "parent should be deleted")

	// Reply is untouched — no cascade. Bob's content survives.
	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		replyID, replyCreatedAt,
	).Scan(&gotDeleted))
	assert.False(t, gotDeleted, "thread reply must survive parent deletion (no cascade)")

	require.NoError(t, session.Query(
		`SELECT deleted FROM thread_messages_by_room WHERE room_id = ? AND bucket = ? AND thread_room_id = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(replyCreatedAt), threadRoomID, replyCreatedAt, replyID,
	).Scan(&gotDeleted))
	assert.False(t, gotDeleted, "thread_messages_by_room reply must survive parent deletion")

	// Parent's tcount is preserved (no decrement on parent-delete; the parent
	// doesn't have its own parent to decrement).
	var gotTcount int
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID,
	).Scan(&gotTcount))
	assert.Equal(t, 1, gotTcount, "parent tcount should be unchanged (replies still exist and are counted)")
}
