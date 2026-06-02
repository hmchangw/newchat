//go:build integration

package service

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
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// setupCassandra provisions an isolated keyspace with message tables and UDTs for service-layer tests.
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
		cql(`CREATE TYPE IF NOT EXISTS %s.reaction_key (emoji TEXT, user_account TEXT)`),
		cql(`CREATE TYPE IF NOT EXISTS %s.reactor_info (user_id TEXT, eng_name TEXT, chn_name TEXT, account TEXT, reacted_at TIMESTAMP)`),
	} {
		require.NoError(t, adminSession.Query(stmt).Exec())
	}

	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_room (
		room_id TEXT, bucket BIGINT, created_at TIMESTAMP, message_id TEXT, thread_room_id TEXT,
		sender FROZEN<"Participant">, msg TEXT,
		mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
		file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
		tshow BOOLEAN, tcount INT, thread_parent_id TEXT, thread_parent_created_at TIMESTAMP,
		quoted_parent_message FROZEN<"QuotedParentMessage">, visible_to TEXT,
		reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
		deleted BOOLEAN,
		type TEXT, sys_msg_data BLOB, site_id TEXT, edited_at TIMESTAMP, updated_at TIMESTAMP,
		PRIMARY KEY ((room_id, bucket), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`)).Exec())

	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_id (
		message_id TEXT, room_id TEXT, thread_room_id TEXT,
		sender FROZEN<"Participant">, msg TEXT,
		mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
		file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
		tshow BOOLEAN, tcount INT, thread_parent_id TEXT, thread_parent_created_at TIMESTAMP,
		quoted_parent_message FROZEN<"QuotedParentMessage">, visible_to TEXT,
		reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
		deleted BOOLEAN,
		type TEXT, sys_msg_data BLOB, site_id TEXT, edited_at TIMESTAMP, created_at TIMESTAMP,
		updated_at TIMESTAMP, pinned_at TIMESTAMP, pinned_by FROZEN<"Participant">,
		PRIMARY KEY (message_id, created_at)
	) WITH CLUSTERING ORDER BY (created_at DESC)`)).Exec())

	// thread_messages_by_thread — needed by TestDeleteMessage_ParentWithReplies_NoCascade
	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_thread (
		thread_room_id TEXT, created_at TIMESTAMP, message_id TEXT, room_id TEXT,
		sender FROZEN<"Participant">, msg TEXT,
		mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
		file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
		thread_parent_id TEXT,
		quoted_parent_message FROZEN<"QuotedParentMessage">, visible_to TEXT,
		reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
		deleted BOOLEAN,
		type TEXT, sys_msg_data BLOB, site_id TEXT, edited_at TIMESTAMP, updated_at TIMESTAMP,
		PRIMARY KEY ((thread_room_id), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`)).Exec())

	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.One
	cluster.Keyspace = keyspace
	ksSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(func() { ksSession.Close() })
	return ksSession
}

type recordingPublisher struct {
	mu   sync.Mutex
	sent []recordedMessage
}

type recordedMessage struct {
	Subject string
	Data    []byte
	MsgID   string
}

func (p *recordingPublisher) Publish(_ context.Context, subj string, data []byte, msgID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	p.sent = append(p.sent, recordedMessage{Subject: subj, Data: cp, MsgID: msgID})
	return nil
}

type alwaysSubscribedRepo struct{}

func (alwaysSubscribedRepo) GetHistorySharedSince(_ context.Context, _, _ string) (*time.Time, bool, error) {
	return nil, true, nil
}

func (alwaysSubscribedRepo) GetSubscription(_ context.Context, _, _ string) (*model.Subscription, error) {
	return nil, nil
}

// stubRoomRepo returns defaults wide enough that edit/delete tests never need a Mongo container.
type stubRoomRepo struct{}

func (stubRoomRepo) GetMinUserLastSeenAt(_ context.Context, _ string) (*time.Time, error) {
	return nil, nil
}

func (stubRoomRepo) GetRoomTimes(_ context.Context, _ string) (lastMsgAt, createdAt time.Time, err error) {
	now := time.Now().UTC()
	return now, now.AddDate(-1, 0, 0), nil
}

func (stubRoomRepo) GetRoomUserCount(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func TestEditMessage_Integration(t *testing.T) {
	session := setupCassandra(t)
	repo := cassrepo.NewRepository(session, msgbucket.New(24*time.Hour), 365)
	pub := &recordingPublisher{}
	svc := New(repo, alwaysSubscribedRepo{}, stubRoomRepo{}, pub, nil, &config.Config{
		MessageHistoryFloorDays: 730,
		LargeRoomThreshold:      500,
		MaxPinnedPerRoom:        10,
		PinEnabled:              true,
	})

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "r-integ"
	msgID := "m-integ"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "original", "",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "original", "",
	).Exec())

	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": roomID})
	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{
		MessageID: msgID,
		NewMsg:    "edited via integration test",
	})
	require.NoError(t, err)
	assert.Equal(t, msgID, resp.MessageID)
	assert.NotZero(t, resp.EditedAt)

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

	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.sent, 1)
	assert.Equal(t, subject.MsgCanonicalUpdated("site-test"), pub.sent[0].Subject)

	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(pub.sent[0].Data, &evt))
	assert.Equal(t, model.EventUpdated, evt.Event)
	assert.Equal(t, "site-test", evt.SiteID)
	assert.NotZero(t, evt.Timestamp)
	assert.Equal(t, msgID, evt.Message.ID)
	assert.Equal(t, roomID, evt.Message.RoomID)
	assert.Equal(t, "edited via integration test", evt.Message.Content)
	require.NotNil(t, evt.Message.EditedAt)
	require.NotNil(t, evt.Message.UpdatedAt)
}

func TestDeleteMessage_Integration(t *testing.T) {
	session := setupCassandra(t)
	repo := cassrepo.NewRepository(session, msgbucket.New(24*time.Hour), 365)
	pub := &recordingPublisher{}
	svc := New(repo, alwaysSubscribedRepo{}, stubRoomRepo{}, pub, nil, &config.Config{
		MessageHistoryFloorDays: 730,
		LargeRoomThreshold:      500,
		MaxPinnedPerRoom:        10,
		PinEnabled:              true,
	})

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "r-del-integ"
	msgID := "m-del-integ"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "content", "", false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "content", "", false,
	).Exec())

	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": roomID})
	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: msgID})
	require.NoError(t, err)
	assert.Equal(t, msgID, resp.MessageID)
	assert.NotZero(t, resp.DeletedAt)

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

	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.sent, 1)
	assert.Equal(t, subject.MsgCanonicalDeleted("site-test"), pub.sent[0].Subject)

	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(pub.sent[0].Data, &evt))
	assert.Equal(t, model.EventDeleted, evt.Event)
	assert.Equal(t, "site-test", evt.SiteID)
	assert.NotZero(t, evt.Timestamp)
	assert.Equal(t, msgID, evt.Message.ID)
	assert.Equal(t, roomID, evt.Message.RoomID)
	require.NotNil(t, evt.Message.UpdatedAt)
}

func TestDeleteMessage_ParentWithReplies_NoCascade(t *testing.T) {
	session := setupCassandra(t)
	repo := cassrepo.NewRepository(session, msgbucket.New(24*time.Hour), 365)
	pub := &recordingPublisher{}
	svc := New(repo, alwaysSubscribedRepo{}, stubRoomRepo{}, pub, nil, &config.Config{
		MessageHistoryFloorDays: 730,
		LargeRoomThreshold:      500,
		MaxPinnedPerRoom:        10,
		PinEnabled:              true,
	})

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "r-parent-cascade"
	threadRoomID := "thread-parent-cascade"
	parentID := "m-parent-casc"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyID := "m-reply-survives"
	replyCreatedAt := parentCreatedAt.Add(10 * time.Second)

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		parentID, roomID, parentCreatedAt, sender, "parent question", "", 1, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID, sender, "parent question", "", 1, false,
	).Exec())

	otherSender := models.Participant{ID: "u2", Account: "bob"}
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		replyID, roomID, replyCreatedAt, otherSender, "bob's reply", parentID, parentCreatedAt, threadRoomID, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, replyCreatedAt, replyID, roomID, otherSender, "bob's reply", parentID, false,
	).Exec())

	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": roomID})
	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: parentID})
	require.NoError(t, err)

	var gotDeleted bool
	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted, "parent should be deleted")

	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		replyID, replyCreatedAt,
	).Scan(&gotDeleted))
	assert.False(t, gotDeleted, "thread reply must survive parent deletion (no cascade)")

	require.NoError(t, session.Query(
		`SELECT deleted FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
		threadRoomID, replyCreatedAt, replyID,
	).Scan(&gotDeleted))
	assert.False(t, gotDeleted, "thread_messages_by_thread reply must survive parent deletion")

	var gotTcount int
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID,
	).Scan(&gotTcount))
	assert.Equal(t, 1, gotTcount, "parent tcount should be unchanged (replies still exist and are counted)")
}
