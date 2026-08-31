//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/userstore"
)

// newTestVaultWrapper constructs an atrest.KeyWrapper backed by the
// shared dev Vault container (started once per test process) and
// registers cleanup. Used by tests that need a real atrest.Cipher.
func newTestVaultWrapper(t *testing.T, ctx context.Context) atrest.KeyWrapper {
	t.Helper()
	v := testutil.Vault(t, ctx)
	w, err := atrest.NewVaultKeyWrapper(ctx, atrest.VaultConfig{
		Address:      v.Address,
		TransitMount: v.TransitMount,
		TransitKey:   v.TransitKey,
		Token:        v.Token,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		// Best-effort: tests can't meaningfully act on a Close failure.
		_ = w.Close()
	})
	return w
}

func setupCassandra(t *testing.T) *gocql.Session {
	t.Helper()
	keyspace, adminSession, host := testutil.CassandraKeyspace(t, "message_worker_test")
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
		fmt.Sprintf(`CREATE TYPE IF NOT EXISTS %s."EncMeta" (
			nonce BLOB
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
		fmt.Sprintf(`CREATE TYPE IF NOT EXISTS %s."File" (
			id   TEXT,
			name TEXT,
			type TEXT
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
			room_id               TEXT,
			bucket                BIGINT,
			created_at            TIMESTAMP,
			message_id            TEXT,
			sender                FROZEN<"Participant">,
			msg                   TEXT,
			site_id               TEXT,
			updated_at            TIMESTAMP,
			edited_at             TIMESTAMP,
			mentions              SET<FROZEN<"Participant">>,
			attachments           LIST<BLOB>,
			card                  FROZEN<"Card">,
			card_action           FROZEN<"CardAction">,
			file                  FROZEN<"File">,
			thread_room_id           TEXT,
			thread_parent_id         TEXT,
			thread_parent_created_at TIMESTAMP,
			tcount                INT,
			thread_last_msg_at TIMESTAMP,
			tshow                 BOOLEAN,
			type                  TEXT,
			sys_msg_data          BLOB,
			quoted_parent_message FROZEN<"QuotedParentMessage">,
			visible_to            TEXT,
			enc_payload           BLOB,
			enc_meta              FROZEN<"EncMeta">,
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
			edited_at                TIMESTAMP,
			mentions                 SET<FROZEN<"Participant">>,
			attachments              LIST<BLOB>,
			card                     FROZEN<"Card">,
			card_action              FROZEN<"CardAction">,
			file                     FROZEN<"File">,
			thread_room_id           TEXT,
			thread_parent_id         TEXT,
			thread_parent_created_at TIMESTAMP,
			tcount                   INT,
			thread_last_msg_at    TIMESTAMP,
			tshow                    BOOLEAN,
			type                     TEXT,
			sys_msg_data             BLOB,
			quoted_parent_message    FROZEN<"QuotedParentMessage">,
			visible_to            TEXT,
			enc_payload              BLOB,
			enc_meta                 FROZEN<"EncMeta">,
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
			edited_at             TIMESTAMP,
			mentions              SET<FROZEN<"Participant">>,
			attachments           LIST<BLOB>,
			card                  FROZEN<"Card">,
			card_action           FROZEN<"CardAction">,
			file                  FROZEN<"File">,
			tshow                 BOOLEAN,
			type                  TEXT,
			sys_msg_data          BLOB,
			quoted_parent_message FROZEN<"QuotedParentMessage">,
			visible_to            TEXT,
			enc_payload           BLOB,
			enc_meta              FROZEN<"EncMeta">,
			deleted               BOOLEAN,
			PRIMARY KEY ((thread_room_id), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`, keyspace),
	}
	for _, stmt := range stmts {
		require.NoError(t, adminSession.Query(stmt).Exec())
	}

	// adminSession is keyspace-unscoped; open a session pinned to our isolated
	// keyspace so test queries can use unqualified table names.
	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = keyspace
	ksSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(func() { ksSession.Close() })
	return ksSession
}

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "message_worker_test")
}

func TestCassandraStore_SaveMessage(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := &cassParticipant{
		ID:          "u-1",
		EngName:     "Alice Wang",
		CompanyName: "愛麗絲",
		Account:     "alice",
	}
	msg := &model.Message{
		ID:          "m-1",
		RoomID:      "r-1",
		UserID:      "u-1",
		UserAccount: "alice",
		Content:     "hello @bob",
		CreatedAt:   now,
		Mentions: []model.Participant{{
			UserID:      "u-bob",
			Account:     "bob",
			ChineseName: "鮑勃",
			EngName:     "Bob Chen",
		}},
	}

	err := store.SaveMessage(ctx, msg, sender, "site-a")
	require.NoError(t, err)

	b := msgbucket.New(24 * time.Hour).Of(now)

	t.Run("messages_by_room row correct", func(t *testing.T) {
		var gotMsg, gotSiteID string
		var gotUpdatedAt time.Time
		err := cassSession.Query(
			`SELECT msg, site_id, updated_at FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"r-1", b, now, "m-1",
		).Scan(&gotMsg, &gotSiteID, &gotUpdatedAt)
		require.NoError(t, err)
		assert.Equal(t, "hello @bob", gotMsg)
		assert.Equal(t, "site-a", gotSiteID)
		assert.Equal(t, now, gotUpdatedAt.UTC().Truncate(time.Millisecond))
	})

	t.Run("messages_by_room mentions persisted", func(t *testing.T) {
		var gotMentions []*cassParticipant
		err := cassSession.Query(
			`SELECT mentions FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"r-1", b, now, "m-1",
		).Scan(&gotMentions)
		require.NoError(t, err)
		require.Len(t, gotMentions, 1)
		assert.Equal(t, "bob", gotMentions[0].Account)
		assert.Equal(t, "Bob Chen", gotMentions[0].EngName)
		assert.Equal(t, "u-bob", gotMentions[0].ID)
	})

	t.Run("messages_by_id row correct", func(t *testing.T) {
		var gotMsg, gotSiteID string
		var gotUpdatedAt time.Time
		err := cassSession.Query(
			`SELECT msg, site_id, updated_at FROM messages_by_id WHERE message_id = ?`,
			"m-1",
		).Scan(&gotMsg, &gotSiteID, &gotUpdatedAt)
		require.NoError(t, err)
		assert.Equal(t, "hello @bob", gotMsg)
		assert.Equal(t, "site-a", gotSiteID)
		assert.Equal(t, now, gotUpdatedAt.UTC().Truncate(time.Millisecond))
	})

	t.Run("messages_by_id mentions persisted", func(t *testing.T) {
		var gotMentions []*cassParticipant
		err := cassSession.Query(
			`SELECT mentions FROM messages_by_id WHERE message_id = ?`,
			"m-1",
		).Scan(&gotMentions)
		require.NoError(t, err)
		require.Len(t, gotMentions, 1)
		assert.Equal(t, "bob", gotMentions[0].Account)
		assert.Equal(t, "Bob Chen", gotMentions[0].EngName)
		assert.Equal(t, "u-bob", gotMentions[0].ID)
	})

	t.Run("messages_by_id room_id persisted", func(t *testing.T) {
		var gotRoomID string
		err := cassSession.Query(
			`SELECT room_id FROM messages_by_id WHERE message_id = ?`,
			"m-1",
		).Scan(&gotRoomID)
		require.NoError(t, err)
		assert.Equal(t, "r-1", gotRoomID)
	})
}

func TestCassandraStore_SaveThreadMessage(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := &cassParticipant{
		ID:          "u-1",
		EngName:     "Alice Wang",
		CompanyName: "愛麗絲",
		Account:     "alice",
	}
	msg := &model.Message{
		ID:                    "m-2",
		RoomID:                "r-1",
		UserID:                "u-1",
		UserAccount:           "alice",
		Content:               "reply @bob",
		CreatedAt:             now,
		ThreadParentMessageID: "m-1",
		Mentions: []model.Participant{{
			UserID:      "u-bob",
			Account:     "bob",
			ChineseName: "鮑勃",
			EngName:     "Bob Chen",
		}},
	}

	const threadRoomID = "tr-test-1"
	_, err := store.SaveThreadMessage(ctx, msg, sender, "site-a", threadRoomID)
	require.NoError(t, err)

	t.Run("thread_messages_by_thread mentions persisted", func(t *testing.T) {
		var gotMentions []*cassParticipant
		err := cassSession.Query(
			`SELECT mentions FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
			threadRoomID, now, "m-2",
		).Scan(&gotMentions)
		require.NoError(t, err)
		require.Len(t, gotMentions, 1)
		assert.Equal(t, "bob", gotMentions[0].Account)
		assert.Equal(t, "Bob Chen", gotMentions[0].EngName)
		assert.Equal(t, "u-bob", gotMentions[0].ID)
	})

	t.Run("messages_by_id mentions persisted for thread", func(t *testing.T) {
		var gotMentions []*cassParticipant
		err := cassSession.Query(
			`SELECT mentions FROM messages_by_id WHERE message_id = ?`,
			"m-2",
		).Scan(&gotMentions)
		require.NoError(t, err)
		require.Len(t, gotMentions, 1)
		assert.Equal(t, "bob", gotMentions[0].Account)
		assert.Equal(t, "Bob Chen", gotMentions[0].EngName)
		assert.Equal(t, "u-bob", gotMentions[0].ID)
	})

	t.Run("messages_by_id thread fields persisted", func(t *testing.T) {
		var gotThreadRoomID, gotThreadParentID string
		err := cassSession.Query(
			`SELECT thread_room_id, thread_parent_id FROM messages_by_id WHERE message_id = ?`,
			"m-2",
		).Scan(&gotThreadRoomID, &gotThreadParentID)
		require.NoError(t, err)
		assert.Equal(t, threadRoomID, gotThreadRoomID)
		assert.Equal(t, "m-1", gotThreadParentID)
	})

	t.Run("messages_by_id room_id persisted for thread message", func(t *testing.T) {
		var gotRoomID string
		err := cassSession.Query(
			`SELECT room_id FROM messages_by_id WHERE message_id = ?`,
			"m-2",
		).Scan(&gotRoomID)
		require.NoError(t, err)
		assert.Equal(t, "r-1", gotRoomID)
	})
}

func TestCassandraStore_GetMessageSender(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := &cassParticipant{
		ID:          "u-1",
		EngName:     "Alice Wang",
		CompanyName: "愛麗絲",
		Account:     "alice",
	}
	msg := &model.Message{
		ID:          "m-sender-test",
		RoomID:      "r-1",
		UserID:      "u-1",
		UserAccount: "alice",
		Content:     "hello",
		CreatedAt:   now,
	}
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	t.Run("existing message returns sender", func(t *testing.T) {
		got, err := store.GetMessageSender(ctx, "m-sender-test")
		require.NoError(t, err)
		assert.Equal(t, "u-1", got.ID)
		assert.Equal(t, "alice", got.Account)
		assert.Equal(t, "Alice Wang", got.EngName)
		assert.Equal(t, "愛麗絲", got.CompanyName)
	})

	t.Run("non-existent message returns error", func(t *testing.T) {
		_, err := store.GetMessageSender(ctx, "does-not-exist")
		require.Error(t, err)
	})
}

func TestCassandraStore_GetMessageCreatedAt(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := &cassParticipant{ID: "u-1", Account: "alice"}
	msg := &model.Message{
		ID:          "m-createdat-test",
		RoomID:      "r-1",
		UserID:      "u-1",
		UserAccount: "alice",
		Content:     "hello",
		CreatedAt:   now,
	}
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	t.Run("existing message returns its createdAt", func(t *testing.T) {
		got, found, err := store.GetMessageCreatedAt(ctx, "m-createdat-test")
		require.NoError(t, err)
		require.True(t, found)
		assert.True(t, got.Equal(now), "want %s, got %s", now, got)
	})

	t.Run("non-existent message returns found=false, no error", func(t *testing.T) {
		_, found, err := store.GetMessageCreatedAt(ctx, "does-not-exist")
		require.NoError(t, err)
		assert.False(t, found)
	})
}

func TestHandler_Integration(t *testing.T) {
	ctx := context.Background()

	cassSession := setupCassandra(t)
	mongoDB := setupMongo(t)

	userCol := mongoDB.Collection("users")
	_, err := userCol.InsertOne(ctx, bson.M{
		"_id":         "u-1",
		"account":     "alice",
		"siteId":      "site-a",
		"engName":     "Alice Wang",
		"chineseName": "愛麗絲",
		"employeeId":  "EMP001",
	})
	require.NoError(t, err)

	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	us := userstore.NewMongoStore(userCol)
	threadStore := newThreadStoreMongo(mongoDB)
	h := NewHandler(store, us, threadStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Message: model.Message{
			ID:          "m-2",
			RoomID:      "r-2",
			UserID:      "u-1",
			UserAccount: "alice",
			Content:     "integration test message",
			CreatedAt:   now,
		},
		SiteID:    "site-a",
		Timestamp: now.UnixMilli(),
	}

	data, err := json.Marshal(evt)
	require.NoError(t, err)

	err = h.processMessage(ctx, data, false)
	require.NoError(t, err)

	var gotMsg string
	err = cassSession.Query(
		`SELECT msg FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		"r-2", msgbucket.New(24*time.Hour).Of(now), now, "m-2",
	).Scan(&gotMsg)
	require.NoError(t, err)
	assert.Equal(t, "integration test message", gotMsg)
}

func TestHandler_Integration_ThreadReply(t *testing.T) {
	ctx := context.Background()

	cassSession := setupCassandra(t)
	db := setupMongo(t)

	userCol := db.Collection("users")
	_, err := userCol.InsertMany(ctx, []interface{}{
		bson.M{
			"_id":         "u-parent",
			"account":     "parent-user",
			"siteId":      "site-a",
			"engName":     "Parent User",
			"chineseName": "家長",
			"employeeId":  "EMP001",
		},
		bson.M{
			"_id":         "u-replier",
			"account":     "replier",
			"siteId":      "site-a",
			"engName":     "Replier User",
			"chineseName": "回覆者",
			"employeeId":  "EMP002",
		},
	})
	require.NoError(t, err)

	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	us := userstore.NewMongoStore(userCol)
	ts := newThreadStoreMongo(db)
	require.NoError(t, ts.EnsureIndexes(ctx))
	h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	})

	now := time.Now().UTC().Truncate(time.Millisecond)

	// First: save the parent message to Cassandra so GetMessageSender can find it
	parentMsg := &model.Message{
		ID:          "msg-parent",
		RoomID:      "r-1",
		UserID:      "u-parent",
		UserAccount: "parent-user",
		Content:     "parent message",
		CreatedAt:   now.Add(-1 * time.Minute),
	}
	parentSender := &cassParticipant{
		ID:      "u-parent",
		EngName: "Parent User",
		Account: "parent-user",
	}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, parentSender, "site-a"))

	// Second: process a thread reply (first reply path)
	replyEvt := model.MessageEvent{
		Message: model.Message{
			ID:                    "msg-reply-1",
			RoomID:                "r-1",
			UserID:                "u-replier",
			UserAccount:           "replier",
			Content:               "first thread reply",
			CreatedAt:             now,
			ThreadParentMessageID: "msg-parent",
		},
		SiteID:    "site-a",
		Timestamp: now.UnixMilli(),
	}
	data, err := json.Marshal(replyEvt)
	require.NoError(t, err)
	require.NoError(t, h.processMessage(ctx, data, false))

	t.Run("thread room created", func(t *testing.T) {
		var room model.ThreadRoom
		err := db.Collection("thread_rooms").FindOne(ctx, bson.M{
			"parentMessageId": "msg-parent",
		}).Decode(&room)
		require.NoError(t, err)
		assert.Equal(t, "msg-parent", room.ParentMessageID)
		assert.Equal(t, "r-1", room.RoomID)
		assert.Equal(t, "site-a", room.SiteID)
		assert.Equal(t, "msg-reply-1", room.LastMsgID)
	})

	t.Run("parent author subscribed", func(t *testing.T) {
		count, err := db.Collection("thread_subscriptions").CountDocuments(ctx, bson.M{
			"userId":          "u-parent",
			"parentMessageId": "msg-parent",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("replier subscribed", func(t *testing.T) {
		count, err := db.Collection("thread_subscriptions").CountDocuments(ctx, bson.M{
			"userId":          "u-replier",
			"parentMessageId": "msg-parent",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	// Third: process a second thread reply (subsequent path)
	reply2Evt := model.MessageEvent{
		Message: model.Message{
			ID:                    "msg-reply-2",
			RoomID:                "r-1",
			UserID:                "u-replier",
			UserAccount:           "replier",
			Content:               "second thread reply",
			CreatedAt:             now.Add(5 * time.Minute),
			ThreadParentMessageID: "msg-parent",
		},
		SiteID:    "site-a",
		Timestamp: now.Add(5 * time.Minute).UnixMilli(),
	}
	data2, err := json.Marshal(reply2Evt)
	require.NoError(t, err)
	require.NoError(t, h.processMessage(ctx, data2, false))

	t.Run("thread room lastMsgId updated", func(t *testing.T) {
		var room model.ThreadRoom
		err := db.Collection("thread_rooms").FindOne(ctx, bson.M{
			"parentMessageId": "msg-parent",
		}).Decode(&room)
		require.NoError(t, err)
		assert.Equal(t, "msg-reply-2", room.LastMsgID)
	})

	t.Run("still only two subscriptions after second reply", func(t *testing.T) {
		count, err := db.Collection("thread_subscriptions").CountDocuments(ctx, bson.M{
			"parentMessageId": "msg-parent",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})
}

func TestHandler_Integration_ThreadReplyWithMention(t *testing.T) {
	ctx := context.Background()

	cassSession := setupCassandra(t)
	db := setupMongo(t)

	userCol := db.Collection("users")
	_, err := userCol.InsertMany(ctx, []interface{}{
		bson.M{"_id": "u-parent", "account": "parent-user", "siteId": "site-a", "engName": "Parent User", "chineseName": "家長", "employeeId": "EMP001"},
		bson.M{"_id": "u-replier", "account": "replier", "siteId": "site-a", "engName": "Replier User", "chineseName": "回覆者", "employeeId": "EMP002"},
		bson.M{"_id": "u-bob", "account": "bob", "siteId": "site-a", "engName": "Bob Chen", "chineseName": "鮑勃", "employeeId": "EMP003"},
	})
	require.NoError(t, err)

	// Bob is a room member (has a subscription with no historySharedSince → full access).
	// markThreadMentions only subscribes mentionees who are room members whose history
	// window admits the parent; a mentionee with no subscription is excluded.
	_, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "sub-bob", "roomId": "r-mention", "u": bson.M{"_id": "u-bob", "account": "bob"},
	})
	require.NoError(t, err)

	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	us := userstore.NewMongoStore(userCol)
	ts := newThreadStoreMongo(db)
	require.NoError(t, ts.EnsureIndexes(ctx))
	h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	})

	now := time.Now().UTC().Truncate(time.Millisecond)

	// Save parent message so GetMessageSender can find it.
	parentMsg := &model.Message{
		ID: "msg-parent-mention", RoomID: "r-mention", UserID: "u-parent", UserAccount: "parent-user",
		Content: "parent message", CreatedAt: now.Add(-1 * time.Minute),
	}
	parentSender := &cassParticipant{ID: "u-parent", EngName: "Parent User", Account: "parent-user"}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, parentSender, "site-a"))

	// Thread reply from replier that mentions @bob (a room member).
	replyEvt := model.MessageEvent{
		Message: model.Message{
			ID: "msg-reply-mention", RoomID: "r-mention", UserID: "u-replier", UserAccount: "replier",
			Content: "hey @bob take a look", CreatedAt: now,
			ThreadParentMessageID: "msg-parent-mention",
		},
		SiteID: "site-a", Timestamp: now.UnixMilli(),
	}
	data, err := json.Marshal(replyEvt)
	require.NoError(t, err)
	require.NoError(t, h.processMessage(ctx, data, false))

	t.Run("bob auto-subscribed with hasMention=true", func(t *testing.T) {
		var got model.ThreadSubscription
		err := db.Collection("thread_subscriptions").FindOne(ctx, bson.M{
			"parentMessageId": "msg-parent-mention",
			"userId":          "u-bob",
		}).Decode(&got)
		require.NoError(t, err)
		assert.Equal(t, "bob", got.UserAccount)
		assert.True(t, got.HasMention, "mentionee must have hasMention=true")
		assert.Nil(t, got.LastSeenAt)
	})

	t.Run("parent author subscribed with hasMention=false (not mentioned)", func(t *testing.T) {
		var got model.ThreadSubscription
		err := db.Collection("thread_subscriptions").FindOne(ctx, bson.M{
			"parentMessageId": "msg-parent-mention",
			"userId":          "u-parent",
		}).Decode(&got)
		require.NoError(t, err)
		assert.False(t, got.HasMention)
	})

	t.Run("replier subscribed with hasMention=false (sender excluded)", func(t *testing.T) {
		var got model.ThreadSubscription
		err := db.Collection("thread_subscriptions").FindOne(ctx, bson.M{
			"parentMessageId": "msg-parent-mention",
			"userId":          "u-replier",
		}).Decode(&got)
		require.NoError(t, err)
		assert.False(t, got.HasMention)
	})

	t.Run("three thread subscriptions total", func(t *testing.T) {
		count, err := db.Collection("thread_subscriptions").CountDocuments(ctx, bson.M{
			"parentMessageId": "msg-parent-mention",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})
}

// A mentionee with no room subscription is not a member and must not be
// thread-subscribed (Option B: non-member mentions are excluded).
func TestHandler_Integration_ThreadReplyMentionNonMemberExcluded(t *testing.T) {
	ctx := context.Background()

	cassSession := setupCassandra(t)
	db := setupMongo(t)

	userCol := db.Collection("users")
	_, err := userCol.InsertMany(ctx, []interface{}{
		bson.M{"_id": "u-parent", "account": "parent-user", "siteId": "site-a", "engName": "Parent User", "chineseName": "家長", "employeeId": "EMP001"},
		bson.M{"_id": "u-replier", "account": "replier", "siteId": "site-a", "engName": "Replier User", "chineseName": "回覆者", "employeeId": "EMP002"},
		bson.M{"_id": "u-bob", "account": "bob", "siteId": "site-a", "engName": "Bob Chen", "chineseName": "鮑勃", "employeeId": "EMP003"},
	})
	require.NoError(t, err)
	// NOTE: no subscriptions row for bob → he is not a room member.

	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	us := userstore.NewMongoStore(userCol)
	ts := newThreadStoreMongo(db)
	require.NoError(t, ts.EnsureIndexes(ctx))
	h := NewHandler(store, us, ts, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	})

	now := time.Now().UTC().Truncate(time.Millisecond)

	parentMsg := &model.Message{
		ID: "msg-parent-nonmember", RoomID: "r-nonmember", UserID: "u-parent", UserAccount: "parent-user",
		Content: "parent message", CreatedAt: now.Add(-1 * time.Minute),
	}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, &cassParticipant{ID: "u-parent", Account: "parent-user"}, "site-a"))

	replyEvt := model.MessageEvent{
		Message: model.Message{
			ID: "msg-reply-nonmember", RoomID: "r-nonmember", UserID: "u-replier", UserAccount: "replier",
			Content: "hey @bob take a look", CreatedAt: now,
			ThreadParentMessageID: "msg-parent-nonmember",
		},
		SiteID: "site-a", Timestamp: now.UnixMilli(),
	}
	data, err := json.Marshal(replyEvt)
	require.NoError(t, err)
	require.NoError(t, h.processMessage(ctx, data, false))

	t.Run("bob not subscribed (non-member)", func(t *testing.T) {
		count, err := db.Collection("thread_subscriptions").CountDocuments(ctx, bson.M{
			"parentMessageId": "msg-parent-nonmember", "userId": "u-bob",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "non-member mentionee must not be thread-subscribed")
	})

	t.Run("only parent author + replier subscribed", func(t *testing.T) {
		count, err := db.Collection("thread_subscriptions").CountDocuments(ctx, bson.M{
			"parentMessageId": "msg-parent-nonmember",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("bob not added to thread_rooms.replyAccounts", func(t *testing.T) {
		var room model.ThreadRoom
		require.NoError(t, db.Collection("thread_rooms").FindOne(ctx, bson.M{
			"parentMessageId": "msg-parent-nonmember",
		}).Decode(&room))
		assert.NotContains(t, room.ReplyAccounts, "bob", "non-member mentionee must not be a thread follower")
	})
}

func TestThreadStoreMongo_AddThreadUnread(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		&model.Subscription{ID: "sA", RoomID: "r1", User: model.SubscriptionUser{ID: "uA", Account: "alice"}},
		&model.Subscription{ID: "sB", RoomID: "r1", User: model.SubscriptionUser{ID: "uB", Account: "bob"}, ThreadUnread: []string{"p1"}},
		&model.Subscription{ID: "sC", RoomID: "r2", User: model.SubscriptionUser{ID: "uA", Account: "alice"}},
	})
	require.NoError(t, err)

	require.NoError(t, store.AddThreadUnread(ctx, "r1", "p1", []string{"alice", "bob"}))
	// Idempotent under redelivery:
	require.NoError(t, store.AddThreadUnread(ctx, "r1", "p1", []string{"alice", "bob"}))

	var a, b, c model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sA"}).Decode(&a))
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sB"}).Decode(&b))
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sC"}).Decode(&c))
	assert.Equal(t, []string{"p1"}, a.ThreadUnread)
	assert.Equal(t, []string{"p1"}, b.ThreadUnread, "$addToSet must not duplicate")
	assert.Nil(t, c.ThreadUnread, "other rooms untouched")
}

func TestThreadStoreMongo_AddThreadUnread_EmptyAccountsNoop(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)

	require.NoError(t, store.AddThreadUnread(ctx, "r1", "p1", nil))
}

func TestThreadStoreMongo_CreateThreadRoom(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	now := time.Now().UTC().Truncate(time.Millisecond)
	room := &model.ThreadRoom{
		ID:              "tr-1",
		ParentMessageID: "msg-parent",
		RoomID:          "r-1",
		SiteID:          "site-a",
		LastMsgAt:       now,
		LastMsgID:       "msg-reply-1",
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	t.Run("first insert succeeds", func(t *testing.T) {
		err := store.CreateThreadRoom(ctx, room)
		require.NoError(t, err)

		got, err := store.GetThreadRoomByParentMessageID(ctx, "msg-parent")
		require.NoError(t, err)
		assert.Equal(t, "tr-1", got.ID)
		assert.Equal(t, "msg-parent", got.ParentMessageID)
		assert.Equal(t, "r-1", got.RoomID)
		assert.Equal(t, "site-a", got.SiteID)
		assert.Equal(t, "msg-reply-1", got.LastMsgID)
	})

	t.Run("duplicate insert returns errThreadRoomExists", func(t *testing.T) {
		dup := &model.ThreadRoom{
			ID:              "tr-2",
			ParentMessageID: "msg-parent",
			RoomID:          "r-1",
			SiteID:          "site-a",
			LastMsgAt:       now,
			LastMsgID:       "msg-reply-2",
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		err := store.CreateThreadRoom(ctx, dup)
		require.ErrorIs(t, err, errThreadRoomExists)
	})
}

func TestThreadStoreMongo_GetThreadRoomByParentMessageID(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	t.Run("not found returns errThreadRoomNotFound", func(t *testing.T) {
		_, err := store.GetThreadRoomByParentMessageID(ctx, "does-not-exist")
		require.ErrorIs(t, err, errThreadRoomNotFound)
	})
}

func TestThreadStoreMongo_InsertThreadSubscription(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	// thread_subscriptions unique key is owned by room-service; create it here so
	// the duplicate-insert path is exercised.
	_, err := db.Collection("thread_subscriptions").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "threadRoomId", Value: 1}, {Key: "userAccount", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Millisecond)
	sub := &model.ThreadSubscription{
		ID:              "ts-1",
		ParentMessageID: "msg-parent",
		RoomID:          "r-1",
		ThreadRoomID:    "tr-1",
		UserID:          "u-1",
		UserAccount:     "alice",
		SiteID:          "site-a",
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	t.Run("insert creates document with correct fields", func(t *testing.T) {
		err := store.InsertThreadSubscription(ctx, sub)
		require.NoError(t, err)

		var got model.ThreadSubscription
		err = db.Collection("thread_subscriptions").FindOne(ctx, bson.M{
			"threadRoomId": "tr-1",
			"userAccount":  "alice",
		}).Decode(&got)
		require.NoError(t, err)
		assert.Equal(t, "ts-1", got.ID)
		assert.Equal(t, "u-1", got.UserID)
		assert.Nil(t, got.LastSeenAt, "lastSeenAt should be nil on insert")
		assert.Equal(t, now, got.CreatedAt.UTC().Truncate(time.Millisecond))
	})

	t.Run("duplicate insert returns error", func(t *testing.T) {
		dup := &model.ThreadSubscription{
			ID:           "ts-dup",
			ThreadRoomID: "tr-1",
			UserID:       "u-1",
			UserAccount:  "alice",
		}
		err := store.InsertThreadSubscription(ctx, dup)
		require.Error(t, err, "second insert with same (threadRoomId, userAccount) must fail")
	})
}

func TestThreadStoreMongo_MarkThreadSubscriptionMention(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	now := time.Now().UTC().Truncate(time.Millisecond)

	t.Run("upsert creates new subscription with hasMention=true", func(t *testing.T) {
		sub := &model.ThreadSubscription{
			ID:              "ts-new-1",
			ParentMessageID: "msg-parent-mk-1",
			RoomID:          "r-1",
			ThreadRoomID:    "tr-mk-1",
			UserID:          "u-mk-new",
			UserAccount:     "bob",
			SiteID:          "site-a",
			LastSeenAt:      nil,
			HasMention:      true,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		require.NoError(t, store.MarkThreadSubscriptionMention(ctx, sub))

		var got model.ThreadSubscription
		err := db.Collection("thread_subscriptions").FindOne(ctx, bson.M{
			"threadRoomId": "tr-mk-1",
			"userAccount":  "bob",
		}).Decode(&got)
		require.NoError(t, err)
		assert.Equal(t, "ts-new-1", got.ID)
		assert.Equal(t, "bob", got.UserAccount)
		assert.True(t, got.HasMention, "hasMention must be true for new mentionee sub")
		assert.Nil(t, got.LastSeenAt)
	})

	t.Run("upsert flips hasMention=true on existing subscription without overwriting other fields", func(t *testing.T) {
		original := &model.ThreadSubscription{
			ID:              "ts-existing-1",
			ParentMessageID: "msg-parent-mk-2",
			RoomID:          "r-1",
			ThreadRoomID:    "tr-mk-2",
			UserID:          "u-mk-existing",
			UserAccount:     "alice",
			SiteID:          "site-a",
			LastSeenAt:      nil,
			HasMention:      false,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		require.NoError(t, store.InsertThreadSubscription(ctx, original))

		later := now.Add(10 * time.Minute)
		mention := &model.ThreadSubscription{
			ID:              "ts-should-be-ignored",
			ParentMessageID: "msg-parent-mk-2",
			RoomID:          "r-1",
			ThreadRoomID:    "tr-mk-2",
			UserID:          "u-mk-existing",
			UserAccount:     "alice",
			SiteID:          "site-a",
			LastSeenAt:      nil,
			HasMention:      true,
			CreatedAt:       later,
			UpdatedAt:       later,
		}
		require.NoError(t, store.MarkThreadSubscriptionMention(ctx, mention))

		var got model.ThreadSubscription
		err := db.Collection("thread_subscriptions").FindOne(ctx, bson.M{
			"threadRoomId": "tr-mk-2",
			"userAccount":  "alice",
		}).Decode(&got)
		require.NoError(t, err)
		assert.Equal(t, "ts-existing-1", got.ID, "original _id preserved on update")
		assert.True(t, got.HasMention, "hasMention flipped to true")
		assert.Equal(t, now, got.CreatedAt.UTC().Truncate(time.Millisecond), "createdAt preserved")
		assert.Equal(t, later, got.UpdatedAt.UTC().Truncate(time.Millisecond), "updatedAt advanced")
	})

	t.Run("repeat call is idempotent", func(t *testing.T) {
		sub := &model.ThreadSubscription{
			ID:              "ts-idem",
			ParentMessageID: "msg-parent-mk-3",
			RoomID:          "r-1",
			ThreadRoomID:    "tr-mk-3",
			UserID:          "u-mk-idem",
			UserAccount:     "charlie",
			SiteID:          "site-a",
			HasMention:      true,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		require.NoError(t, store.MarkThreadSubscriptionMention(ctx, sub))
		require.NoError(t, store.MarkThreadSubscriptionMention(ctx, sub))

		count, err := db.Collection("thread_subscriptions").CountDocuments(ctx, bson.M{
			"threadRoomId": "tr-mk-3",
			"userAccount":  "charlie",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count, "second upsert must not create a second row")
	})

	t.Run("does not resurrect hasMention on an already-read subscription (#467)", func(t *testing.T) {
		readAt := now.Add(time.Minute) // read after the sub was created, before the mention lands
		original := &model.ThreadSubscription{
			ID:              "ts-read",
			ParentMessageID: "msg-parent-mk-4",
			RoomID:          "r-1",
			ThreadRoomID:    "tr-mk-4",
			UserID:          "u-mk-read",
			UserAccount:     "dave",
			SiteID:          "site-a",
			LastSeenAt:      &readAt,
			HasMention:      false,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		require.NoError(t, store.InsertThreadSubscription(ctx, original))

		mention := &model.ThreadSubscription{
			ID:              "ts-read-mention",
			ParentMessageID: "msg-parent-mk-4",
			RoomID:          "r-1",
			ThreadRoomID:    "tr-mk-4",
			UserID:          "u-mk-read",
			UserAccount:     "dave",
			SiteID:          "site-a",
			HasMention:      true,
			CreatedAt:       now, // the mentioning message predates the read
			UpdatedAt:       now,
		}
		require.NoError(t, store.MarkThreadSubscriptionMention(ctx, mention))

		var got model.ThreadSubscription
		err := db.Collection("thread_subscriptions").FindOne(ctx, bson.M{
			"threadRoomId": "tr-mk-4",
			"userAccount":  "dave",
		}).Decode(&got)
		require.NoError(t, err)
		assert.False(t, got.HasMention, "already-read subscription must not be re-flagged")

		count, err := db.Collection("thread_subscriptions").CountDocuments(ctx, bson.M{
			"threadRoomId": "tr-mk-4",
			"userAccount":  "dave",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count, "guarded update must not upsert a duplicate")
	})
}

func TestThreadStoreMongo_UpdateThreadRoomLastMessage(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	now := time.Now().UTC().Truncate(time.Millisecond)
	room := &model.ThreadRoom{
		ID:              "tr-update",
		ParentMessageID: "msg-parent-update",
		RoomID:          "r-1",
		SiteID:          "site-a",
		LastMsgAt:       now,
		LastMsgID:       "msg-1",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	require.NoError(t, store.CreateThreadRoom(ctx, room))

	later := now.Add(10 * time.Minute)
	err := store.UpdateThreadRoomLastMessage(ctx, "tr-update", "msg-5", []string{"bob"}, later)
	require.NoError(t, err)

	got, err := store.GetThreadRoomByParentMessageID(ctx, "msg-parent-update")
	require.NoError(t, err)
	assert.Equal(t, "msg-5", got.LastMsgID)
	assert.Equal(t, later, got.LastMsgAt.UTC().Truncate(time.Millisecond))
	assert.Contains(t, got.ReplyAccounts, "bob", "replier account should be added to ReplyAccounts")
}

func TestCassandraStore_SaveThreadMessage_IncrementsParentTcount(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := msgbucket.New(24 * time.Hour).Of(parentCreatedAt)
	replyCreatedAt := parentCreatedAt.Add(5 * time.Minute)

	parentSender := &cassParticipant{ID: "u-parent", Account: "alice", EngName: "Alice"}
	parentMsg := &model.Message{
		ID:        "tcount-parent",
		RoomID:    "tcount-room",
		UserID:    "u-parent",
		CreatedAt: parentCreatedAt,
		Content:   "parent message",
	}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, parentSender, "site-a"))

	replySender := &cassParticipant{ID: "u-replier", Account: "bob", EngName: "Bob"}
	replyMsg := &model.Message{
		ID:                           "tcount-reply-1",
		RoomID:                       "tcount-room",
		UserID:                       "u-replier",
		Content:                      "first reply",
		CreatedAt:                    replyCreatedAt,
		ThreadParentMessageID:        "tcount-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	_, err := store.SaveThreadMessage(ctx, replyMsg, replySender, "site-a", "tr-tcount-1")
	require.NoError(t, err)

	t.Run("tcount incremented to 1 in messages_by_id", func(t *testing.T) {
		var tcount int
		err := cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ?`,
			"tcount-parent",
		).Scan(&tcount)
		require.NoError(t, err)
		assert.Equal(t, 1, tcount)
	})

	t.Run("tcount incremented to 1 in messages_by_room", func(t *testing.T) {
		var tcount int
		err := cassSession.Query(
			`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"tcount-room", parentBucket, parentCreatedAt, "tcount-parent",
		).Scan(&tcount)
		require.NoError(t, err)
		assert.Equal(t, 1, tcount)
	})

	// A second reply must increment tcount to 2.
	reply2CreatedAt := replyCreatedAt.Add(5 * time.Minute)
	replyMsg2 := &model.Message{
		ID:                           "tcount-reply-2",
		RoomID:                       "tcount-room",
		UserID:                       "u-replier",
		Content:                      "second reply",
		CreatedAt:                    reply2CreatedAt,
		ThreadParentMessageID:        "tcount-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	_, err2 := store.SaveThreadMessage(ctx, replyMsg2, replySender, "site-a", "tr-tcount-1")
	require.NoError(t, err2)

	t.Run("tcount incremented to 2 in messages_by_id after second reply", func(t *testing.T) {
		var tcount int
		err := cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ?`,
			"tcount-parent",
		).Scan(&tcount)
		require.NoError(t, err)
		assert.Equal(t, 2, tcount)
	})

	t.Run("tcount incremented to 2 in messages_by_room after second reply", func(t *testing.T) {
		var tcount int
		err := cassSession.Query(
			`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"tcount-room", parentBucket, parentCreatedAt, "tcount-parent",
		).Scan(&tcount)
		require.NoError(t, err)
		assert.Equal(t, 2, tcount)
	})

	t.Run("nil ThreadParentMessageCreatedAt skips tcount update without error", func(t *testing.T) {
		noTsReply := &model.Message{
			ID:                    "tcount-reply-nots",
			RoomID:                "tcount-room",
			UserID:                "u-replier",
			Content:               "reply without parent ts",
			CreatedAt:             reply2CreatedAt.Add(5 * time.Minute),
			ThreadParentMessageID: "tcount-parent",
			// ThreadParentMessageCreatedAt intentionally nil
		}
		_, err := store.SaveThreadMessage(ctx, noTsReply, replySender, "site-a", "tr-tcount-1")
		assert.NoError(t, err)

		// tcount must stay at 2 — nil timestamp skips the increment
		var tcount int
		err = cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ?`,
			"tcount-parent",
		).Scan(&tcount)
		require.NoError(t, err)
		assert.Equal(t, 2, tcount)
	})
}

// Once the thread partition reaches the scan limit, tcount maintenance must
// switch from the exact partition scan to the O(1) incremental CAS path and
// keep advancing the stamped count in both parent tables.
func TestCassandraStore_SaveThreadMessage_PastScanLimit_IncrementalTcount(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	store.threadPolicy.ScanLimit = 3
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := msgbucket.New(24 * time.Hour).Of(parentCreatedAt)

	parentSender := &cassParticipant{ID: "u-mega-parent", Account: "alice", EngName: "Alice"}
	parentMsg := &model.Message{
		ID:        "mega-parent",
		RoomID:    "mega-room",
		UserID:    "u-mega-parent",
		CreatedAt: parentCreatedAt,
		Content:   "parent message",
	}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, parentSender, "site-a"))

	replySender := &cassParticipant{ID: "u-mega-replier", Account: "bob", EngName: "Bob"}
	var lastReplyAt time.Time
	// Replies 1–2 stay under the limit (exact scan); replies 3–5 hit or exceed
	// it and must go through the incremental path with no discontinuity.
	for i := 1; i <= 5; i++ {
		lastReplyAt = parentCreatedAt.Add(time.Duration(i) * time.Minute)
		reply := &model.Message{
			ID:                           fmt.Sprintf("mega-reply-%d", i),
			RoomID:                       "mega-room",
			UserID:                       "u-mega-replier",
			Content:                      "reply",
			CreatedAt:                    lastReplyAt,
			ThreadParentMessageID:        "mega-parent",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
		}
		n, err := store.SaveThreadMessage(ctx, reply, replySender, "site-a", "tr-mega")
		require.NoError(t, err)
		require.NotNil(t, n)
		assert.Equal(t, i, *n, "reply %d must report tcount %d", i, i)
	}

	t.Run("tcount and tlm stamped in messages_by_id", func(t *testing.T) {
		var (
			tcount int
			tlm    time.Time
		)
		require.NoError(t, cassSession.Query(
			`SELECT tcount, thread_last_msg_at FROM messages_by_id WHERE message_id = ?`,
			"mega-parent",
		).Scan(&tcount, &tlm))
		assert.Equal(t, 5, tcount)
		assert.Equal(t, lastReplyAt.UnixMilli(), tlm.UTC().UnixMilli())
	})

	t.Run("tcount and tlm mirrored in messages_by_room", func(t *testing.T) {
		var (
			tcount int
			tlm    time.Time
		)
		require.NoError(t, cassSession.Query(
			`SELECT tcount, thread_last_msg_at FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"mega-room", parentBucket, parentCreatedAt, "mega-parent",
		).Scan(&tcount, &tlm))
		assert.Equal(t, 5, tcount)
		assert.Equal(t, lastReplyAt.UnixMilli(), tlm.UTC().UnixMilli())
	})
}

// Past the scan limit a redelivery must not count the same reply twice. Repair
// of any accumulated drift is the re-anchor's job, not the redelivery's — a
// retry no longer forces the full partition scan it used to.
func TestCassandraStore_SaveThreadMessage_PastScanLimit_Redelivery(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	store.threadPolicy.ScanLimit = 3
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := msgbucket.New(24 * time.Hour).Of(parentCreatedAt)
	parentSender := &cassParticipant{ID: "u-redeliv-parent", Account: "alice", EngName: "Alice"}
	require.NoError(t, store.SaveMessage(ctx, &model.Message{
		ID: "redeliv-parent", RoomID: "redeliv-room", UserID: "u-redeliv-parent",
		CreatedAt: parentCreatedAt, Content: "parent",
	}, parentSender, "site-a"))

	replySender := &cassParticipant{ID: "u-redeliv-replier", Account: "bob", EngName: "Bob"}
	mkReply := func(i int) *model.Message {
		return &model.Message{
			ID:                           fmt.Sprintf("redeliv-reply-%d", i),
			RoomID:                       "redeliv-room",
			UserID:                       "u-redeliv-replier",
			Content:                      "reply",
			CreatedAt:                    parentCreatedAt.Add(time.Duration(i) * time.Minute),
			ThreadParentMessageID:        "redeliv-parent",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
		}
	}
	readCount := func(t *testing.T) int {
		t.Helper()
		var got int
		require.NoError(t, cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ?`, "redeliv-parent",
		).Scan(&got))
		return got
	}

	store.threadPolicy.ReanchorBudget = 0 // pin the approximate path
	for i := 1; i <= 5; i++ {
		_, err := store.SaveThreadMessage(ctx, mkReply(i), replySender, "site-a", "tr-redeliv")
		require.NoError(t, err)
	}
	require.Equal(t, 5, readCount(t))

	t.Run("redelivery does not count the reply twice", func(t *testing.T) {
		n, err := store.SaveThreadMessage(natsutil.WithRedelivery(ctx), mkReply(5), replySender, "site-a", "tr-redeliv")
		require.NoError(t, err)
		require.NotNil(t, n)
		assert.Equal(t, 5, *n)
		assert.Equal(t, 5, readCount(t))
	})

	t.Run("re-anchor repairs a count that drifted high", func(t *testing.T) {
		// Only an exact recount can walk a too-high count back; the +/-1 path
		// never can.
		require.NoError(t, cassSession.Query(
			`UPDATE messages_by_id SET tcount = ? WHERE message_id = ?`, 99, "redeliv-parent",
		).Exec())

		store.threadPolicy.ReanchorBudget = 1_000_000 // force it
		defer func() { store.threadPolicy.ReanchorBudget = 0 }()

		n, err := store.SaveThreadMessage(ctx, mkReply(6), replySender, "site-a", "tr-redeliv")
		require.NoError(t, err)
		require.NotNil(t, n)
		assert.Equal(t, 6, *n, "the exact recount replaces the drifted estimate")
		assert.Equal(t, 6, readCount(t))

		var mirrored int
		require.NoError(t, cassSession.Query(
			`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"redeliv-room", parentBucket, parentCreatedAt, "redeliv-parent",
		).Scan(&mirrored))
		assert.Equal(t, 6, mirrored, "the mirror follows the authority")
	})
}

// With re-anchoring pinned off, a reply past the scan limit takes the purely
// approximate path: it reads the stamped count, adds one, and never scans the
// partition. The stamped value is deliberately far from the real row count, so
// a scan-derived answer would be obvious.
func TestCassandraStore_SaveThreadMessage_PastScanLimit_AdjustsWithoutScanning(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	store.threadPolicy.ScanLimit = 3
	store.threadPolicy.ReanchorBudget = 0 // never re-anchor, so the estimate is what we assert on
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := msgbucket.New(24 * time.Hour).Of(parentCreatedAt)
	parentSender := &cassParticipant{ID: "u-approx-parent", Account: "alice", EngName: "Alice"}
	require.NoError(t, store.SaveMessage(ctx, &model.Message{
		ID: "approx-parent", RoomID: "approx-room", UserID: "u-approx-parent",
		CreatedAt: parentCreatedAt, Content: "parent",
	}, parentSender, "site-a"))

	// Stamp a count unrelated to the (empty) partition: 900 replies "already"
	// counted. Only the stamped value may drive the result.
	require.NoError(t, cassSession.Query(
		`UPDATE messages_by_id SET tcount = ? WHERE message_id = ?`, 900, "approx-parent",
	).Exec())

	replyAt := parentCreatedAt.Add(time.Minute)
	n, err := store.SaveThreadMessage(ctx, &model.Message{
		ID:                           "approx-reply-1",
		RoomID:                       "approx-room",
		UserID:                       "u-approx-replier",
		Content:                      "reply",
		CreatedAt:                    replyAt,
		ThreadParentMessageID:        "approx-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}, &cassParticipant{ID: "u-approx-replier", Account: "bob", EngName: "Bob"}, "site-a", "tr-approx")
	require.NoError(t, err)
	require.NotNil(t, n)
	assert.Equal(t, 901, *n, "the stamped count moves by one; the partition is never counted")

	var (
		tcount int
		tlm    time.Time
	)
	require.NoError(t, cassSession.Query(
		`SELECT tcount, thread_last_msg_at FROM messages_by_id WHERE message_id = ?`, "approx-parent",
	).Scan(&tcount, &tlm))
	assert.Equal(t, 901, tcount)
	assert.Equal(t, replyAt.UnixMilli(), tlm.UTC().UnixMilli())

	require.NoError(t, cassSession.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		"approx-room", parentBucket, parentCreatedAt, "approx-parent",
	).Scan(&tcount))
	assert.Equal(t, 901, tcount, "the mirror follows the authority")
}

func TestCassandraStore_SaveThreadMessage_IdempotentOnRedelivery(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentBucket := msgbucket.New(24 * time.Hour).Of(parentCreatedAt)
	replyCreatedAt := parentCreatedAt.Add(5 * time.Minute)

	parentSender := &cassParticipant{ID: "u-idem-parent", Account: "alice", EngName: "Alice"}
	parentMsg := &model.Message{
		ID:        "idem-parent",
		RoomID:    "idem-room",
		UserID:    "u-idem-parent",
		CreatedAt: parentCreatedAt,
		Content:   "parent message",
	}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, parentSender, "site-a"))

	replySender := &cassParticipant{ID: "u-idem-replier", Account: "bob", EngName: "Bob"}
	replyMsg := &model.Message{
		ID:                           "idem-reply-1",
		RoomID:                       "idem-room",
		UserID:                       "u-idem-replier",
		Content:                      "reply message",
		CreatedAt:                    replyCreatedAt,
		ThreadParentMessageID:        "idem-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}

	// First delivery.
	_, err := store.SaveThreadMessage(ctx, replyMsg, replySender, "site-a", "tr-idem-1")
	require.NoError(t, err)

	// JetStream redelivery — same message ID, must not increment tcount again.
	_, err = store.SaveThreadMessage(ctx, replyMsg, replySender, "site-a", "tr-idem-1")
	require.NoError(t, err)

	t.Run("tcount stays at 1 in messages_by_id after redelivery", func(t *testing.T) {
		var tcount int
		err := cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ?`,
			"idem-parent",
		).Scan(&tcount)
		require.NoError(t, err)
		assert.Equal(t, 1, tcount)
	})

	t.Run("tcount stays at 1 in messages_by_room after redelivery", func(t *testing.T) {
		var tcount int
		err := cassSession.Query(
			`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"idem-room", parentBucket, parentCreatedAt, "idem-parent",
		).Scan(&tcount)
		require.NoError(t, err)
		assert.Equal(t, 1, tcount)
	})
}

func TestCassandraStore_SaveMessage_WithQuotedParent(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	parentCreatedAt := now.Add(-time.Hour).Truncate(time.Millisecond)
	threadParentCreatedAt := now.Add(-2 * time.Hour).Truncate(time.Millisecond)

	sender := &cassParticipant{
		ID: "u-1", EngName: "Alice Wang", CompanyName: "愛麗絲", Account: "alice",
	}
	snapshot := &cassandra.QuotedParentMessage{
		MessageID: "parent-msg-uuid",
		RoomID:    "r-1",
		Sender:    cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
		CreatedAt: parentCreatedAt,
		Msg:       "the original message",
		Mentions: []cassandra.Participant{
			{ID: "u-carol", Account: "carol", EngName: "Carol Lee"},
		},
		MessageLink:           "http://localhost:3000/r-1/parent-msg-uuid",
		ThreadParentID:        "thread-parent-uuid",
		ThreadParentCreatedAt: &threadParentCreatedAt,
	}
	msg := &model.Message{
		ID:                  "m-quote-1",
		RoomID:              "r-1",
		UserID:              "u-1",
		UserAccount:         "alice",
		Content:             "great point!",
		CreatedAt:           now,
		QuotedParentMessage: snapshot,
	}

	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	t.Run("messages_by_room round-trips QuotedParentMessage including thread context", func(t *testing.T) {
		var got cassandra.QuotedParentMessage
		err := cassSession.Query(
			`SELECT quoted_parent_message FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"r-1", msgbucket.New(24*time.Hour).Of(now), now, "m-quote-1",
		).Scan(&got)
		require.NoError(t, err)
		assert.Equal(t, "parent-msg-uuid", got.MessageID)
		assert.Equal(t, "r-1", got.RoomID)
		assert.Equal(t, "the original message", got.Msg)
		assert.Equal(t, "bob", got.Sender.Account)
		assert.Equal(t, "Bob Chen", got.Sender.EngName)
		assert.Equal(t, parentCreatedAt, got.CreatedAt.UTC().Truncate(time.Millisecond))
		assert.Equal(t, "http://localhost:3000/r-1/parent-msg-uuid", got.MessageLink)
		require.Len(t, got.Mentions, 1)
		assert.Equal(t, "carol", got.Mentions[0].Account)
		assert.Equal(t, "thread-parent-uuid", got.ThreadParentID)
		require.NotNil(t, got.ThreadParentCreatedAt)
		assert.Equal(t, threadParentCreatedAt, got.ThreadParentCreatedAt.UTC().Truncate(time.Millisecond))
	})

	t.Run("messages_by_id round-trips QuotedParentMessage including thread context", func(t *testing.T) {
		var got cassandra.QuotedParentMessage
		err := cassSession.Query(
			`SELECT quoted_parent_message FROM messages_by_id WHERE message_id = ?`,
			"m-quote-1",
		).Scan(&got)
		require.NoError(t, err)
		assert.Equal(t, "parent-msg-uuid", got.MessageID)
		assert.Equal(t, "the original message", got.Msg)
		assert.Equal(t, "thread-parent-uuid", got.ThreadParentID)
		require.NotNil(t, got.ThreadParentCreatedAt)
		assert.Equal(t, threadParentCreatedAt, got.ThreadParentCreatedAt.UTC().Truncate(time.Millisecond))
	})
}

func TestCassandraStore_GetQuotedParentSnapshot(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := &cassParticipant{ID: "u-bob", EngName: "Bob Chen", Account: "bob"}
	parent := &model.Message{
		ID:          "parent-reproj",
		RoomID:      "r-reproj",
		UserID:      "u-bob",
		UserAccount: "bob",
		Content:     "the authoritative original",
		CreatedAt:   now,
		Mentions:    []model.Participant{{UserID: "u-carol", Account: "carol", EngName: "Carol Lee"}},
	}
	require.NoError(t, store.SaveMessage(ctx, parent, sender, "site-a"))

	t.Run("re-projects the authoritative snapshot from messages_by_id", func(t *testing.T) {
		got, found, err := store.GetQuotedParentSnapshot(ctx, "parent-reproj")
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, got)
		assert.Equal(t, "parent-reproj", got.MessageID)
		assert.Equal(t, "r-reproj", got.RoomID)
		assert.Equal(t, "the authoritative original", got.Msg)
		assert.Equal(t, "bob", got.Sender.Account)
		assert.Equal(t, "Bob Chen", got.Sender.EngName)
		assert.Equal(t, now, got.CreatedAt.UTC().Truncate(time.Millisecond))
		require.Len(t, got.Mentions, 1)
		assert.Equal(t, "carol", got.Mentions[0].Account)
		assert.Empty(t, got.MessageLink, "store leaves MessageLink to the caller")
		assert.Empty(t, got.ThreadParentID)
	})

	t.Run("returns found=false for an absent parent", func(t *testing.T) {
		got, found, err := store.GetQuotedParentSnapshot(ctx, "no-such-message")
		require.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, got)
	})
}

func TestCassandraStore_GetQuotedParentSnapshot_Encrypted(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	store := NewCassandraStore(session, msgbucket.New(24*time.Hour), cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := &cassParticipant{ID: "u-bob", EngName: "Bob Chen", Account: "bob"}
	parent := &model.Message{
		ID:          "parent-enc-reproj",
		RoomID:      "r-enc-reproj",
		UserID:      "u-bob",
		UserAccount: "bob",
		Content:     "the encrypted original",
		CreatedAt:   now,
	}
	require.NoError(t, store.SaveMessage(ctx, parent, sender, "site-a"))

	// The msg column is NULL on the encrypted row; GetQuotedParentSnapshot must
	// decrypt enc_payload to recover the body.
	got, found, err := store.GetQuotedParentSnapshot(ctx, "parent-enc-reproj")
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, got)
	assert.Equal(t, "the encrypted original", got.Msg, "encrypted body must be decrypted")
	assert.Equal(t, "bob", got.Sender.Account)
	assert.Equal(t, "r-enc-reproj", got.RoomID)
}

func TestCassandraStore_SaveMessage_NilQuotedParent(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := &cassParticipant{ID: "u-1", Account: "alice"}
	msg := &model.Message{
		ID:          "m-no-quote",
		RoomID:      "r-1",
		UserID:      "u-1",
		UserAccount: "alice",
		Content:     "plain message",
		CreatedAt:   now,
		// QuotedParentMessage intentionally nil — verifies gocql marshals nil
		// pointer as a null UDT (the open question from the spec).
	}

	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	var got *cassandra.QuotedParentMessage
	err := cassSession.Query(
		`SELECT quoted_parent_message FROM messages_by_id WHERE message_id = ?`,
		"m-no-quote",
	).Scan(&got)
	require.NoError(t, err)
	assert.Nil(t, got, "nil pointer must round-trip as null UDT")
}

func TestSaveMessage_BindsBucket(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)

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

func TestSaveThreadMessage_PartitionsByThreadRoom(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)
	ctx := context.Background()

	parentCreatedAt := time.Date(2026, 4, 30, 9, 0, 0, 0, time.UTC)
	parentBucket := bucket.Of(parentCreatedAt)
	parentID := "parent-1"

	// seed parent so incrementParentTcount has a row to update.
	sender := &cassParticipant{ID: "u1", Account: "alice"}
	require.NoError(t, store.SaveMessage(ctx, &model.Message{
		ID:        parentID,
		RoomID:    "room-thread-1",
		Content:   "parent",
		CreatedAt: parentCreatedAt,
		Type:      "text",
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
	_, errSave := store.SaveThreadMessage(ctx, reply, sender, "site-A", "thread-room-1")
	require.NoError(t, errSave)

	// 1. The reply must land in the partition keyed by thread_room_id.
	var gotRoomID string
	require.NoError(t, cassSession.Query(
		`SELECT room_id FROM thread_messages_by_thread
		 WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
		"thread-room-1", reply.CreatedAt, reply.ID,
	).Scan(&gotRoomID))
	assert.Equal(t, reply.RoomID, gotRoomID)

	// 2. incrementParentTcount still uses the parent's bucket in messages_by_room.
	var tcount int
	require.NoError(t, cassSession.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		reply.RoomID, parentBucket, parentCreatedAt, parentID,
	).Scan(&tcount))
	assert.Equal(t, 1, tcount)
}

func TestCassandraStore_SaveThreadMessage_ReturnsTcount(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyCreatedAt := parentCreatedAt.Add(5 * time.Minute)

	parentSender := &cassParticipant{ID: "u-parent-ret", Account: "alice", EngName: "Alice"}
	parentMsg := &model.Message{
		ID:        "ret-parent",
		RoomID:    "ret-room",
		UserID:    "u-parent-ret",
		CreatedAt: parentCreatedAt,
		Content:   "parent for return-value test",
	}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, parentSender, "site-a"))

	replySender := &cassParticipant{ID: "u-replier-ret", Account: "bob", EngName: "Bob"}

	// First reply: returned tcount must be non-nil and equal 1.
	reply1 := &model.Message{
		ID:                           "ret-reply-1",
		RoomID:                       "ret-room",
		UserID:                       "u-replier-ret",
		Content:                      "first reply",
		CreatedAt:                    replyCreatedAt,
		ThreadParentMessageID:        "ret-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	tcount1, err := store.SaveThreadMessage(ctx, reply1, replySender, "site-a", "tr-ret-1")
	require.NoError(t, err)
	require.NotNil(t, tcount1, "SaveThreadMessage must return non-nil tcount for a reply with ThreadParentMessageCreatedAt set")
	assert.Equal(t, 1, *tcount1, "first reply must produce tcount == 1")

	// Second reply: returned tcount must be non-nil and equal 2.
	reply2CreatedAt := replyCreatedAt.Add(5 * time.Minute)
	reply2 := &model.Message{
		ID:                           "ret-reply-2",
		RoomID:                       "ret-room",
		UserID:                       "u-replier-ret",
		Content:                      "second reply",
		CreatedAt:                    reply2CreatedAt,
		ThreadParentMessageID:        "ret-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	tcount2, err := store.SaveThreadMessage(ctx, reply2, replySender, "site-a", "tr-ret-1")
	require.NoError(t, err)
	require.NotNil(t, tcount2, "SaveThreadMessage must return non-nil tcount after second reply")
	assert.Equal(t, 2, *tcount2, "second reply must produce tcount == 2")

	// Reply with nil ThreadParentMessageCreatedAt: returned tcount must be nil.
	reply3 := &model.Message{
		ID:                    "ret-reply-3",
		RoomID:                "ret-room",
		UserID:                "u-replier-ret",
		Content:               "reply without parent timestamp",
		CreatedAt:             reply2CreatedAt.Add(5 * time.Minute),
		ThreadParentMessageID: "ret-parent",
		// ThreadParentMessageCreatedAt intentionally nil
	}
	tcount3, err := store.SaveThreadMessage(ctx, reply3, replySender, "site-a", "tr-ret-1")
	require.NoError(t, err)
	assert.Nil(t, tcount3, "SaveThreadMessage must return nil tcount when ThreadParentMessageCreatedAt is nil")
}

func TestCassandraStore_SaveThreadMessage_WithQuotedParent(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	parentCreatedAt := now.Add(-time.Hour).Truncate(time.Millisecond)

	sender := &cassParticipant{ID: "u-1", Account: "alice"}
	snapshot := &cassandra.QuotedParentMessage{
		MessageID: "parent-msg-uuid",
		RoomID:    "r-1",
		Sender:    cassandra.Participant{ID: "u-bob", Account: "bob"},
		CreatedAt: parentCreatedAt,
		Msg:       "original",
	}
	msg := &model.Message{
		ID:                    "m-thread-quote",
		RoomID:                "r-1",
		UserID:                "u-1",
		UserAccount:           "alice",
		Content:               "thread reply with quote",
		CreatedAt:             now,
		ThreadParentMessageID: "thread-parent-uuid",
		QuotedParentMessage:   snapshot,
	}

	const threadRoomID = "tr-quote-1"
	_, errThread := store.SaveThreadMessage(ctx, msg, sender, "site-a", threadRoomID)
	require.NoError(t, errThread)

	t.Run("thread_messages_by_thread round-trips QuotedParentMessage", func(t *testing.T) {
		var got cassandra.QuotedParentMessage
		err := cassSession.Query(
			`SELECT quoted_parent_message FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
			threadRoomID, now, "m-thread-quote",
		).Scan(&got)
		require.NoError(t, err)
		assert.Equal(t, "parent-msg-uuid", got.MessageID)
		assert.Equal(t, "original", got.Msg)
	})

	t.Run("messages_by_id round-trips QuotedParentMessage for thread message", func(t *testing.T) {
		var got cassandra.QuotedParentMessage
		err := cassSession.Query(
			`SELECT quoted_parent_message FROM messages_by_id WHERE message_id = ?`,
			"m-thread-quote",
		).Scan(&got)
		require.NoError(t, err)
		assert.Equal(t, "parent-msg-uuid", got.MessageID)
	})
}

func TestSaveMessage_EncryptsBody(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	store := NewCassandraStore(session, msgbucket.New(24*time.Hour), cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := &cassParticipant{ID: "u-1", Account: "alice"}
	msg := &model.Message{
		ID:          "m-enc-1",
		RoomID:      "r-enc-1",
		UserID:      "u-1",
		UserAccount: "alice",
		Content:     "secret body",
		CreatedAt:   now,
	}
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	// Direct CQL: msg column is NULL (not just empty string), enc_payload
	// is non-nil. Scanning into *string distinguishes NULL from "" so we
	// actually prove the encrypted write didn't bind the plaintext column.
	var (
		msgCol     *string
		encPayload []byte
		encNonce   []byte
	)
	require.NoError(t, session.Query(
		`SELECT msg, enc_payload, enc_meta.nonce FROM messages_by_room WHERE room_id=? AND message_id=? LIMIT 1 ALLOW FILTERING`,
		"r-enc-1", "m-enc-1",
	).Scan(&msgCol, &encPayload, &encNonce))
	assert.Nil(t, msgCol, "msg column must be NULL on encrypted rows, not empty string")
	require.NotEmpty(t, encPayload)
	require.Len(t, encNonce, 12)

	// Decrypt confirms the body.
	plain, err := cipher.Decrypt(ctx, "r-enc-1", encPayload, atrest.EncMeta{Nonce: encNonce})
	require.NoError(t, err)
	assert.Equal(t, "secret body", plain.Msg)
}

// TestSaveMessage_RedeliveryOverLegacyRow_NullsPlaintextColumns proves
// the hybrid-row hazard is fixed: a JetStream redelivery (or federation
// replay) of a pre-rollout legacy message running under cipher-enabled
// message-worker must NOT leave the row with both plaintext attachments
// and enc_payload set. Without the explicit `msg = null, attachments =
// null, ...` clauses in saveMessageEncrypted's INSERT, Cassandra's
// upsert semantics would preserve the legacy plaintext columns and the
// next decryptIfNeeded → ApplyDecryptedFields would silently drop them.
func TestSaveMessage_RedeliveryOverLegacyRow_NullsPlaintextColumns(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	sizer := msgbucket.New(24 * time.Hour)
	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	store := NewCassandraStore(session, sizer, cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	roomID := "r-redelivery"
	msgID := "m-redelivery"
	originalAttachments := [][]byte{[]byte("legacy-attachment-1"), []byte("legacy-attachment-2")}
	originalSysMsgData := []byte{0xFA, 0xCE, 0xFE, 0xED}
	sender := &cassParticipant{ID: "u-1", Account: "alice"}

	// Pre-write a legacy plaintext row with attachments + sys_msg_data set
	// (simulating a message persisted before the at-rest rollout).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, msg, attachments, sys_msg_data, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, msgID, "legacy body", originalAttachments, originalSysMsgData, "site-a",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, created_at, room_id, msg, attachments, sys_msg_data, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, now, roomID, "legacy body", originalAttachments, originalSysMsgData, "site-a",
	).Exec())

	// Simulate the JetStream redelivery: message-worker (cipher enabled)
	// receives the same (room_id, created_at, message_id) again from the
	// canonical stream and runs SaveMessage, which dispatches to the
	// encrypted path.
	msg := &model.Message{
		ID:          msgID,
		RoomID:      roomID,
		UserID:      "u-1",
		UserAccount: "alice",
		Content:     "legacy body",
		CreatedAt:   now,
		SysMsgData:  originalSysMsgData,
	}
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	// The encrypted legacy plaintext columns (msg, attachments) must now be
	// NULL on both tables; otherwise decryptIfNeeded → ApplyDecryptedFields
	// will overwrite them with empty values from the new bundle on read,
	// silently losing the attachments. sys_msg_data is not encrypted, so the
	// redelivered insert rewrites it as plaintext and it survives.
	for _, tableQuery := range []struct {
		name string
		q    string
		args []any
	}{
		{
			name: "messages_by_room",
			q:    `SELECT msg, attachments, sys_msg_data FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=? LIMIT 1`,
			args: []any{roomID, sizer.Of(now), now, msgID},
		},
		{
			name: "messages_by_id",
			q:    `SELECT msg, attachments, sys_msg_data FROM messages_by_id WHERE message_id=? LIMIT 1`,
			args: []any{msgID},
		},
	} {
		var (
			msgCol      *string
			attachments [][]byte
			sysMsgData  []byte
		)
		require.NoError(t, session.Query(tableQuery.q, tableQuery.args...).Scan(&msgCol, &attachments, &sysMsgData),
			"select from %s", tableQuery.name)
		assert.Nil(t, msgCol, "%s: msg must be NULL after redelivered encrypted insert", tableQuery.name)
		assert.Nil(t, attachments, "%s: attachments must be NULL after redelivered encrypted insert", tableQuery.name)
		assert.Equal(t, originalSysMsgData, sysMsgData, "%s: un-encrypted sys_msg_data must be preserved after redelivered encrypted insert", tableQuery.name)
	}
}

// TestSaveThreadMessage_EncryptedPath_SkipsTcountOnRedelivery verifies that
// saveThreadMessageEncrypted uses an IF NOT EXISTS guard on messages_by_id so
// that a JetStream redelivery of the same reply does not double-increment the
// parent's tcount. On first delivery the INSERT must be applied and tcount
// must reach 1; on redelivery the INSERT must be skipped and tcount must stay
// at 1.
func TestSaveThreadMessage_EncryptedPath_SkipsTcountOnRedelivery(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(session, bucket, cipher)

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyCreatedAt := parentCreatedAt.Add(5 * time.Minute)

	parentSender := &cassParticipant{ID: "u-parent", Account: "alice", EngName: "Alice"}
	parentMsg := &model.Message{
		ID:        "enc-tcount-parent",
		RoomID:    "enc-tcount-room",
		UserID:    "u-parent",
		CreatedAt: parentCreatedAt,
		Content:   "parent message",
	}
	require.NoError(t, store.SaveMessage(ctx, parentMsg, parentSender, "site-a"))

	replySender := &cassParticipant{ID: "u-replier", Account: "bob", EngName: "Bob"}
	replyMsg := &model.Message{
		ID:                           "enc-tcount-reply",
		RoomID:                       "enc-tcount-room",
		UserID:                       "u-replier",
		Content:                      "first reply",
		CreatedAt:                    replyCreatedAt,
		ThreadParentMessageID:        "enc-tcount-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}

	// First delivery — must succeed and increment tcount to 1.
	_, err := store.SaveThreadMessage(ctx, replyMsg, replySender, "site-a", "enc-tr-tcount-1")
	require.NoError(t, err)

	t.Run("tcount 1 after first delivery", func(t *testing.T) {
		var tcount int
		require.NoError(t, session.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ?`,
			"enc-tcount-parent",
		).Scan(&tcount))
		assert.Equal(t, 1, tcount)
	})

	// Redelivery — same message, same coordinates.  Must NOT double-increment.
	_, err = store.SaveThreadMessage(ctx, replyMsg, replySender, "site-a", "enc-tr-tcount-1")
	require.NoError(t, err)

	t.Run("tcount still 1 after redelivery — no double-increment", func(t *testing.T) {
		var tcount int
		require.NoError(t, session.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ?`,
			"enc-tcount-parent",
		).Scan(&tcount))
		assert.Equal(t, 1, tcount, "redelivery must not double-increment tcount")
	})

	t.Run("tcount still 1 in messages_by_room after redelivery", func(t *testing.T) {
		parentBucket := bucket.Of(parentCreatedAt)
		var tcount int
		require.NoError(t, session.Query(
			`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"enc-tcount-room", parentBucket, parentCreatedAt, "enc-tcount-parent",
		).Scan(&tcount))
		assert.Equal(t, 1, tcount, "redelivery must not double-increment tcount in messages_by_room")
	})
}

// TestCassandraStore_SaveThreadMessage_TShowDualWrite verifies the
// "also send to channel" persistence contract: a TShow=true thread reply gets
// a third INSERT into messages_by_room — keyed by the reply's own created_at
// and bucket — carrying tshow, thread_parent_id, and thread_parent_created_at
// (history-service redacts TShow rows that lack the parent fields). A
// TShow=false reply must keep the legacy two-table shape.
func TestCassandraStore_SaveThreadMessage_TShowDualWrite(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)
	replyCreatedAt := parentCreatedAt.Add(30 * time.Second)
	sender := &cassParticipant{ID: "u-1", Account: "alice", EngName: "Alice Wang"}

	msg := &model.Message{
		ID:                           "m-tshow-1",
		RoomID:                       "r-tshow-1",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "tshow reply",
		CreatedAt:                    replyCreatedAt,
		ThreadParentMessageID:        "m-tshow-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
		TShow:                        true,
	}
	const threadRoomID = "tr-tshow-1"
	_, err := store.SaveThreadMessage(ctx, msg, sender, "site-a", threadRoomID)
	require.NoError(t, err)

	t.Run("messages_by_room row exists with tshow and thread-parent fields", func(t *testing.T) {
		var gotMsg, gotThreadParentID, gotThreadRoomID, gotSiteID string
		var gotTShow bool
		var gotThreadParentCreatedAt, gotUpdatedAt time.Time
		require.NoError(t, cassSession.Query(
			`SELECT msg, tshow, thread_parent_id, thread_parent_created_at, thread_room_id, site_id, updated_at
			 FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"r-tshow-1", bucket.Of(replyCreatedAt), replyCreatedAt, "m-tshow-1",
		).Scan(&gotMsg, &gotTShow, &gotThreadParentID, &gotThreadParentCreatedAt, &gotThreadRoomID, &gotSiteID, &gotUpdatedAt))
		assert.Equal(t, "tshow reply", gotMsg)
		assert.True(t, gotTShow)
		assert.Equal(t, "m-tshow-parent", gotThreadParentID)
		assert.Equal(t, parentCreatedAt, gotThreadParentCreatedAt.UTC())
		assert.Equal(t, threadRoomID, gotThreadRoomID)
		assert.Equal(t, "site-a", gotSiteID)
		assert.Equal(t, replyCreatedAt, gotUpdatedAt.UTC())
	})

	t.Run("messages_by_id and thread_messages_by_thread still written", func(t *testing.T) {
		var gotMsg string
		require.NoError(t, cassSession.Query(
			`SELECT msg FROM messages_by_id WHERE message_id = ?`,
			"m-tshow-1",
		).Scan(&gotMsg))
		assert.Equal(t, "tshow reply", gotMsg)
		require.NoError(t, cassSession.Query(
			`SELECT msg FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
			threadRoomID, replyCreatedAt, "m-tshow-1",
		).Scan(&gotMsg))
		assert.Equal(t, "tshow reply", gotMsg)
	})

	t.Run("TShow=false reply writes no messages_by_room row", func(t *testing.T) {
		noShowCreatedAt := replyCreatedAt.Add(time.Second)
		noShow := &model.Message{
			ID:                           "m-tshow-2",
			RoomID:                       "r-tshow-2",
			UserID:                       "u-1",
			UserAccount:                  "alice",
			Content:                      "plain reply",
			CreatedAt:                    noShowCreatedAt,
			ThreadParentMessageID:        "m-tshow-parent-2",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
		}
		_, err := store.SaveThreadMessage(ctx, noShow, sender, "site-a", "tr-tshow-2")
		require.NoError(t, err)

		var gotMsg string
		err = cassSession.Query(
			`SELECT msg FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			"r-tshow-2", bucket.Of(noShowCreatedAt), noShowCreatedAt, "m-tshow-2",
		).Scan(&gotMsg)
		require.ErrorIs(t, err, gocql.ErrNotFound, "TShow=false reply must not appear in messages_by_room")
	})
}

// TestCassandraStore_SaveThreadMessage_TShowDualWrite_Encrypted verifies the
// cipher-enabled variant: the messages_by_room dual-write row carries the
// encrypted bundle (enc_payload + enc_meta), NULL plaintext body columns, and
// the same tshow/thread-parent metadata as the plaintext path.
func TestCassandraStore_SaveThreadMessage_TShowDualWrite_Encrypted(t *testing.T) {
	ctx := context.Background()
	cassSession := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, cipher)

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)
	replyCreatedAt := parentCreatedAt.Add(30 * time.Second)
	sender := &cassParticipant{ID: "u-1", Account: "alice", EngName: "Alice Wang"}

	msg := &model.Message{
		ID:                           "m-enc-tshow-1",
		RoomID:                       "r-enc-tshow-1",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "encrypted tshow reply",
		CreatedAt:                    replyCreatedAt,
		ThreadParentMessageID:        "m-enc-tshow-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
		TShow:                        true,
	}
	_, err := store.SaveThreadMessage(ctx, msg, sender, "site-a", "tr-enc-tshow-1")
	require.NoError(t, err)

	var gotMsg *string
	var gotPayload []byte
	var gotTShow bool
	var gotThreadParentID string
	var gotThreadParentCreatedAt time.Time
	require.NoError(t, cassSession.Query(
		`SELECT msg, enc_payload, tshow, thread_parent_id, thread_parent_created_at
		 FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		"r-enc-tshow-1", bucket.Of(replyCreatedAt), replyCreatedAt, "m-enc-tshow-1",
	).Scan(&gotMsg, &gotPayload, &gotTShow, &gotThreadParentID, &gotThreadParentCreatedAt))
	assert.Nil(t, gotMsg, "plaintext msg column must be NULL on the encrypted path")
	assert.NotEmpty(t, gotPayload, "enc_payload must carry the encrypted bundle")
	assert.True(t, gotTShow)
	assert.Equal(t, "m-enc-tshow-parent", gotThreadParentID)
	assert.Equal(t, parentCreatedAt, gotThreadParentCreatedAt.UTC())
}

// TestCassandraStore_SaveThreadMessage_TShowPersistedInThread verifies that
// tshow is written into thread_messages_by_thread for both TShow=true and
// TShow=false replies. The read path (GetThreadMessages) returns this column;
// clients use it to distinguish "also send to channel" replies in thread context.
func TestCassandraStore_SaveThreadMessage_TShowPersistedInThread(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)
	replyCreatedAt := parentCreatedAt.Add(30 * time.Second)
	sender := &cassParticipant{ID: "u-1", Account: "alice", EngName: "Alice Wang"}

	t.Run("tshow=true persisted in thread_messages_by_thread", func(t *testing.T) {
		msg := &model.Message{
			ID:                           "m-tshow-thread-1",
			RoomID:                       "r-tshow-thread-1",
			Content:                      "tshow thread reply",
			CreatedAt:                    replyCreatedAt,
			ThreadParentMessageID:        "m-tshow-thread-parent",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
			TShow:                        true,
		}
		_, err := store.SaveThreadMessage(ctx, msg, sender, "site-a", "tr-tshow-thread-1")
		require.NoError(t, err)

		var gotTShow bool
		require.NoError(t, cassSession.Query(
			`SELECT tshow FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
			"tr-tshow-thread-1", replyCreatedAt, "m-tshow-thread-1",
		).Scan(&gotTShow))
		assert.True(t, gotTShow, "tshow=true must be persisted in thread_messages_by_thread")
	})

	t.Run("tshow=false persisted in thread_messages_by_thread", func(t *testing.T) {
		noShowAt := replyCreatedAt.Add(time.Second)
		msg := &model.Message{
			ID:                           "m-tshow-thread-2",
			RoomID:                       "r-tshow-thread-2",
			Content:                      "plain thread reply",
			CreatedAt:                    noShowAt,
			ThreadParentMessageID:        "m-tshow-thread-parent-2",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
			TShow:                        false,
		}
		_, err := store.SaveThreadMessage(ctx, msg, sender, "site-a", "tr-tshow-thread-2")
		require.NoError(t, err)

		var gotTShow bool
		require.NoError(t, cassSession.Query(
			`SELECT tshow FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
			"tr-tshow-thread-2", noShowAt, "m-tshow-thread-2",
		).Scan(&gotTShow))
		// The write path always binds tshow, so a tshow=false reply persists an
		// explicit false (not null) — assert that, so a missing-write regression fails.
		assert.False(t, gotTShow, "tshow=false must be persisted as false in thread_messages_by_thread")
	})
}

func TestAdvanceThreadSubscriptionLastSeen_OnlyAdvances(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	t1 := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.InsertThreadSubscription(ctx, &model.ThreadSubscription{
		ID: "ts-adv", ParentMessageID: "msg-p", RoomID: "r-adv", ThreadRoomID: "tr-adv",
		UserID: "u-adv", UserAccount: "alice", SiteID: "site-a", LastSeenAt: &t1, CreatedAt: t1, UpdatedAt: t1,
	}))

	read := func() time.Time {
		var sub model.ThreadSubscription
		require.NoError(t, db.Collection("thread_subscriptions").
			FindOne(ctx, bson.M{"threadRoomId": "tr-adv", "userAccount": "alice"}).Decode(&sub))
		require.NotNil(t, sub.LastSeenAt)
		return sub.LastSeenAt.UTC()
	}

	t2 := t1.Add(time.Minute)
	require.NoError(t, store.AdvanceThreadSubscriptionLastSeen(ctx, "tr-adv", "alice", t2))
	assert.WithinDuration(t, t2, read(), time.Millisecond, "newer time advances")

	t0 := t1.Add(-time.Minute)
	require.NoError(t, store.AdvanceThreadSubscriptionLastSeen(ctx, "tr-adv", "alice", t0))
	assert.WithinDuration(t, t2, read(), time.Millisecond, "$max never regresses")

	// Missing subscription is a best-effort no-op.
	require.NoError(t, store.AdvanceThreadSubscriptionLastSeen(ctx, "no-room", "nobody", t2))
}

func TestThreadStoreMongo_GetHistorySharedSince(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := newThreadStoreMongo(db)

	shared := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "hss-al", User: model.SubscriptionUser{ID: "u-al", Account: "alice"}, RoomID: "r-hss", HistorySharedSince: &shared},
		model.Subscription{ID: "hss-bo", User: model.SubscriptionUser{ID: "u-bo", Account: "bob"}, RoomID: "r-hss"},
	})
	require.NoError(t, err)

	got, err := store.GetHistorySharedSince(ctx, "r-hss", []string{"alice", "bob", "carol"})
	require.NoError(t, err)
	require.NotNil(t, got["alice"])
	assert.Equal(t, shared.UnixMilli(), got["alice"].UTC().UnixMilli())
	bobWindow, bobPresent := got["bob"]
	require.True(t, bobPresent, "member with a nil window must still be present in the map")
	assert.Nil(t, bobWindow, "member without window decodes to nil")
	_, present := got["carol"]
	assert.False(t, present, "non-member is absent from the map")

	empty, err := store.GetHistorySharedSince(ctx, "r-hss", nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestCassandraStore_SaveThreadMessage_TShowWritesAllTables(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	parentCreatedAt := now.Add(-time.Hour)
	sender := &cassParticipant{ID: "u-1", Account: "alice"}
	msg := &model.Message{
		ID:                           "m-tshow",
		RoomID:                       "r-tshow",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "visible reply",
		CreatedAt:                    now,
		ThreadParentMessageID:        "m-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
		TShow:                        true,
	}

	_, err := store.SaveThreadMessage(ctx, msg, sender, "site-a", "tr-tshow-1")
	require.NoError(t, err)

	// The batch must land in all three tables. Assert the reply's own rows by exact
	// key so the parent's tcount-UPDATE upsert (same partitions) can't interfere.
	var count int
	require.NoError(t, cassSession.Query(`SELECT COUNT(*) FROM messages_by_id WHERE message_id = ?`, "m-tshow").WithContext(ctx).Scan(&count))
	assert.Equal(t, 1, count, "messages_by_id reply row written")

	require.NoError(t, cassSession.Query(`SELECT COUNT(*) FROM thread_messages_by_thread WHERE thread_room_id = ?`, "tr-tshow-1").WithContext(ctx).Scan(&count))
	assert.Equal(t, 1, count, "thread_messages_by_thread row written")

	var gotTShow bool
	require.NoError(t, cassSession.Query(
		`SELECT tshow FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		"r-tshow", bucket.Of(now), now, "m-tshow").WithContext(ctx).Scan(&gotTShow))
	assert.True(t, gotTShow, "tshow mirror row written to messages_by_room with tshow=true")
}

// editStatements mirror history-service/internal/cassrepo/write.go. The edit and
// the create live in different services, so these tests reproduce the edit here
// rather than importing it; keep them in step with that file.
const (
	testEditMsgByID   = `UPDATE messages_by_id SET msg = ?, enc_payload = null, enc_meta = null, edited_at = ?, updated_at = ? WHERE message_id = ?`
	testEditMsgByRoom = `UPDATE messages_by_room SET msg = ?, enc_payload = null, enc_meta = null, edited_at = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	testEditThreadMsg = `UPDATE thread_messages_by_thread SET msg = ?, enc_payload = null, enc_meta = null, edited_at = ?, updated_at = ? WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`
)

// TestCassandraStore_SaveMessage_RedeliveryDoesNotRevertEdit reproduces the
// edit-revert race end to end. Cassandra resolves per cell by write timestamp,
// so an unpinned create re-executed after the author's edit outranks it and
// silently restores the original body. Creates bind USING TIMESTAMP from
// CreatedAt, so the redelivery replays the identical write and the edit stands.
//
// CreatedAt is set an hour in the past so the two orderings are unambiguous:
// pinned, the create is an hour behind the edit; unpinned, it lands after it.
func TestCassandraStore_SaveMessage_RedeliveryDoesNotRevertEdit(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)
	ctx := context.Background()

	createdAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	roomID := "revert-room"
	msgID := "revert-msg-1"
	sender := &cassParticipant{ID: "u-revert", Account: "alice", EngName: "Alice"}
	msg := &model.Message{
		ID:        msgID,
		RoomID:    roomID,
		UserID:    "u-revert",
		CreatedAt: createdAt,
		Content:   "helllo world",
	}

	// First delivery: persists, then a later step in the handler fails and NAKs.
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	// The author fixes the typo while the event is still queued for redelivery.
	editedAt := time.Now().UTC()
	require.NoError(t, cassSession.Query(
		testEditMsgByID, "hello world", editedAt, editedAt, msgID,
	).WithContext(ctx).Exec())
	require.NoError(t, cassSession.Query(
		testEditMsgByRoom, "hello world", editedAt, editedAt,
		roomID, bucket.Of(createdAt), createdAt, msgID,
	).WithContext(ctx).Exec())

	// JetStream redelivers the original event, unchanged.
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	t.Run("messages_by_id keeps the edit", func(t *testing.T) {
		var body string
		require.NoError(t, cassSession.Query(
			`SELECT msg FROM messages_by_id WHERE message_id = ?`, msgID,
		).WithContext(ctx).Scan(&body))
		assert.Equal(t, "hello world", body)
	})

	t.Run("messages_by_room keeps the edit", func(t *testing.T) {
		var body string
		require.NoError(t, cassSession.Query(
			`SELECT msg FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			roomID, bucket.Of(createdAt), createdAt, msgID,
		).WithContext(ctx).Scan(&body))
		assert.Equal(t, "hello world", body)
	})
}

// TestCassandraStore_SaveThreadMessage_RedeliveryDoesNotRevertEdit is the thread
// counterpart. This is the path that actually reaches the race in production:
// SaveThreadMessage commits and publishThreadReplyEvent runs after it, so a
// failed publish NAKs an event whose Cassandra rows are already durable.
func TestCassandraStore_SaveThreadMessage_RedeliveryDoesNotRevertEdit(t *testing.T) {
	cassSession := setupCassandra(t)
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(cassSession, bucket, nil)
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Millisecond)
	replyCreatedAt := parentCreatedAt.Add(time.Hour)
	roomID := "revert-thread-room"
	threadRoomID := "tr-revert-1"
	replyID := "revert-reply-1"

	parentSender := &cassParticipant{ID: "u-revert-parent", Account: "alice", EngName: "Alice"}
	require.NoError(t, store.SaveMessage(ctx, &model.Message{
		ID:        "revert-parent",
		RoomID:    roomID,
		UserID:    "u-revert-parent",
		CreatedAt: parentCreatedAt,
		Content:   "parent message",
	}, parentSender, "site-a"))

	replySender := &cassParticipant{ID: "u-revert-replier", Account: "bob", EngName: "Bob"}
	reply := &model.Message{
		ID:                           replyID,
		RoomID:                       roomID,
		UserID:                       "u-revert-replier",
		Content:                      "teh answer",
		CreatedAt:                    replyCreatedAt,
		ThreadParentMessageID:        "revert-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}

	// First delivery: rows commit, then publishThreadReplyEvent fails and NAKs.
	_, err := store.SaveThreadMessage(ctx, reply, replySender, "site-a", threadRoomID)
	require.NoError(t, err)

	editedAt := time.Now().UTC()
	require.NoError(t, cassSession.Query(
		testEditMsgByID, "the answer", editedAt, editedAt, replyID,
	).WithContext(ctx).Exec())
	require.NoError(t, cassSession.Query(
		testEditThreadMsg, "the answer", editedAt, editedAt,
		threadRoomID, replyCreatedAt, replyID,
	).WithContext(ctx).Exec())

	// JetStream redelivers the original reply event.
	_, err = store.SaveThreadMessage(ctx, reply, replySender, "site-a", threadRoomID)
	require.NoError(t, err)

	t.Run("messages_by_id keeps the edit", func(t *testing.T) {
		var body string
		require.NoError(t, cassSession.Query(
			`SELECT msg FROM messages_by_id WHERE message_id = ?`, replyID,
		).WithContext(ctx).Scan(&body))
		assert.Equal(t, "the answer", body)
	})

	t.Run("thread_messages_by_thread keeps the edit", func(t *testing.T) {
		var body string
		require.NoError(t, cassSession.Query(
			`SELECT msg FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
			threadRoomID, replyCreatedAt, replyID,
		).WithContext(ctx).Scan(&body))
		assert.Equal(t, "the answer", body)
	})
}

// TestSaveMessage_EncryptedRedeliveryKeepsTheRowDecryptable asserts what an
// encrypted create actually has to guarantee across a JetStream redelivery:
// that enc_payload and enc_meta still come from the SAME encryption attempt.
//
// This test used to assert the opposite behaviour — that a pinned create could
// not outrank a later edit — and that pin is exactly what made the row
// undecryptable. atrest.Encrypt draws a fresh random nonce per call, so a
// redelivery produces a different ciphertext AND a different nonce. Pinned,
// both attempts write at one timestamp, and Cassandra breaks a same-timestamp
// conflict per cell by comparing values, independently. The row can end up
// holding attempt A's ciphertext beside attempt B's nonce, which AES-GCM
// cannot open and nothing repairs.
//
// Unpinned, each redelivery is strictly newer, so one attempt wins both cells.
// The accepted cost is visible below: the redelivered create DOES overwrite an
// edit made in between. That is recoverable content loss traded against an
// unrecoverable, undecryptable row. Restoring the pin safely needs the
// ciphertext and nonce in one reconciled cell, or a deterministic encrypted
// bundle — see CLAUDE.md.
func TestSaveMessage_EncryptedRedeliveryKeepsTheRowDecryptable(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	bucket := msgbucket.New(24 * time.Hour)
	store := NewCassandraStore(session, bucket, cipher)

	// An hour in the past so the two orderings are unambiguous: pinned, the
	// create sits an hour behind the edit; unpinned, it would land after it.
	createdAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	roomID, msgID := "r-enc-revert", "m-enc-revert"
	sender := &cassParticipant{ID: "u-1", Account: "alice"}
	msg := &model.Message{
		ID:          msgID,
		RoomID:      roomID,
		UserID:      "u-1",
		UserAccount: "alice",
		Content:     "helllo world",
		CreatedAt:   createdAt,
	}

	// First delivery: rows commit, then a later step in the handler NAKs.
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	// The author fixes the typo while the event is still queued for redelivery.
	// Mirrors editMsgByRoomEncrypted / editMsgByIDEncrypted in
	// history-service/internal/cassrepo/write.go.
	edited := []byte("edited-ciphertext")
	editedAt := time.Now().UTC()
	require.NoError(t, session.Query(
		`UPDATE messages_by_id SET enc_payload = ?, msg = null, attachments = null, card = null, card_action = null, edited_at = ?, updated_at = ? WHERE message_id = ?`,
		edited, editedAt, editedAt, msgID,
	).WithContext(ctx).Exec())
	require.NoError(t, session.Query(
		`UPDATE messages_by_room SET enc_payload = ?, msg = null, attachments = null, card = null, card_action = null, edited_at = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		edited, editedAt, editedAt, roomID, bucket.Of(createdAt), createdAt, msgID,
	).WithContext(ctx).Exec())

	// JetStream redelivers the original event, unchanged.
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	for _, tc := range []struct {
		name string
		q    string
		args []any
	}{
		{
			name: "messages_by_id",
			q:    `SELECT enc_payload, enc_meta FROM messages_by_id WHERE message_id = ?`,
			args: []any{msgID},
		},
		{
			name: "messages_by_room",
			q:    `SELECT enc_payload, enc_meta FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			args: []any{roomID, bucket.Of(createdAt), createdAt, msgID},
		},
	} {
		t.Run(tc.name+" still decrypts", func(t *testing.T) {
			var encPayload []byte
			var encMeta cassandra.EncMeta
			require.NoError(t, session.Query(tc.q, tc.args...).WithContext(ctx).Scan(&encPayload, &encMeta))
			require.NotEmpty(t, encPayload)
			require.Len(t, encMeta.Nonce, 12, "%s: nonce must survive the redelivery intact", tc.name)

			// The load-bearing assertion. A mismatched pair fails here rather
			// than being noticed by a user who can no longer read the message.
			plain, err := cipher.Decrypt(ctx, roomID, encPayload, atrest.EncMeta{Nonce: encMeta.Nonce})
			require.NoError(t, err,
				"%s: ciphertext and nonce came from different encryption attempts", tc.name)

			// The documented trade: the redelivery won, so the edit is gone and
			// the original body is back. Recoverable, unlike an unopenable row.
			assert.Equal(t, msg.Content, plain.Msg,
				"%s: the redelivered create is expected to overwrite the edit", tc.name)
			assert.NotEqual(t, edited, encPayload,
				"%s: an unpinned redelivery must be the write that won", tc.name)
		})
	}
}

// A save that is NOT marked as a redelivery is a first delivery by definition,
// so it counts. This pins the contract that replaced the old pre-insert
// existence probe: redelivery is a property of the JetStream delivery, stamped
// by the consumer, not something the store re-derives with a database read on
// every reply. Suppression/reconcile behaviour for real redeliveries is covered
// by TestCassandraStore_SaveThreadMessage_PastScanLimit_RedeliveryReconciles.
func TestCassandraStore_SaveThreadMessage_PastScanLimit_UnmarkedSaveCounts(t *testing.T) {
	cassSession := setupCassandra(t)
	store := NewCassandraStore(cassSession, msgbucket.New(24*time.Hour), nil)
	store.threadPolicy.ScanLimit = 3
	ctx := context.Background()

	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	parentSender := &cassParticipant{ID: "u-redel-parent", Account: "alice", EngName: "Alice"}
	require.NoError(t, store.SaveMessage(ctx, &model.Message{
		ID: "redel-parent", RoomID: "redel-room", UserID: "u-redel-parent",
		CreatedAt: parentCreatedAt, Content: "parent message",
	}, parentSender, "site-a"))

	replySender := &cassParticipant{ID: "u-redel-replier", Account: "bob", EngName: "Bob"}
	newReply := func(i int) *model.Message {
		return &model.Message{
			ID:                           fmt.Sprintf("redel-reply-%d", i),
			RoomID:                       "redel-room",
			UserID:                       "u-redel-replier",
			Content:                      "reply",
			CreatedAt:                    parentCreatedAt.Add(time.Duration(i) * time.Minute),
			ThreadParentMessageID:        "redel-parent",
			ThreadParentMessageCreatedAt: &parentCreatedAt,
		}
	}
	for i := 1; i <= 5; i++ {
		n, err := store.SaveThreadMessage(ctx, newReply(i), replySender, "site-a", "tr-redel")
		require.NoError(t, err)
		require.NotNil(t, n)
		assert.Equal(t, i, *n)
	}

	var tcount int
	require.NoError(t, cassSession.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ?`, "redel-parent",
	).Scan(&tcount))
	assert.Equal(t, 5, tcount)
}
