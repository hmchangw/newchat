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

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
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
			data          BLOB
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
			mentions              SET<FROZEN<"Participant">>,
			attachments           LIST<BLOB>,
			card                  FROZEN<"Card">,
			card_action           FROZEN<"CardAction">,
			file                  FROZEN<"File">,
			tcount                INT,
			tshow                 BOOLEAN,
			type                  TEXT,
			sys_msg_data          BLOB,
			quoted_parent_message FROZEN<"QuotedParentMessage">,
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
			mentions                 SET<FROZEN<"Participant">>,
			attachments              LIST<BLOB>,
			card                     FROZEN<"Card">,
			card_action              FROZEN<"CardAction">,
			file                     FROZEN<"File">,
			thread_room_id           TEXT,
			thread_parent_id         TEXT,
			thread_parent_created_at TIMESTAMP,
			tcount                   INT,
			tshow                    BOOLEAN,
			type                     TEXT,
			sys_msg_data             BLOB,
			quoted_parent_message    FROZEN<"QuotedParentMessage">,
			enc_payload              BLOB,
			enc_meta                 FROZEN<"EncMeta">,
			PRIMARY KEY (message_id, created_at)
		) WITH CLUSTERING ORDER BY (created_at DESC)`, keyspace),
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
			file                  FROZEN<"File">,
			tshow                 BOOLEAN,
			type                  TEXT,
			sys_msg_data          BLOB,
			quoted_parent_message FROZEN<"QuotedParentMessage">,
			enc_payload           BLOB,
			enc_meta              FROZEN<"EncMeta">,
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
			`SELECT msg, site_id, updated_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"m-1", now,
		).Scan(&gotMsg, &gotSiteID, &gotUpdatedAt)
		require.NoError(t, err)
		assert.Equal(t, "hello @bob", gotMsg)
		assert.Equal(t, "site-a", gotSiteID)
		assert.Equal(t, now, gotUpdatedAt.UTC().Truncate(time.Millisecond))
	})

	t.Run("messages_by_id mentions persisted", func(t *testing.T) {
		var gotMentions []*cassParticipant
		err := cassSession.Query(
			`SELECT mentions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"m-1", now,
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
			`SELECT room_id FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"m-1", now,
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
	err := store.SaveThreadMessage(ctx, msg, sender, "site-a", threadRoomID)
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
			`SELECT mentions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"m-2", now,
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
			`SELECT thread_room_id, thread_parent_id FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"m-2", now,
		).Scan(&gotThreadRoomID, &gotThreadParentID)
		require.NoError(t, err)
		assert.Equal(t, threadRoomID, gotThreadRoomID)
		assert.Equal(t, "m-1", gotThreadParentID)
	})

	t.Run("messages_by_id room_id persisted for thread message", func(t *testing.T) {
		var gotRoomID string
		err := cassSession.Query(
			`SELECT room_id FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"m-2", now,
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

	err = h.processMessage(ctx, data)
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
	require.NoError(t, h.processMessage(ctx, data))

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
	require.NoError(t, h.processMessage(ctx, data2))

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

	// Thread reply from replier that mentions @bob (non-participant).
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
	require.NoError(t, h.processMessage(ctx, data))

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

	t.Run("thread_rooms.replyAccounts contains replier + parent author + mentioned user", func(t *testing.T) {
		var got model.ThreadRoom
		err := db.Collection("thread_rooms").FindOne(ctx, bson.M{
			"parentMessageId": "msg-parent-mention",
		}).Decode(&got)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"replier", "parent-user", "bob"}, got.ReplyAccounts,
			"replyAccounts should match thread_subscriptions members so notification-worker "+
				"can use this single field as the follower set")
	})
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
	require.NoError(t, store.SaveThreadMessage(ctx, replyMsg, replySender, "site-a", "tr-tcount-1"))

	t.Run("tcount incremented to 1 in messages_by_id", func(t *testing.T) {
		var tcount int
		err := cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"tcount-parent", parentCreatedAt,
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
	require.NoError(t, store.SaveThreadMessage(ctx, replyMsg2, replySender, "site-a", "tr-tcount-1"))

	t.Run("tcount incremented to 2 in messages_by_id after second reply", func(t *testing.T) {
		var tcount int
		err := cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"tcount-parent", parentCreatedAt,
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
		err := store.SaveThreadMessage(ctx, noTsReply, replySender, "site-a", "tr-tcount-1")
		assert.NoError(t, err)

		// tcount must stay at 2 — nil timestamp skips the increment
		var tcount int
		err = cassSession.Query(
			`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"tcount-parent", parentCreatedAt,
		).Scan(&tcount)
		require.NoError(t, err)
		assert.Equal(t, 2, tcount)
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
			`SELECT quoted_parent_message FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"m-quote-1", now,
		).Scan(&got)
		require.NoError(t, err)
		assert.Equal(t, "parent-msg-uuid", got.MessageID)
		assert.Equal(t, "the original message", got.Msg)
		assert.Equal(t, "thread-parent-uuid", got.ThreadParentID)
		require.NotNil(t, got.ThreadParentCreatedAt)
		assert.Equal(t, threadParentCreatedAt, got.ThreadParentCreatedAt.UTC().Truncate(time.Millisecond))
	})
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
		`SELECT quoted_parent_message FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		"m-no-quote", now,
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
	require.NoError(t, store.SaveThreadMessage(ctx, reply, sender, "site-A", "thread-room-1"))

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
	require.NoError(t, store.SaveThreadMessage(ctx, msg, sender, "site-a", threadRoomID))

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
			`SELECT quoted_parent_message FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			"m-thread-quote", now,
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
			q:    `SELECT msg, attachments, sys_msg_data FROM messages_by_id WHERE message_id=? AND created_at=? LIMIT 1`,
			args: []any{msgID, now},
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
