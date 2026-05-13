//go:build integration

package cassrepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestRepository_GetMessageByID(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "user1"}
	ts := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg) VALUES (?, ?, ?, ?, ?)`,
		"m1", "r1", ts, sender, "hello",
	).Exec())

	msg, err := repo.GetMessageByID(ctx, "m1")
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "m1", msg.MessageID)
	assert.Equal(t, "r1", msg.RoomID)
	assert.Equal(t, "hello", msg.Msg)
}

func TestRepository_GetMessageByID_NotFound(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	msg, err := repo.GetMessageByID(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, msg)
}

func TestRepository_FullRow_AllColumns(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	ts := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	editedAt := ts.Add(5 * time.Minute)
	updatedAt := ts.Add(10 * time.Minute)
	threadParent := ts.Add(-1 * time.Hour)

	sender := models.Participant{ID: "u1", EngName: "Alice", CompanyName: "Acme", AppID: "app1", AppName: "MyApp", IsBot: false, Account: "alice"}
	target := models.Participant{ID: "u2", Account: "bob"}
	mentionUser := models.Participant{ID: "u3", Account: "charlie"}
	reactUser := models.Participant{ID: "u4", Account: "dave"}
	file := models.File{ID: "f1", Name: "doc.pdf", Type: "application/pdf"}
	card := models.Card{Template: "approval", Data: []byte("card-data")}
	cardAction := models.CardAction{Verb: "approve", Text: "Approve", CardID: "c1", DisplayText: "Click", HideExecLog: true, CardTmID: "tm1", Data: []byte("action-data")}
	quotedSender := models.Participant{ID: "u5", Account: "eve"}
	quotedMsg := models.QuotedParentMessage{
		MessageID: "m-quoted", RoomID: "r-full", Sender: quotedSender,
		CreatedAt: ts.Add(-30 * time.Minute), Msg: "original message", MessageLink: "https://chat.example.com/r-full/m-quoted",
	}
	pinnedAt := ts.Add(2 * time.Hour)
	pinnedBy := models.Participant{ID: "u9", Account: "pinner"}

	insertCQL := `INSERT INTO messages_by_id (room_id, created_at, message_id, sender, target_user, msg, mentions, attachments, file, card, card_action, tshow, thread_parent_id, thread_parent_created_at, quoted_parent_message, visible_to, reactions, deleted, type, sys_msg_data, site_id, edited_at, updated_at, thread_room_id, pinned_at, pinned_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertArgs := []any{
		"r-full", ts, "m-full",
		sender, target, "hello world",
		[]models.Participant{mentionUser},
		[][]byte{[]byte("attach1"), []byte("attach2")},
		file, card, cardAction,
		true, "m-parent", threadParent, quotedMsg, "u1",
		map[string][]models.Participant{"thumbsup": {reactUser}},
		true, "user_joined", []byte("sys-data"),
		"site-remote", editedAt, updatedAt,
		"N/A", pinnedAt, pinnedBy,
	}
	require.NoError(t, session.Query(insertCQL, insertArgs...).Exec())

	msg, err := repo.GetMessageByID(ctx, "m-full")
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Primary key fields
	assert.Equal(t, "r-full", msg.RoomID)
	assert.Equal(t, ts.UTC(), msg.CreatedAt.UTC())
	assert.Equal(t, "m-full", msg.MessageID)

	// Sender UDT (all fields)
	assert.Equal(t, "u1", msg.Sender.ID)
	assert.Equal(t, "alice", msg.Sender.Account)
	assert.Equal(t, "Alice", msg.Sender.EngName)
	assert.Equal(t, "Acme", msg.Sender.CompanyName)
	assert.Equal(t, "app1", msg.Sender.AppID)
	assert.Equal(t, "MyApp", msg.Sender.AppName)
	assert.False(t, msg.Sender.IsBot)

	// Target user UDT
	require.NotNil(t, msg.TargetUser)
	assert.Equal(t, "u2", msg.TargetUser.ID)
	assert.Equal(t, "bob", msg.TargetUser.Account)

	// Text
	assert.Equal(t, "hello world", msg.Msg)

	// Mentions (SET<FROZEN<Participant>>)
	require.Len(t, msg.Mentions, 1)
	assert.Equal(t, "u3", msg.Mentions[0].ID)
	assert.Equal(t, "charlie", msg.Mentions[0].Account)

	// Attachments (LIST<BLOB>)
	require.Len(t, msg.Attachments, 2)
	assert.Equal(t, []byte("attach1"), msg.Attachments[0])
	assert.Equal(t, []byte("attach2"), msg.Attachments[1])

	// File UDT
	require.NotNil(t, msg.File)
	assert.Equal(t, "f1", msg.File.ID)
	assert.Equal(t, "doc.pdf", msg.File.Name)
	assert.Equal(t, "application/pdf", msg.File.Type)

	// Card UDT
	require.NotNil(t, msg.Card)
	assert.Equal(t, "approval", msg.Card.Template)
	assert.Equal(t, []byte("card-data"), msg.Card.Data)

	// CardAction UDT
	require.NotNil(t, msg.CardAction)
	assert.Equal(t, "approve", msg.CardAction.Verb)
	assert.Equal(t, "Approve", msg.CardAction.Text)
	assert.Equal(t, "c1", msg.CardAction.CardID)
	assert.Equal(t, "Click", msg.CardAction.DisplayText)
	assert.True(t, msg.CardAction.HideExecLog)
	assert.Equal(t, "tm1", msg.CardAction.CardTmID)
	assert.Equal(t, []byte("action-data"), msg.CardAction.Data)

	// Boolean/string fields
	assert.True(t, msg.TShow)
	assert.Equal(t, "m-parent", msg.ThreadParentID)
	require.NotNil(t, msg.ThreadParentCreatedAt)
	assert.Equal(t, threadParent.UTC(), msg.ThreadParentCreatedAt.UTC())

	// QuotedParentMessage UDT
	require.NotNil(t, msg.QuotedParentMessage)
	assert.Equal(t, "m-quoted", msg.QuotedParentMessage.MessageID)
	assert.Equal(t, "r-full", msg.QuotedParentMessage.RoomID)
	assert.Equal(t, "u5", msg.QuotedParentMessage.Sender.ID)
	assert.Equal(t, "eve", msg.QuotedParentMessage.Sender.Account)
	assert.Equal(t, "original message", msg.QuotedParentMessage.Msg)
	assert.Equal(t, "https://chat.example.com/r-full/m-quoted", msg.QuotedParentMessage.MessageLink)

	assert.Equal(t, "u1", msg.VisibleTo)
	assert.True(t, msg.Deleted)
	assert.Equal(t, "user_joined", msg.Type)
	assert.Equal(t, []byte("sys-data"), msg.SysMsgData)
	assert.Equal(t, "site-remote", msg.SiteID)

	// Timestamps
	require.NotNil(t, msg.EditedAt)
	assert.Equal(t, editedAt.UTC(), msg.EditedAt.UTC())
	require.NotNil(t, msg.UpdatedAt)
	assert.Equal(t, updatedAt.UTC(), msg.UpdatedAt.UTC())

	// Reactions (MAP<TEXT, FROZEN<SET<FROZEN<Participant>>>>)
	require.Contains(t, msg.Reactions, "thumbsup")
	require.Len(t, msg.Reactions["thumbsup"], 1)
	assert.Equal(t, "u4", msg.Reactions["thumbsup"][0].ID)
	assert.Equal(t, "dave", msg.Reactions["thumbsup"][0].Account)

	// messages_by_id extra columns
	assert.Equal(t, "N/A", msg.ThreadRoomID)
	require.NotNil(t, msg.PinnedAt)
	assert.Equal(t, pinnedAt.UTC(), msg.PinnedAt.UTC())
	require.NotNil(t, msg.PinnedBy)
	assert.Equal(t, "u9", msg.PinnedBy.ID)
	assert.Equal(t, "pinner", msg.PinnedBy.Account)
}

func TestRepository_GetMessagesByIDs(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	ts1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg) VALUES (?, ?, ?, ?, ?)`,
		"m-batch-1", "r1", ts1, sender, "hello",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg) VALUES (?, ?, ?, ?, ?)`,
		"m-batch-2", "r1", ts2, sender, "world",
	).Exec())

	msgs, err := repo.GetMessagesByIDs(ctx, []string{"m-batch-1", "m-batch-2"})
	require.NoError(t, err)
	assert.Len(t, msgs, 2)
	ids := []string{msgs[0].MessageID, msgs[1].MessageID}
	assert.ElementsMatch(t, []string{"m-batch-1", "m-batch-2"}, ids)
}

func TestRepository_GetMessagesByIDs_Empty(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	msgs, err := repo.GetMessagesByIDs(ctx, []string{})
	require.NoError(t, err)
	assert.Empty(t, msgs)
}

func TestRepository_GetMessagesByIDs_MissingID(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg) VALUES (?, ?, ?, ?, ?)`,
		"m-exists", "r1", ts, sender, "hi",
	).Exec())

	msgs, err := repo.GetMessagesByIDs(ctx, []string{"m-exists", "m-missing"})
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Equal(t, "m-exists", msgs[0].MessageID)
}
