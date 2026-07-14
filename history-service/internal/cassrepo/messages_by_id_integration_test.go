//go:build integration

package cassrepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	cassmodels "github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestRepository_GetMessageByID(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	msg, err := repo.GetMessageByID(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, msg)
}

func TestRepository_FullRow_AllColumns(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	ts := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	editedAt := ts.Add(5 * time.Minute)
	updatedAt := ts.Add(10 * time.Minute)
	threadParent := ts.Add(-1 * time.Hour)

	sender := models.Participant{ID: "u1", EngName: "Alice", CompanyName: "Acme", AppID: "app1", AppName: "MyApp", IsBot: false, Account: "alice"}
	mentionUser := models.Participant{ID: "u3", Account: "charlie"}
	card := models.Card{Template: "approval", Data: []byte("card-data")}
	cardAction := models.CardAction{Verb: "approve", Text: "Approve", CardID: "c1", DisplayText: "Click", HideExecLog: true, CardTmID: "tm1", Data: []byte("action-data")}
	quotedSender := models.Participant{ID: "u5", Account: "eve"}
	quotedMsg := models.QuotedParentMessage{
		MessageID: "m-quoted", RoomID: "r-full", Sender: quotedSender,
		CreatedAt: ts.Add(-30 * time.Minute), Msg: "original message", MessageLink: "https://chat.example.com/r-full/m-quoted",
	}
	pinnedAt := ts.Add(2 * time.Hour)
	pinnedBy := models.Participant{ID: "u9", Account: "pinner"}
	reactedAt := ts.Add(15 * time.Minute).Truncate(time.Millisecond)
	reactions := map[cassmodels.ReactionKey]cassmodels.ReactorInfo{
		{Emoji: "👍", UserAccount: "dave"}: {UserID: "u4", EngName: "Dave", Account: "dave", ReactedAt: reactedAt},
	}

	insertCQL := `INSERT INTO messages_by_id (room_id, created_at, message_id, sender, msg, mentions, attachments, card, card_action, tshow, thread_parent_id, thread_parent_created_at, quoted_parent_message, visible_to, reactions, deleted, type, sys_msg_data, site_id, edited_at, updated_at, thread_room_id, pinned_at, pinned_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertArgs := []any{
		"r-full", ts, "m-full",
		sender, "hello world",
		[]models.Participant{mentionUser},
		[][]byte{[]byte("attach1"), []byte("attach2")},
		card, cardAction,
		true, "m-parent", threadParent, quotedMsg, "u1",
		reactions,
		true, "user_joined", []byte("sys-data"),
		"site-remote", editedAt, updatedAt,
		"N/A", pinnedAt, pinnedBy,
	}
	require.NoError(t, session.Query(insertCQL, insertArgs...).Exec())

	msg, err := repo.GetMessageByID(ctx, "m-full")
	require.NoError(t, err)
	require.NotNil(t, msg)

	assert.Equal(t, "r-full", msg.RoomID)
	assert.Equal(t, ts.UTC(), msg.CreatedAt.UTC())
	assert.Equal(t, "m-full", msg.MessageID)

	assert.Equal(t, "u1", msg.Sender.ID)
	assert.Equal(t, "alice", msg.Sender.Account)
	assert.Equal(t, "Alice", msg.Sender.EngName)
	assert.Equal(t, "Acme", msg.Sender.CompanyName)
	assert.Equal(t, "app1", msg.Sender.AppID)
	assert.Equal(t, "MyApp", msg.Sender.AppName)
	assert.False(t, msg.Sender.IsBot)

	assert.Equal(t, "hello world", msg.Msg)

	require.Len(t, msg.Mentions, 1)
	assert.Equal(t, "u3", msg.Mentions[0].ID)
	assert.Equal(t, "charlie", msg.Mentions[0].Account)

	require.Len(t, msg.Attachments, 2)
	assert.Equal(t, []byte("attach1"), msg.Attachments[0])
	assert.Equal(t, []byte("attach2"), msg.Attachments[1])

	require.NotNil(t, msg.Card)
	assert.Equal(t, "approval", msg.Card.Template)
	assert.Equal(t, []byte("card-data"), msg.Card.Data)

	require.NotNil(t, msg.CardAction)
	assert.Equal(t, "approve", msg.CardAction.Verb)
	assert.Equal(t, "Approve", msg.CardAction.Text)
	assert.Equal(t, "c1", msg.CardAction.CardID)
	assert.Equal(t, "Click", msg.CardAction.DisplayText)
	assert.True(t, msg.CardAction.HideExecLog)
	assert.Equal(t, "tm1", msg.CardAction.CardTmID)
	assert.Equal(t, []byte("action-data"), msg.CardAction.Data)

	assert.True(t, msg.TShow)
	assert.Equal(t, "m-parent", msg.ThreadParentID)
	require.NotNil(t, msg.ThreadParentCreatedAt)
	assert.Equal(t, threadParent.UTC(), msg.ThreadParentCreatedAt.UTC())

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

	require.NotNil(t, msg.EditedAt)
	assert.Equal(t, editedAt.UTC(), msg.EditedAt.UTC())
	require.NotNil(t, msg.UpdatedAt)
	assert.Equal(t, updatedAt.UTC(), msg.UpdatedAt.UTC())

	require.Len(t, msg.Reactions, 1)
	gotReaction, ok := msg.Reactions[cassmodels.ReactionKey{Emoji: "👍", UserAccount: "dave"}]
	require.True(t, ok)
	assert.Equal(t, "u4", gotReaction.UserID)
	assert.Equal(t, "Dave", gotReaction.EngName)
	assert.Equal(t, "dave", gotReaction.Account)
	assert.Equal(t, reactedAt, gotReaction.ReactedAt.UTC())

	assert.Equal(t, "N/A", msg.ThreadRoomID)
	require.NotNil(t, msg.PinnedAt)
	assert.Equal(t, pinnedAt.UTC(), msg.PinnedAt.UTC())
	require.NotNil(t, msg.PinnedBy)
	assert.Equal(t, "u9", msg.PinnedBy.ID)
	assert.Equal(t, "pinner", msg.PinnedBy.Account)
}

func TestRepository_GetMessagesByIDs(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	msgs, err := repo.GetMessagesByIDs(ctx, []string{})
	require.NoError(t, err)
	assert.Empty(t, msgs)
}

func TestRepository_GetMessagesByIDs_MissingID(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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

// TestRepository_GetMessageByID_ReactionsRoundTrip verifies the MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>
// column round-trips correctly via structScan's positional-Scan path (MapScan replacement).
func TestRepository_GetMessageByID_ReactionsRoundTrip(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	ts := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

	// Cassandra TIMESTAMP precision is milliseconds — truncate so byte comparison
	// after read-back is exact.
	reactedAt := time.Now().UTC().Truncate(time.Millisecond)

	wantReactions := cassmodels.Reactions{
		cassmodels.ReactionKey{Emoji: "👍", UserAccount: "alice"}: cassmodels.ReactorInfo{
			UserID: "u1", EngName: "Alice", ChnName: "爱丽丝", Account: "alice", ReactedAt: reactedAt,
		},
		cassmodels.ReactionKey{Emoji: "❤️", UserAccount: "bob"}: cassmodels.ReactorInfo{
			UserID: "u2", EngName: "Bob", ChnName: "鲍勃", Account: "bob", ReactedAt: reactedAt,
		},
	}

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, reactions) VALUES (?, ?, ?, ?, ?, ?)`,
		"m-reactions", "r-reactions", ts, sender, "hello", map[cassmodels.ReactionKey]cassmodels.ReactorInfo(wantReactions),
	).Exec())

	msg, err := repo.GetMessageByID(ctx, "m-reactions")
	require.NoError(t, err)
	require.NotNil(t, msg)

	assert.Equal(t, "m-reactions", msg.MessageID)
	require.Len(t, msg.Reactions, 2, "expected both reactions to be returned")

	for k, want := range wantReactions {
		got, ok := msg.Reactions[k]
		require.True(t, ok, "missing reaction key %+v", k)
		assert.Equal(t, want.UserID, got.UserID)
		assert.Equal(t, want.EngName, got.EngName)
		assert.Equal(t, want.ChnName, got.ChnName)
		assert.Equal(t, want.Account, got.Account)
		assert.Equal(t, want.ReactedAt.UTC(), got.ReactedAt.UTC())
	}
}
