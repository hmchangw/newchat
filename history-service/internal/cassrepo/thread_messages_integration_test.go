//go:build integration

package cassrepo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

func seedThreadMessages(t *testing.T, session *gocql.Session, roomID, threadRoomID, parentID string, base time.Time, count int) {
	t.Helper()
	sizer := msgbucket.New(24 * time.Hour)
	sender := models.Participant{ID: "u1", Account: "user1"}
	for i := 0; i < count; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		bucket := sizer.Of(ts)
		err := session.Query(
			`INSERT INTO thread_messages_by_room (room_id, bucket, thread_room_id, created_at, message_id, thread_parent_id, sender, msg) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			roomID, bucket, threadRoomID, ts, fmt.Sprintf("%s-reply-%d", threadRoomID, i), parentID, sender, fmt.Sprintf("reply-%d", i),
		).Exec()
		require.NoError(t, err)
	}
}

func seedThreadMessage(t *testing.T, session *gocql.Session, roomID, threadRoomID, messageID string, createdAt time.Time) {
	t.Helper()
	sizer := msgbucket.New(24 * time.Hour)
	bucket := sizer.Of(createdAt)
	err := session.Query(
		`INSERT INTO thread_messages_by_room (room_id, bucket, thread_room_id, created_at, message_id, sender, msg, site_id, updated_at, type)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucket, threadRoomID, createdAt, messageID,
		map[string]interface{}{"id": "u1", "account": "alice"},
		"m", "site-A", createdAt, "text",
	).Exec()
	require.NoError(t, err)
}

func TestRepository_GetThreadMessages_IsolatesByThreadRoomID(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	seedThreadMessages(t, session, "r-A", "tr-1", "m-parent-1", base, 3)
	seedThreadMessages(t, session, "r-A", "tr-2", "m-parent-2", base, 5)

	q, err := ParsePageRequest("", 100)
	require.NoError(t, err)

	page1, err := repo.GetThreadMessages(ctx, "r-A", "tr-1", base.Add(time.Hour), base.AddDate(0, 0, -1), q)
	require.NoError(t, err)
	assert.Len(t, page1.Data, 3)
	for _, m := range page1.Data {
		assert.Equal(t, "tr-1", m.ThreadRoomID)
		assert.Equal(t, "m-parent-1", m.ThreadParentID)
	}

	page2, err := repo.GetThreadMessages(ctx, "r-A", "tr-2", base.Add(time.Hour), base.AddDate(0, 0, -1), q)
	require.NoError(t, err)
	assert.Len(t, page2.Data, 5)
	for _, m := range page2.Data {
		assert.Equal(t, "tr-2", m.ThreadRoomID)
	}
}

func TestRepository_GetThreadMessages_IsolatesByRoomID(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	seedThreadMessages(t, session, "r-A", "tr-1", "m-parent-A", base, 2)
	seedThreadMessages(t, session, "r-B", "tr-1", "m-parent-B", base, 4)

	q, err := ParsePageRequest("", 100)
	require.NoError(t, err)

	pageA, err := repo.GetThreadMessages(ctx, "r-A", "tr-1", base.Add(time.Hour), base.AddDate(0, 0, -1), q)
	require.NoError(t, err)
	assert.Len(t, pageA.Data, 2)
	for _, m := range pageA.Data {
		assert.Equal(t, "r-A", m.RoomID)
	}

	pageB, err := repo.GetThreadMessages(ctx, "r-B", "tr-1", base.Add(time.Hour), base.AddDate(0, 0, -1), q)
	require.NoError(t, err)
	assert.Len(t, pageB.Data, 4)
	for _, m := range pageB.Data {
		assert.Equal(t, "r-B", m.RoomID)
	}
}

func TestRepository_GetThreadMessages_OrdersDescByCreatedAt(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	seedThreadMessages(t, session, "r-A", "tr-1", "m-parent-1", base, 4)

	q, err := ParsePageRequest("", 100)
	require.NoError(t, err)

	page, err := repo.GetThreadMessages(ctx, "r-A", "tr-1", base.Add(time.Hour), base.AddDate(0, 0, -1), q)
	require.NoError(t, err)
	require.Len(t, page.Data, 4)
	for i := 0; i < len(page.Data)-1; i++ {
		assert.True(t, page.Data[i].CreatedAt.After(page.Data[i+1].CreatedAt),
			"expected DESC order at index %d", i)
	}
}

func TestRepository_GetThreadMessages_Pagination(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	seedThreadMessages(t, session, "r-A", "tr-1", "m-parent-1", base, 7)

	q, err := ParsePageRequest("", 3)
	require.NoError(t, err)

	page1, err := repo.GetThreadMessages(ctx, "r-A", "tr-1", base.Add(time.Hour), base.AddDate(0, 0, -1), q)
	require.NoError(t, err)
	assert.Len(t, page1.Data, 3)
	assert.True(t, page1.HasNext)
	require.NotEmpty(t, page1.NextCursor)

	q2, err := ParsePageRequest(page1.NextCursor, 3)
	require.NoError(t, err)
	page2, err := repo.GetThreadMessages(ctx, "r-A", "tr-1", base.Add(time.Hour), base.AddDate(0, 0, -1), q2)
	require.NoError(t, err)
	assert.Len(t, page2.Data, 3)

	q3, err := ParsePageRequest(page2.NextCursor, 3)
	require.NoError(t, err)
	page3, err := repo.GetThreadMessages(ctx, "r-A", "tr-1", base.Add(time.Hour), base.AddDate(0, 0, -1), q3)
	require.NoError(t, err)
	assert.Len(t, page3.Data, 1)
	assert.False(t, page3.HasNext)

	// No overlap, no gaps.
	seen := map[string]bool{}
	for _, m := range page1.Data {
		seen[m.MessageID] = true
	}
	for _, m := range page2.Data {
		assert.False(t, seen[m.MessageID], "page2 overlaps page1: %s", m.MessageID)
		seen[m.MessageID] = true
	}
	for _, m := range page3.Data {
		assert.False(t, seen[m.MessageID], "page3 overlaps earlier pages: %s", m.MessageID)
		seen[m.MessageID] = true
	}
	assert.Len(t, seen, 7)
}

func TestRepository_GetThreadMessages_EmptyWhenThreadUnknown(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetThreadMessages(ctx, "r-A", "tr-nonexistent", time.Now().UTC().Add(time.Hour), time.Now().UTC().AddDate(0, 0, -1), q)
	require.NoError(t, err)
	assert.Empty(t, page.Data)
	assert.False(t, page.HasNext)
	assert.Empty(t, page.NextCursor)
}

func TestRepository_GetThreadMessages_ColumnScan(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	ts := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	editedAt := ts.Add(5 * time.Minute)
	updatedAt := ts.Add(10 * time.Minute)

	sizer := msgbucket.New(24 * time.Hour)
	bucket := sizer.Of(ts)

	sender := models.Participant{ID: "u1", EngName: "Alice", CompanyName: "Acme", AppID: "app1", AppName: "MyApp", IsBot: false, Account: "alice"}
	target := models.Participant{ID: "u2", Account: "bob"}
	mentionUser := models.Participant{ID: "u3", Account: "charlie"}
	reactUser := models.Participant{ID: "u4", Account: "dave"}
	file := models.File{ID: "f1", Name: "doc.pdf", Type: "application/pdf"}
	card := models.Card{Template: "approval", Data: []byte("card-data")}
	cardAction := models.CardAction{Verb: "approve", Text: "Approve", CardID: "c1", DisplayText: "Click", HideExecLog: true, CardTmID: "tm1", Data: []byte("action-data")}
	quotedSender := models.Participant{ID: "u5", Account: "eve"}
	quotedMsg := models.QuotedParentMessage{
		MessageID: "m-quoted", RoomID: "r-thread-full", Sender: quotedSender,
		CreatedAt: ts.Add(-30 * time.Minute), Msg: "original message", MessageLink: "https://chat.example.com/r-thread-full/m-quoted",
	}

	insertCQL := `INSERT INTO thread_messages_by_room (
        room_id, bucket, thread_room_id, created_at, message_id, thread_parent_id,
        sender, target_user, msg, mentions, attachments, file, card, card_action,
        quoted_parent_message, visible_to, reactions, deleted,
        type, sys_msg_data, site_id, edited_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertArgs := []any{
		"r-thread-full", bucket, "tr-full", ts, "m-reply-full", "m-thread-parent",
		sender, target, "thread reply body",
		[]models.Participant{mentionUser},
		[][]byte{[]byte("attach1"), []byte("attach2")},
		file, card, cardAction,
		quotedMsg, "u1",
		map[string][]models.Participant{"thumbsup": {reactUser}},
		true, "user_joined", []byte("sys-data"),
		"site-remote", editedAt, updatedAt,
	}
	require.NoError(t, session.Query(insertCQL, insertArgs...).Exec())

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetThreadMessages(ctx, "r-thread-full", "tr-full", ts.Add(time.Hour), ts.AddDate(0, 0, -1), q)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	msg := page.Data[0]

	// Primary key + thread linkage
	assert.Equal(t, "r-thread-full", msg.RoomID)
	assert.Equal(t, "tr-full", msg.ThreadRoomID)
	assert.Equal(t, ts.UTC(), msg.CreatedAt.UTC())
	assert.Equal(t, "m-reply-full", msg.MessageID)
	assert.Equal(t, "m-thread-parent", msg.ThreadParentID)

	// Sender UDT
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
	assert.Equal(t, "thread reply body", msg.Msg)

	// Mentions
	require.Len(t, msg.Mentions, 1)
	assert.Equal(t, "u3", msg.Mentions[0].ID)
	assert.Equal(t, "charlie", msg.Mentions[0].Account)

	// Attachments
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

	// QuotedParentMessage UDT
	require.NotNil(t, msg.QuotedParentMessage)
	assert.Equal(t, "m-quoted", msg.QuotedParentMessage.MessageID)
	assert.Equal(t, "r-thread-full", msg.QuotedParentMessage.RoomID)
	assert.Equal(t, "u5", msg.QuotedParentMessage.Sender.ID)
	assert.Equal(t, "eve", msg.QuotedParentMessage.Sender.Account)
	assert.Equal(t, "original message", msg.QuotedParentMessage.Msg)
	assert.Equal(t, "https://chat.example.com/r-thread-full/m-quoted", msg.QuotedParentMessage.MessageLink)

	// Scalars
	assert.Equal(t, "u1", msg.VisibleTo)
	assert.True(t, msg.Deleted)
	assert.Equal(t, "user_joined", msg.Type)
	assert.Equal(t, []byte("sys-data"), msg.SysMsgData)
	assert.Equal(t, "site-remote", msg.SiteID)

	// Reactions (MAP<TEXT, FROZEN<SET<FROZEN<Participant>>>>)
	require.Contains(t, msg.Reactions, "thumbsup")
	require.Len(t, msg.Reactions["thumbsup"], 1)
	assert.Equal(t, "u4", msg.Reactions["thumbsup"][0].ID)
	assert.Equal(t, "dave", msg.Reactions["thumbsup"][0].Account)

	// Timestamps
	require.NotNil(t, msg.EditedAt)
	assert.Equal(t, editedAt.UTC(), msg.EditedAt.UTC())
	require.NotNil(t, msg.UpdatedAt)
	assert.Equal(t, updatedAt.UTC(), msg.UpdatedAt.UTC())

	// Columns that DON'T exist on thread_messages_by_room must remain at zero value.
	assert.False(t, msg.TShow)
	assert.Nil(t, msg.ThreadParentCreatedAt)
	assert.Nil(t, msg.PinnedAt)
	assert.Nil(t, msg.PinnedBy)
}

func TestGetThreadMessages_CrossBucketWalk(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	roomID := "room-thread-walk"
	threadRoomID := "thread-1"
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		seedThreadMessage(t, session, roomID, threadRoomID, fmt.Sprintf("t%d", i), base.AddDate(0, 0, -i))
	}
	floor := base.AddDate(0, 0, -10)

	page, err := repo.GetThreadMessages(ctx, roomID, threadRoomID, base.Add(time.Second), floor, PageRequest{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, page.Data, 4)
	assert.Equal(t, "t0", page.Data[0].MessageID)
	assert.False(t, page.HasNext)
}
