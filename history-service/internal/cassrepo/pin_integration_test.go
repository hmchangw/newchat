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

func seedMessageFull(t *testing.T, repo *Repository, m *models.Message) {
	t.Helper()
	b := msgbucket.New(24 * time.Hour).Of(m.CreatedAt)
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?)`,
		m.MessageID, m.RoomID, m.CreatedAt, m.Sender, m.Msg, false,
	).WithContext(context.Background()).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		m.RoomID, b, m.CreatedAt, m.MessageID, m.Sender, m.Msg, false,
	).WithContext(context.Background()).Exec())
}

func TestRepository_PinAndUnpinMessage(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	created := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	msg := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: created,
		Sender: models.Participant{ID: "u1", Account: "alice"}, Msg: "hello",
	}
	seedMessageFull(t, repo, msg)

	pinnedAt := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	pinnedBy := models.Participant{ID: "u2", Account: "mod"}
	require.NoError(t, repo.PinMessage(ctx, msg, pinnedAt, pinnedBy))

	got, err := repo.GetMessageByID(ctx, "m1")
	require.NoError(t, err)
	require.NotNil(t, got.PinnedAt)
	assert.True(t, got.PinnedAt.Equal(pinnedAt), "pinned_at must equal pinnedAt")
	require.NotNil(t, got.PinnedBy)
	assert.Equal(t, "mod", got.PinnedBy.Account)

	pinned, err := repo.GetAllPinnedMessages(ctx, "r1")
	require.NoError(t, err)
	require.Len(t, pinned, 1)
	assert.Equal(t, "m1", pinned[0].MessageID)
	require.NotNil(t, pinned[0].PinnedAt)
	assert.True(t, pinned[0].PinnedAt.Equal(pinnedAt))
	require.NotNil(t, pinned[0].PinnedBy)
	assert.Equal(t, "mod", pinned[0].PinnedBy.Account)

	got.PinnedAt = &pinnedAt
	require.NoError(t, repo.UnpinMessage(ctx, got))

	after, err := repo.GetMessageByID(ctx, "m1")
	require.NoError(t, err)
	assert.Nil(t, after.PinnedAt)
	assert.Nil(t, after.PinnedBy)

	emptyAfter, err := repo.GetAllPinnedMessages(ctx, "r1")
	require.NoError(t, err)
	assert.Empty(t, emptyAfter)
}

func TestRepository_GetPinnedMessages_OrderAndEmpty(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	empty, err := repo.GetAllPinnedMessages(ctx, "empty-room")
	require.NoError(t, err)
	assert.Empty(t, empty)

	base := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	for i, id := range []string{"a", "b", "c"} {
		m := &models.Message{
			MessageID: id, RoomID: "r2", CreatedAt: base,
			Sender: models.Participant{ID: "u1", Account: "alice"}, Msg: id,
		}
		seedMessageFull(t, repo, m)
		require.NoError(t, repo.PinMessage(ctx, m,
			base.Add(time.Duration(i)*time.Hour),
			models.Participant{ID: "u1", Account: "alice"}))
	}

	pinned, err := repo.GetAllPinnedMessages(ctx, "r2")
	require.NoError(t, err)
	require.Len(t, pinned, 3)
	assert.Equal(t, "c", pinned[0].MessageID)
	assert.Equal(t, "a", pinned[2].MessageID)
}

func TestRepository_GetPinnedMessages_BindsBothTimestamps(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	created := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	pinnedAt := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	sender := models.Participant{ID: "u1", Account: "alice"}
	msg := &models.Message{MessageID: "m1", RoomID: "r3", CreatedAt: created, Sender: sender, Msg: "hello"}
	seedMessageFull(t, repo, msg)
	require.NoError(t, repo.PinMessage(ctx, msg, pinnedAt, sender))

	pinned, err := repo.GetAllPinnedMessages(ctx, "r3")
	require.NoError(t, err)
	require.Len(t, pinned, 1)
	assert.True(t, pinned[0].CreatedAt.Equal(created), "CreatedAt must hold the message's true creation time, not the pin time")
	require.NotNil(t, pinned[0].PinnedAt)
	assert.True(t, pinned[0].PinnedAt.Equal(pinnedAt), "PinnedAt must hold the pin time")
}

func TestRepository_GetPinnedMessages_Paginates(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	// Seed 5 pins; ask for page size 2 → expect three pages: 2, 2, 1.
	base := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	sender := models.Participant{ID: "u1", Account: "alice"}
	for i, id := range []string{"a", "b", "c", "d", "e"} {
		m := &models.Message{MessageID: id, RoomID: "r4", CreatedAt: base, Sender: sender, Msg: id}
		seedMessageFull(t, repo, m)
		require.NoError(t, repo.PinMessage(ctx, m, base.Add(time.Duration(i)*time.Hour), sender))
	}

	var (
		seen    []string
		cursor  *Cursor
		pages   int
		hasNext = true
	)
	for hasNext {
		pages++
		require.LessOrEqual(t, pages, 5, "infinite-loop guard")
		page, err := repo.GetPinnedMessages(ctx, "r4", PageRequest{Cursor: cursor, PageSize: 2})
		require.NoError(t, err)
		for _, m := range page.Data {
			seen = append(seen, m.MessageID)
		}
		hasNext = page.HasNext
		if hasNext {
			c, err := NewCursor(page.NextCursor)
			require.NoError(t, err)
			cursor = c
		}
	}
	assert.Len(t, seen, 5, "every pin returned exactly once across pages")
	assert.ElementsMatch(t, []string{"a", "b", "c", "d", "e"}, seen)
}
