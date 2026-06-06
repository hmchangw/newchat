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

// reactionFixture builds a repo + bucket sizer + canonical alice key/reactor.
func reactionFixture(t *testing.T) (*Repository, msgbucket.Sizer, time.Time, models.ReactionKey, models.ReactorInfo) {
	t.Helper()
	session := setupCassandra(t)
	bucketSizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, bucketSizer, 365, nil)
	createdAt := time.Now().UTC().Truncate(time.Millisecond)
	reactedAt := createdAt.Add(time.Minute).Truncate(time.Millisecond)
	key := models.ReactionKey{Emoji: "👍", UserAccount: "alice"}
	reactor := models.ReactorInfo{
		UserID:    "u-alice",
		EngName:   "Alice",
		ChnName:   "爱丽丝",
		Account:   "alice",
		ReactedAt: reactedAt,
	}
	return repo, bucketSizer, createdAt, key, reactor
}

func TestRepository_AddReaction_TopLevel(t *testing.T) {
	repo, bucketSizer, createdAt, key, reactor := reactionFixture(t)
	ctx := context.Background()

	sender := models.Participant{ID: "u-bob", Account: "bob"}
	roomID := "room-react-top"
	msgID := "m-react-top"

	// Seed a top-level message into messages_by_id and messages_by_room.
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "hello", "",
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucketSizer.Of(createdAt), createdAt, msgID, sender, "hello", "",
	).Exec())

	msg := &models.Message{
		MessageID: msgID,
		RoomID:    roomID,
		CreatedAt: createdAt,
		Sender:    sender,
	}
	require.NoError(t, repo.AddReaction(ctx, msg, key, reactor))

	// messages_by_id has the reaction.
	var fromByID models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&fromByID))
	require.Contains(t, fromByID, key)
	assert.Equal(t, reactor.Account, fromByID[key].Account)
	assert.Equal(t, reactor.EngName, fromByID[key].EngName)

	// messages_by_room has the reaction too.
	var fromByRoom models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, bucketSizer.Of(createdAt), createdAt, msgID,
	).Scan(&fromByRoom))
	require.Contains(t, fromByRoom, key)
}

func TestRepository_AddReaction_ThreadReply(t *testing.T) {
	repo, _, createdAt, key, reactor := reactionFixture(t)
	ctx := context.Background()

	sender := models.Participant{ID: "u-bob", Account: "bob"}
	roomID := "room-thread-react"
	msgID := "m-thread-reply"
	threadRoomID := "thread-room-1"
	threadParentID := "thread-parent-1"

	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_room_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "reply", threadParentID, threadRoomID,
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		threadRoomID, createdAt, msgID, sender, "reply", threadParentID,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		Sender:         sender,
		ThreadParentID: threadParentID,
		ThreadRoomID:   threadRoomID,
	}
	require.NoError(t, repo.AddReaction(ctx, msg, key, reactor))

	// messages_by_id has the reaction.
	var fromByID models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&fromByID))
	require.Contains(t, fromByID, key)

	// thread_messages_by_thread has the reaction.
	var fromThread models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
		threadRoomID, createdAt, msgID,
	).Scan(&fromThread))
	require.Contains(t, fromThread, key)

	// A thread reply must NOT also appear in messages_by_room — the writer
	// routes mutually exclusively. We don't assert absence here because the
	// row was never seeded.
}

// Pinned messages: reactions are NOT mirrored to pinned_messages_by_room.
// The pinned panel does not render reactions, so writing them there is dead
// work. Add must succeed (touching messages_by_id and messages_by_room) and
// leave the pinned row untouched.
func TestRepository_AddReaction_Pinned(t *testing.T) {
	repo, bucketSizer, createdAt, key, reactor := reactionFixture(t)
	ctx := context.Background()

	sender := models.Participant{ID: "u-bob", Account: "bob"}
	roomID := "room-pin-react"
	msgID := "m-pin-react"
	pinnedAt := createdAt.Add(2 * time.Hour).Truncate(time.Millisecond)

	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, pinned_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "pinned msg", "", pinnedAt,
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, pinned_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucketSizer.Of(createdAt), createdAt, msgID, sender, "pinned msg", "", pinnedAt,
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO pinned_messages_by_room (room_id, created_at, message_id, sender, msg) VALUES (?, ?, ?, ?, ?)`,
		roomID, pinnedAt, msgID, sender, "pinned msg",
	).Exec())

	msg := &models.Message{
		MessageID: msgID,
		RoomID:    roomID,
		CreatedAt: createdAt,
		Sender:    sender,
		PinnedAt:  &pinnedAt,
	}
	require.NoError(t, repo.AddReaction(ctx, msg, key, reactor))

	// messages_by_id has the reaction (source of truth).
	var primaryReactions models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&primaryReactions))
	require.Contains(t, primaryReactions, key)

	// pinned_messages_by_room.updated_at must NOT have been touched by the
	// reaction write; the pinned row's updated_at stays at its insertion
	// default (no UPDATE issued by AddReaction).
	var pinnedUpdatedAt time.Time
	err := repo.session.Query(
		`SELECT updated_at FROM pinned_messages_by_room WHERE room_id = ? AND created_at = ? AND message_id = ?`,
		roomID, pinnedAt, msgID,
	).Scan(&pinnedUpdatedAt)
	require.NoError(t, err)
	assert.True(t, pinnedUpdatedAt.IsZero() || pinnedUpdatedAt.Before(createdAt),
		"pinned_messages_by_room.updated_at must not be bumped by AddReaction")
}

func TestRepository_AddReaction_Idempotent(t *testing.T) {
	repo, _, createdAt, key, reactor := reactionFixture(t)
	ctx := context.Background()

	sender := models.Participant{ID: "u-bob", Account: "bob"}
	roomID := "room-idem"
	msgID := "m-idem"

	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "hello", "",
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "hello", "",
	).Exec())

	msg := &models.Message{MessageID: msgID, RoomID: roomID, CreatedAt: createdAt, Sender: sender}

	// First add.
	require.NoError(t, repo.AddReaction(ctx, msg, key, reactor))

	// Same key + new reactor info (later reactedAt) — must overwrite, not duplicate.
	updatedReactor := reactor
	updatedReactor.ReactedAt = reactor.ReactedAt.Add(5 * time.Minute)
	updatedReactor.EngName = "Alice (updated)"
	require.NoError(t, repo.AddReaction(ctx, msg, key, updatedReactor))

	var got models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&got))
	require.Len(t, got, 1, "same key must not create a second map entry")
	assert.Equal(t, "Alice (updated)", got[key].EngName, "overwrite should replace ReactorInfo")
}

func TestRepository_RemoveReaction_TopLevel(t *testing.T) {
	repo, bucketSizer, createdAt, key, reactor := reactionFixture(t)
	ctx := context.Background()

	sender := models.Participant{ID: "u-bob", Account: "bob"}
	roomID := "room-remove"
	msgID := "m-remove"

	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "hello", "",
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucketSizer.Of(createdAt), createdAt, msgID, sender, "hello", "",
	).Exec())

	msg := &models.Message{MessageID: msgID, RoomID: roomID, CreatedAt: createdAt, Sender: sender}

	// Add at one time, remove at a strictly later time so we can prove
	// updated_at is bumped by the remove (not stuck at the add's value).
	addedAt := reactor.ReactedAt
	removedAt := addedAt.Add(5 * time.Minute).Truncate(time.Millisecond)
	require.NoError(t, repo.AddReaction(ctx, msg, key, reactor))
	require.NoError(t, repo.RemoveReaction(ctx, msg, key, removedAt))

	var got models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&got))
	assert.NotContains(t, got, key, "removed cell must be gone from messages_by_id")

	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, bucketSizer.Of(createdAt), createdAt, msgID,
	).Scan(&got))
	assert.NotContains(t, got, key, "removed cell must be gone from messages_by_room")

	// Verify updated_at was bumped on Remove (regression guard for the
	// '_ = updatedAt' bug where RemoveReaction silently discarded the
	// timestamp and left updated_at frozen at the add time).
	var gotUpdatedAt time.Time
	require.NoError(t, repo.session.Query(
		`SELECT updated_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotUpdatedAt))
	assert.WithinDuration(t, removedAt, gotUpdatedAt, time.Second,
		"messages_by_id.updated_at must reflect the remove time")

	require.NoError(t, repo.session.Query(
		`SELECT updated_at FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, bucketSizer.Of(createdAt), createdAt, msgID,
	).Scan(&gotUpdatedAt))
	assert.WithinDuration(t, removedAt, gotUpdatedAt, time.Second,
		"messages_by_room.updated_at must reflect the remove time")
}

// Pinned messages: Remove must succeed without touching pinned_messages_by_room.
func TestRepository_RemoveReaction_Pinned(t *testing.T) {
	repo, bucketSizer, createdAt, key, reactor := reactionFixture(t)
	ctx := context.Background()

	sender := models.Participant{ID: "u-bob", Account: "bob"}
	roomID := "room-pin-remove"
	msgID := "m-pin-remove"
	pinnedAt := createdAt.Add(2 * time.Hour).Truncate(time.Millisecond)

	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, pinned_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "pinned msg", "", pinnedAt,
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, pinned_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucketSizer.Of(createdAt), createdAt, msgID, sender, "pinned msg", "", pinnedAt,
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO pinned_messages_by_room (room_id, created_at, message_id, sender, msg) VALUES (?, ?, ?, ?, ?)`,
		roomID, pinnedAt, msgID, sender, "pinned msg",
	).Exec())

	msg := &models.Message{
		MessageID: msgID, RoomID: roomID, CreatedAt: createdAt,
		Sender: sender, PinnedAt: &pinnedAt,
	}

	addedAt := reactor.ReactedAt
	removedAt := addedAt.Add(5 * time.Minute).Truncate(time.Millisecond)
	require.NoError(t, repo.AddReaction(ctx, msg, key, reactor))
	require.NoError(t, repo.RemoveReaction(ctx, msg, key, removedAt))

	// messages_by_id reflects the remove.
	var primaryReactions models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&primaryReactions))
	assert.NotContains(t, primaryReactions, key)

	// pinned_messages_by_room.updated_at must NOT have been touched.
	var pinnedUpdatedAt time.Time
	require.NoError(t, repo.session.Query(
		`SELECT updated_at FROM pinned_messages_by_room WHERE room_id = ? AND created_at = ? AND message_id = ?`,
		roomID, pinnedAt, msgID,
	).Scan(&pinnedUpdatedAt))
	assert.True(t, pinnedUpdatedAt.IsZero() || pinnedUpdatedAt.Before(removedAt),
		"pinned_messages_by_room.updated_at must not be bumped by RemoveReaction")
}

func TestRepository_RemoveReaction_Idempotent_AbsentCell(t *testing.T) {
	repo, _, createdAt, key, _ := reactionFixture(t)
	ctx := context.Background()

	sender := models.Participant{ID: "u-bob", Account: "bob"}
	roomID := "room-noop"
	msgID := "m-noop"

	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "hello", "",
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "hello", "",
	).Exec())

	msg := &models.Message{MessageID: msgID, RoomID: roomID, CreatedAt: createdAt, Sender: sender}

	// Remove without a prior add — Cassandra map-cell DELETE is a no-op on absent cells.
	require.NoError(t, repo.RemoveReaction(ctx, msg, key, createdAt))
}

func TestRepository_MultipleReactions_DifferentEmojiAndUsers(t *testing.T) {
	repo, _, createdAt, _, _ := reactionFixture(t)
	ctx := context.Background()

	sender := models.Participant{ID: "u-bob", Account: "bob"}
	roomID := "room-multi"
	msgID := "m-multi"

	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "hello", "",
	).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "hello", "",
	).Exec())

	msg := &models.Message{MessageID: msgID, RoomID: roomID, CreatedAt: createdAt, Sender: sender}
	reactedAt := createdAt.Add(time.Minute).Truncate(time.Millisecond)

	// alice 👍, carol 👍 (same emoji, different users), bob ❤️ (different emoji).
	cases := []struct {
		key     models.ReactionKey
		reactor models.ReactorInfo
	}{
		{
			key:     models.ReactionKey{Emoji: "👍", UserAccount: "alice"},
			reactor: models.ReactorInfo{UserID: "u-alice", Account: "alice", ReactedAt: reactedAt},
		},
		{
			key:     models.ReactionKey{Emoji: "👍", UserAccount: "carol"},
			reactor: models.ReactorInfo{UserID: "u-carol", Account: "carol", ReactedAt: reactedAt.Add(time.Second)},
		},
		{
			key:     models.ReactionKey{Emoji: "❤️", UserAccount: "bob"},
			reactor: models.ReactorInfo{UserID: "u-bob", Account: "bob", ReactedAt: reactedAt.Add(2 * time.Second)},
		},
	}
	for _, c := range cases {
		require.NoError(t, repo.AddReaction(ctx, msg, c.key, c.reactor))
	}

	var got models.Reactions
	require.NoError(t, repo.session.Query(
		`SELECT reactions FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&got))
	require.Len(t, got, 3, "three distinct (emoji, user) pairs must produce three map entries")
	for _, c := range cases {
		require.Contains(t, got, c.key)
		assert.Equal(t, c.reactor.Account, got[c.key].Account)
	}
}
