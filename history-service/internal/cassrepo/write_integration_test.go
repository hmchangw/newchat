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

func TestRepository_UpdateMessageContent_TopLevel(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-top"
	msgID := "m-top"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed a top-level message in both tables (ThreadParentID == "").
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "original", "",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "original", "",
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		Sender:         sender,
		ThreadParentID: "",
	}
	editedAt := createdAt.Add(time.Minute)
	require.NoError(t, repo.UpdateMessageContent(ctx, msg, "edited", editedAt))

	// messages_by_id updated
	var gotMsg string
	var gotEditedAt time.Time
	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg)
	assert.WithinDuration(t, editedAt, gotEditedAt, time.Second)

	// messages_by_room updated
	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg)
	assert.WithinDuration(t, editedAt, gotEditedAt, time.Second)

	// No phantom-row check on thread_messages_by_thread: a top-level message has
	// empty ThreadRoomID, and Cassandra refuses empty-string partition keys, so
	// the write is impossible at the driver layer.
}

func TestRepository_UpdateMessageContent_ThreadReply(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-thread"
	threadRoomID := "thread-1"
	parentID := "m-parent"
	msgID := "m-reply"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed a thread reply in messages_by_id and thread_messages_by_thread.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_room_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "original", parentID, threadRoomID,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, createdAt, msgID, roomID, sender, "original", parentID,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		Sender:         sender,
		ThreadParentID: parentID,
		ThreadRoomID:   threadRoomID,
	}
	editedAt := createdAt.Add(time.Minute)
	require.NoError(t, repo.UpdateMessageContent(ctx, msg, "edited", editedAt))

	// messages_by_id updated
	var gotMsg string
	var gotEditedAt time.Time
	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg)
	assert.WithinDuration(t, editedAt, gotEditedAt, time.Second)

	// thread_messages_by_thread updated (verify with the full PK)
	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
		threadRoomID, createdAt, msgID,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg)
	assert.WithinDuration(t, editedAt, gotEditedAt, time.Second)

	// messages_by_room must NOT have a phantom row for this thread reply
	var roomCount int
	require.NoError(t, session.Query(
		`SELECT COUNT(*) FROM messages_by_room WHERE room_id = ? AND bucket = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt),
	).Scan(&roomCount))
	assert.Equal(t, 0, roomCount, "thread-reply edit must not write to messages_by_room")
}

func TestRepository_UpdateMessageContent_Pinned(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-pin"
	msgID := "m-pin"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)
	pinnedAt := createdAt.Add(10 * time.Second)

	// Seed a top-level pinned message in all three tables.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, pinned_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "original", "", pinnedAt,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "original", "",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO pinned_messages_by_room (room_id, pinned_at, message_id, sender, msg, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		roomID, pinnedAt, msgID, sender, "original", createdAt,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		Sender:         sender,
		ThreadParentID: "",
		PinnedAt:       &pinnedAt,
	}
	editedAt := createdAt.Add(time.Minute)
	require.NoError(t, repo.UpdateMessageContent(ctx, msg, "edited", editedAt))

	// All three affected tables updated
	var gotMsg string
	var gotEditedAt time.Time

	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg, "messages_by_id should reflect the edit")
	assert.WithinDuration(t, editedAt, gotEditedAt, time.Second)

	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg, "messages_by_room should reflect the edit")
	assert.WithinDuration(t, editedAt, gotEditedAt, time.Second)

	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM pinned_messages_by_room WHERE room_id = ? AND pinned_at = ? AND message_id = ?`,
		roomID, pinnedAt, msgID,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg, "pinned_messages_by_room should reflect the edit")
	assert.WithinDuration(t, editedAt, gotEditedAt, time.Second)
}

func TestRepository_SoftDeleteMessage_TopLevel(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-del-top"
	msgID := "m-del-top"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed a top-level message in both tables.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "original", "", false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "original", "", false,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		Sender:         sender,
		ThreadParentID: "",
	}
	deletedAt := createdAt.Add(time.Minute)
	_, applied, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err)
	require.True(t, applied, "first delete should apply")

	// messages_by_id: deleted = true, msg retained, updated_at advanced
	var gotDeleted bool
	var gotMsg string
	var gotUpdatedAt time.Time
	require.NoError(t, session.Query(
		`SELECT deleted, msg, updated_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotDeleted, &gotMsg, &gotUpdatedAt))
	assert.True(t, gotDeleted, "messages_by_id.deleted should be true")
	assert.Equal(t, "original", gotMsg, "msg content must be preserved")
	assert.WithinDuration(t, deletedAt, gotUpdatedAt, time.Second)

	// messages_by_room: same assertions
	require.NoError(t, session.Query(
		`SELECT deleted, msg, updated_at FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID,
	).Scan(&gotDeleted, &gotMsg, &gotUpdatedAt))
	assert.True(t, gotDeleted)
	assert.Equal(t, "original", gotMsg, "msg content must be preserved")
	assert.WithinDuration(t, deletedAt, gotUpdatedAt, time.Second)

	// No phantom-row check on thread_messages_by_thread: top-level messages have
	// empty ThreadRoomID, and Cassandra refuses empty-string partition keys, so
	// the write is impossible at the driver layer.
}

func TestRepository_SoftDeleteMessage_ThreadReply(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-del-thread"
	threadRoomID := "thread-del-1"
	parentID := "m-del-parent"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyID := "m-del-reply"
	replyCreatedAt := parentCreatedAt.Add(10 * time.Second)

	// Seed the parent (so the thread_parent_created_at reference is real and
	// Task 7's tcount decrement has a target row to work with later).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, deleted, tcount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		parentID, roomID, parentCreatedAt, sender, "parent", "", false, 1,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted, tcount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID, sender, "parent", "", false, 1,
	).Exec())

	// Seed the thread reply in messages_by_id and thread_messages_by_thread.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		replyID, roomID, replyCreatedAt, sender, "reply", parentID, parentCreatedAt, threadRoomID, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, replyCreatedAt, replyID, roomID, sender, "reply", parentID, false,
	).Exec())

	parentCreatedAtPtr := parentCreatedAt
	msg := &models.Message{
		MessageID:             replyID,
		RoomID:                roomID,
		CreatedAt:             replyCreatedAt,
		Sender:                sender,
		ThreadParentID:        parentID,
		ThreadParentCreatedAt: &parentCreatedAtPtr,
		ThreadRoomID:          threadRoomID,
	}
	deletedAt := replyCreatedAt.Add(time.Minute)
	_, applied, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err)
	require.True(t, applied, "first delete should apply")

	// messages_by_id: reply now deleted
	var gotDeleted bool
	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		replyID, replyCreatedAt,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted)

	// thread_messages_by_thread: reply now deleted (full PK)
	require.NoError(t, session.Query(
		`SELECT deleted FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
		threadRoomID, replyCreatedAt, replyID,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted)

	// messages_by_room must NOT have a phantom row for this thread reply. Scoped
	// to the reply's exact PK because the parent (a top-level message) legitimately
	// shares the same daily bucket and would otherwise be counted.
	var replyInRoom int
	require.NoError(t, session.Query(
		`SELECT COUNT(*) FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(replyCreatedAt), replyCreatedAt, replyID,
	).Scan(&replyInRoom))
	assert.Equal(t, 0, replyInRoom, "thread-reply soft-delete must not write to messages_by_room")

	// Parent's tcount should have been decremented from 1 to 0 — see Task 7.
	var gotTcount int
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID,
	).Scan(&gotTcount))
	assert.Equal(t, 0, gotTcount, "tcount should be decremented on thread-reply soft-delete")
}

func TestRepository_SoftDeleteMessage_Pinned(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-del-pin"
	msgID := "m-del-pin"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)
	pinnedAt := createdAt.Add(10 * time.Second)

	// Seed a top-level pinned message in all three tables.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, pinned_at, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "content", "", pinnedAt, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "content", "", false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO pinned_messages_by_room (room_id, pinned_at, message_id, sender, msg, deleted, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, pinnedAt, msgID, sender, "content", false, createdAt,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		Sender:         sender,
		ThreadParentID: "",
		PinnedAt:       &pinnedAt,
	}
	deletedAt := createdAt.Add(time.Minute)
	_, applied, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err)
	require.True(t, applied, "first delete should apply")

	// All three tables should reflect deleted = true
	var gotDeleted bool

	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted, "messages_by_id should be deleted")

	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted, "messages_by_room should be deleted")

	require.NoError(t, session.Query(
		`SELECT deleted FROM pinned_messages_by_room WHERE room_id = ? AND pinned_at = ? AND message_id = ?`,
		roomID, pinnedAt, msgID,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted, "pinned_messages_by_room should be deleted")
}

func TestRepository_SoftDeleteMessage_DecrementsParentTcount(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-tcount"
	threadRoomID := "thread-tcount"
	parentID := "m-tcount-parent"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyID := "m-tcount-reply"
	replyCreatedAt := parentCreatedAt.Add(10 * time.Second)

	// Parent has tcount = 3 (three replies, of which we're about to delete one).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		parentID, roomID, parentCreatedAt, sender, "parent", "", 3, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID, sender, "parent", "", 3, false,
	).Exec())

	// Seed the reply we're deleting.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		replyID, roomID, replyCreatedAt, sender, "reply", parentID, parentCreatedAt, threadRoomID, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, replyCreatedAt, replyID, roomID, sender, "reply", parentID, false,
	).Exec())

	parentCreatedAtPtr := parentCreatedAt
	msg := &models.Message{
		MessageID:             replyID,
		RoomID:                roomID,
		CreatedAt:             replyCreatedAt,
		Sender:                sender,
		ThreadParentID:        parentID,
		ThreadParentCreatedAt: &parentCreatedAtPtr,
		ThreadRoomID:          threadRoomID,
	}
	_, applied, err := repo.SoftDeleteMessage(ctx, msg, replyCreatedAt.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, applied, "first delete should apply")

	// Both tables' tcount should now be 2.
	var gotTcount int
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).Scan(&gotTcount))
	assert.Equal(t, 2, gotTcount, "messages_by_id.tcount should decrement 3 -> 2")

	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID,
	).Scan(&gotTcount))
	assert.Equal(t, 2, gotTcount, "messages_by_room.tcount should decrement 3 -> 2")
}

func TestRepository_SoftDeleteMessage_TopLevelDoesNotTouchTcount(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-tcount-top"
	msgID := "m-tcount-top"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed a top-level message with tcount=5 (pretend it has 5 thread replies).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "top", "", 5, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "top", "", 5, false,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		Sender:         sender,
		ThreadParentID: "",
	}
	_, applied, err := repo.SoftDeleteMessage(ctx, msg, createdAt.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, applied, "first delete should apply")

	// tcount stays at 5 — top-level delete does not cascade / decrement (spec §8.2).
	var gotTcount int
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID,
	).Scan(&gotTcount))
	assert.Equal(t, 5, gotTcount, "top-level soft-delete must not touch tcount — replies are preserved (no cascade)")
}

func TestRepository_UpdateMessageContent_MissingThreadRoomID_ReturnsError(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_room_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"m-no-tr", "r-no-tr", createdAt, sender, "original", "m-parent", "",
	).Exec())

	msg := &models.Message{
		MessageID:      "m-no-tr",
		RoomID:         "r-no-tr",
		CreatedAt:      createdAt,
		ThreadParentID: "m-parent",
		ThreadRoomID:   "",
	}
	err := repo.UpdateMessageContent(ctx, msg, "edited", createdAt.Add(time.Minute))
	require.Error(t, err, "expected error when ThreadRoomID is empty for a thread reply")

	// Validation must fire before any DB write — messages_by_id must be unchanged.
	var gotMsg string
	var gotEditedAt *time.Time
	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		"m-no-tr", createdAt,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "original", gotMsg, "msg must not be updated on validation failure")
	assert.Nil(t, gotEditedAt, "edited_at must remain nil on validation failure")
}

func TestRepository_SoftDeleteMessage_MissingThreadRoomID_ReturnsError(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_room_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"m-no-tr-del", "r-no-tr-del", createdAt, sender, "content", "m-parent", "", false,
	).Exec())

	msg := &models.Message{
		MessageID:      "m-no-tr-del",
		RoomID:         "r-no-tr-del",
		CreatedAt:      createdAt,
		ThreadParentID: "m-parent",
		ThreadRoomID:   "",
	}
	_, _, err := repo.SoftDeleteMessage(ctx, msg, createdAt.Add(time.Minute))
	require.Error(t, err, "expected error when ThreadRoomID is empty for a thread reply")

	// Validation must fire before any DB write — messages_by_id must be unchanged.
	var gotDeleted bool
	var gotUpdatedAt *time.Time
	require.NoError(t, session.Query(
		`SELECT deleted, updated_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		"m-no-tr-del", createdAt,
	).Scan(&gotDeleted, &gotUpdatedAt))
	assert.False(t, gotDeleted, "deleted must remain false on validation failure")
	assert.Nil(t, gotUpdatedAt, "updated_at must remain nil on validation failure")
}

// TestRepository_SoftDeleteMessage_LWTGatesDoubleDecrement covers the
// concurrent-delete race: a thread reply that's already been soft-deleted
// must not have its parent's tcount decremented again on a second delete.
// This is the load-bearing test for the IF deleted != true CAS gate.
func TestRepository_SoftDeleteMessage_LWTGatesDoubleDecrement(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-lwt"
	threadRoomID := "thread-lwt-1"
	parentID := "m-lwt-parent"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyID := "m-lwt-reply"
	replyCreatedAt := parentCreatedAt.Add(10 * time.Second)

	// Seed parent with tcount = 1 (one reply).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, deleted, tcount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		parentID, roomID, parentCreatedAt, sender, "parent", "", false, 1,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted, tcount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID, sender, "parent", "", false, 1,
	).Exec())

	// Seed the reply in both messages_by_id and thread_messages_by_thread.
	// Note: message-worker doesn't write `deleted` at INSERT time, so deleted
	// will be NULL on a real reply row. We mirror that here by NOT setting
	// `deleted` in the INSERT — this also exercises the IF deleted != true
	// branch that must match NULL.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		replyID, roomID, replyCreatedAt, sender, "reply", parentID, parentCreatedAt, threadRoomID,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, replyCreatedAt, replyID, roomID, sender, "reply", parentID,
	).Exec())

	msg := &models.Message{
		MessageID:             replyID,
		RoomID:                roomID,
		CreatedAt:             replyCreatedAt,
		Sender:                sender,
		ThreadParentID:        parentID,
		ThreadParentCreatedAt: &parentCreatedAt,
		ThreadRoomID:          threadRoomID,
	}

	// First delete: LWT applies (deleted was NULL → matches != true).
	firstAt := replyCreatedAt.Add(time.Minute)
	gotAt1, applied1, err := repo.SoftDeleteMessage(ctx, msg, firstAt)
	require.NoError(t, err)
	require.True(t, applied1, "first delete must apply (deleted was NULL)")
	assert.Equal(t, firstAt.UnixMilli(), gotAt1.UnixMilli())

	// Confirm tcount went 1 -> 0 on both parent rows.
	var tcount int
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).Scan(&tcount))
	assert.Equal(t, 0, tcount, "first delete should have decremented messages_by_id.tcount")
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID,
	).Scan(&tcount))
	assert.Equal(t, 0, tcount, "first delete should have decremented messages_by_room.tcount")

	// Second delete on the same reply: LWT must NOT apply (deleted is now
	// true), and tcount must NOT be decremented a second time. Pass the same
	// hydrated msg (Deleted=false) to simulate a stale read; the repo's CAS
	// is authoritative.
	secondAt := firstAt.Add(time.Second)
	gotAt2, applied2, err := repo.SoftDeleteMessage(ctx, msg, secondAt)
	require.NoError(t, err)
	require.False(t, applied2, "second delete must NOT apply — deleted is already true")
	assert.Equal(t, firstAt.UnixMilli(), gotAt2.UnixMilli(), "actualDeletedAt should reflect the winning goroutine's timestamp")

	// tcount must still be 0 (not -1 / not double-decremented). casDecrement
	// also clamps at zero, but the LWT gate is the proper defense.
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).Scan(&tcount))
	assert.Equal(t, 0, tcount, "second delete must not double-decrement messages_by_id.tcount")
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID,
	).Scan(&tcount))
	assert.Equal(t, 0, tcount, "second delete must not double-decrement messages_by_room.tcount")
}

// TestRepository_UpdateMessageContent_RoundTrip verifies that after editing a
// top-level message, GetMessageByID returns the updated content and edited_at
// via the struct-scan read path (Tasks 1–2).
func TestRepository_UpdateMessageContent_RoundTrip(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-rt-edit"
	msgID := "m-rt-edit"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "original", "",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "original", "",
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		ThreadParentID: "",
	}
	editedAt := createdAt.Add(time.Minute)
	require.NoError(t, repo.UpdateMessageContent(ctx, msg, "updated content", editedAt))

	got, err := repo.GetMessageByID(ctx, msgID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "updated content", got.Msg)
	require.NotNil(t, got.EditedAt)
	assert.Equal(t, editedAt.UTC(), got.EditedAt.UTC())
	require.NotNil(t, got.UpdatedAt)
	assert.Equal(t, editedAt.UTC(), got.UpdatedAt.UTC())
}

// TestRepository_SoftDeleteMessage_RoundTrip verifies that after soft-deleting
// a top-level message, GetMessageByID returns deleted=true via struct scan.
func TestRepository_SoftDeleteMessage_RoundTrip(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-rt-del"
	msgID := "m-rt-del"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "content", "", false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "content", "", false,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		ThreadParentID: "",
	}
	deletedAt := createdAt.Add(time.Minute)
	gotAt, applied, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err)
	require.True(t, applied)
	assert.Equal(t, deletedAt.UnixMilli(), gotAt.UnixMilli())

	got, err := repo.GetMessageByID(ctx, msgID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Deleted, "GetMessageByID should return deleted=true after SoftDeleteMessage")
	require.NotNil(t, got.UpdatedAt)
	assert.Equal(t, deletedAt.UTC(), got.UpdatedAt.UTC())
}

// TestRepository_SoftDeleteMessage_RowCreatedByLWT confirms no panic and no
// unexpected error when SoftDeleteMessage is called on a non-existent row.
// The Cassandra LWT (IF deleted != true) applies against NULL and materialises
// a partial phantom row; the method must handle this gracefully.
func TestRepository_SoftDeleteMessage_RowCreatedByLWT(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	msg := &models.Message{
		MessageID:      "m-ghost",
		RoomID:         "r-ghost",
		CreatedAt:      time.Now().UTC().Truncate(time.Millisecond),
		ThreadParentID: "",
	}
	deletedAt := msg.CreatedAt.Add(time.Minute)

	_, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err, "SoftDeleteMessage must not return an error on a non-existent row")
}

// TestRepository_SoftDeleteMessage_ThreadParent_SetsTypeRemoved verifies that
// deleting a top-level thread parent (TCount > 0) sets type = 'message_removed'
// atomically in messages_by_id and messages_by_room.
func TestRepository_SoftDeleteMessage_ThreadParent_SetsTypeRemoved(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-tp-del"
	msgID := "m-tp-del"
	tcount := 2
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed messages_by_id with tcount = 2 (has active replies — thread parent).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "parent msg", tcount, false,
	).Exec())

	// Seed messages_by_room (top-level message: no thread_parent_id).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "parent msg", tcount, false,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		TCount:         &tcount,
		ThreadParentID: "", // top-level
	}

	deletedAt := createdAt.Add(time.Minute)
	_, applied, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err)
	require.True(t, applied)

	// Verify messages_by_id: deleted = true AND type = 'message_removed'.
	var gotDeleted bool
	var gotType string
	require.NoError(t, session.Query(
		`SELECT deleted, type FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotDeleted, &gotType))
	assert.True(t, gotDeleted)
	assert.Equal(t, MessageTypeRemoved, gotType, "messages_by_id must have type='message_removed' for thread parent")

	// Verify messages_by_room: deleted = true AND type = 'message_removed'.
	require.NoError(t, session.Query(
		`SELECT deleted, type FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID,
	).Scan(&gotDeleted, &gotType))
	assert.True(t, gotDeleted)
	assert.Equal(t, MessageTypeRemoved, gotType, "messages_by_room must have type='message_removed' for thread parent")
}

// TestRepository_SoftDeleteMessage_NonThreadParent_NoTypeChange verifies that
// deleting a regular message (TCount nil) does NOT set type = 'message_removed'.
func TestRepository_SoftDeleteMessage_NonThreadParent_NoTypeChange(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-non-tp"
	msgID := "m-non-tp"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed with no tcount (regular message, never had a thread).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "regular msg", false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(createdAt), createdAt, msgID, sender, "regular msg", false,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		TCount:         nil, // no replies — not a thread parent
		ThreadParentID: "",
	}

	deletedAt := createdAt.Add(time.Minute)
	_, applied, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err)
	require.True(t, applied)

	// type column should be empty (not set).
	var gotType string
	require.NoError(t, session.Query(
		`SELECT type FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotType))
	assert.Empty(t, gotType, "regular message delete must NOT set type")
}

// TestRepository_SoftDeleteMessage_ReplyThreadParent_SetsTypeRemoved verifies that
// deleting a reply that is itself a thread parent (ThreadParentID != "" AND TCount > 0)
// sets type = 'message_removed' in messages_by_id and thread_messages_by_thread.
func TestRepository_SoftDeleteMessage_ReplyThreadParent_SetsTypeRemoved(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-rtp"
	threadRoomID := "tr-rtp"
	parentMsgID := "m-rtp-parent"
	msgID := "m-rtp"
	tcount := 1
	parentCreatedAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Truncate(time.Millisecond)

	// Seed messages_by_id: this message is a reply (ThreadParentID set) AND a parent (TCount = 1).
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_room_id, thread_parent_created_at, tcount, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "nested thread parent", parentMsgID, threadRoomID, parentCreatedAt, tcount, false,
	).Exec())

	// Seed thread_messages_by_thread (message is a reply in the parent's thread).
	// Note: thread_messages_by_thread has no tcount column — tcount only lives in messages_by_id.
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, createdAt, msgID, roomID, sender, "nested thread parent", false,
	).Exec())

	msg := &models.Message{
		MessageID:             msgID,
		RoomID:                roomID,
		ThreadRoomID:          threadRoomID,
		CreatedAt:             createdAt,
		ThreadParentID:        parentMsgID,
		ThreadParentCreatedAt: &parentCreatedAt,
		TCount:                &tcount,
	}

	deletedAt := createdAt.Add(time.Minute)
	_, applied, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err)
	require.True(t, applied)

	// Verify messages_by_id: type = 'message_removed'.
	var gotType string
	require.NoError(t, session.Query(
		`SELECT type FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotType))
	assert.Equal(t, MessageTypeRemoved, gotType, "messages_by_id must have type='message_removed'")

	// Verify thread_messages_by_thread: type = 'message_removed'.
	require.NoError(t, session.Query(
		`SELECT type FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
		threadRoomID, createdAt, msgID,
	).Scan(&gotType))
	assert.Equal(t, MessageTypeRemoved, gotType, "thread_messages_by_thread must have type='message_removed'")
}
