//go:build integration

package cassrepo

import (
	"context"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/atrest"
	cassmodel "github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestRepository_UpdateMessageContent_TopLevel(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	_, applied, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	_, applied, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	_, applied, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-tcount"
	threadRoomID := "thread-tcount"
	parentID := "m-tcount-parent"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyID := "m-tcount-reply"
	replyCreatedAt := parentCreatedAt.Add(10 * time.Second)

	// Parent has no pre-seeded tcount — countAndSetParentTcount computes it from
	// thread_messages_by_thread rather than CAS-decrementing a stored value.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		parentID, roomID, parentCreatedAt, sender, "parent", "", false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID, sender, "parent", "", false,
	).Exec())

	// Seed 3 replies in thread_messages_by_thread: 2 survivors + the reply being deleted.
	// After SoftDeleteMessage marks replyID as deleted=true, COUNT gives 2.
	survivor1At := parentCreatedAt.Add(5 * time.Second)
	survivor2At := parentCreatedAt.Add(7 * time.Second)
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, survivor1At, "m-tcount-survivor-1", roomID, sender, "survivor 1", parentID, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, survivor2At, "m-tcount-survivor-2", roomID, sender, "survivor 2", parentID, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		replyID, roomID, replyCreatedAt, sender, "reply to delete", parentID, parentCreatedAt, threadRoomID, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, replyCreatedAt, replyID, roomID, sender, "reply to delete", parentID, false,
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
	_, applied, newTcount, err := repo.SoftDeleteMessage(ctx, msg, replyCreatedAt.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, applied, "first delete should apply")
	// SoftDeleteMessage returns the COUNT of non-deleted thread replies so the
	// caller can publish a ThreadMetadataUpdatedEvent without an extra round-trip.
	require.NotNil(t, newTcount, "newTcount must be non-nil after a successful thread-reply delete")
	assert.Equal(t, 2, *newTcount, "tcount = non-deleted COUNT (3 seeded - 1 deleted = 2)")

	// Both tables' tcount should now be 2.
	var gotTcount int
	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).Scan(&gotTcount))
	assert.Equal(t, 2, gotTcount, "messages_by_id.tcount = count-based 2")

	require.NoError(t, session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID,
	).Scan(&gotTcount))
	assert.Equal(t, 2, gotTcount, "messages_by_room.tcount = count-based 2")
}

func TestRepository_SoftDeleteMessage_TopLevelDoesNotTouchTcount(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	_, applied, _, err := repo.SoftDeleteMessage(ctx, msg, createdAt.Add(time.Minute))
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	_, _, _, err := repo.SoftDeleteMessage(ctx, msg, createdAt.Add(time.Minute))
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	gotAt1, applied1, _, err := repo.SoftDeleteMessage(ctx, msg, firstAt)
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
	gotAt2, applied2, _, err := repo.SoftDeleteMessage(ctx, msg, secondAt)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	gotAt, applied, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	msg := &models.Message{
		MessageID:      "m-ghost",
		RoomID:         "r-ghost",
		CreatedAt:      time.Now().UTC().Truncate(time.Millisecond),
		ThreadParentID: "",
	}
	deletedAt := msg.CreatedAt.Add(time.Minute)

	_, _, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err, "SoftDeleteMessage must not return an error on a non-existent row")
}

// TestRepository_SoftDeleteMessage_ThreadParent_SetsTypeRemoved verifies that
// deleting a top-level thread parent (TCount > 0) sets type = 'message_removed'
// atomically in messages_by_id and messages_by_room.
func TestRepository_SoftDeleteMessage_ThreadParent_SetsTypeRemoved(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	_, applied, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	_, applied, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
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
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
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
	_, applied, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
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

// TestEditMessage_EncryptsBody verifies that when a cipher is configured, edits
// re-encrypt the new body and null the legacy plaintext msg column.
func TestEditMessage_EncryptsBody(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	roomID := "r-edit-1"

	// Pre-seed an encrypted row directly with CQL.
	enc := atrest.EncryptedFields{Msg: "original body"}
	payload, meta, err := cipher.Encrypt(ctx, roomID, enc)
	require.NoError(t, err)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, enc_payload, enc_meta, site_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, "m1", payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, created_at, room_id, enc_payload, enc_meta, site_id) VALUES (?, ?, ?, ?, ?, ?)`,
		"m1", now, roomID, payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())

	editedAt := now.Add(time.Minute)
	require.NoError(t, repo.UpdateMessageContent(ctx, &models.Message{
		RoomID: roomID, MessageID: "m1", CreatedAt: now,
	}, "new body", editedAt))

	// Direct CQL: msg column is null, enc_payload is non-nil and decrypts to "new body".
	var (
		msgCol     string
		encPayload []byte
		encNonce   []byte
	)
	require.NoError(t, session.Query(
		`SELECT msg, enc_payload, enc_meta.nonce FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=? LIMIT 1`,
		roomID, sizer.Of(now), now, "m1",
	).Scan(&msgCol, &encPayload, &encNonce))
	assert.Empty(t, msgCol)
	require.NotEmpty(t, encPayload)

	plain, err := cipher.Decrypt(ctx, roomID, encPayload, atrest.EncMeta{Nonce: encNonce})
	require.NoError(t, err)
	assert.Equal(t, "new body", plain.Msg)
}

// TestEditMessage_PreservesOtherEncryptedFields verifies that editing a
// message's body re-encrypts a bundle that still contains the original
// non-Msg user-authored fields (here, Attachments). Without the
// decrypt-mutate-encrypt cycle in buildEditPayload these fields would be
// silently dropped on edit.
func TestEditMessage_PreservesOtherEncryptedFields(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	roomID := "r-edit-pres-1"

	originalAttachments := [][]byte{[]byte("att-1.bin"), []byte("att-2.bin")}
	enc := atrest.EncryptedFields{Msg: "original body", Attachments: originalAttachments}
	payload, meta, err := cipher.Encrypt(ctx, roomID, enc)
	require.NoError(t, err)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, enc_payload, enc_meta, site_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, "m1", payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, created_at, room_id, enc_payload, enc_meta, site_id) VALUES (?, ?, ?, ?, ?, ?)`,
		"m1", now, roomID, payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())

	editedAt := now.Add(time.Minute)
	require.NoError(t, repo.UpdateMessageContent(ctx, &models.Message{
		RoomID: roomID, MessageID: "m1", CreatedAt: now,
	}, "new body", editedAt))

	var (
		encPayload []byte
		encNonce   []byte
	)
	require.NoError(t, session.Query(
		`SELECT enc_payload, enc_meta.nonce FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=? LIMIT 1`,
		roomID, sizer.Of(now), now, "m1",
	).Scan(&encPayload, &encNonce))

	plain, err := cipher.Decrypt(ctx, roomID, encPayload, atrest.EncMeta{Nonce: encNonce})
	require.NoError(t, err)
	assert.Equal(t, "new body", plain.Msg)
	assert.Equal(t, originalAttachments, plain.Attachments, "non-Msg encrypted fields must survive an edit")
}

// TestEditMessage_LegacyRow_PreservesPlaintextAttachments verifies that
// editing a legacy plaintext row (written before the at-rest rollout)
// under cipher-enabled history-service preserves its existing plaintext
// attachments through the re-encryption cycle (without readEncryptedFields
// promoting the legacy attachments into the bundle, ApplyDecryptedFields
// would silently overwrite them with nil on the next read), and that the
// un-encrypted sys_msg_data column is left intact by the edit.
func TestEditMessage_LegacyRow_PreservesPlaintextAttachments(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	roomID := "r-legacy-edit-1"
	originalAttachments := [][]byte{[]byte("file-a.bin"), []byte("file-b.bin")}
	originalSysMsgData := []byte{0xCA, 0xFE, 0xBA, 0xBE}

	// Seed a legacy plaintext row directly with CQL — no enc_payload.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, msg, attachments, sys_msg_data, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, "m-legacy", "original body", originalAttachments, originalSysMsgData, "site-a",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, created_at, room_id, msg, attachments, sys_msg_data, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"m-legacy", now, roomID, "original body", originalAttachments, originalSysMsgData, "site-a",
	).Exec())

	editedAt := now.Add(time.Minute)
	require.NoError(t, repo.UpdateMessageContent(ctx, &models.Message{
		RoomID: roomID, MessageID: "m-legacy", CreatedAt: now,
	}, "edited body", editedAt))

	// Decrypt directly to verify the re-encrypted bundle carries the
	// legacy plaintext fields forward.
	var (
		encPayload []byte
		encNonce   []byte
		sysMsgData []byte
	)
	require.NoError(t, session.Query(
		`SELECT enc_payload, enc_meta.nonce, sys_msg_data FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=? LIMIT 1`,
		roomID, sizer.Of(now), now, "m-legacy",
	).Scan(&encPayload, &encNonce, &sysMsgData))
	require.NotEmpty(t, encPayload, "edit must produce an enc_payload")

	plain, err := cipher.Decrypt(ctx, roomID, encPayload, atrest.EncMeta{Nonce: encNonce})
	require.NoError(t, err)
	assert.Equal(t, "edited body", plain.Msg)
	assert.Equal(t, originalAttachments, plain.Attachments, "legacy plaintext attachments must survive the cipher-enabled edit")
	// sys_msg_data is not encrypted: it stays in its plaintext column across the edit.
	assert.Equal(t, originalSysMsgData, sysMsgData, "legacy plaintext sys_msg_data must survive the cipher-enabled edit")
}

// TestDeleteMessage_NullsEncryptedColumns verifies that soft-deleting an
// encrypted row also nulls enc_payload and enc_meta.
func TestDeleteMessage_NullsEncryptedColumns(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	roomID := "r-del-1"

	// Pre-seed an encrypted row.
	enc := atrest.EncryptedFields{Msg: "doomed body"}
	payload, meta, err := cipher.Encrypt(ctx, roomID, enc)
	require.NoError(t, err)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, enc_payload, enc_meta, site_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, "m1", payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, created_at, room_id, enc_payload, enc_meta, site_id) VALUES (?, ?, ?, ?, ?, ?)`,
		"m1", now, roomID, payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())

	_, applied, _, err := repo.SoftDeleteMessage(ctx, &models.Message{
		RoomID: roomID, MessageID: "m1", CreatedAt: now,
	}, now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, applied)

	var (
		deleted    bool
		encPayload []byte
	)
	require.NoError(t, session.Query(
		`SELECT deleted, enc_payload FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=? LIMIT 1`,
		roomID, sizer.Of(now), now, "m1",
	).Scan(&deleted, &encPayload))
	assert.True(t, deleted)
	assert.Nil(t, encPayload)
}

// TestUpdateMessageContent_NonExistent_CipherEnabled_ReturnsErrMessageNotFound
// verifies that on the cipher-enabled path, editing a (message_id, created_at)
// that does not exist short-circuits with ErrMessageNotFound instead of
// materialising a ghost row via CQL UPDATE's upsert semantics. buildEditPayload
// must read the canonical row to re-encrypt, so the missing row is caught there
// before any UPDATE runs. The cipher-disabled path issues a plain UPDATE and
// relies on the service layer's findMessage to gate existence.
func TestUpdateMessageContent_NonExistent_CipherEnabled_ReturnsErrMessageNotFound(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)
	sizer := msgbucket.New(24 * time.Hour)

	now := time.Now().UTC().Truncate(time.Millisecond)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	repo := NewRepository(session, sizer, 365, cipher)

	err := repo.UpdateMessageContent(ctx, &models.Message{
		RoomID: "r-ghost", MessageID: "m-ghost-enc", CreatedAt: now,
	}, "should not land", now.Add(time.Minute))
	require.ErrorIs(t, err, ErrMessageNotFound, "edit of non-existent message must surface ErrMessageNotFound on the cipher path")

	// No ghost row was materialised.
	var got string
	err = session.Query(
		`SELECT msg FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		"m-ghost-enc", now,
	).Scan(&got)
	require.ErrorIs(t, err, gocql.ErrNotFound, "no row must be upserted by the failed edit")
}

// TestEditMessage_Encrypted_NullsLegacyPlaintextColumns verifies that
// editing a legacy plaintext row through the cipher-enabled path nulls
// every encrypted legacy body column (attachments, card, card_action) in
// addition to msg. Without this, the very edit that's supposed to move the
// body into enc_payload would leave stale plaintext on disk. The un-encrypted
// sys_msg_data column is preserved.
func TestEditMessage_Encrypted_NullsLegacyPlaintextColumns(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	roomID := "r-legacy-null-1"

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, msg, attachments, sys_msg_data, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, "m-null", "original",
		[][]byte{[]byte("legacy-att")}, []byte{0xAA}, "site-a",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, created_at, room_id, msg, attachments, sys_msg_data, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"m-null", now, roomID, "original",
		[][]byte{[]byte("legacy-att")}, []byte{0xAA}, "site-a",
	).Exec())

	require.NoError(t, repo.UpdateMessageContent(ctx, &models.Message{
		RoomID: roomID, MessageID: "m-null", CreatedAt: now,
	}, "edited", now.Add(time.Minute)))

	for _, table := range []struct {
		name string
		q    string
		args []any
	}{
		{
			name: "messages_by_room",
			q:    `SELECT msg, attachments, sys_msg_data FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=?`,
			args: []any{roomID, sizer.Of(now), now, "m-null"},
		},
		{
			name: "messages_by_id",
			q:    `SELECT msg, attachments, sys_msg_data FROM messages_by_id WHERE message_id=? AND created_at=?`,
			args: []any{"m-null", now},
		},
	} {
		var (
			msgCol      *string
			attachments [][]byte
			sysMsgData  []byte
		)
		require.NoError(t, session.Query(table.q, table.args...).Scan(&msgCol, &attachments, &sysMsgData), "select from %s", table.name)
		assert.Nil(t, msgCol, "%s: msg must be NULL after encrypted edit", table.name)
		assert.Nil(t, attachments, "%s: legacy attachments must be NULL after encrypted edit", table.name)
		assert.Equal(t, []byte{0xAA}, sysMsgData, "%s: un-encrypted sys_msg_data must be preserved after encrypted edit", table.name)
	}
}

// TestEditMessage_Plaintext_NullsEncryptedColumns verifies that editing
// an encrypted row through the cipher-disabled path nulls enc_payload and
// enc_meta. Without this, a rollback of the at-rest rollout that edits a
// previously-encrypted row would leave a hybrid row whose stale enc_payload
// silently overrides the new plaintext msg on the next cipher-enabled read.
func TestEditMessage_Plaintext_NullsEncryptedColumns(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)
	sizer := msgbucket.New(24 * time.Hour)

	// Seed the row with the cipher-enabled repo, then drop the cipher to
	// simulate a rollback and edit the row through the plaintext path.
	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})

	now := time.Now().UTC().Truncate(time.Millisecond)
	roomID := "r-rollback-1"

	enc := atrest.EncryptedFields{Msg: "v1"}
	payload, meta, err := cipher.Encrypt(ctx, roomID, enc)
	require.NoError(t, err)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, enc_payload, enc_meta, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, "m-rb", payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, created_at, room_id, enc_payload, enc_meta, site_id)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		"m-rb", now, roomID, payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())

	// Rollback: cipher==nil. Edit goes through the plaintext path.
	repo := NewRepository(session, sizer, 365, nil)
	require.NoError(t, repo.UpdateMessageContent(ctx, &models.Message{
		RoomID: roomID, MessageID: "m-rb", CreatedAt: now,
	}, "v2", now.Add(time.Minute)))

	for _, table := range []struct {
		name string
		q    string
		args []any
	}{
		{
			name: "messages_by_room",
			q:    `SELECT msg, enc_payload, enc_meta FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=?`,
			args: []any{roomID, sizer.Of(now), now, "m-rb"},
		},
		{
			name: "messages_by_id",
			q:    `SELECT msg, enc_payload, enc_meta FROM messages_by_id WHERE message_id=? AND created_at=?`,
			args: []any{"m-rb", now},
		},
	} {
		var (
			msgCol  string
			encCol  []byte
			metaCol *cassmodel.EncMeta
		)
		require.NoError(t, session.Query(table.q, table.args...).Scan(&msgCol, &encCol, &metaCol), "select from %s", table.name)
		assert.Equal(t, "v2", msgCol, "%s: plaintext edit must write the new msg", table.name)
		assert.Nil(t, encCol, "%s: enc_payload must be NULL after plaintext edit (rollback hygiene)", table.name)
		assert.Nil(t, metaCol, "%s: enc_meta must be NULL after plaintext edit (rollback hygiene)", table.name)
	}
}

// TestEditMessage_Encrypted_NullsLegacyQuotedParent verifies that editing
// a legacy row with a plaintext quoted_parent_message UDT under the
// cipher-enabled path nulls the on-disk UDT column. Without this, the
// re-encryption cycle promotes the quoted body INTO the bundle (via
// readEncryptedFields) but leaves the plaintext column on disk — defeating
// the at-rest goal for the quoted parent body.
func TestEditMessage_Encrypted_NullsLegacyQuotedParent(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	parentCreatedAt := now.Add(-time.Hour)
	roomID := "r-quote-null"

	quoted := &cassmodel.QuotedParentMessage{
		MessageID:   "q-parent",
		RoomID:      roomID,
		Sender:      cassmodel.Participant{ID: "u-q", Account: "alice"},
		CreatedAt:   parentCreatedAt,
		Msg:         "the secret quoted body",
		Attachments: [][]byte{[]byte("quoted-blob")},
	}

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, msg, quoted_parent_message, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, "m-q", "original", quoted, "site-a",
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, created_at, room_id, msg, quoted_parent_message, site_id)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		"m-q", now, roomID, "original", quoted, "site-a",
	).Exec())

	require.NoError(t, repo.UpdateMessageContent(ctx, &models.Message{
		RoomID: roomID, MessageID: "m-q", CreatedAt: now,
	}, "edited body", now.Add(time.Minute)))

	for _, table := range []struct {
		name string
		q    string
		args []any
	}{
		{
			name: "messages_by_room",
			q:    `SELECT quoted_parent_message FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=?`,
			args: []any{roomID, sizer.Of(now), now, "m-q"},
		},
		{
			name: "messages_by_id",
			q:    `SELECT quoted_parent_message FROM messages_by_id WHERE message_id=? AND created_at=?`,
			args: []any{"m-q", now},
		},
	} {
		var got *cassmodel.QuotedParentMessage
		require.NoError(t, session.Query(table.q, table.args...).Scan(&got), "select from %s", table.name)
		assert.Nil(t, got, "%s: quoted_parent_message must be NULL after encrypted edit — the body has been promoted into enc_payload", table.name)
	}
}

// Note: editing a soft-deleted row is gated at the service layer
// (EditMessage's msg.Deleted check, covered by
// TestHistoryService_EditMessage_AlreadyDeleted), not at the store layer.
// UpdateMessageContent issues plain UPDATEs and no longer LWT-gates on
// `deleted`, matching main's pre-encryption edit behavior.

// TestRepository_UpdateMessageContent_TShowThreadReply verifies edit
// propagation for a TShow ("also send to channel") thread reply: the reply is
// dual-written into messages_by_room at create time, so an edit must update
// messages_by_id, thread_messages_by_thread, AND the messages_by_room copy.
func TestRepository_UpdateMessageContent_TShowThreadReply(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-tshow-edit"
	threadRoomID := "thread-tshow-edit-1"
	parentID := "m-tshow-edit-parent"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	msgID := "m-tshow-edit-reply"
	createdAt := parentCreatedAt.Add(10 * time.Second)
	bucket := msgbucket.New(24 * time.Hour).Of(createdAt)

	// Seed the TShow reply in all three tables, as message-worker's
	// SaveThreadMessage dual-write does.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id, tshow) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msgID, roomID, createdAt, sender, "original", parentID, parentCreatedAt, threadRoomID, true,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, createdAt, msgID, roomID, sender, "original", parentID,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_room_id, thread_parent_id, thread_parent_created_at, tshow) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucket, createdAt, msgID, sender, "original", threadRoomID, parentID, parentCreatedAt, true,
	).Exec())

	msg := &models.Message{
		MessageID:      msgID,
		RoomID:         roomID,
		CreatedAt:      createdAt,
		Sender:         sender,
		ThreadParentID: parentID,
		ThreadRoomID:   threadRoomID,
		TShow:          true,
	}
	editedAt := createdAt.Add(time.Minute)
	require.NoError(t, repo.UpdateMessageContent(ctx, msg, "edited", editedAt))

	var gotMsg string
	var gotEditedAt time.Time

	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		msgID, createdAt,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg)

	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
		threadRoomID, createdAt, msgID,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg)

	// The channel-timeline copy must not go stale.
	require.NoError(t, session.Query(
		`SELECT msg, edited_at FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, bucket, createdAt, msgID,
	).Scan(&gotMsg, &gotEditedAt))
	assert.Equal(t, "edited", gotMsg, "TShow reply edit must propagate to the messages_by_room copy")
	assert.WithinDuration(t, editedAt, gotEditedAt, time.Second)
}

// TestRepository_SoftDeleteMessage_TShowThreadReply verifies delete
// propagation for a TShow thread reply: soft-delete must mark deleted on
// messages_by_id, thread_messages_by_thread, AND the dual-written
// messages_by_room copy, or the reply stays visible in the channel timeline.
func TestRepository_SoftDeleteMessage_TShowThreadReply(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	sender := models.Participant{ID: "u1", Account: "alice"}
	roomID := "room-tshow-del"
	threadRoomID := "thread-tshow-del-1"
	parentID := "m-tshow-del-parent"
	parentCreatedAt := time.Now().UTC().Truncate(time.Millisecond)
	replyID := "m-tshow-del-reply"
	replyCreatedAt := parentCreatedAt.Add(10 * time.Second)
	bucket := msgbucket.New(24 * time.Hour).Of(replyCreatedAt)

	// Seed the parent so the tcount recount has a target row.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, deleted, tcount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		parentID, roomID, parentCreatedAt, sender, "parent", "", false, 1,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_parent_id, deleted, tcount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, msgbucket.New(24*time.Hour).Of(parentCreatedAt), parentCreatedAt, parentID, sender, "parent", "", false, 1,
	).Exec())

	// Seed the TShow reply in all three tables.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, thread_parent_id, thread_parent_created_at, thread_room_id, tshow, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		replyID, roomID, replyCreatedAt, sender, "reply", parentID, parentCreatedAt, threadRoomID, true, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, replyCreatedAt, replyID, roomID, sender, "reply", parentID, false,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_room_id, thread_parent_id, thread_parent_created_at, tshow, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucket, replyCreatedAt, replyID, sender, "reply", threadRoomID, parentID, parentCreatedAt, true, false,
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
		TShow:                 true,
	}
	deletedAt := replyCreatedAt.Add(time.Minute)
	_, applied, _, err := repo.SoftDeleteMessage(ctx, msg, deletedAt)
	require.NoError(t, err)
	require.True(t, applied, "first delete should apply")

	var gotDeleted bool
	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		replyID, replyCreatedAt,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted)

	require.NoError(t, session.Query(
		`SELECT deleted FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`,
		threadRoomID, replyCreatedAt, replyID,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted)

	// The channel-timeline copy must be soft-deleted too.
	require.NoError(t, session.Query(
		`SELECT deleted FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		roomID, bucket, replyCreatedAt, replyID,
	).Scan(&gotDeleted))
	assert.True(t, gotDeleted, "TShow reply soft-delete must propagate to the messages_by_room copy")
}
