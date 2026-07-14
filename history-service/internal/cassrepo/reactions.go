package cassrepo

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// One UnloggedBatch writes messages_by_id + room-or-thread mirror per call —
// transport grouping, not atomic; half-apply heals via canonical event re-publish
// + per-cell idempotency (precedent: pin.go). Remove skips updated_at: no consumer
// reads it for reaction freshness, so the touch is dead work.

const (
	addReactionMsgByID   = `UPDATE messages_by_id SET reactions[?] = ?, updated_at = ? WHERE message_id = ?`
	addReactionMsgByRoom = `UPDATE messages_by_room SET reactions[?] = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	addReactionThreadMsg = `UPDATE thread_messages_by_thread SET reactions[?] = ?, updated_at = ? WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`

	removeReactionMsgByID   = `DELETE reactions[?] FROM messages_by_id WHERE message_id = ?`
	removeReactionMsgByRoom = `DELETE reactions[?] FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	removeReactionThreadMsg = `DELETE reactions[?] FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`
)

// AddReaction writes the (emoji, user_account) cell via one UnloggedBatch; re-writes overwrite.
//
//nolint:gocritic // hugeParam: reactor passed by value to match Sender/Mentions in models.Message
func (r *Repository) AddReaction(ctx context.Context, msg *models.Message, key models.ReactionKey, reactor models.ReactorInfo) error {
	if msg.ThreadParentID != "" && msg.ThreadRoomID == "" {
		return fmt.Errorf("react thread message %s: ThreadParentID %q is set but ThreadRoomID is empty", msg.MessageID, msg.ThreadParentID)
	}
	reactedAt := reactor.ReactedAt

	batch := r.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(addReactionMsgByID, key, reactor, reactedAt, msg.MessageID)
	if msg.ThreadParentID == "" {
		b := r.bucket.Of(msg.CreatedAt)
		batch.Query(addReactionMsgByRoom, key, reactor, reactedAt, msg.RoomID, b, msg.CreatedAt, msg.MessageID)
		if err := r.session.ExecuteBatch(batch); err != nil {
			return fmt.Errorf("add reaction on message %s in room %s via batch(messages_by_id, messages_by_room): %w", msg.MessageID, msg.RoomID, err)
		}
		return nil
	}
	batch.Query(addReactionThreadMsg, key, reactor, reactedAt, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID)
	if err := r.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("add reaction on thread message %s in thread %s via batch(messages_by_id, thread_messages_by_thread): %w", msg.MessageID, msg.ThreadRoomID, err)
	}
	return nil
}

// RemoveReaction deletes the cell via one UnloggedBatch. See file comment for why updated_at is untouched.
func (r *Repository) RemoveReaction(ctx context.Context, msg *models.Message, key models.ReactionKey) error {
	if msg.ThreadParentID != "" && msg.ThreadRoomID == "" {
		return fmt.Errorf("unreact thread message %s: ThreadParentID %q is set but ThreadRoomID is empty", msg.MessageID, msg.ThreadParentID)
	}

	batch := r.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(removeReactionMsgByID, key, msg.MessageID)
	if msg.ThreadParentID == "" {
		b := r.bucket.Of(msg.CreatedAt)
		batch.Query(removeReactionMsgByRoom, key, msg.RoomID, b, msg.CreatedAt, msg.MessageID)
		if err := r.session.ExecuteBatch(batch); err != nil {
			return fmt.Errorf("remove reaction on message %s in room %s via batch(messages_by_id, messages_by_room): %w", msg.MessageID, msg.RoomID, err)
		}
		return nil
	}
	batch.Query(removeReactionThreadMsg, key, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID)
	if err := r.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("remove reaction on thread message %s in thread %s via batch(messages_by_id, thread_messages_by_thread): %w", msg.MessageID, msg.ThreadRoomID, err)
	}
	return nil
}
