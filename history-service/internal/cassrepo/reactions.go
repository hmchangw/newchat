package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// v3 reactions storage: one map-cell per (emoji, user_account) per row.
// Writes hit messages_by_id (source of truth) then the room-or-thread mirror.
// pinned_messages_by_room is NOT a reaction mirror — the pinned panel does
// not render reactions, so writing them there is dead work.

const (
	addReactionMsgByID   = `UPDATE messages_by_id SET reactions[?] = ?, updated_at = ? WHERE message_id = ? AND created_at = ?`
	addReactionMsgByRoom = `UPDATE messages_by_room SET reactions[?] = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	addReactionThreadMsg = `UPDATE thread_messages_by_thread SET reactions[?] = ?, updated_at = ? WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`

	removeReactionMsgByID   = `DELETE reactions[?] FROM messages_by_id WHERE message_id = ? AND created_at = ?`
	removeReactionMsgByRoom = `DELETE reactions[?] FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	removeReactionThreadMsg = `DELETE reactions[?] FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`
	touchUpdatedAtMsgByID   = `UPDATE messages_by_id SET updated_at = ? WHERE message_id = ? AND created_at = ?`
	touchUpdatedAtMsgByRoom = `UPDATE messages_by_room SET updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	touchUpdatedAtThreadMsg = `UPDATE thread_messages_by_thread SET updated_at = ? WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`
)

// AddReaction writes one (emoji, user_account) map-cell to messages_by_id and
// the room-or-thread mirror; idempotent re-writes overwrite the ReactorInfo.
//
//nolint:gocritic // hugeParam: reactor passed by value to match Sender/Mentions in models.Message
func (r *Repository) AddReaction(ctx context.Context, msg *models.Message, key models.ReactionKey, reactor models.ReactorInfo) error {
	if msg.ThreadParentID != "" && msg.ThreadRoomID == "" {
		return fmt.Errorf("react thread message %s: ThreadParentID %q is set but ThreadRoomID is empty", msg.MessageID, msg.ThreadParentID)
	}
	reactedAt := reactor.ReactedAt

	if err := r.session.Query(addReactionMsgByID, key, reactor, reactedAt, msg.MessageID, msg.CreatedAt).
		WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("add reaction to messages_by_id for message %s: %w", msg.MessageID, err)
	}

	if msg.ThreadParentID == "" {
		b := r.bucket.Of(msg.CreatedAt)
		if err := r.session.Query(addReactionMsgByRoom, key, reactor, reactedAt, msg.RoomID, b, msg.CreatedAt, msg.MessageID).
			WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("add reaction on messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
		}
		return nil
	}
	if err := r.session.Query(addReactionThreadMsg, key, reactor, reactedAt, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID).
		WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("add reaction on thread_messages_by_thread for message %s thread %s: %w", msg.MessageID, msg.ThreadRoomID, err)
	}
	return nil
}

// RemoveReaction deletes one (emoji, user_account) map-cell from messages_by_id
// and the room-or-thread mirror; idempotent on an absent cell.
func (r *Repository) RemoveReaction(ctx context.Context, msg *models.Message, key models.ReactionKey, updatedAt time.Time) error {
	if msg.ThreadParentID != "" && msg.ThreadRoomID == "" {
		return fmt.Errorf("unreact thread message %s: ThreadParentID %q is set but ThreadRoomID is empty", msg.MessageID, msg.ThreadParentID)
	}

	if err := r.session.Query(removeReactionMsgByID, key, msg.MessageID, msg.CreatedAt).
		WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("remove reaction from messages_by_id for message %s: %w", msg.MessageID, err)
	}
	if err := r.session.Query(touchUpdatedAtMsgByID, updatedAt, msg.MessageID, msg.CreatedAt).
		WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("touch updated_at on messages_by_id for message %s: %w", msg.MessageID, err)
	}

	if msg.ThreadParentID == "" {
		b := r.bucket.Of(msg.CreatedAt)
		if err := r.session.Query(removeReactionMsgByRoom, key, msg.RoomID, b, msg.CreatedAt, msg.MessageID).
			WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("remove reaction on messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
		}
		if err := r.session.Query(touchUpdatedAtMsgByRoom, updatedAt, msg.RoomID, b, msg.CreatedAt, msg.MessageID).
			WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("touch updated_at on messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
		}
		return nil
	}
	if err := r.session.Query(removeReactionThreadMsg, key, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID).
		WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("remove reaction on thread_messages_by_thread for message %s thread %s: %w", msg.MessageID, msg.ThreadRoomID, err)
	}
	if err := r.session.Query(touchUpdatedAtThreadMsg, updatedAt, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID).
		WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("touch updated_at on thread_messages_by_thread for message %s thread %s: %w", msg.MessageID, msg.ThreadRoomID, err)
	}
	return nil
}
