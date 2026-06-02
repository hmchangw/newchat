package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
)

const (
	pinMsgByID   = `UPDATE messages_by_id SET pinned_at = ?, pinned_by = ? WHERE message_id = ? AND created_at = ?`
	unpinMsgByID = `UPDATE messages_by_id SET pinned_at = null, pinned_by = null WHERE message_id = ? AND created_at = ?`

	insertPinnedMsg = `INSERT INTO pinned_messages_by_room (
		room_id, pinned_at, message_id, sender, msg, mentions,
		attachments, file, card, card_action, quoted_parent_message, visible_to,
		reactions, deleted, type, sys_msg_data, site_id, edited_at, updated_at, pinned_by,
		created_at, tshow, thread_parent_id, thread_parent_created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	deletePinnedRow = `DELETE FROM pinned_messages_by_room WHERE room_id = ? AND pinned_at = ? AND message_id = ?`

	pinnedColumns = "room_id, pinned_at, message_id, sender, " +
		"msg, mentions, attachments, file, card, card_action, quoted_parent_message, " +
		"visible_to, reactions, deleted, type, sys_msg_data, site_id, edited_at, " +
		"updated_at, pinned_by, created_at, tshow, thread_parent_id, thread_parent_created_at"

	pinnedByRoomQuery = "SELECT " + pinnedColumns + " FROM pinned_messages_by_room WHERE room_id = ?"
)

// PinMessage writes the pin via a single UnloggedBatch across messages_by_id
// and pinned_messages_by_room — transport grouping, not atomic; half-apply on
// coordinator failure is possible (caller-side heal in service/pin.go).
func (r *Repository) PinMessage(ctx context.Context, msg *models.Message, pinnedAt time.Time, pinnedBy models.Participant) error { //nolint:gocritic // hugeParam: Participant is passed by value to match the service.MessageWriter interface
	batch := r.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(pinMsgByID, pinnedAt, pinnedBy, msg.MessageID, msg.CreatedAt)
	batch.Query(insertPinnedMsg,
		msg.RoomID, pinnedAt, msg.MessageID, msg.Sender, msg.Msg, msg.Mentions,
		msg.Attachments, msg.File, msg.Card, msg.CardAction, msg.QuotedParentMessage, msg.VisibleTo,
		msg.Reactions, msg.Deleted, msg.Type, msg.SysMsgData, msg.SiteID, msg.EditedAt, msg.UpdatedAt, pinnedBy,
		msg.CreatedAt, msg.TShow, msg.ThreadParentID, msg.ThreadParentCreatedAt,
	)
	if err := r.session.ExecuteBatch(batch); err != nil {
		// Name both tables so on-call knows which to read back on half-apply.
		return fmt.Errorf("pin message %s in room %s via batch(messages_by_id, pinned_messages_by_room): %w", msg.MessageID, msg.RoomID, err)
	}
	return nil
}

// UnpinMessage clears the pin via UnloggedBatch (msg.PinnedAt keys the pinned row to DELETE).
func (r *Repository) UnpinMessage(ctx context.Context, msg *models.Message) error {
	if msg.PinnedAt == nil {
		return fmt.Errorf("unpin message %s: PinnedAt is nil", msg.MessageID)
	}
	batch := r.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(unpinMsgByID, msg.MessageID, msg.CreatedAt)
	batch.Query(deletePinnedRow, msg.RoomID, *msg.PinnedAt, msg.MessageID)
	if err := r.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("unpin message %s in room %s via batch(messages_by_id, pinned_messages_by_room): %w", msg.MessageID, msg.RoomID, err)
	}
	return nil
}

// GetPinnedMessages returns one page of pins for a room (redaction is service-side).
func (r *Repository) GetPinnedMessages(ctx context.Context, roomID string, pageReq PageRequest) (Page[models.Message], error) {
	builder := NewQueryBuilder(r.session.Query(pinnedByRoomQuery, roomID).WithContext(ctx)).
		WithPageSize(pageReq.PageSize).
		WithCursor(pageReq.Cursor)

	rows := make([]models.Message, 0, pageReq.PageSize)
	var scanErr error
	nextCursor, err := builder.Fetch(func(iter *gocql.Iter) {
		for {
			var m models.Message
			found, sErr := structScan(iter, &m)
			if sErr != nil {
				scanErr = sErr
				return
			}
			if !found {
				break
			}
			rows = append(rows, m)
		}
	})
	if scanErr != nil {
		return Page[models.Message]{}, fmt.Errorf("scan pinned message row for room %s: %w", roomID, scanErr)
	}
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("query pinned messages for room %s: %w", roomID, err)
	}
	return Page[models.Message]{Data: rows, NextCursor: nextCursor, HasNext: nextCursor != ""}, nil
}

// GetAllPinnedMessages returns every pin for a room (internal: cap + orphan scan need it all).
func (r *Repository) GetAllPinnedMessages(ctx context.Context, roomID string) ([]models.Message, error) {
	iter := r.session.Query(pinnedByRoomQuery, roomID).WithContext(ctx).Iter()
	rows := make([]models.Message, 0)
	for {
		var m models.Message
		found, scanErr := structScan(iter, &m)
		if scanErr != nil {
			_ = iter.Close()
			return nil, fmt.Errorf("scan pinned message row for room %s: %w", roomID, scanErr)
		}
		if !found {
			break
		}
		rows = append(rows, m)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("query all pinned messages for room %s: %w", roomID, err)
	}
	return rows, nil
}
