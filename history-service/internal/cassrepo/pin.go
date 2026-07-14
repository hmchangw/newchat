package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
)

const (
	pinMsgByID   = `UPDATE messages_by_id SET pinned_at = ?, pinned_by = ? WHERE message_id = ?`
	unpinMsgByID = `UPDATE messages_by_id SET pinned_at = null, pinned_by = null WHERE message_id = ?`

	// messages_by_room mirrors pinned_at only (the timeline indicator needs no
	// more); pinned_by stays a point lookup on messages_by_id.
	pinMsgByRoom   = `UPDATE messages_by_room SET pinned_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	unpinMsgByRoom = `UPDATE messages_by_room SET pinned_at = null WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`

	// reactions is intentionally absent: the pinned panel does not render reactions;
	// callers that need them side-fetch from messages_by_id.
	insertPinnedMsg = `INSERT INTO pinned_messages_by_room (
		room_id, pinned_at, message_id, sender, msg, mentions,
		attachments, card, card_action, quoted_parent_message, visible_to,
		deleted, type, sys_msg_data, site_id, edited_at, updated_at, pinned_by,
		created_at, tshow, thread_parent_id, thread_parent_created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	deletePinnedRow = `DELETE FROM pinned_messages_by_room WHERE room_id = ? AND pinned_at = ? AND message_id = ?`

	pinnedColumns = "room_id, pinned_at, message_id, sender, " +
		"msg, mentions, attachments, card, card_action, quoted_parent_message, " +
		"visible_to, deleted, type, sys_msg_data, site_id, edited_at, " +
		"updated_at, pinned_by, created_at, tshow, thread_parent_id, thread_parent_created_at"

	pinnedByRoomQuery = "SELECT " + pinnedColumns + " FROM pinned_messages_by_room WHERE room_id = ?"
)

// hasRoomTimelineRow reports whether msg has a messages_by_room row: channel
// messages and tshow=true thread replies do; tshow=false thread replies live
// only in thread_messages_by_thread. The pin mirror must gate on this because
// a Cassandra UPDATE is an upsert — an unguarded pinned_at write for a
// thread-only reply would create a ghost timeline row holding nothing but the
// primary key and pin column.
func hasRoomTimelineRow(msg *models.Message) bool {
	return msg.ThreadParentID == "" || msg.TShow
}

// pinBatchTables names the tables in a pin/unpin batch so on-call knows which
// to read back on half-apply.
func pinBatchTables(withRoomRow bool) string {
	if withRoomRow {
		return "messages_by_id, pinned_messages_by_room, messages_by_room"
	}
	return "messages_by_id, pinned_messages_by_room"
}

// PinMessage writes the pin via a single UnloggedBatch across messages_by_id,
// pinned_messages_by_room and (for messages with a timeline row) the
// messages_by_room pinned_at mirror — transport grouping, not atomic;
// half-apply on coordinator failure is possible (caller-side heal in
// service/pin.go).
func (r *Repository) PinMessage(ctx context.Context, msg *models.Message, pinnedAt time.Time, pinnedBy models.Participant) error { //nolint:gocritic // hugeParam: Participant is passed by value to match the service.MessageWriter interface
	batch := r.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(pinMsgByID, pinnedAt, pinnedBy, msg.MessageID)
	batch.Query(insertPinnedMsg,
		msg.RoomID, pinnedAt, msg.MessageID, msg.Sender, msg.Msg, msg.Mentions,
		msg.Attachments, msg.Card, msg.CardAction, msg.QuotedParentMessage, msg.VisibleTo,
		msg.Deleted, msg.Type, msg.SysMsgData, msg.SiteID, msg.EditedAt, msg.UpdatedAt, pinnedBy,
		msg.CreatedAt, msg.TShow, msg.ThreadParentID, msg.ThreadParentCreatedAt,
	)
	withRoomRow := hasRoomTimelineRow(msg)
	if withRoomRow {
		batch.Query(pinMsgByRoom, pinnedAt, msg.RoomID, r.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.MessageID)
	}
	if err := r.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("pin message %s in room %s via batch(%s): %w", msg.MessageID, msg.RoomID, pinBatchTables(withRoomRow), err)
	}
	return nil
}

// UnpinMessage clears the pin via UnloggedBatch (msg.PinnedAt keys the pinned row to DELETE).
func (r *Repository) UnpinMessage(ctx context.Context, msg *models.Message) error {
	if msg.PinnedAt == nil {
		return fmt.Errorf("unpin message %s: PinnedAt is nil", msg.MessageID)
	}
	batch := r.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(unpinMsgByID, msg.MessageID)
	batch.Query(deletePinnedRow, msg.RoomID, *msg.PinnedAt, msg.MessageID)
	withRoomRow := hasRoomTimelineRow(msg)
	if withRoomRow {
		// Setting pinned_at = null on the existing row only writes a tombstone;
		// the guard still applies so a thread-only reply doesn't get one.
		batch.Query(unpinMsgByRoom, msg.RoomID, r.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.MessageID)
	}
	if err := r.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("unpin message %s in room %s via batch(%s): %w", msg.MessageID, msg.RoomID, pinBatchTables(withRoomRow), err)
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
