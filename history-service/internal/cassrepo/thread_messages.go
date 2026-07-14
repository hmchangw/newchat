package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// Subset of columns present in thread_messages_by_thread (no thread_parent_created_at or pinned_* columns).
const threadMessageColumns = "room_id, thread_room_id, created_at, message_id, thread_parent_id, " +
	"sender, msg, mentions, attachments, card, card_action, " +
	"quoted_parent_message, visible_to, reactions, deleted, " +
	"type, sys_msg_data, site_id, edited_at, updated_at, " +
	"tshow, enc_payload, enc_meta"

// Cross-room safety lives in the service layer (findMessage's msg.RoomID check) —
// the table partitions by thread_room_id alone so room_id never enters the query.
func (r *Repository) GetThreadMessages(
	ctx context.Context, threadRoomID string,
	before time.Time, floor time.Time,
	pageReq PageRequest,
) (Page[models.Message], error) {
	q := r.session.Query(
		"SELECT "+threadMessageColumns+
			` FROM thread_messages_by_thread WHERE thread_room_id = ? AND created_at < ? AND created_at >= ? ORDER BY created_at DESC`,
		threadRoomID, before, floor,
	).WithContext(ctx)

	scan := r.scanMessagesUpTo(ctx)
	var rows []models.Message
	var scanErr error
	nextCursor, err := NewQueryBuilder(q).
		WithCursor(pageReq.Cursor).
		WithPageSize(pageReq.PageSize).
		Fetch(func(iter *gocql.Iter) {
			rows, scanErr = scan(iter, pageReq.PageSize)
		})
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get thread messages: %w", err)
	}
	if scanErr != nil {
		return Page[models.Message]{}, fmt.Errorf("get thread messages: scan: %w", scanErr)
	}

	return Page[models.Message]{
		Data:       rows,
		NextCursor: nextCursor,
		HasNext:    nextCursor != "",
	}, nil
}
