package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// Subset of columns present in thread_messages_by_room (no tshow, thread_parent_created_at, or pinned_* columns).
const threadMessageColumns = "room_id, thread_room_id, created_at, message_id, thread_parent_id, " +
	"sender, target_user, msg, mentions, attachments, file, card, card_action, " +
	"quoted_parent_message, visible_to, reactions, deleted, " +
	"type, sys_msg_data, site_id, edited_at, updated_at"

func (r *Repository) GetThreadMessages(
	ctx context.Context, roomID, threadRoomID string,
	before time.Time, floor time.Time,
	pageReq PageRequest,
) (Page[models.Message], error) {
	floorBucket := r.bucket.Of(floor)
	startBucket, initialPageState, err := startBucketFromCursor(pageReq, walkDesc, r.bucket.Of(before), floorBucket)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get thread messages: %w", err)
	}

	queryFn := func(bucket int64, firstBucket bool) *gocql.Query {
		if firstBucket {
			return r.session.Query(
				"SELECT "+threadMessageColumns+
					` FROM thread_messages_by_room WHERE room_id = ? AND bucket = ? AND thread_room_id = ? AND created_at < ? ORDER BY created_at DESC`,
				roomID, bucket, threadRoomID, before,
			)
		}
		return r.session.Query(
			"SELECT "+threadMessageColumns+
				` FROM thread_messages_by_room WHERE room_id = ? AND bucket = ? AND thread_room_id = ? ORDER BY created_at DESC`,
			roomID, bucket, threadRoomID,
		)
	}

	res, err := fillPage[models.Message](
		ctx, r.bucket, walkDesc, startBucket, floorBucket, r.maxBuckets,
		pageReq.PageSize, initialPageState, queryFn, scanMessagesUpTo,
	)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get thread messages: %w", err)
	}
	return res.toPage(), nil
}
