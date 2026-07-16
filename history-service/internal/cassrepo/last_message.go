package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
)

// lastMessageSkipTypes excludes system types from the PREVIEW (the pointer keeps them). Exactly
// model.SystemMessageTypeSet; MessageTypeRemoved is excluded earlier in scanBucket, so it needs no entry.
var lastMessageSkipTypes = model.SystemMessageTypeSet()

// lastMessageColumns is the walk's minimal projection (row selection + preview + decrypt), narrower than baseColumns to cut IO on the delete-path walk.
const lastMessageColumns = "room_id, created_at, message_id, sender, msg, attachments, " +
	"type, deleted, edited_at, enc_payload, enc_meta"

const lastMessageByRoomQuery = "SELECT " + lastMessageColumns + " FROM messages_by_room"

// GetLastRoomMessage walks messages_by_room newest→oldest in [floor, before) for two
// rows: the pointer (newest non-deleted row, system INCLUDED) and preview (newest non-system).
func (r *Repository) GetLastRoomMessage(ctx context.Context, roomID string, before, floor time.Time) (*model.LastMessagePointer, *models.Message, error) {
	floorBucket := r.bucket.Of(floor)
	bucket := r.bucket.Of(before)

	// previewLookbackRows caps TOTAL rows examined across buckets (all deleted/system ⇒ no preview); also the page size.
	remaining := r.previewLookbackRows
	var pointer *model.LastMessagePointer
	for walked := 0; walked < r.maxBuckets && bucket >= floorBucket && remaining > 0; walked++ {
		var q *gocql.Query
		if walked == 0 {
			q = r.session.Query(
				lastMessageByRoomQuery+` WHERE room_id = ? AND bucket = ? AND created_at < ? ORDER BY created_at DESC`,
				roomID, bucket, before,
			)
		} else {
			q = r.session.Query(
				lastMessageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at DESC`,
				roomID, bucket,
			)
		}

		iter := q.WithContext(ctx).PageSize(remaining).Iter()
		ptr, found, stop, err := r.scanBucket(ctx, iter, floor, &remaining, pointer == nil)
		if err != nil {
			return nil, nil, fmt.Errorf("get last room message: scan bucket %d: %w", bucket, err)
		}
		if pointer == nil {
			pointer = ptr
		}
		if found != nil {
			return pointer, found, nil
		}
		if stop {
			break
		}
		bucket = r.bucket.Prev(bucket)
	}
	return pointer, nil, nil
}

// scanBucket drains iter for the pointer (newest non-deleted row, any type) and preview
// (newest non-system) rows. stop=true ends the walk: budget spent, or a row older than floor seen (DESC).
func (r *Repository) scanBucket(ctx context.Context, iter *gocql.Iter, floor time.Time, remaining *int, needPointer bool) (ptr *model.LastMessagePointer, found *models.Message, stop bool, err error) {
	// Single close on every return path; a Close transport error is combined with, never overwrites, a prior error.
	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("close after error (%w): %w", err, closeErr)
			} else {
				err = fmt.Errorf("close iterator: %w", closeErr)
			}
		}
	}()
	for {
		if *remaining <= 0 {
			return ptr, nil, true, nil
		}
		var m models.Message
		ok, scanErr := structScan(iter, &m)
		if scanErr != nil {
			return ptr, nil, false, fmt.Errorf("scan message row: %w", scanErr)
		}
		if !ok {
			break
		}
		*remaining--
		if m.CreatedAt.Before(floor) {
			return ptr, nil, true, nil
		}
		if m.Deleted || m.Type == MessageTypeRemoved {
			continue
		}
		if needPointer && ptr == nil {
			ptr = &model.LastMessagePointer{MessageID: m.MessageID, CreatedAt: m.CreatedAt}
		}
		if _, skip := lastMessageSkipTypes[m.Type]; skip {
			continue
		}
		if decErr := r.decryptIfNeeded(ctx, &m); decErr != nil {
			return ptr, nil, false, fmt.Errorf("decrypt last-message row: %w", decErr)
		}
		return ptr, &m, false, nil
	}
	return ptr, nil, false, nil
}
