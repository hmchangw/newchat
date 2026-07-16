package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
)

// lastMessageScanMaxRows bounds the TOTAL rows examined per walk (across
// buckets). Product decision: the preview looks back at most 10 rows — if
// the 10 newest candidate rows are all deleted/system, the room simply shows
// no preview (the next real message self-heals it). Deliberately tiny: it
// keeps the per-delete Cassandra cost near-constant, and doubles as the
// fetch page size so a walk never transfers rows it won't examine.
const lastMessageScanMaxRows = 10

// lastMessageSkipTypes are the type values that never qualify as a room's
// last-message preview: the canonical system set (model.SystemMessageTypes)
// plus the repo-local removed-thread-parent placeholder. Derived, not
// hand-enumerated, so a newly added system type cannot silently leak into
// previews. Ordinary user messages carry an empty type (see
// docs/client-api.md — "regular messages omit it"), so the filter is a
// skip-set: anything listed here is passed over, everything else qualifies.
var lastMessageSkipTypes = func() map[string]struct{} {
	s := make(map[string]struct{}, len(model.SystemMessageTypes)+1)
	for typ := range model.SystemMessageTypes {
		s[typ] = struct{}{}
	}
	s[MessageTypeRemoved] = struct{}{}
	return s
}()

// GetLastRoomMessage walks messages_by_room newest→oldest from before down to
// floor and resolves two values in one pass:
//
//   - pointer: the first row that is neither soft-deleted nor a
//     removed-parent placeholder — system notices INCLUDED. This is the value
//     rooms.{lastMsgId,lastMsgAt} track for room sorting.
//   - message: the first row that additionally is not a system type — the
//     preview row, decrypted like every other read.
//
// Returns (nil, nil, nil) when nothing qualifies within the bounds (all
// deleted/removed, empty room, budget or maxBuckets exhausted); a non-nil
// pointer with a nil message means only system messages survive. A row older
// than floor terminates the walk — DESC order proves nothing newer remains,
// so out-of-window history can never leak out. Unlike the paginated readers
// it does not use fillPage: there is no cursor to hand back, and gocql's
// iterator already drains a bucket transparently page by page.
func (r *Repository) GetLastRoomMessage(ctx context.Context, roomID string, before, floor time.Time) (*model.LastMessagePointer, *models.Message, error) {
	floorBucket := r.bucket.Of(floor)
	bucket := r.bucket.Of(before)

	remaining := lastMessageScanMaxRows
	var pointer *model.LastMessagePointer
	for walked := 0; walked < r.maxBuckets && bucket >= floorBucket && remaining > 0; walked++ {
		var q *gocql.Query
		if walked == 0 {
			q = r.session.Query(
				messageByRoomQuery+` WHERE room_id = ? AND bucket = ? AND created_at < ? ORDER BY created_at DESC`,
				roomID, bucket, before,
			)
		} else {
			q = r.session.Query(
				messageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at DESC`,
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

// scanBucket drains iter resolving the pointer row (first non-deleted,
// non-removed row of any type — captured only while needPointer) and the
// preview row (first row additionally not a system type), decrypting only
// the preview winner. Skipped rows are filtered on the plaintext deleted/type
// columns. Every examined row spends one unit of *remaining. stop=true ends
// the whole walk: the budget ran out, or a row older than floor was seen
// (DESC order — everything after is older still).
func (r *Repository) scanBucket(ctx context.Context, iter *gocql.Iter, floor time.Time, remaining *int, needPointer bool) (ptr *model.LastMessagePointer, found *models.Message, stop bool, err error) {
	for {
		if *remaining <= 0 {
			if closeErr := iter.Close(); closeErr != nil {
				return ptr, nil, true, fmt.Errorf("close iterator: %w", closeErr)
			}
			return ptr, nil, true, nil
		}
		var m models.Message
		ok, scanErr := structScan(iter, &m)
		if scanErr != nil {
			// Preserve any transport error alongside the scan failure.
			if closeErr := iter.Close(); closeErr != nil {
				return ptr, nil, false, fmt.Errorf("close after scan failure (%w): %w", scanErr, closeErr)
			}
			return ptr, nil, false, scanErr
		}
		if !ok {
			break
		}
		*remaining--
		if m.CreatedAt.Before(floor) {
			if closeErr := iter.Close(); closeErr != nil {
				return ptr, nil, true, fmt.Errorf("close iterator: %w", closeErr)
			}
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
		if closeErr := iter.Close(); closeErr != nil {
			return ptr, nil, false, fmt.Errorf("close iterator: %w", closeErr)
		}
		if decErr := r.decryptIfNeeded(ctx, &m); decErr != nil {
			return ptr, nil, false, fmt.Errorf("decrypt last-message row: %w", decErr)
		}
		return ptr, &m, false, nil
	}
	if closeErr := iter.Close(); closeErr != nil {
		return ptr, nil, false, fmt.Errorf("close iterator: %w", closeErr)
	}
	return ptr, nil, false, nil
}
