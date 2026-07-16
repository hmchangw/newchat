package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
)

// lastMessageScanPageSize keeps each Cassandra round-trip generous: the walk
// wants a single row, but a naive PageSize(1) would degenerate into one
// round-trip per tombstone across a run of deleted/system rows.
const lastMessageScanPageSize = 100

// lastMessageScanMaxRows bounds the TOTAL rows examined per walk (across
// buckets): a deleted tail deeper than this degrades to "no preview" — the
// same ponytail trade-off as rooms.get (5 pages × 50). Without it a
// mass-deleted busy room would drain arbitrarily deep partitions on every
// delete fan-out.
const lastMessageScanMaxRows = 250

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
// floor and returns the first row that is neither soft-deleted nor a
// system-type message, decrypted like every other read. Returns (nil, nil)
// when no qualifying row exists within the bounds (all deleted/system, empty
// room, or maxBuckets exhausted). Unlike the paginated readers it does not use
// fillPage: there is no cursor to hand back, and gocql's iterator already
// drains a bucket transparently page by page.
func (r *Repository) GetLastRoomMessage(ctx context.Context, roomID string, before, floor time.Time) (*models.Message, error) {
	floorBucket := r.bucket.Of(floor)
	bucket := r.bucket.Of(before)

	remaining := lastMessageScanMaxRows
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

		iter := q.WithContext(ctx).PageSize(lastMessageScanPageSize).Iter()
		found, err := r.scanFirstQualifying(ctx, iter, &remaining)
		if err != nil {
			return nil, fmt.Errorf("get last room message: scan bucket %d: %w", bucket, err)
		}
		if found != nil {
			return found, nil
		}
		bucket = r.bucket.Prev(bucket)
	}
	return nil, nil
}

// scanFirstQualifying drains iter until it hits the first non-deleted,
// non-system row, decrypts it, and returns it; (nil, nil) when the bucket has
// no qualifying row. Skipped rows are filtered on the plaintext deleted/type
// columns, so only the winning row pays a decrypt. Every examined row spends
// one unit of *remaining; when the budget hits zero the scan stops early —
// the caller degrades to "no preview".
func (r *Repository) scanFirstQualifying(ctx context.Context, iter *gocql.Iter, remaining *int) (*models.Message, error) {
	for *remaining > 0 {
		var m models.Message
		ok, err := structScan(iter, &m)
		if err != nil {
			// Preserve any transport error alongside the scan failure.
			if closeErr := iter.Close(); closeErr != nil {
				return nil, fmt.Errorf("close after scan failure (%w): %w", err, closeErr)
			}
			return nil, err
		}
		if !ok {
			break
		}
		*remaining--
		if m.Deleted {
			continue
		}
		if _, skip := lastMessageSkipTypes[m.Type]; skip {
			continue
		}
		if closeErr := iter.Close(); closeErr != nil {
			return nil, fmt.Errorf("close iterator: %w", closeErr)
		}
		if err := r.decryptIfNeeded(ctx, &m); err != nil {
			return nil, fmt.Errorf("decrypt last-message row: %w", err)
		}
		return &m, nil
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("close iterator: %w", err)
	}
	return nil, nil
}
