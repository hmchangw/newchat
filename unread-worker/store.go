package main

import (
	"context"
	"time"
)

// No mockgen directive: the tests use hand-written stubs (flush_test.go) because
// they assert call order and context cancellation, which gomock expectations do
// not express here. A generated mock would be regenerated but never compiled
// against.

// Store is the unread-state write surface. Every method issues a single unordered
// BulkWrite and is safe to replay out of order — the flush path retries whole
// batches, so any write may be applied more than once and after a newer one.
type Store interface {
	// BulkUpdateRoomLastMessage sets rooms.lastMsgAt/lastMsgId/updatedAt,
	// skipping any room already at or beyond the supplied time so a stale
	// replay cannot regress the pointer. A non-zero lastMentionAllAt is written
	// separately, via $max and matched on _id alone: the @all badge is its own
	// monotonic dimension and must survive a replay that loses the pointer race.
	BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error
	// BulkAdvanceLastSeen advances each subscription's lastSeenAt via $max, so
	// it never regresses a user who has already read further.
	BulkAdvanceLastSeen(ctx context.Context, updates map[subKey]time.Time) error
	// BulkSetMentions flags each subscription as mentioned unless that account
	// already read past the mentioning message — otherwise an async mention
	// write can clobber a read-clear that happened first (#467).
	BulkSetMentions(ctx context.Context, updates map[subKey]time.Time) error
}
