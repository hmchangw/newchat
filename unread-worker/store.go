package main

import (
	"context"
	"time"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store

// Store is the unread-state write surface. Every method issues a single unordered
// BulkWrite and is safe to replay out of order — the flush path retries whole
// batches, so any write may be applied more than once and after a newer one.
type Store interface {
	// BulkUpdateRoomLastMessage sets rooms.lastMsgAt/lastMsgId/updatedAt (and
	// lastMentionAllAt when non-zero), skipping any room already at or beyond
	// the supplied time so a stale replay cannot regress the pointer.
	BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error
	// BulkAdvanceLastSeen advances each subscription's lastSeenAt via $max, so
	// it never regresses a user who has already read further.
	BulkAdvanceLastSeen(ctx context.Context, updates map[subKey]time.Time) error
	// BulkSetMentions flags each subscription as mentioned unless that account
	// already read past the mentioning message — otherwise an async mention
	// write can clobber a read-clear that happened first (#467).
	BulkSetMentions(ctx context.Context, updates map[subKey]time.Time) error
}
