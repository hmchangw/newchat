// Package threadcount derives the exact thread reply count (tcount) from the
// Cassandra thread_messages_by_thread partition. Shared by message-worker
// (reply-add) and history-service (reply-delete) so the two writers can't diverge.
package threadcount

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

const (
	// Rows per round trip (gocql pages by row count, not bytes) — bounds memory per
	// fetch, not the total counted. Both projected columns are fixed-width.
	cassPageSize = 5000

	// Ceiling on a whole scan: above gocql's 10s per-round-trip timeout so a slow
	// page still completes, below the 25s shutdown budget so it can't outlive shutdown.
	scanTimeout = 15 * time.Second
)

// Count returns the number of non-deleted replies in threadRoomID's partition.
func Count(ctx context.Context, session *gocql.Session, threadRoomID string) (int, error) {
	n, _, err := CountAndLatest(ctx, session, threadRoomID)
	return n, err
}

// CountAndLatest returns the exact non-deleted reply count and the latest surviving
// reply's created_at (nil when none survive). Scans the full partition under
// scanTimeout, so cost grows with thread length; session must have its Keyspace set.
func CountAndLatest(ctx context.Context, session *gocql.Session, threadRoomID string) (int, *time.Time, error) {
	// Backstop for an unbounded partition: no caller sets a deadline, and gocql's
	// timeout is per round trip, not per scan. Only ever shortens a caller's ctx.
	ctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()

	// No LIMIT/COUNT(*): soft-deleted rows live in the partition and must be
	// walked past rather than counted.
	iter := session.Query(
		`SELECT deleted, created_at FROM thread_messages_by_thread WHERE thread_room_id = ?`,
		threadRoomID,
	).WithContext(ctx).PageSize(cassPageSize).Iter()

	var (
		deleted   *bool
		createdAt time.Time
		latest    *time.Time
	)
	n := 0
	for iter.Scan(&deleted, &createdAt) {
		// message-worker omits deleted on INSERT, so NULL means not-deleted.
		if deleted == nil || !*deleted {
			n++
			if latest == nil || createdAt.After(*latest) {
				t := createdAt
				latest = &t
			}
		}
	}
	if err := iter.Close(); err != nil {
		return 0, nil, fmt.Errorf("count thread replies for thread %s: %w", threadRoomID, err)
	}
	return n, latest, nil
}
