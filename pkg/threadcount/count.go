// Package threadcount derives the thread reply-count badge (tcount) from the
// Cassandra thread_messages_by_thread partition. Counting stops at Cap, so the
// per-write cost is bounded to ~Cap surviving rows (plus any soft-deleted rows
// clustered ahead of them) rather than the whole partition. message-worker
// (reply-add path) uses Count; history-service (reply-delete path) uses
// CountAndLatest, which also returns the latest surviving reply's timestamp.
// Both share the same Cap, so the two authoritative writers of tcount can never
// compute different counts.
package threadcount

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

// Cap is the maximum value Count returns. A thread with more than Cap
// non-deleted replies reports exactly Cap; the frontend renders tcount >= Cap as
// "Cap+". 99 keeps virtually all real threads exact while bounding the per-write
// partition read to ~Cap rows.
const Cap = 99

// Count returns min(non-deleted replies in threadRoomID's partition, Cap).
//
// It is CountAndLatest without the latest-survivor timestamp — the add path
// (message-worker) needs only the count. created_at is a clustering key, so
// selecting it for the discarded timestamp adds no meaningful read cost, and
// sharing one scan keeps the two writers' counts provably identical.
func Count(ctx context.Context, session *gocql.Session, threadRoomID string) (int, error) {
	n, _, err := CountAndLatest(ctx, session, threadRoomID)
	return n, err
}

// CountAndLatest returns min(non-deleted replies in threadRoomID's partition,
// Cap) and the created_at of the latest non-deleted reply (nil when none
// survive), from a single bounded scan. The delete path (history-service) needs
// both: removing a reply can change the count and the thread-last-message
// timestamp (tlm).
//
// It scans thread_messages_by_thread in clustering order and stops once the
// non-deleted tally reaches Cap, so the read materializes at most ~Cap rows
// (plus any soft-deleted rows interspersed before them). No CQL LIMIT is used:
// soft-deleted rows (deleted = true) live in the partition, so a hard LIMIT
// could return Cap rows of which some are deleted and undercount the live total.
// The deleted column may be NULL (message-worker does not write it on INSERT),
// so NULL is treated as not-deleted. The partition's DESC clustering order
// (created_at DESC) places the latest surviving reply first, so the bounded scan
// still observes the true maximum created_at. session must have its Keyspace set.
func CountAndLatest(ctx context.Context, session *gocql.Session, threadRoomID string) (int, *time.Time, error) {
	iter := session.Query(
		`SELECT deleted, created_at FROM thread_messages_by_thread WHERE thread_room_id = ?`,
		threadRoomID,
	).WithContext(ctx).PageSize(Cap).Iter()

	var (
		deleted   *bool
		createdAt time.Time
		latest    *time.Time
	)
	n := 0
	for n < Cap && iter.Scan(&deleted, &createdAt) {
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
