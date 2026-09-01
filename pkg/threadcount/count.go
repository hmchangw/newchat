// Package threadcount maintains the thread reply count (tcount) stamped on a
// thread parent, from the Cassandra thread_messages_by_thread partition.
// Shared by message-worker and bot-message-worker (reply add) and
// history-service (reply delete) via Maintain, which owns the whole policy so
// the three writers cannot drift apart.
//
// Maintenance is circuit-broken on thread length: a thread shorter than
// Policy.ScanLimit is recounted from its partition, and past that the stamped
// count is adjusted by one instead, keeping per-reply cost independent of
// thread length. The resulting count is approximate above the limit; see
// Maintain and ShouldReanchor.
//
// The recount below the limit is exact, and idempotent under redelivery, only
// when the read reached the end of the partition. Soft-deleted replies keep
// their rows, so a read can fill its cap on tombstones alone while live replies
// survive past it; such a read cannot recount and falls back to adjusting, with
// the same redelivery guard the approximate path uses.
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

	// DefaultScanLimit is where exact counting stops. Under it a reply pays one
	// bounded scan over two narrow fixed-width columns in a single partition and
	// the count is exact, idempotent and self-healing; over it the scan is
	// skipped entirely and the count becomes an approximation.
	//
	// So this is chosen for how much exactness is worth, not for a cost cliff:
	// the approximate path is cheaper at every thread length, and the only thing
	// a higher limit buys is a wider range in which the count is exactly right
	// and repairs itself. 1000 keeps the scan well inside one page.
	DefaultScanLimit = 1000

	// DefaultReanchorBudget is the expected number of thread-partition rows one
	// reply contributes to re-anchoring, amortized. See ShouldReanchor.
	DefaultReanchorBudget = 50

	// DefaultReconcileRowLimit caps the PHYSICAL rows one re-anchor may read.
	//
	// ShouldReanchor prices a re-anchor at the live count, but the partition
	// holds every reply ever written: soft deletes stay. A thread with a
	// thousand survivors and a million tombstones would otherwise be sampled as
	// if a scan cost a thousand rows and charged a million, which is the
	// timeout this package exists to avoid. The cap keeps the worst case at
	// DefaultReconcileRowLimit * ReanchorBudget / ScanLimit rows per reply
	// amortized (~2500, about 4.5ms) instead of unbounded; a partition deeper
	// than this simply keeps its approximate count, which the design already
	// tolerates. At the measured 1.8us/row a full 50k-row read is ~92ms, well
	// inside scanTimeout.
	DefaultReconcileRowLimit = 50000
)

// Policy is the tuning a writer hands to Maintain. Writers hold one of these
// rather than loose ints so a new knob does not mean touching every store.
type Policy struct {
	// ScanLimit is the stamped count at or above which a reply stops recounting
	// the partition and starts adjusting the stamped value instead.
	ScanLimit int
	// ReanchorBudget is the expected partition rows one reply spends re-deriving
	// an approximate count from an exact scan; 0 disables re-anchoring.
	ReanchorBudget int
	// ReconcileRowLimit caps the physical rows a re-anchor may read before it
	// gives up rather than stamp a count it cannot verify; 0 removes the cap.
	ReconcileRowLimit int
}

// DefaultPolicy is the production tuning, shared by every writer.
func DefaultPolicy() Policy {
	return Policy{
		ScanLimit:         DefaultScanLimit,
		ReanchorBudget:    DefaultReanchorBudget,
		ReconcileRowLimit: DefaultReconcileRowLimit,
	}
}

// Parent locates the thread parent's two denormalized rows: messages_by_id is
// the authority, messages_by_room the mirror. Bucket is the caller's
// msgbucket.Sizer applied to CreatedAt.
type Parent struct {
	MessageID string
	RoomID    string
	CreatedAt time.Time
	Bucket    int64
}

// Result is what Maintain stamped. TLM is the thread_last_msg_at written, nil
// when the column was cleared or deliberately left alone.
type Result struct {
	Count int
	TLM   *time.Time
}

// countAndLatest counts a thread's surviving replies newest-first, reading at
// most limit rows so one reply cannot pay for the whole partition. limit 0
// removes the cap and walks to the end.
//
// complete reports whether the read reached the end of the partition. It is the
// only thing that makes the count trustworthy: the cap bounds rows READ, and
// soft-deleted replies still occupy rows, so a truncated read can return 0 with
// a nil latest while live replies survive just past the cap. A caller may stamp
// an exact count, and may clear thread_last_msg_at, only when complete.
//
// The counting happens here rather than in CQL because soft-deleted replies
// still occupy the partition: COUNT(*) would include them, and a LIMIT caps
// rows read, not rows alive. Latest is the newest survivor among the rows read,
// which the newest-first clustering makes correct for a complete read and a
// lower bound for a truncated one.
func countAndLatest(ctx context.Context, session *gocql.Session, threadRoomID string, limit int) (int, *time.Time, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()

	q := `SELECT deleted, created_at FROM thread_messages_by_thread WHERE thread_room_id = ?`
	args := []any{threadRoomID}
	if limit > 0 {
		// One row past the cap: reading it is how a truncated read is told
		// apart from a partition that simply ends on the boundary. It is
		// counted as evidence only, never into n.
		q += ` LIMIT ?`
		args = append(args, limit+1)
	}
	iter := session.Query(q, args...).WithContext(ctx).PageSize(cassPageSize).Iter()

	var (
		deleted   bool // NULL unmarshals to false; the write path omits the column
		createdAt time.Time
		latest    *time.Time
	)
	n, read, complete := 0, 0, true
	for iter.Scan(&deleted, &createdAt) {
		read++
		if limit > 0 && read > limit {
			complete = false
			break
		}
		if deleted {
			continue
		}
		n++
		if latest == nil || createdAt.After(*latest) {
			t := createdAt
			latest = &t
		}
	}
	if err := iter.Close(); err != nil {
		return 0, nil, false, fmt.Errorf("count thread replies for thread %s: %w", threadRoomID, err)
	}
	return n, latest, complete, nil
}
