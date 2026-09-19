// Package threadcount keeps the reply count on a thread's parent message up to
// date, working from the replies stored in Cassandra. All three writers use it —
// message-worker and bot-message-worker when a reply is added, history-service
// when one is deleted — so none of them can drift apart.
//
// The work is capped by thread length. A thread shorter than Policy.ScanLimit
// has its replies counted directly; past that the saved number is moved by one
// instead, so the cost per reply stops growing with the thread. That makes a
// long thread's count an estimate; see Maintain and ShouldReanchor.
//
// Counting a short thread is exact, and safe to repeat, only when the read got
// all the way through the thread. A deleted reply still leaves a row behind, so
// a read can use up its whole allowance on those while live replies sit just out
// of reach. Such a read cannot be trusted as a count and falls back to moving
// the number by one, with the same retry guard the long-thread route uses —
// unless that would land on zero, which a read that stopped early can never
// justify, and which readers act on by not looking at the replies at all. That
// one case pays for counting the whole thread rather than call a live thread
// empty.
package threadcount

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

const (
	// Rows fetched per round trip (the driver batches by row count, not bytes).
	// This bounds memory per fetch, not the total counted; both columns read are
	// fixed width.
	cassPageSize = 5000

	// Time limit for a whole read: above the driver's 10s per-round-trip timeout
	// so a slow batch still finishes, below the 25s shutdown budget so it cannot
	// outlive a shutdown.
	scanTimeout = 15 * time.Second

	// DefaultScanLimit is where counting stops. Below it, a reply pays for one
	// capped read of two narrow columns and the count comes out exact,
	// self-correcting, and safe to repeat; above it the read is skipped and the
	// count becomes an estimate.
	//
	// The number is chosen for how much accuracy is worth, not to dodge a cost
	// cliff: skipping the read is cheaper at every thread length, and all a
	// higher limit buys is a wider range in which the count is exactly right and
	// fixes itself. 1000 keeps the read inside a single round trip.
	DefaultScanLimit = 1000

	// DefaultReanchorBudget is how many rows one reply contributes, on average,
	// towards recounting the thread. See ShouldReanchor.
	DefaultReanchorBudget = 50

	// DefaultReconcileRowLimit caps how many rows one full recount may read.
	//
	// ShouldReanchor decides how often to recount based on the live reply count,
	// but the thread keeps a row for every reply ever written, deleted ones
	// included. A thread with a thousand live replies and a million deleted ones
	// would be treated as if counting cost a thousand rows while actually costing
	// a million — the timeout this package exists to avoid. The cap holds the
	// worst case to roughly 2500 rows per reply on average (about 4.5ms) instead
	// of letting it run away; a thread with more rows than this simply keeps its
	// estimate, which the design already allows. At the measured 1.8us per row a
	// full 50k-row read is about 92ms, comfortably inside the time limit.
	DefaultReconcileRowLimit = 50000
)

// Policy is the tuning a writer hands to Maintain. Writers hold one of these
// rather than loose numbers, so adding a setting does not mean touching every
// writer.
type Policy struct {
	// ScanLimit is the reply count at or above which a reply stops counting the
	// thread and starts moving the saved number instead. Raising it widens the
	// range where the count is exact and self-correcting, and makes every reply
	// under the new limit pay for a longer read.
	ScanLimit int
	// ReanchorBudget is how many rows one reply spends, on average, replacing the
	// estimate with a real count; 0 turns that off.
	ReanchorBudget int
	// ReconcileRowLimit caps how many rows a full recount may read before giving
	// up rather than save a number it cannot verify; 0 removes the cap.
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

// Parent locates everything this package touches for one thread: the two copies
// of the parent message, and the replies themselves.
//
// Build it with NewParent rather than by hand. Bucket has to be worked out from
// the very same CreatedAt that gets written, and filling both fields separately
// is the one mistake here that silently writes to the wrong row.
type Parent struct {
	MessageID string
	RoomID    string
	CreatedAt time.Time
	Bucket    int64
	// ThreadRoomID names where this parent's replies are kept. It travels with
	// the parent because every route needs both.
	ThreadRoomID string
}

// NewParent locates a thread parent. Deriving Bucket here is the point: no
// caller can bucket one time and write another.
//
// It returns a pointer because the whole package passes the parent around
// read-only, and copying it into every step of the walk is wasted work on a
// path that runs per reply.
func NewParent(messageID, roomID, threadRoomID string, createdAt time.Time, bucket msgbucket.Sizer) *Parent {
	return &Parent{
		MessageID:    messageID,
		RoomID:       roomID,
		CreatedAt:    createdAt,
		Bucket:       bucket.Of(createdAt),
		ThreadRoomID: threadRoomID,
	}
}

// Result is what the parent says after Maintain ran.
//
// TLM is the thread's last-reply time as it now stands — either the value just
// written, or the one already there when the route deliberately left it alone.
// It is nil only when the parent genuinely has no time recorded, because callers
// put it straight onto the event clients receive, where a missing time means
// "this thread has no replies left".
type Result struct {
	Count int
	TLM   *time.Time
}

// countReplies counts a thread's live replies, newest first, reading at most
// limit rows so a single reply never pays to read the whole thread. A limit of 0
// means no cap: read to the end.
//
// reachedEnd says whether the read got all the way through. It is the only thing
// that makes the count trustworthy, because the limit counts rows looked at and
// a deleted reply still leaves a row behind. A read that stopped early can come
// back with zero and no newest reply while live replies sit just out of reach.
// Only a read that reached the end may be saved as an exact count, or used to
// clear the last-reply time.
//
// The counting happens here rather than in the query because deleted replies
// still occupy rows: asking the database to count would include them, and a row
// limit caps rows read, not replies alive. newest is the newest live reply among
// the rows looked at — correct for a read that reached the end, and a lower
// bound for one that stopped early.
func countReplies(ctx context.Context, session *gocql.Session, threadRoomID string, limit int) (int, *time.Time, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()

	q := `SELECT deleted, created_at FROM thread_messages_by_thread WHERE thread_room_id = ?`
	args := []any{threadRoomID}
	if limit > 0 {
		// One row past the limit. Getting that extra row is how a read that
		// stopped early is told apart from a thread that simply ends on the
		// boundary. It counts as evidence only, never towards the total.
		q += ` LIMIT ?`
		args = append(args, limit+1)
	}
	iter := session.Query(q, args...).WithContext(ctx).PageSize(cassPageSize).Iter()

	var (
		deleted   bool // a missing value reads as false; the write path omits the column
		createdAt time.Time
		newest    *time.Time
	)
	live, read, reachedEnd := 0, 0, true
	for iter.Scan(&deleted, &createdAt) {
		read++
		if limit > 0 && read > limit {
			reachedEnd = false
			break
		}
		if deleted {
			continue
		}
		live++
		if newest == nil || createdAt.After(*newest) {
			t := createdAt
			newest = &t
		}
	}
	if err := iter.Close(); err != nil {
		return 0, nil, false, fmt.Errorf("count replies in thread %s: %w", threadRoomID, err)
	}
	return live, newest, reachedEnd, nil
}
