package threadcount

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used
	"time"

	"github.com/gocql/gocql"

	o11ycassandra "github.com/flywindy/o11y/cassandra"
)

// parentCounts is what a thread's parent message currently says about its
// replies: how many there are, and when the newest one arrived.
//
// The two are read together and written together. Updating one against a value
// read at a different moment is the mistake this type exists to prevent.
//
// replies is nil when nobody has counted this thread yet, or when the parent's
// row is missing. Both mean the same thing here: there is no number to work
// from.
type parentCounts struct {
	replies     *int
	lastReplyAt *time.Time
}

// shortEnough reports whether the thread is small enough to count its replies
// directly, instead of adjusting the number already saved. A thread nobody has
// counted yet is short by definition — there is nothing to adjust.
func (c parentCounts) shortEnough(limit int) bool {
	return c.replies == nil || *c.replies < limit
}

// Maintain updates a thread parent's reply count after one reply was added
// (change +1, with replyAt set to the new reply's time) or deleted (change -1,
// replyAt nil). All three writers call this, so none of them can drift apart.
//
// It reads the saved count first, and that number picks one of four routes:
//
//   - Short thread — count the replies for real. See countFromReplies.
//   - Long thread, retry — rewrite what is already saved, without counting the
//     same reply twice. See handleRetry.
//   - Long thread, occasionally — recount everything to correct the drift the
//     cheap route builds up. See Reconcile and ShouldReanchor.
//   - Long thread, normally — add or subtract one and write it. See
//     changeWithoutCounting.
//
// Skipping the count on a long thread is the whole point: the work per reply
// stops growing with the thread, which is what keeps a busy thread from timing
// out and losing replies. The price is that a long thread's number is an
// estimate — two replies landing at once can lose one of the adjustments, with
// no error to notice. Getting that last bit of accuracy would mean serializing
// every reply to the busiest threads through a database transaction, to buy
// precision nothing actually reads. ShouldReanchor keeps the estimate honest
// instead.
func Maintain(ctx context.Context, session *gocql.Session, p *Parent, pol Policy, change int, replyAt *time.Time, isRetry bool) (Result, error) {
	saved, err := readCounts(ctx, session, p.MessageID)
	if err != nil {
		return Result{}, fmt.Errorf("maintain thread %s count: %w", p.ThreadRoomID, err)
	}

	// A retry may already have been counted, and counting one reply twice is
	// worse than being one short, which the next full recount fixes. So it
	// changes the number by nothing — but it still writes, because the two
	// copies of the count are written in one batch that is not all-or-nothing,
	// and the attempt that failed may have updated one and not the other.
	// Writing both again from the saved number is the only thing that repairs
	// that before the retry is acknowledged.
	if isRetry {
		change = 0
	}

	if saved.shortEnough(pol.ScanLimit) {
		return countFromReplies(ctx, session, p, pol, saved, change, replyAt)
	}

	// From here the saved count is a real number: shortEnough already caught
	// the never-counted case. A burst of retries is the worst moment to add
	// expensive reads, so a retry skips the recount too.
	if !isRetry && ShouldReanchor(*saved.replies, pol.ReanchorBudget) {
		res, recountErr := Reconcile(ctx, session, p, pol)
		if recountErr == nil {
			return res, nil
		}
		// Correcting the drift is a bonus, never a requirement. Failing to do
		// it leaves the saved number exactly as it was, which is no worse than
		// never having tried, so it must not fail the reply.
		slog.WarnContext(ctx, "thread reply count re-anchor failed — keeping the estimate",
			"error", recountErr, "thread_room_id", p.ThreadRoomID, "parent_message_id", p.MessageID)
		// That attempt may have run for seconds, and other replies will have
		// updated the parent while it did. Work from what the parent says now,
		// not from what it said before.
		saved = rereadCounts(ctx, session, p.MessageID, saved)
	}

	return changeWithoutCounting(ctx, session, p, saved, change, replyAt)
}

// countFromReplies handles a thread short enough to count, which the read may or
// may not manage within the rows it is allowed to look at.
//
// If the read got all the way through the thread, it is the truth: it replaces
// whatever was saved before, which makes it exact, self-correcting, and safe to
// repeat. Neither change nor isRetry matters in that case.
//
// If it stopped early, it proves much less. The limit counts rows looked at, and
// a deleted reply still leaves a row behind, so every row it saw could be a
// deleted one while live replies sit just out of reach. The number it came back
// with is therefore a minimum, and the newest reply it saw may not be the real
// newest. Such a read may only push the count up and the time forward — never
// down, and never to nothing.
func countFromReplies(ctx context.Context, session *gocql.Session, p *Parent, pol Policy, saved parentCounts, change int, replyAt *time.Time) (Result, error) {
	found, newestFound, reachedEnd, err := countReplies(ctx, session, p.ThreadRoomID, pol.ScanLimit)
	if err != nil {
		return Result{}, fmt.Errorf("maintain thread %s count: %w", p.ThreadRoomID, err)
	}

	if reachedEnd {
		// On a delete, whatever the read found is the answer — including
		// nothing, since reaching the end proves there is nothing left to point
		// at. This is the only place clearing the time is correct. On an add,
		// the new reply's own time is merged in rather than assumed newer or
		// older: a reply handled out of order must not drag the time backwards,
		// and a database replica that has not caught up with this reply yet must
		// not lose it.
		return saveCounts(ctx, session, p, found, keepNewer(newestFound, replyAt), true)
	}

	replies := max(found, plus(saved.replies, change))
	if replies == 0 {
		// Nothing here has shown the thread is empty. The read filled up on rows
		// left behind by deleted replies, and the minimum is zero only because
		// the parent had no number to raise it — a thread nobody has counted,
		// being deleted from, which brings no reply time of its own either.
		//
		// Saving zero would be read as "every reply is gone": history-service
		// stops looking in the database entirely once the count is zero. And the
		// mistake would stick, because zero is below the limit, so every later
		// reply would come back here and reach the same wrong answer.
		//
		// Only counting the whole thread can tell an empty thread from one whose
		// replies are merely out of reach, so this is the one case worth paying
		// for that.
		res, recountErr := Reconcile(ctx, session, p, pol)
		if recountErr != nil {
			// Too expensive to count as well. Write nothing: leaving the number
			// blank is what sends a reader to look at the replies themselves,
			// which beats a zero we cannot back up.
			return Result{}, fmt.Errorf("maintain thread %s count: refusing to save an unproven zero: %w", p.ThreadRoomID, recountErr)
		}
		return res, nil
	}

	// Pushed forward by the newest reply the read did see, then by this reply's
	// own time. Never cleared: a time we could not work out must not make a
	// thread that still has replies look like it has none.
	lastReplyAt := keepNewer(keepNewer(saved.lastReplyAt, newestFound), replyAt)
	return saveCounts(ctx, session, p, replies, lastReplyAt, lastReplyAt != nil)
}

// changeWithoutCounting is the ordinary long-thread route: move the saved number
// by one and never look at the replies.
//
// The time moves forward only when a reply was added. On a delete it is left
// exactly as it was, because working out whether the deleted reply was the
// newest needs the very count this route exists to avoid. That is why the
// decision to write it is "was a reply added", not "do we have a value". Leaving
// it untouched also means a reply landing at the same moment cannot have its
// time overwritten by an older one we happened to read.
//
// The value is still reported back as what the parent now says, never as
// nothing: the caller puts it straight onto the event clients receive, where a
// missing time means "this thread has no replies left".
func changeWithoutCounting(ctx context.Context, session *gocql.Session, p *Parent, saved parentCounts, change int, replyAt *time.Time) (Result, error) {
	lastReplyAt := keepNewer(saved.lastReplyAt, replyAt)
	return saveCounts(ctx, session, p, plus(saved.replies, change), lastReplyAt, replyAt != nil)
}

// keepNewer returns whichever of the two times is later, so a reply handled out
// of order can never drag the thread's activity time backwards, and a database
// replica that has not caught up with this reply yet cannot lose it.
//
// With no reply time to compare against it hands back the saved one untouched.
// That is how a delete is told apart from an add throughout this file.
func keepNewer(saved *time.Time, replyAt *time.Time) *time.Time {
	if replyAt == nil {
		return saved
	}
	if saved != nil && saved.After(*replyAt) {
		return saved
	}
	newest := *replyAt
	return &newest
}

// saveCounts writes the pair to both copies and reports back what the parent
// now says.
func saveCounts(ctx context.Context, session *gocql.Session, p *Parent, replies int, lastReplyAt *time.Time, writeTime bool) (Result, error) {
	if err := writeToBothRows(ctx, session, p, replies, lastReplyAt, writeTime); err != nil {
		return Result{}, fmt.Errorf("maintain thread %s count: %w", p.ThreadRoomID, err)
	}
	return Result{Count: replies, TLM: lastReplyAt}, nil
}

// Reconcile counts every reply in the thread, saves the result, and returns it.
//
// This is what keeps the cheap route honest. On a long thread the count is moved
// by one without coordinating, so replies quietly lose adjustments; counting
// everything replaces the accumulated estimate with the truth instead of letting
// the error grow. It reads the whole thread, so only reach it through
// ShouldReanchor or the rare cases that need certainty, and treat failure as
// survivable.
//
// Policy.ReconcileRowLimit caps how many rows it will read. A thread with more
// than that cannot be counted at a price worth paying, so it gives up rather
// than save a number it cannot stand behind — leaving the caller's estimate in
// place, which is exactly the state this design tolerates.
func Reconcile(ctx context.Context, session *gocql.Session, p *Parent, pol Policy) (Result, error) {
	replies, newest, reachedEnd, err := countReplies(ctx, session, p.ThreadRoomID, pol.ReconcileRowLimit)
	if err != nil {
		return Result{}, fmt.Errorf("reconcile thread %s: %w", p.ThreadRoomID, err)
	}
	if !reachedEnd {
		return Result{}, fmt.Errorf("reconcile thread %s: thread holds more than the %d rows a full recount may read", p.ThreadRoomID, pol.ReconcileRowLimit)
	}
	// The time is written even when there is none: having reached the end proves
	// no reply survives, so clearing it is right here in a way it never is after
	// a read that stopped early.
	if err := writeToBothRows(ctx, session, p, replies, newest, true); err != nil {
		return Result{}, fmt.Errorf("reconcile thread %s: %w", p.ThreadRoomID, err)
	}
	return Result{Count: replies, TLM: newest}, nil
}

// ShouldReanchor reports whether this write should count the whole thread rather
// than move the saved number by one.
//
// The chance is budget/saved, so the rows read per reply average out to budget
// however long the thread grows. A fixed "every Nth reply" rule would not do
// that: each recount reads the whole thread, so the cost per reply would grow
// with the thread — the very behaviour this package exists to avoid. Deciding by
// chance also needs no coordination between writers and no record of when a
// thread was last recounted.
//
// Below the budget, counting outright is cheaper than the sampling would save,
// so it always counts.
func ShouldReanchor(saved, budget int) bool {
	if saved <= 0 || budget <= 0 {
		return false
	}
	if saved <= budget {
		return true
	}
	// #nosec G404 -- deciding when to tidy up, nothing security-sensitive
	return rand.IntN(saved) < budget
}

// plus moves a saved count by change, never below zero: a thread nobody has
// counted starts from nothing, and no run of deletes may push a count negative.
func plus(saved *int, change int) int {
	n := change
	if saved != nil {
		n = *saved + change
	}
	return max(n, 0)
}

// readCounts reads what the parent currently says. A missing row and a parent
// nobody has counted both come back as no number, which every route treats the
// same way.
func readCounts(ctx context.Context, session *gocql.Session, parentMessageID string) (parentCounts, error) {
	var c parentCounts
	err := session.Query(
		`SELECT tcount, thread_last_msg_at FROM messages_by_id WHERE message_id = ?`,
		parentMessageID,
	).WithContext(ctx).Scan(&c.replies, &c.lastReplyAt)
	if errors.Is(err, gocql.ErrNotFound) {
		return parentCounts{}, nil
	}
	if err != nil {
		return parentCounts{}, fmt.Errorf("read parent %s reply count: %w", parentMessageID, err)
	}
	return c, nil
}

// rereadCounts reads the parent again after an attempt that may have run for
// seconds, so this reply is applied to what the parent says now rather than to
// what it said before.
//
// It never fails the caller. A read error, a row that has since vanished, and a
// parent nobody has counted all leave the caller's values alone: an optional
// tidy-up that already failed must not also fail the reply, and neither a
// missing row nor a missing number is evidence that the values it would replace
// are wrong. The count and the time are replaced together or not at all, so this
// reply is never applied to half of a pair read at two different moments.
func rereadCounts(ctx context.Context, session *gocql.Session, parentMessageID string, saved parentCounts) parentCounts {
	fresh, err := readCounts(ctx, session, parentMessageID)
	if err != nil {
		slog.WarnContext(ctx, "re-reading a thread parent's reply count failed — using the value read earlier",
			"error", err, "parent_message_id", parentMessageID)
		return saved
	}
	if fresh.replies == nil {
		return saved
	}
	return fresh
}

// writeToBothRows saves the count, and the time when writeTime, to the parent's
// two copies in one batch.
//
// writeTime false leaves the time exactly as it was. writeTime true with no time
// clears it, which only a count that read the whole thread may ask for: a time
// we could not work out must never make a thread that still has replies look
// like it has none.
func writeToBothRows(ctx context.Context, session *gocql.Session, p *Parent, replies int, lastReplyAt *time.Time, writeTime bool) error {
	batch := session.NewBatch(gocql.UnloggedBatch)
	if writeTime {
		batch.Query(
			`UPDATE messages_by_id SET tcount = ?, thread_last_msg_at = ? WHERE message_id = ?`,
			replies, lastReplyAt, p.MessageID)
		batch.Query(
			`UPDATE messages_by_room SET tcount = ?, thread_last_msg_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			replies, lastReplyAt, p.RoomID, p.Bucket, p.CreatedAt, p.MessageID)
	} else {
		batch.Query(
			`UPDATE messages_by_id SET tcount = ? WHERE message_id = ?`,
			replies, p.MessageID)
		batch.Query(
			`UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			replies, p.RoomID, p.Bucket, p.CreatedAt, p.MessageID)
	}
	// o11ycassandra.ExecuteBatch, not session.ExecuteBatch: gocql reports batches
	// through a separate observer that only this seam drives, so a direct call is
	// a hole in the traces even though the session is instrumented. It binds ctx
	// onto the batch itself and no-ops cleanly on an uninstrumented session.
	if err := o11ycassandra.ExecuteBatch(ctx, session, batch); err != nil {
		return fmt.Errorf("save reply count on parent %s: %w", p.MessageID, err)
	}
	return nil
}
