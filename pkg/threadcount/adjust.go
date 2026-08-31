package threadcount

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used
	"time"

	"github.com/gocql/gocql"
)

// Maintain stamps the parent's tcount and thread_last_msg_at after one reply
// was added (delta +1, replyAt set to the new reply's time) or soft-deleted
// (delta -1, replyAt nil). It owns the whole policy so the add-path and
// delete-path writers cannot drift apart; callers supply only what genuinely
// differs between them.
//
// The stamped count is read first, because it decides how the count is kept:
//
//   - Under Policy.ScanLimit the count is recomputed from a bounded partition
//     scan and blind-stamped. Exact, idempotent under redelivery, self-healing
//     — the reply rows are the source of truth, so whatever was stamped before
//     is replaced by what is actually there.
//   - At or past it the scan is skipped and the stamped count moves by delta.
//     Per-reply cost stops depending on thread length, which is what keeps a
//     long thread out of the timeout → NAK → redelivery storm a full partition
//     walk caused.
//
// Above the limit the count is therefore approximate: concurrent replies can
// lose an adjustment and neither leaves an error to react to. That is the
// trade — a lightweight transaction would serialize every reply to the busiest
// threads through Paxos on one row to buy an exactness no reader needs.
// ShouldReanchor bounds how far the estimate wanders.
//
// redelivered marks a JetStream retry, which above the limit re-stamps what is
// already there instead of adjusting again: the reply may already be counted,
// and counting it twice is worse than the at-most-one undercount a re-anchor
// erases. Below the limit it is irrelevant, since recounting is idempotent.
//
// Both rows are written in one batch: messages_by_id is the authority and
// messages_by_room its mirror, and they are always stamped together.
func Maintain(ctx context.Context, session *gocql.Session, threadRoomID string, p Parent, pol Policy, delta int, replyAt *time.Time, redelivered bool) (Result, error) {
	stamped, stampedTLM, err := readParent(ctx, session, p.MessageID)
	if err != nil {
		return Result{}, fmt.Errorf("maintain thread %s count: %w", threadRoomID, err)
	}

	if stamped == nil || *stamped < pol.ScanLimit {
		n, latest, complete, scanErr := countAndLatest(ctx, session, threadRoomID, pol.ScanLimit)
		if scanErr != nil {
			return Result{}, fmt.Errorf("maintain thread %s count: %w", threadRoomID, scanErr)
		}
		if complete {
			// The delete path takes the scan's newest survivor, nil included:
			// the read reached the end of the partition, so nil proves there is
			// nothing left to point at. The add path merges its own reply time
			// in rather than assuming either side is newer — a reply processed
			// out of order must not drag the column back, and a scan served by
			// a replica that has not yet seen this reply must not lose it.
			tlm := latest
			if replyAt != nil {
				t := LaterOf(latest, *replyAt)
				tlm = &t
			}
			if err := stampParent(ctx, session, p, n, tlm, true); err != nil {
				return Result{}, fmt.Errorf("maintain thread %s count: %w", threadRoomID, err)
			}
			return Result{Count: n, TLM: tlm}, nil
		}
		// The scan filled its cap, so the partition holds at least ScanLimit
		// physical rows and n counted only the newest of them. Soft-deleted
		// replies occupy rows, so every row read can be a tombstone while live
		// replies survive just past the cap: n is a lower bound and latest may
		// miss the real newest. A truncated read may therefore only raise the
		// count and advance the timestamp — stamping it as exact is what would
		// write tcount=0 over a live thread and clear its last-reply time.
		n = max(n, adjusted(stamped, delta))
		tlm := stampedTLM
		if latest != nil {
			t := LaterOf(stampedTLM, *latest)
			tlm = &t
		}
		if replyAt != nil {
			t := LaterOf(tlm, *replyAt)
			tlm = &t
		}
		if err := stampParent(ctx, session, p, n, tlm, tlm != nil); err != nil {
			return Result{}, fmt.Errorf("maintain thread %s count: %w", threadRoomID, err)
		}
		return Result{Count: n, TLM: tlm}, nil
	}

	if redelivered {
		// A retry must not repeat the adjustment, but it must not do nothing
		// either. stampParent batches two different partitions unlogged, so the
		// delivery that failed can have applied to the authority and not the
		// mirror; re-writing both from the authority is idempotent and is the
		// only thing that repairs that divergence before the retry is acked.
		// Advancing tlm to the reply's own time is idempotent too — unlike the
		// count, a duplicate cannot inflate it — and is what keeps a reply that
		// was persisted before the first stamp failed from being invisible in
		// the parent's activity time. Re-anchoring is deliberately not sampled
		// here: a retry burst is the worst moment to add scans.
		tlm := stampedTLM
		if replyAt != nil {
			t := LaterOf(stampedTLM, *replyAt)
			tlm = &t
		}
		if err := stampParent(ctx, session, p, *stamped, tlm, tlm != nil); err != nil {
			return Result{}, fmt.Errorf("maintain thread %s count: %w", threadRoomID, err)
		}
		return Result{Count: *stamped, TLM: tlm}, nil
	}

	if ShouldReanchor(*stamped, pol.ReanchorBudget) {
		res, reconcileErr := Reconcile(ctx, session, threadRoomID, p, pol)
		if reconcileErr == nil {
			return res, nil
		}
		// Best-effort: a failed re-anchor leaves the stamped count, which is
		// never worse than not having tried, so it must not fail the reply.
		slog.WarnContext(ctx, "thread tcount re-anchor failed — keeping the approximate count",
			"error", reconcileErr, "thread_room_id", threadRoomID, "parent_message_id", p.MessageID)
	}

	// tlm moves forward only on the add path. A delete leaves it alone: the
	// removed reply may or may not have been the newest, and resolving that
	// needs the scan this path exists to skip.
	var tlm *time.Time
	if replyAt != nil {
		t := LaterOf(stampedTLM, *replyAt)
		tlm = &t
	}
	n := adjusted(stamped, delta)
	if err := stampParent(ctx, session, p, n, tlm, tlm != nil); err != nil {
		return Result{}, fmt.Errorf("maintain thread %s count: %w", threadRoomID, err)
	}
	return Result{Count: n, TLM: tlm}, nil
}

// Reconcile recomputes the count and thread_last_msg_at from an exact scan and
// stamps both parent rows, returning what it wrote.
//
// This is what keeps the approximate path honest. Above the scan limit the
// count is adjusted without coordinating, so replies lose adjustments silently;
// re-anchoring replaces the accumulated estimate with the truth rather than
// letting the error compound. It walks the partition, so reach it only through
// ShouldReanchor or a rare retry path, and treat failure as non-fatal.
//
// Policy.ReconcileRowLimit caps the physical rows it will read. A partition
// deeper than that cannot be counted exactly at a price the budget covers, so
// it errors rather than stamp a number it cannot stand behind — the caller
// keeps its approximate count, which is exactly the state the design tolerates.
func Reconcile(ctx context.Context, session *gocql.Session, threadRoomID string, p Parent, pol Policy) (Result, error) {
	n, latest, complete, err := countAndLatest(ctx, session, threadRoomID, pol.ReconcileRowLimit)
	if err != nil {
		return Result{}, fmt.Errorf("reconcile thread %s: %w", threadRoomID, err)
	}
	if !complete {
		return Result{}, fmt.Errorf("reconcile thread %s: partition holds more than the %d rows a re-anchor may read", threadRoomID, pol.ReconcileRowLimit)
	}
	// latest is written even when nil: an exact scan proves no reply survives,
	// so clearing the column is correct here in a way it never is after a
	// truncated read.
	if err := stampParent(ctx, session, p, n, latest, true); err != nil {
		return Result{}, fmt.Errorf("reconcile thread %s: %w", threadRoomID, err)
	}
	return Result{Count: n, TLM: latest}, nil
}

// ReanchorIfDue re-derives the parent's count from an exact scan, but only when
// this write draws the re-anchor sample, so a caller outside Maintain pays the
// same amortized budget as one inside it. Best-effort: it reports whether it
// stamped anything, and any error is the caller's to log and ignore.
//
// For repair paths that sit outside the add/delete flow — notably a delete
// whose LWT reports the message was already deleted, which is what the retry
// of a partly applied delete looks like.
func ReanchorIfDue(ctx context.Context, session *gocql.Session, threadRoomID string, p Parent, pol Policy) (bool, error) {
	stamped, _, err := readParent(ctx, session, p.MessageID)
	if err != nil {
		return false, fmt.Errorf("re-anchor thread %s count: %w", threadRoomID, err)
	}
	if stamped == nil || !ShouldReanchor(*stamped, pol.ReanchorBudget) {
		return false, nil
	}
	if _, err := Reconcile(ctx, session, threadRoomID, p, pol); err != nil {
		return false, fmt.Errorf("re-anchor thread %s count: %w", threadRoomID, err)
	}
	return true, nil
}

// ShouldReanchor reports whether this write should re-derive the count from a
// full exact scan rather than adjust the stamped one.
//
// The probability is budget/stamped, so the expected rows scanned per reply is
// stamped * (budget/stamped) = budget — flat, however long the thread grows. A
// fixed "every Nth reply" rule would not have that property: each anchor scans
// the whole partition, so it would cost O(thread length) per reply amortized,
// which is the quadratic behaviour this package exists to avoid. Sampling is
// stateless, so writers need no coordination and no column recording when a
// thread was last anchored.
//
// Below the budget a full scan is cheaper than the sampling would save, so it
// always runs.
func ShouldReanchor(stamped, budget int) bool {
	if stamped <= 0 || budget <= 0 {
		return false
	}
	if stamped <= budget {
		return true
	}
	// #nosec G404 -- maintenance sampling, not security-sensitive
	return rand.IntN(stamped) < budget
}

// LaterOf returns the later of the stamped thread_last_msg_at and a candidate
// reply time, so an out-of-order redelivery can never regress tlm.
func LaterOf(cur *time.Time, candidate time.Time) time.Time {
	if cur != nil && cur.After(candidate) {
		return *cur
	}
	return candidate
}

// adjusted applies delta to a stamped count, clamped at zero: an unstamped
// parent starts from nothing, and no sequence of deletes may drive the count
// negative.
func adjusted(stamped *int, delta int) int {
	n := delta
	if stamped != nil {
		n = *stamped + delta
	}
	if n < 0 {
		return 0
	}
	return n
}

// readParent reads the parent's stamped tcount and tlm. A missing row and an
// unstamped one are both reported as a nil count, which every caller handles
// identically.
func readParent(ctx context.Context, session *gocql.Session, parentMessageID string) (*int, *time.Time, error) {
	var (
		cur *int
		tlm *time.Time
	)
	err := session.Query(
		`SELECT tcount, thread_last_msg_at FROM messages_by_id WHERE message_id = ?`,
		parentMessageID,
	).WithContext(ctx).Scan(&cur, &tlm)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read parent %s tcount: %w", parentMessageID, err)
	}
	return cur, tlm, nil
}

// stampParent writes tcount, and tlm when writeTLM, to both the authority row
// and its mirror in one batch. writeTLM=false leaves thread_last_msg_at alone;
// a nil tlm with writeTLM=true clears it, which only an exact count may ask
// for — an unresolved timestamp must never render a thread with replies as
// reply-less.
func stampParent(ctx context.Context, session *gocql.Session, p Parent, n int, tlm *time.Time, writeTLM bool) error {
	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	if writeTLM {
		batch.Query(
			`UPDATE messages_by_id SET tcount = ?, thread_last_msg_at = ? WHERE message_id = ?`,
			n, tlm, p.MessageID)
		batch.Query(
			`UPDATE messages_by_room SET tcount = ?, thread_last_msg_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			n, tlm, p.RoomID, p.Bucket, p.CreatedAt, p.MessageID)
	} else {
		batch.Query(
			`UPDATE messages_by_id SET tcount = ? WHERE message_id = ?`,
			n, p.MessageID)
		batch.Query(
			`UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
			n, p.RoomID, p.Bucket, p.CreatedAt, p.MessageID)
	}
	if err := session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("stamp tcount on parent %s: %w", p.MessageID, err)
	}
	return nil
}
