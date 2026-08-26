package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/preview"
)

// roomPreview is one room-doc preview update derived from an inserted message.
//
// Every main-timeline insert produces one, eligible or not: an ineligible
// message (a join notice, a rename) leaves the stored body alone but still
// advances the room's newest message, and the freshness key has to follow it or
// the still-correct body reads as stale. MsgID is therefore the room's newest
// message, not the preview's.
type roomPreview struct {
	RoomID string
	MsgID  string
	At     time.Time
	// Preview is the sealed body, nil when MsgID is ineligible or previews are off.
	// Nil still advances the key: the stored body is the room's last eligible message.
	Preview *preview.Sealed
	// PreviewFailed means MsgID was eligible but sealing failed, so the stored body is
	// stale and the write clears it. Never true alongside a non-nil Preview (#224).
	PreviewFailed bool
}

// roomPreviewUpdate is the per-room state buffered between flushes. Fields take the max by
// clustering position; pvw/pvwFailed ride pvwAt so an ineligible message can't displace them.
type roomPreviewUpdate struct {
	msgID     string
	at        time.Time
	pvw       *preview.Sealed
	pvwFailed bool
	pvwAt     time.Time
	// pvwMsgID is the message that last moved the preview clock, so the preview and the
	// room's newest message break ties the same way rather than on separate rules.
	pvwMsgID string
}

// maxPendingPreviews bounds how many buffered rooms may hold a sealed preview body. The
// freshness key is tiny and required; the preview body is the large, optional half, so a
// stalled flush sheds bodies and keeps the key rather than growing without limit. A shed
// room simply has no stored preview, which the reader already handles by walking (#289).
const maxPendingPreviews = 5000

// maxFlushDuration bounds one bulk write, so a stalled Mongo cannot hold the drained
// batch (and the replacement map filling behind it) for the process's lifetime.
const maxFlushDuration = 30 * time.Second

// bulkRoomPreviewWriter is the flush boundary, kept off Store so the contract stays narrow.
type bulkRoomPreviewWriter interface {
	BulkUpdateRoomPreview(ctx context.Context, updates map[string]roomPreviewUpdate) error
}

// previewWriter buffers the latest preview state per room and drains it through a single
// Mongo BulkWrite on a ticker. Memory is bounded by the count of distinct active rooms
// within a flush interval — coalescing collapses any number of messages for the same room
// into one map entry — not by message rate.
//
// # Why this is the only MongoDB write left in broadcast-worker
//
// The room's own pointer (lastMsgAt / lastMsgId / lastMentionAllAt), the sender's
// lastSeenAt and the mention badges all moved to unread-worker, which holds their
// messages un-acked until MongoDB takes them and so cannot lose a write to an outage.
// The preview did not move with them, because sealing one needs the resolved users, the
// mention participants and the decoded attachments that this service assembles for the
// fan-out anyway, and that unread-worker deliberately holds none of (its event projection
// decodes nine fields and reads no MongoDB at all). Rebuilding that enrichment there
// would cost a user-store read per message on the service whose whole contract is that it
// performs none.
//
// Splitting them is safe because the preview fields carry their own watermark
// (previewAsOf) and are guarded against it, not against lastMsgAt: the two halves of the
// document order themselves independently and neither can overwrite the other's newer
// state. What the split gives up is atomicity between the freshness key and lastMsgId,
// and the cost of that is bounded and self-repairing:
//
//   - An ELIGIBLE insert writes body and key together under GuardedSetFields, whose only
//     condition is the watermark. Ordinary messages — nearly all traffic — are unaffected.
//   - An INELIGIBLE insert takes GuardedAdvanceKeyFields, which lands only while the
//     stored key still equals the stored lastMsgId. If unread-worker's flush moved
//     lastMsgId first, that equality fails and the advance is skipped; the room then reads
//     as a miss and history-service's lazy walk resolves and warms it back. One walk,
//     repaired permanently, on system messages only.
//
// That is the same self-healing miss the eager design already accepts for out-of-order
// inserts — the eager writer exists to make misses rare, not impossible, and the read path
// does not assume it did.
//
// Failures are logged at flush time, never propagated to the handler: the message is
// already persisted to Cassandra and already broadcast, and a room with no stored preview
// is a room history-service walks for. This service must never NAK a delivered message
// over an optional write.
type previewWriter struct {
	bulk bulkRoomPreviewWriter

	mu      sync.Mutex
	pending map[string]roomPreviewUpdate
	// pendingPreviews counts buffered rooms currently holding a body, so the cap is a
	// counter rather than a scan of pending on every message.
	pendingPreviews int
}

func newPreviewWriter(bulk bulkRoomPreviewWriter) *previewWriter {
	return &previewWriter{bulk: bulk, pending: make(map[string]roomPreviewUpdate)}
}

// buffer merges one insert's preview state. A nil writer is inert, which is how a
// deployment with preview persistence off disables it.
//
//nolint:gocritic // hugeParam: by-value keeps the buffered copy obviously independent of the caller's.
func (w *previewWriter) buffer(upd roomPreview) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	cur := w.pending[upd.RoomID]
	if msgbucket.NewerRow(upd.At, upd.MsgID, cur.at, cur.msgID) {
		cur.msgID = upd.MsgID
		cur.at = upd.At
	}
	// Against pvwAt, not at: a later ineligible message must not evict the preview it
	// cannot replace. A seal failure moves this clock too, so an older seal cannot win.
	if (upd.Preview != nil || upd.PreviewFailed) && msgbucket.NewerRow(upd.At, upd.MsgID, cur.pvwAt, cur.pvwMsgID) {
		had := cur.pvw != nil
		switch {
		case upd.PreviewFailed:
			cur.pvw, cur.pvwFailed = nil, true
		case had || w.pendingPreviews < maxPendingPreviews:
			cur.pvw, cur.pvwFailed = upd.Preview, false
		default:
			// Over the cap: shed the BODY, and record that an eligible message arrived
			// without one — which is a seal FAILURE, not an ineligible message. Leaving
			// it as neither would take the flush's key-advance branch and stamp the new
			// id over the room's previous body, certifying a preview for a message it
			// does not describe. That is #224, reintroduced by the cap that bounds #289.
			cur.pvw, cur.pvwFailed = nil, true
		}
		switch now := cur.pvw != nil; {
		case !had && now:
			w.pendingPreviews++
		case had && !now:
			w.pendingPreviews--
		}
		cur.pvwAt = upd.At
		cur.pvwMsgID = upd.MsgID
	}
	w.pending[upd.RoomID] = cur
}

// Flush drains the buffer, holding the lock only to swap the map so buffering isn't blocked.
func (w *previewWriter) Flush(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	if len(w.pending) == 0 {
		w.mu.Unlock()
		return nil
	}
	batch := w.pending
	w.pending = make(map[string]roomPreviewUpdate, len(batch))
	w.pendingPreviews = 0
	w.mu.Unlock()
	// Bounded: the drained batch stays live for the whole write, and handlers fill the
	// replacement map behind it, so an unbounded write is an unbounded pair of maps.
	ctx, cancel := context.WithTimeout(ctx, maxFlushDuration)
	defer cancel()
	return w.bulk.BulkUpdateRoomPreview(ctx, batch)
}

// Run drives the periodic flush loop until ctx is cancelled. On cancellation a
// final flush runs against a fresh context with finalTimeout so a buffered
// batch still lands even if the supplied ctx is already done.
func (w *previewWriter) Run(ctx context.Context, interval, finalTimeout time.Duration) {
	if w == nil {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.Background(), finalTimeout)
			w.guardedFlush(finalCtx, "final flush of room preview buffer failed")
			cancel()
			return
		case <-t.C:
			w.guardedFlush(ctx, "flush room preview buffer failed")
		}
	}
}

// guardedFlush runs one flush with panic recovery. This goroutine drives
// user-derived data — a preview composed from message content — through
// BulkWrite, and an unrecovered panic here would take the whole process down.
// That is not a proportionate outcome for the one write in this service that is
// explicitly optional and droppable: it would stop message fan-out for the
// entire site over a room-list preview. Recover, log, keep ticking; the room
// simply has no stored preview, which the reader already handles by walking.
func (w *previewWriter) guardedFlush(ctx context.Context, failMsg string) {
	jobguard.Guard("room preview flush", func() {
		if err := w.Flush(ctx); err != nil {
			slog.ErrorContext(ctx, failMsg, "error", err)
		}
	})
}
