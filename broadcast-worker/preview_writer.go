package main

import (
	"context"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/flushloop"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/preview"
)

// roomPreview is one preview update from an inserted message.
//
// Every insert makes one. An ineligible message (a join notice, a rename) leaves
// the stored body alone but still advances the room's newest message, and the
// key has to follow or a correct body reads as stale. So MsgID is the room's
// newest message, not the preview's.
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

// previewWriter buffers the newest preview per room and drains them in one Mongo
// BulkWrite on a ticker. Memory scales with distinct active rooms per interval,
// not with message rate, since messages for one room collapse into one entry.
//
// # Why this is broadcast-worker's only MongoDB write
//
// The room pointer, the sender's lastSeenAt and the mention badges all moved to
// unread-worker, which holds messages un-acked until MongoDB takes them. The
// preview stayed because sealing one needs the users, mention participants and
// attachments this service already resolved for the fan-out, and that
// unread-worker deliberately does not read.
//
// Splitting is safe because the preview fields have their own watermark
// (previewAsOf) and are guarded against it, not against lastMsgAt, so neither
// half of the document can overwrite the other. What it gives up is atomicity
// between the preview key and lastMsgId:
//
//   - An eligible insert writes body and key together, guarded only by the
//     watermark. Ordinary messages are unaffected.
//   - An ineligible one advances the key only while it still equals lastMsgId.
//     If unread-worker flushed first that fails, and the room reads as a miss
//     until history-service walks Cassandra and warms it back. One walk, on
//     system messages only.
//
// That is the same miss the eager design already accepts for out-of-order
// inserts: it exists to make misses rare, not impossible.
//
// Flush failures are logged, never returned to the handler. The message is
// already in Cassandra and already broadcast, and a room with no stored preview
// is one history-service walks for — never NAK a delivered message over this.
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
	return w.bulk.BulkUpdateRoomPreview(ctx, batch)
}

// Run drives the periodic flush loop until ctx is cancelled, then runs one
// final flush so a buffered batch still lands even though the supplied ctx is
// already done.
//
// PerFlush carries the bound rather than Flush imposing its own: the drained batch
// stays live for the whole write while handlers fill the replacement map behind it,
// so an unbounded write is an unbounded pair of maps. Keeping it on the shared knob
// means there is one place to look when a flush hangs, as in unread-worker. The
// final drain takes flushloop.DefaultFinalTimeout.
//
// A flush failure is logged and never returned to the handler. The message is
// already in Cassandra and already broadcast, and a room with no stored preview
// is one history-service walks for — never NAK a delivered message over this.
func (w *previewWriter) Run(ctx context.Context, interval time.Duration) {
	if w == nil {
		return
	}
	flushloop.Run(ctx, flushloop.Config{
		Name:     "room preview flush",
		Interval: interval,
		PerFlush: maxFlushDuration,
	}, w.Flush)
}
