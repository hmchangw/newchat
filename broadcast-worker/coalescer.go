package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/preview"
)

// roomLastMessage is one room-doc update from an inserted message. The preview rides
// along: previewForMsgId only means anything against the lastMsgId written beside it.
type roomLastMessage struct {
	RoomID     string
	MsgID      string
	At         time.Time
	MentionAll bool
	// Preview is the sealed preview, nil when MsgID is ineligible or previews are off.
	// Nil still advances the key: the stored body is the room's last eligible message.
	Preview *preview.Sealed
	// PreviewFailed means MsgID was eligible but sealing failed, so the stored body is
	// stale and the write clears it. Never true alongside a non-nil Preview (#224).
	PreviewFailed bool
}

// roomLastMsgUpdate is the per-room state buffered between flushes. Fields take the
// max by createdAt; pvw/pvwFailed ride pvwAt so an ineligible message can't displace them.
type roomLastMsgUpdate struct {
	msgID            string
	at               time.Time
	lastMentionAllAt time.Time
	pvw              *preview.Sealed
	pvwFailed        bool
	pvwAt            time.Time
	// pvwMsgID is the message that last moved the preview clock, so the preview and the
	// room tuple break ties the same way rather than on separate rules.
	pvwMsgID string
}

// maxPendingPreviews bounds how many buffered rooms may hold a sealed preview body. The
// room tuple is tiny and required; the preview is the large, optional half, so a stalled
// flush sheds previews and keeps ordering rather than growing without limit. A shed room
// simply has no stored preview, which the reader already handles by walking (#289).
const maxPendingPreviews = 5000

// maxFlushDuration bounds one bulk write, so a stalled Mongo cannot hold the drained
// batch (and the replacement map filling behind it) for the process's lifetime.
const maxFlushDuration = 30 * time.Second

// newerRow reports whether (at, id) sorts newer than (curAt, curID) in messages_by_room's
// clustering order: created_at DESC, message_id DESC. created_at alone does not order two
// rows, so comparing it alone leaves same-instant messages resolved by arrival -- which
// need not match the order the preview walk reads them back in (#293).
func newerRow(at time.Time, id string, curAt time.Time, curID string) bool {
	// Compared at the precision Cassandra STORES, not the precision Go carries. created_at
	// is a Cassandra timestamp — milliseconds — so two messages that differ only in
	// sub-millisecond digits are one clustering position there. Comparing full Go
	// precision would take the timestamp branch and skip the id tiebreaker that exists to
	// match that position, which is the whole point of the comparator.
	a, b := at.UnixMilli(), curAt.UnixMilli()
	if a != b {
		return a > b
	}
	return id > curID
}

// bulkRoomLastMsgWriter is the flush boundary, kept off Store so the contract stays narrow.
type bulkRoomLastMsgWriter interface {
	BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error
}

// roomActivityRefresh is one room's coalesced position, handed to the publisher
// so destination sites can order a room they hold no rooms doc for.
type roomActivityRefresh struct {
	roomID string
	at     time.Time
}

// coalescingStore wraps an inner Store and intercepts UpdateRoomLastMessage,
// buffering the latest (msgID, createdAt, mentionAll) per roomID in memory.
// Flush periodically drains the buffer through a single Mongo BulkWrite.
//
// Memory is bounded by the count of distinct active rooms within a flush
// interval — coalescing collapses any number of messages for the same room
// into one map entry — not by message rate.
//
// Trade-off: errors from the buffered write (e.g. ErrNoDocuments for a room
// that vanished between message and flush) are logged at flush time rather
// than propagated to the handler. lastMsgAt is a derived/decorative field;
// the message itself was already persisted to Cassandra by message-worker
// before this code runs, so dropping the rooms-collection update is safe.
type coalescingStore struct {
	Store
	bulk bulkRoomLastMsgWriter

	// crossSite reports whether a room has members off this site; nil disables
	// the refresh entirely. Backed by the room-meta cache, so the flush pays a
	// cache hit rather than a read — the handler just resolved the same room.
	crossSite func(ctx context.Context, roomID string) bool
	// publishActivity emits one refresh; nil disables. Errors are logged, never
	// propagated — see Flush.
	publishActivity func(ctx context.Context, r roomActivityRefresh) error
	// refreshInterval throttles refreshes to at most one per room per interval,
	// independently of the (much shorter) Mongo flush cadence. Non-positive
	// publishes on every flush. See refreshRemoteActivity for why this is safe.
	refreshInterval time.Duration
	// lastRefreshed is the per-room watermark backing that throttle. Pruned each
	// flush, so it stays bounded by rooms active within the interval rather than
	// growing with every room ever seen.
	lastRefreshed map[string]time.Time
	now           func() time.Time

	mu      sync.Mutex
	pending map[string]roomLastMsgUpdate
	// pendingPreviews counts buffered rooms currently holding a preview, so the cap is a
	// counter rather than a scan of pending on every message.
	pendingPreviews int
}

func (c *coalescingStore) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func newCoalescingStore(inner Store, bulk bulkRoomLastMsgWriter) *coalescingStore {
	return &coalescingStore{
		Store:   inner,
		bulk:    bulk,
		pending: make(map[string]roomLastMsgUpdate),
	}
}

// UpdateRoomLastMessage buffers the update; Flush performs the write asynchronously.
//
//nolint:gocritic // hugeParam: roomLastMessage is the Store.UpdateRoomLastMessage contract shared with the mock; by-value keeps the buffered copy obviously independent of the caller's.
func (c *coalescingStore) UpdateRoomLastMessage(_ context.Context, upd roomLastMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.pending[upd.RoomID]
	if newerRow(upd.At, upd.MsgID, cur.at, cur.msgID) {
		cur.msgID = upd.MsgID
		cur.at = upd.At
	}
	if upd.MentionAll && upd.At.After(cur.lastMentionAllAt) {
		cur.lastMentionAllAt = upd.At
	}
	// Against pvwAt, not at: a later ineligible message must not evict the preview it
	// cannot replace. A seal failure moves this clock too, so an older seal cannot win.
	if (upd.Preview != nil || upd.PreviewFailed) && newerRow(upd.At, upd.MsgID, cur.pvwAt, cur.pvwMsgID) {
		had := cur.pvw != nil
		switch {
		case upd.PreviewFailed:
			cur.pvw, cur.pvwFailed = nil, true
		case had || c.pendingPreviews < maxPendingPreviews:
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
			c.pendingPreviews++
		case had && !now:
			c.pendingPreviews--
		}
		cur.pvwAt = upd.At
		cur.pvwMsgID = upd.MsgID
	}
	c.pending[upd.RoomID] = cur
	return nil
}

// Flush drains the buffer, holding the lock only to swap the map so writes aren't blocked.
func (c *coalescingStore) Flush(ctx context.Context) error {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return nil
	}
	batch := c.pending
	c.pending = make(map[string]roomLastMsgUpdate, len(batch))
	c.pendingPreviews = 0
	c.mu.Unlock()
	// Bounded: the drained batch stays live for the whole write, and handlers fill the
	// replacement map behind it, so an unbounded write is an unbounded pair of maps.
	ctx, cancel := context.WithTimeout(ctx, maxFlushDuration)
	defer cancel()
	c.refreshRemoteActivity(ctx, batch)
	return c.bulk.BulkUpdateRoomLastMessage(ctx, batch)
}

// refreshRemoteActivity emits at most one activity refresh per cross-site room
// per refreshInterval. Cost scales with distinct active cross-site rooms per
// interval, not with message rate — coalescing collapses the window, and the
// throttle decouples the announce rate from the (much shorter) Mongo flush.
//
// The interval can be generous because the consumer cannot see the difference:
// the subscription list serves ordering from a cache whose own TTL is an order
// of magnitude longer, so a position a few seconds behind is indistinguishable
// from a fresh one. The cost of the throttle is that a room going quiet right
// after a burst keeps the position from its last announce — stale by at most
// one interval, and repaired by its next message.
//
// Failures are logged, not returned: the position is decorative, the next
// message re-establishes it, and the room batch this rides with is the write
// that actually matters.
func (c *coalescingStore) refreshRemoteActivity(ctx context.Context, batch map[string]roomLastMsgUpdate) {
	if c.publishActivity == nil || c.crossSite == nil {
		return
	}
	now := c.clock()
	for roomID, u := range batch {
		if !c.crossSite(ctx, roomID) {
			continue
		}
		if c.throttled(roomID, now) {
			continue
		}
		if err := c.publishActivity(ctx, roomActivityRefresh{roomID: roomID, at: u.at}); err != nil {
			slog.WarnContext(ctx, "publish room activity refresh failed", "room_id", roomID, "error", err)
		}
	}
	c.pruneWatermarks(now)
}

// throttled reports whether roomID was announced within refreshInterval, and
// records the announce when it was not.
func (c *coalescingStore) throttled(roomID string, now time.Time) bool {
	if c.refreshInterval <= 0 {
		return false
	}
	if last, ok := c.lastRefreshed[roomID]; ok && now.Sub(last) < c.refreshInterval {
		return true
	}
	if c.lastRefreshed == nil {
		c.lastRefreshed = make(map[string]time.Time)
	}
	c.lastRefreshed[roomID] = now
	return false
}

// pruneWatermarks drops rooms that have gone quiet for longer than the
// interval; their next message re-announces immediately, so keeping them would
// only leak memory.
func (c *coalescingStore) pruneWatermarks(now time.Time) {
	for roomID, last := range c.lastRefreshed {
		if now.Sub(last) >= c.refreshInterval {
			delete(c.lastRefreshed, roomID)
		}
	}
}

// Run drives the periodic flush loop until ctx is cancelled. On cancellation a
// final flush runs against a fresh context with finalTimeout so a buffered
// batch still lands even if the supplied ctx is already done.
func (c *coalescingStore) Run(ctx context.Context, interval, finalTimeout time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.Background(), finalTimeout)
			if err := c.Flush(finalCtx); err != nil {
				slog.Error("final flush of room last-msg buffer failed", "error", err)
			}
			cancel()
			return
		case <-t.C:
			if err := c.Flush(ctx); err != nil {
				slog.Error("flush room last-msg buffer failed", "error", err)
			}
		}
	}
}
