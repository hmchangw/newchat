package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// roomLastMsgUpdate is the per-room state buffered between flushes.
//
// Coalescing semantics:
//   - msgID/at carry the LATEST observed message for the room (max by createdAt).
//   - lastMentionAllAt carries the latest createdAt among messages whose
//     mentionAll flag was true; it sticks across subsequent non-mention-all
//     messages until a newer mention-all arrives.
type roomLastMsgUpdate struct {
	msgID            string
	at               time.Time
	lastMentionAllAt time.Time
}

// bulkRoomLastMsgWriter is the persistence boundary the coalescer flushes to.
// Kept separate from the Store interface so the handler-facing contract stays
// narrow; the production implementation lives on *mongoStore.
type bulkRoomLastMsgWriter interface {
	BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error
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
// roomActivityRefresh is one room's coalesced position, handed to the publisher
// so destination sites can order a room they hold no rooms doc for.
type roomActivityRefresh struct {
	roomID string
	at     time.Time
}

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

	mu      sync.Mutex
	pending map[string]roomLastMsgUpdate
}

func newCoalescingStore(inner Store, bulk bulkRoomLastMsgWriter) *coalescingStore {
	return &coalescingStore{
		Store:   inner,
		bulk:    bulk,
		pending: make(map[string]roomLastMsgUpdate),
	}
}

// UpdateRoomLastMessage buffers the update. Always returns nil; the buffered
// write is performed asynchronously by Flush.
func (c *coalescingStore) UpdateRoomLastMessage(_ context.Context, roomID, msgID string, at time.Time, mentionAll bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.pending[roomID]
	if at.After(cur.at) {
		cur.msgID = msgID
		cur.at = at
	}
	if mentionAll && at.After(cur.lastMentionAllAt) {
		cur.lastMentionAllAt = at
	}
	c.pending[roomID] = cur
	return nil
}

// Flush drains the pending buffer and writes it via the bulk writer. Safe to
// call concurrently with UpdateRoomLastMessage; takes the lock only to swap
// the map so the BulkWrite itself runs without blocking new updates.
func (c *coalescingStore) Flush(ctx context.Context) error {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return nil
	}
	batch := c.pending
	c.pending = make(map[string]roomLastMsgUpdate, len(batch))
	c.mu.Unlock()
	c.refreshRemoteActivity(ctx, batch)
	return c.bulk.BulkUpdateRoomLastMessage(ctx, batch)
}

// refreshRemoteActivity emits one activity refresh per cross-site room in the
// batch. Cost scales with distinct active cross-site rooms per flush interval,
// not with message rate — coalescing has already collapsed the window.
//
// Failures are logged, not returned: the position is decorative, the next
// message in the room re-establishes it, and the room batch this rides with is
// the write that actually matters.
func (c *coalescingStore) refreshRemoteActivity(ctx context.Context, batch map[string]roomLastMsgUpdate) {
	if c.publishActivity == nil || c.crossSite == nil {
		return
	}
	for roomID, u := range batch {
		if !c.crossSite(ctx, roomID) {
			continue
		}
		if err := c.publishActivity(ctx, roomActivityRefresh{roomID: roomID, at: u.at}); err != nil {
			slog.WarnContext(ctx, "publish room activity refresh failed", "room_id", roomID, "error", err)
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
