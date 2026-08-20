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
}

// bulkRoomLastMsgWriter is the flush boundary, kept off Store so the contract stays narrow.
type bulkRoomLastMsgWriter interface {
	BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error
}

// coalescingStore buffers UpdateRoomLastMessage per room and drains it through one
// BulkWrite. Flush errors are logged, not propagated — the message is already in Cassandra.
type coalescingStore struct {
	Store
	bulk bulkRoomLastMsgWriter

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

// UpdateRoomLastMessage buffers the update; Flush performs the write asynchronously.
//
//nolint:gocritic // hugeParam: roomLastMessage is the Store.UpdateRoomLastMessage contract shared with the mock; by-value keeps the buffered copy obviously independent of the caller's.
func (c *coalescingStore) UpdateRoomLastMessage(_ context.Context, upd roomLastMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.pending[upd.RoomID]
	if upd.At.After(cur.at) {
		cur.msgID = upd.MsgID
		cur.at = upd.At
	}
	if upd.MentionAll && upd.At.After(cur.lastMentionAllAt) {
		cur.lastMentionAllAt = upd.At
	}
	// Against pvwAt, not at: a later ineligible message must not evict the preview it
	// cannot replace. A seal failure moves this clock too, so an older seal cannot win.
	if (upd.Preview != nil || upd.PreviewFailed) && upd.At.After(cur.pvwAt) {
		cur.pvw = upd.Preview
		cur.pvwFailed = upd.PreviewFailed
		cur.pvwAt = upd.At
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
	c.mu.Unlock()
	return c.bulk.BulkUpdateRoomLastMessage(ctx, batch)
}

// Run drives the flush loop until ctx is cancelled; the final flush uses a fresh context.
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
