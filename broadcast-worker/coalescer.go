package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// roomLastMsgUpdate is one room's flush-buffered state; pointer and preview advance independently, touchedAt (→updatedAt) never regresses.
type roomLastMsgUpdate struct {
	msgID            string
	at               time.Time
	touchedAt        time.Time
	lastMentionAllAt time.Time
	preview          *model.LastMessagePreview
}

// bulkRoomLastMsgWriter is the coalescer's flush target (implemented by *mongoStore), separate from Store to keep the handler contract narrow.
type bulkRoomLastMsgWriter interface {
	BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error
}

// coalescingStore buffers per-room lastMsg* updates and drains them via a periodic BulkWrite
// (collapsing many per room into one). lastMsg* is decorative — durable in Cassandra — so flush errors are logged, not propagated.
type coalescingStore struct {
	Store
	bulk bulkRoomLastMsgWriter

	mu      sync.Mutex
	pending map[string]roomLastMsgUpdate
	// inflight = rooms in the current BulkWrite (nil idle). A rewind/edit for such a room waits on
	// flushDone so its write can't be overtaken and resurrect a deleted preview; other rooms don't wait.
	inflight  map[string]roomLastMsgUpdate
	flushDone *sync.Cond // Broadcast when a flush completes; waiters re-check inflight. Guards on mu.
}

func newCoalescingStore(inner Store, bulk bulkRoomLastMsgWriter) *coalescingStore {
	c := &coalescingStore{
		Store:   inner,
		bulk:    bulk,
		pending: make(map[string]roomLastMsgUpdate),
	}
	c.flushDone = sync.NewCond(&c.mu)
	return c
}

// waitForRoomFlush blocks until roomID isn't in an in-flight flush batch, so a guarded rewrite
// lands after that batch's BulkWrite. Must hold c.mu; Cond.Wait releases/re-acquires it while parked.
func (c *coalescingStore) waitForRoomFlush(roomID string) {
	for {
		if _, inFlight := c.inflight[roomID]; !inFlight {
			return
		}
		c.flushDone.Wait()
	}
}

// GetRoom overlays any newer buffered pointer/preview onto the stored room, so the delete skip-path
// sees state a not-yet-flushed create superseded. Returns a copy; never mutates the store object.
func (c *coalescingStore) GetRoom(ctx context.Context, roomID string) (*model.Room, error) {
	room, err := c.Store.GetRoom(ctx, roomID)
	if err != nil {
		return room, err
	}
	c.mu.Lock()
	u, ok := c.pending[roomID]
	c.mu.Unlock()
	if !ok {
		return room, nil
	}
	overlaid := *room
	if !u.at.IsZero() && (overlaid.LastMsgAt == nil || u.at.After(*overlaid.LastMsgAt) ||
		(u.at.Equal(*overlaid.LastMsgAt) && u.msgID > overlaid.LastMsgID)) {
		at := u.at
		overlaid.LastMsgID = u.msgID
		overlaid.LastMsgAt = &at
	}
	if u.preview != nil && (overlaid.LastMsg == nil ||
		u.preview.CreatedAt.After(overlaid.LastMsg.CreatedAt) ||
		(u.preview.CreatedAt.Equal(overlaid.LastMsg.CreatedAt) && u.preview.MessageID > overlaid.LastMsg.MessageID)) {
		// Copy so the returned room never aliases the live pending-buffer preview pointer.
		pv := *u.preview
		overlaid.LastMsg = &pv
	}
	return &overlaid, nil
}

// UpdateRoomLastMessage buffers the update (always nil; Flush writes async); pointer and preview advance independently, allowing a {sys pointer, user preview} drift.
func (c *coalescingStore) UpdateRoomLastMessage(_ context.Context, roomID, msgID string, at time.Time, mentionAll bool, preview *model.LastMessagePreview) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.pending[roomID]
	// Advance on a newer createdAt, or equal createdAt + higher message_id — mirrors Cassandra's
	// (created_at DESC, message_id DESC) so two messages in the same ms don't drop the later one.
	if at.After(cur.at) || (at.Equal(cur.at) && msgID > cur.msgID) {
		cur.msgID = msgID
		cur.at = at
	}
	// preview gated by its own createdAt (same tiebreak) — a user message behind a newer system notice still wins.
	if preview != nil && (cur.preview == nil || at.After(cur.preview.CreatedAt) ||
		(at.Equal(cur.preview.CreatedAt) && preview.MessageID > cur.preview.MessageID)) {
		cur.preview = preview
	}
	if at.After(cur.touchedAt) {
		cur.touchedAt = at
	}
	if mentionAll && at.After(cur.lastMentionAllAt) {
		cur.lastMentionAllAt = at
	}
	c.pending[roomID] = cur
	return nil
}

// RewindRoomLastMessage reconciles the buffer with a delete before the guarded Mongo rewind, else a buffered create would resurrect the deleted preview.
func (c *coalescingStore) RewindRoomLastMessage(ctx context.Context, roomID, deletedMsgID string, pointer *model.LastMessagePointer, survivor *model.LastMessagePreview, updatedAt time.Time) error {
	// A non-nil survivor implies a pointer; derive it so reconciliation updates the entry, not drops it.
	if pointer == nil && survivor != nil {
		pointer = &model.LastMessagePointer{MessageID: survivor.MessageID, CreatedAt: survivor.CreatedAt}
	}
	c.mu.Lock()
	// Wait out this room's in-flight flush so the rewind below lands after it (see inflight).
	c.waitForRoomFlush(roomID)
	if cur, ok := c.pending[roomID]; ok {
		touched := false
		switch {
		case cur.msgID == deletedMsgID:
			if pointer != nil {
				cur.msgID = pointer.MessageID
				cur.at = pointer.CreatedAt
				cur.preview = survivor
				touched = true
			} else {
				// Room emptied: drop the entry (the delegate clears the pointer fields anyway).
				delete(c.pending, roomID)
			}
		case cur.preview != nil && cur.preview.MessageID == deletedMsgID:
			// Drift: a newer system message owns the pointer but the preview still shows the deleted
			// message. Carry the survivor (nil leaves the delegate's preview rewind in place).
			cur.preview = survivor
			touched = true
		}
		if touched {
			// Mutation despite the pointer moving back — updatedAt must not regress to the survivor's time.
			if updatedAt.After(cur.touchedAt) {
				cur.touchedAt = updatedAt
			}
			c.pending[roomID] = cur
		}
	}
	c.mu.Unlock()
	return c.Store.RewindRoomLastMessage(ctx, roomID, deletedMsgID, pointer, survivor, updatedAt)
}

// SetRoomLastMessageEdited patches the buffered preview (copied — its pointer may be published) before the guarded Mongo patch, so the flush carries the post-edit body.
func (c *coalescingStore) SetRoomLastMessageEdited(ctx context.Context, roomID, editedMsgID, newMsg string, encMsg json.RawMessage, editedAt time.Time) error {
	c.mu.Lock()
	// Wait out this room's in-flight flush so the patch below lands after it (see inflight).
	c.waitForRoomFlush(roomID)
	if cur, ok := c.pending[roomID]; ok && cur.preview != nil && cur.preview.MessageID == editedMsgID {
		patched := *cur.preview
		patched.Msg = newMsg
		patched.EncMsg = encMsg
		at := editedAt
		patched.EditedAt = &at
		cur.preview = &patched
		if editedAt.After(cur.touchedAt) {
			cur.touchedAt = editedAt
		}
		c.pending[roomID] = cur
	}
	c.mu.Unlock()
	return c.Store.SetRoomLastMessageEdited(ctx, roomID, editedMsgID, newMsg, encMsg, editedAt)
}

// Flush swaps out the buffer into one BulkWrite; the create path isn't blocked (lock only for the swap), and rewind/edit for a batched room waits via inflight/flushDone.
func (c *coalescingStore) Flush(ctx context.Context) error {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return nil
	}
	batch := c.pending
	c.pending = make(map[string]roomLastMsgUpdate, len(batch))
	c.inflight = batch // rewind/edit for these rooms now waits until the BulkWrite commits
	c.mu.Unlock()

	err := c.bulk.BulkUpdateRoomLastMessage(ctx, batch)

	c.mu.Lock()
	c.inflight = nil
	if err != nil {
		// Requeue the failed batch (merge under monotonic rules — a newer entry since the swap wins)
		// so a transient Mongo error doesn't strand a rewind/edit preview in a quiet room.
		for roomID, u := range batch {
			if cur, ok := c.pending[roomID]; ok {
				c.pending[roomID] = mergeRoomLastMsg(&cur, &u)
			} else {
				c.pending[roomID] = u
			}
		}
	}
	c.flushDone.Broadcast() // wake any rewind/edit waiting on a room in this batch
	c.mu.Unlock()
	return err
}

// mergeRoomLastMsg field-wise-maxes two updates for one room (createdAt then message_id; touchedAt/lastMentionAllAt max); order-independent.
func mergeRoomLastMsg(a, b *roomLastMsgUpdate) roomLastMsgUpdate {
	out := *a
	if b.at.After(out.at) || (b.at.Equal(out.at) && b.msgID > out.msgID) {
		out.msgID = b.msgID
		out.at = b.at
	}
	if b.preview != nil && (out.preview == nil ||
		b.preview.CreatedAt.After(out.preview.CreatedAt) ||
		(b.preview.CreatedAt.Equal(out.preview.CreatedAt) && b.preview.MessageID > out.preview.MessageID)) {
		out.preview = b.preview
	}
	if b.touchedAt.After(out.touchedAt) {
		out.touchedAt = b.touchedAt
	}
	if b.lastMentionAllAt.After(out.lastMentionAllAt) {
		out.lastMentionAllAt = b.lastMentionAllAt
	}
	return out
}

// Run flushes on interval until ctx is cancelled, then one final flush on a fresh context (finalTimeout) so a buffered batch still lands.
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
				// Warn: the batch is requeued and retried next tick, so a blip needn't log Error every interval.
				slog.Warn("flush room last-msg buffer failed", "error", err)
			}
		}
	}
}
