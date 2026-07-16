package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// roomLastMsgUpdate is the per-room state buffered between flushes.
//
// Coalescing semantics:
//   - msgID/at carry the LATEST observed message of any type (system notices
//     included) — the pointer pair rooms.{lastMsgId,lastMsgAt} track.
//   - preview/previewAt carry the latest observed message that CARRIED a
//     preview (user messages; system messages pass nil). Tracked separately
//     from the pointer so an out-of-order user message behind a newer system
//     notice still wins the preview.
//   - touchedAt is the newest mutation time seen (create/rewind/edit) and is
//     what the flush writes as rooms.updatedAt — never the pointer time,
//     which a rewind can legitimately move backwards.
//   - lastMentionAllAt carries the latest createdAt among messages whose
//     mentionAll flag was true; it sticks across subsequent non-mention-all
//     messages until a newer mention-all arrives.
type roomLastMsgUpdate struct {
	msgID            string
	at               time.Time
	previewAt        time.Time
	touchedAt        time.Time
	lastMentionAllAt time.Time
	preview          *model.LastMessagePreview
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

// UpdateRoomLastMessage buffers the update. Always returns nil; the buffered
// write is performed asynchronously by Flush.
//
// The pointer (msgID/at) and the preview advance INDEPENDENTLY: a nil
// preview (system message) advances only the pointer, and a user message
// arriving out of order behind a newer system notice still advances the
// preview — the flush then lands the drift state
// {lastMsgId: system, lastMsg: newest user message}, mirroring what the
// direct (non-coalesced) writes produce.
func (c *coalescingStore) UpdateRoomLastMessage(_ context.Context, roomID, msgID string, at time.Time, mentionAll bool, preview *model.LastMessagePreview) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.pending[roomID]
	if at.After(cur.at) {
		cur.msgID = msgID
		cur.at = at
	}
	if preview != nil && at.After(cur.previewAt) {
		cur.preview = preview
		cur.previewAt = at
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

// RewindRoomLastMessage reconciles the pending buffer with a delete BEFORE
// delegating the guarded Mongo rewind: the flush write is unguarded, so a
// buffered create of the deleted message would otherwise resurrect its
// preview (full content) right after the rewind removed it.
func (c *coalescingStore) RewindRoomLastMessage(ctx context.Context, roomID, deletedMsgID string, pointer *model.LastMessagePointer, survivor *model.LastMessagePreview, updatedAt time.Time) error {
	c.mu.Lock()
	if cur, ok := c.pending[roomID]; ok {
		touched := false
		switch {
		case cur.msgID == deletedMsgID:
			if pointer != nil {
				cur.msgID = pointer.MessageID
				cur.at = pointer.CreatedAt
				cur.preview = survivor
				cur.previewAt = time.Time{}
				if survivor != nil {
					cur.previewAt = survivor.CreatedAt
				}
				touched = true
			} else {
				// Room emptied: drop the entry. A pending lastMentionAllAt is
				// dropped with it — decorative, and the delegate clears the
				// room's pointer fields anyway.
				delete(c.pending, roomID)
			}
		case cur.preview != nil && cur.preview.MessageID == deletedMsgID:
			// Drift: a newer (system) message owns the buffered pointer but
			// the buffered preview still shows the deleted message. Carry the
			// survivor instead (nil leaves the delegate's preview rewind in
			// place — the flush then writes no lastMsg field at all).
			cur.preview = survivor
			cur.previewAt = time.Time{}
			if survivor != nil {
				cur.previewAt = survivor.CreatedAt
			}
			touched = true
		}
		if touched {
			// The rewind is a mutation NOW even though the pointer moved
			// backwards — rooms.updatedAt must never regress to the
			// survivor's create time.
			if updatedAt.After(cur.touchedAt) {
				cur.touchedAt = updatedAt
			}
			c.pending[roomID] = cur
		}
	}
	c.mu.Unlock()
	return c.Store.RewindRoomLastMessage(ctx, roomID, deletedMsgID, pointer, survivor, updatedAt)
}

// SetRoomLastMessageEdited patches a buffered preview of the edited message
// BEFORE delegating the guarded Mongo patch: if the create is still buffered,
// the Mongo guard misses (benign) and the flush must carry the post-edit body
// instead of the stale one. The buffered preview is copied, never mutated in
// place — the original pointer may already have been published.
func (c *coalescingStore) SetRoomLastMessageEdited(ctx context.Context, roomID, editedMsgID, newMsg string, encMsg json.RawMessage, editedAt time.Time) error {
	c.mu.Lock()
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
	return c.bulk.BulkUpdateRoomLastMessage(ctx, batch)
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
