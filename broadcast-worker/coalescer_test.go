package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

type fakeBulkWriter struct {
	mu     sync.Mutex
	calls  []map[string]roomLastMsgUpdate
	err    error
	signal chan struct{} // closed/sent on each call when non-nil
}

func (f *fakeBulkWriter) BulkUpdateRoomLastMessage(_ context.Context, updates map[string]roomLastMsgUpdate) error {
	f.mu.Lock()
	cp := make(map[string]roomLastMsgUpdate, len(updates))
	for k, v := range updates {
		cp[k] = v
	}
	f.calls = append(f.calls, cp)
	err := f.err
	f.mu.Unlock()
	if f.signal != nil {
		select {
		case f.signal <- struct{}{}:
		default:
		}
	}
	return err
}

func (f *fakeBulkWriter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeBulkWriter) lastCall() map[string]roomLastMsgUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return nil
	}
	return f.calls[len(f.calls)-1]
}

func newCoalescer(bulk bulkRoomLastMsgWriter) *coalescingStore {
	return newCoalescingStore(nil, bulk) // Store unused in these unit tests
}

func TestCoalescingStore_UpdateRoomLastMessage_BuffersWithoutFlush(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false, nil))
	assert.Equal(t, 0, bulk.callCount(), "buffered updates must not hit Mongo until Flush")
}

func TestCoalescingStore_Flush_WritesPendingBatch(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t0 := time.Unix(1700000000, 0).UTC()
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-a", "msg-a", t0, false, nil))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-b", "msg-b", t0.Add(time.Second), true, nil))

	require.NoError(t, c.Flush(context.Background()))

	require.Equal(t, 1, bulk.callCount())
	got := bulk.lastCall()
	require.Len(t, got, 2)
	assert.Equal(t, "msg-a", got["room-a"].msgID)
	assert.Equal(t, t0, got["room-a"].at)
	assert.True(t, got["room-a"].lastMentionAllAt.IsZero())
	assert.Equal(t, "msg-b", got["room-b"].msgID)
	assert.Equal(t, t0.Add(time.Second), got["room-b"].lastMentionAllAt, "mentionAll=true must record lastMentionAllAt")
}

func TestCoalescingStore_Update_LatestMessageWinsPerRoom(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t1 := time.Unix(1700000000, 0).UTC()
	t2 := t1.Add(500 * time.Millisecond)
	t3 := t2.Add(500 * time.Millisecond)

	// Send in order: t1, t3, t2. Latest (t3) must win regardless of arrival order.
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "msg-1", t1, false, nil))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "msg-3", t3, false, nil))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "msg-2", t2, false, nil))

	require.NoError(t, c.Flush(context.Background()))

	got := bulk.lastCall()
	require.Contains(t, got, "room-x")
	assert.Equal(t, "msg-3", got["room-x"].msgID, "latest by createdAt must win")
	assert.Equal(t, t3, got["room-x"].at)
}

func TestCoalescingStore_Update_MentionAllStickyOnLatestMentionAll(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t1 := time.Unix(1700000000, 0).UTC()
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)

	// t1: mentionAll=true. t2: mentionAll=false (later). t3: mentionAll=true (latest).
	// Expected lastMentionAllAt == t3 (latest among mention-all messages).
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m1", t1, true, nil))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m2", t2, false, nil))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m3", t3, true, nil))

	require.NoError(t, c.Flush(context.Background()))

	got := bulk.lastCall()["room-x"]
	assert.Equal(t, "m3", got.msgID)
	assert.Equal(t, t3, got.at)
	assert.Equal(t, t3, got.lastMentionAllAt)
}

func TestCoalescingStore_Update_MentionAllPreservedWhenLaterMessageHasNone(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t1 := time.Unix(1700000000, 0).UTC()
	t2 := t1.Add(time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m1", t1, true, nil))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m2", t2, false, nil))

	require.NoError(t, c.Flush(context.Background()))

	got := bulk.lastCall()["room-x"]
	assert.Equal(t, "m2", got.msgID, "latest msgID wins")
	assert.Equal(t, t1, got.lastMentionAllAt, "lastMentionAllAt sticks at the older mention-all timestamp")
}

func TestCoalescingStore_Flush_EmptyBufferIsNoOp(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	require.NoError(t, c.Flush(context.Background()))
	assert.Equal(t, 0, bulk.callCount(), "empty flush must not call the bulk writer")
}

func TestCoalescingStore_Flush_ClearsPendingAfterWrite(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false, nil))
	require.NoError(t, c.Flush(context.Background()))
	require.NoError(t, c.Flush(context.Background()))

	assert.Equal(t, 1, bulk.callCount(), "second flush with empty buffer must not call bulk writer")
}

func TestCoalescingStore_Flush_PropagatesBulkError(t *testing.T) {
	wantErr := errors.New("bulk failed")
	bulk := &fakeBulkWriter{err: wantErr}
	c := newCoalescer(bulk)

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false, nil))

	err := c.Flush(context.Background())
	assert.ErrorIs(t, err, wantErr)
}

// #11: two distinct messages sharing a millisecond — the later-arriving one with the
// higher message_id must still win (Cassandra (created_at DESC, message_id DESC)),
// not be silently dropped by a strict After() comparison.
func TestCoalescingStore_Update_EqualTimestampHigherIDWins(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	t0 := time.Unix(1700000000, 0).UTC()
	lo := &model.LastMessagePreview{MessageID: "m-aaa", Msg: "first", CreatedAt: t0}
	hi := &model.LastMessagePreview{MessageID: "m-bbb", Msg: "second", CreatedAt: t0}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m-aaa", t0, false, lo))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m-bbb", t0, false, hi))
	require.NoError(t, c.Flush(context.Background()))
	got := bulk.lastCall()["r1"]
	assert.Equal(t, "m-bbb", got.msgID, "higher message_id wins the pointer on equal createdAt")
	require.NotNil(t, got.preview)
	assert.Equal(t, "m-bbb", got.preview.MessageID, "higher message_id wins the preview on equal createdAt")
}

// #11: the reverse — a lower message_id arriving second on an equal timestamp must not overwrite.
func TestCoalescingStore_Update_EqualTimestampLowerIDIgnored(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	t0 := time.Unix(1700000000, 0).UTC()
	hi := &model.LastMessagePreview{MessageID: "m-bbb", CreatedAt: t0}
	lo := &model.LastMessagePreview{MessageID: "m-aaa", CreatedAt: t0}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m-bbb", t0, false, hi))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m-aaa", t0, false, lo))
	require.NoError(t, c.Flush(context.Background()))
	got := bulk.lastCall()["r1"]
	assert.Equal(t, "m-bbb", got.msgID, "lower id on equal ts must not overwrite the pointer")
	require.NotNil(t, got.preview)
	assert.Equal(t, "m-bbb", got.preview.MessageID)
}

// #10: a failed BulkWrite must requeue its batch into pending so a transient Mongo
// error doesn't permanently strand the state; the next flush then lands it.
func TestCoalescingStore_Flush_RequeuesBatchOnBulkError(t *testing.T) {
	bulk := &fakeBulkWriter{err: errors.New("mongo down")}
	c := newCoalescer(bulk)
	t0 := time.Unix(1700000000, 0).UTC()
	p := &model.LastMessagePreview{MessageID: "m-1", Msg: "keep me", CreatedAt: t0}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m-1", t0, false, p))

	require.Error(t, c.Flush(context.Background()), "flush must surface the bulk error")
	c.mu.Lock()
	requeued, ok := c.pending["r1"]
	c.mu.Unlock()
	require.True(t, ok, "failed batch must be requeued into pending")
	assert.Equal(t, "m-1", requeued.msgID)
	require.NotNil(t, requeued.preview, "the rewind/edit-bearing preview must survive a failed flush")
	assert.Equal(t, "m-1", requeued.preview.MessageID)

	bulk.mu.Lock()
	bulk.err = nil
	bulk.mu.Unlock()
	require.NoError(t, c.Flush(context.Background()), "recovery flush must succeed")
	assert.Equal(t, "m-1", bulk.lastCall()["r1"].msgID)
}

// #10: the requeue merge keeps the newer side per field regardless of argument order.
func TestMergeRoomLastMsg_KeepsNewerPerField(t *testing.T) {
	t0 := time.Unix(1700000000, 0).UTC()
	older := roomLastMsgUpdate{msgID: "m-old", at: t0, touchedAt: t0,
		preview: &model.LastMessagePreview{MessageID: "m-old", CreatedAt: t0}}
	newer := roomLastMsgUpdate{msgID: "m-new", at: t0.Add(time.Minute), touchedAt: t0.Add(time.Minute),
		preview: &model.LastMessagePreview{MessageID: "m-new", CreatedAt: t0.Add(time.Minute)}}
	for _, m := range []roomLastMsgUpdate{mergeRoomLastMsg(&older, &newer), mergeRoomLastMsg(&newer, &older)} {
		assert.Equal(t, "m-new", m.msgID)
		require.NotNil(t, m.preview)
		assert.Equal(t, "m-new", m.preview.MessageID)
		assert.True(t, m.touchedAt.Equal(t0.Add(time.Minute)), "touchedAt keeps the max")
	}
}

// #10: merging a batch's user preview with a newer system-only entry yields the drift
// state (newer system pointer, older user preview retained).
func TestMergeRoomLastMsg_DriftKeepsSystemPointerOlderPreview(t *testing.T) {
	t0 := time.Unix(1700000000, 0).UTC()
	batch := roomLastMsgUpdate{msgID: "m-user", at: t0, touchedAt: t0,
		preview: &model.LastMessagePreview{MessageID: "m-user", CreatedAt: t0}}
	cur := roomLastMsgUpdate{msgID: "m-sys", at: t0.Add(time.Minute), touchedAt: t0.Add(time.Minute), preview: nil}
	m := mergeRoomLastMsg(&cur, &batch)
	assert.Equal(t, "m-sys", m.msgID, "newer system pointer wins")
	require.NotNil(t, m.preview, "older user preview retained under drift")
	assert.Equal(t, "m-user", m.preview.MessageID)
}

func TestCoalescingStore_ConcurrentUpdatesAreThreadSafe(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	const goroutines = 50
	const perGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			base := time.Unix(1700000000, 0).UTC()
			for i := 0; i < perGoroutine; i++ {
				_ = c.UpdateRoomLastMessage(context.Background(), "room-shared", "msg", base.Add(time.Duration(g*1000+i)*time.Millisecond), false, nil)
			}
		}(g)
	}
	wg.Wait()

	require.NoError(t, c.Flush(context.Background()))
	require.Equal(t, 1, bulk.callCount())
	got := bulk.lastCall()
	require.Len(t, got, 1, "all writes coalesced into a single room entry")
}

func TestCoalescingStore_Update_PreviewCarriedToBulk(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t0 := time.Unix(1700000000, 0).UTC()
	p := &model.LastMessagePreview{MessageID: "msg-a", SenderAccount: "alice", SenderName: "Alice Wang", Msg: "hi", CreatedAt: t0}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-a", "msg-a", t0, false, p))

	require.NoError(t, c.Flush(context.Background()))

	got := bulk.lastCall()["room-a"]
	require.NotNil(t, got.preview, "the buffered update must carry the preview to the bulk write")
	assert.Equal(t, "msg-a", got.preview.MessageID)
	assert.Equal(t, "hi", got.preview.Msg)
}

func TestCoalescingStore_Update_LatestPreviewWinsPerRoom(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t1 := time.Unix(1700000000, 0).UTC()
	t2 := t1.Add(time.Second)
	p1 := &model.LastMessagePreview{MessageID: "m1", SenderAccount: "alice", CreatedAt: t1}
	p2 := &model.LastMessagePreview{MessageID: "m2", SenderAccount: "bob", CreatedAt: t2}

	// Out-of-order arrival: the later message's preview must win, and the
	// stale arrival must not clobber it.
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m2", t2, false, p2))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m1", t1, false, p1))

	require.NoError(t, c.Flush(context.Background()))

	got := bulk.lastCall()["room-x"]
	assert.Equal(t, "m2", got.msgID)
	require.NotNil(t, got.preview)
	assert.Equal(t, "m2", got.preview.MessageID, "preview must track the latest message, not the latest arrival")
}

// #5: GetRoom overlays newer buffered state so the delete skip-path never reuses a
// stale persisted preview a not-yet-flushed create has superseded.
func TestCoalescingStore_GetRoom_OverlaysNewerBufferedState(t *testing.T) {
	c, inner := newCoalescerWithStore(t, &fakeBulkWriter{})
	stored := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	newer := stored.Add(time.Hour)
	inner.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel,
		LastMsgID: "m-stored", LastMsgAt: &stored,
		LastMsg: &model.LastMessagePreview{MessageID: "m-stored", CreatedAt: stored, Msg: "stored"},
	}, nil)
	p := &model.LastMessagePreview{MessageID: "m-buf", CreatedAt: newer, Msg: "buffered"}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m-buf", newer, false, p))

	got, err := c.GetRoom(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, "m-buf", got.LastMsgID, "pointer overlaid with newer buffered state")
	require.NotNil(t, got.LastMsgAt)
	assert.True(t, got.LastMsgAt.Equal(newer))
	require.NotNil(t, got.LastMsg)
	assert.Equal(t, "m-buf", got.LastMsg.MessageID, "preview overlaid with newer buffered state")
}

// #5: an older/absent buffer entry must not regress newer persisted state, and the
// underlying (cached) store object is never mutated.
func TestCoalescingStore_GetRoom_KeepsStoredWhenBufferOlder(t *testing.T) {
	c, inner := newCoalescerWithStore(t, &fakeBulkWriter{})
	stored := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	older := stored.Add(-time.Hour)
	shared := &model.Room{
		ID: "r1", Type: model.RoomTypeChannel,
		LastMsgID: "m-stored", LastMsgAt: &stored,
		LastMsg: &model.LastMessagePreview{MessageID: "m-stored", CreatedAt: stored},
	}
	inner.EXPECT().GetRoom(gomock.Any(), "r1").Return(shared, nil)
	p := &model.LastMessagePreview{MessageID: "m-old", CreatedAt: older}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m-old", older, false, p))

	got, err := c.GetRoom(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, "m-stored", got.LastMsgID, "older buffer must not regress the stored pointer")
	require.NotNil(t, got.LastMsg)
	assert.Equal(t, "m-stored", got.LastMsg.MessageID)
	assert.Equal(t, "m-stored", shared.LastMsgID, "underlying cached room must not be mutated")
	assert.Equal(t, "m-stored", shared.LastMsg.MessageID)
}

func TestCoalescingStore_Run_FlushesPeriodicallyUntilCancel(t *testing.T) {
	bulk := &fakeBulkWriter{signal: make(chan struct{}, 4)}
	c := newCoalescer(bulk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx, 10*time.Millisecond, 100*time.Millisecond)
		close(done)
	}()

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false, nil))

	select {
	case <-bulk.signal:
	case <-time.After(time.Second):
		t.Fatal("periodic flush never fired")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestCoalescingStore_Run_FinalFlushOnShutdown(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	// Interval is long so the ONLY flush comes from the shutdown path.
	go func() {
		c.Run(ctx, time.Hour, 500*time.Millisecond)
		close(done)
	}()

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false, nil))
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	assert.Equal(t, 1, bulk.callCount(), "shutdown must perform a final flush of buffered updates")
}

// newCoalescerWithStore builds a coalescer with a mock inner Store for the guarded-write overrides (rewind/edit).
func newCoalescerWithStore(t *testing.T, bulk bulkRoomLastMsgWriter) (*coalescingStore, *MockStore) {
	t.Helper()
	inner := NewMockStore(gomock.NewController(t))
	return newCoalescingStore(inner, bulk), inner
}

// A system message advances the pointer but carries no preview; the newest user
// preview must keep riding the entry so the flush lands the drift state.
func TestCoalescingStore_Update_NilPreviewKeepsBufferedPreview(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)

	prev := &model.LastMessagePreview{MessageID: "m1", Msg: "hi", CreatedAt: t0}
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m1", t0, false, prev))
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-sys", t0.Add(time.Second), false, nil))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	assert.Equal(t, "m-sys", got.msgID, "pointer tracks the newest message including system notices")
	require.NotNil(t, got.preview, "system update must not clobber the buffered user preview")
	assert.Equal(t, "m1", got.preview.MessageID)
}

func TestCoalescingStore_Rewind_ReplacesBufferedDeleteTargetWithSurvivor(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := t0.Add(time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-del", t0, false,
		&model.LastMessagePreview{MessageID: "m-del", Msg: "bye", CreatedAt: t0}))

	survivor := &model.LastMessagePreview{MessageID: "m-old", Msg: "still here", CreatedAt: t0.Add(-time.Minute)}
	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", ptrOf(survivor), survivor, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", ptrOf(survivor), survivor, deletedAt))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	assert.Equal(t, "m-old", got.msgID, "flush must never resurrect the deleted message")
	assert.True(t, got.at.Equal(survivor.CreatedAt))
	require.NotNil(t, got.preview)
	assert.Equal(t, "m-old", got.preview.MessageID)
}

func TestCoalescingStore_Rewind_NoSurvivorDropsBufferedEntry(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := t0.Add(time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-del", t0, false,
		&model.LastMessagePreview{MessageID: "m-del", Msg: "bye", CreatedAt: t0}))

	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", nil, nil, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", nil, nil, deletedAt))
	require.NoError(t, c.Flush(ctx))

	assert.Equal(t, 0, bulk.callCount(), "an emptied room has nothing left to flush")
}

// Drift: the buffered pointer moved on to a system message but the buffered
// preview still shows the deleted user message — only the preview is purged.
func TestCoalescingStore_Rewind_DriftReplacesPreviewKeepsPointer(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := t0.Add(2 * time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-del", t0, false,
		&model.LastMessagePreview{MessageID: "m-del", Msg: "bye", CreatedAt: t0}))
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-sys", t0.Add(time.Second), false, nil))

	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", nil, nil, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", nil, nil, deletedAt))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	assert.Equal(t, "m-sys", got.msgID, "pointer keeps tracking the newest (system) message")
	assert.Nil(t, got.preview, "the deleted preview must not survive in the buffer")
}

func TestCoalescingStore_Rewind_UnrelatedBufferedEntryUntouched(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := t0.Add(time.Second)

	newer := &model.LastMessagePreview{MessageID: "m-newer", Msg: "unrelated", CreatedAt: t0}
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-newer", t0, false, newer))

	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-old-del", nil, nil, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-old-del", nil, nil, deletedAt))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	assert.Equal(t, "m-newer", got.msgID)
	require.NotNil(t, got.preview)
	assert.Equal(t, "m-newer", got.preview.MessageID)
}

func TestCoalescingStore_SetEdited_PatchesBufferedPreview(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	editedAt := t0.Add(time.Second)

	original := &model.LastMessagePreview{MessageID: "m1", Msg: "original", CreatedAt: t0}
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m1", t0, false, original))

	inner.EXPECT().SetRoomLastMessageEdited(gomock.Any(), "r1", "m1", "rewritten", gomock.Nil(), editedAt).Return(nil)
	require.NoError(t, c.SetRoomLastMessageEdited(ctx, "r1", "m1", "rewritten", nil, editedAt))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	require.NotNil(t, got.preview)
	assert.Equal(t, "rewritten", got.preview.Msg, "flush must carry the post-edit body")
	require.NotNil(t, got.preview.EditedAt)
	assert.True(t, got.preview.EditedAt.Equal(editedAt))
	assert.Equal(t, "original", original.Msg, "the caller's preview pointer must not be mutated (it may already be published)")
}

func TestCoalescingStore_SetEdited_UnrelatedBufferedPreviewUntouched(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	editedAt := t0.Add(time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m1", t0, false,
		&model.LastMessagePreview{MessageID: "m1", Msg: "original", CreatedAt: t0}))

	inner.EXPECT().SetRoomLastMessageEdited(gomock.Any(), "r1", "m-other", "rewritten", gomock.Nil(), editedAt).Return(nil)
	require.NoError(t, c.SetRoomLastMessageEdited(ctx, "r1", "m-other", "rewritten", nil, editedAt))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	require.NotNil(t, got.preview)
	assert.Equal(t, "original", got.preview.Msg)
	assert.Nil(t, got.preview.EditedAt)
}

// A user message arriving out of order behind a newer system notice must still
// win the preview — pointer and preview advance independently.
func TestCoalescingStore_Update_OutOfOrderUserPreviewStillWins(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	ctx := context.Background()
	t1 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	t3 := t1.Add(2 * time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m1", t1, false,
		&model.LastMessagePreview{MessageID: "m1", Msg: "first", CreatedAt: t1}))
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-sys", t3, false, nil))
	// m2 (t2) arrives after the system notice (t3): pointer stays at t3, but
	// m2 is the newest USER message and must own the preview.
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m2", t2, false,
		&model.LastMessagePreview{MessageID: "m2", Msg: "second", CreatedAt: t2}))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	assert.Equal(t, "m-sys", got.msgID, "pointer keeps the newest message incl. system")
	assert.True(t, got.at.Equal(t3))
	require.NotNil(t, got.preview)
	assert.Equal(t, "m2", got.preview.MessageID, "out-of-order user message still wins the preview")
}

// A rewind moves the pointer time backwards, but the flush's updatedAt must
// reflect the mutation time (the delete), never the survivor's old create time.
func TestCoalescingStore_Rewind_TouchedAtNotRegressedBySurvivor(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := t0.Add(time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-del", t0, false,
		&model.LastMessagePreview{MessageID: "m-del", Msg: "bye", CreatedAt: t0}))

	survivor := &model.LastMessagePreview{MessageID: "m-old", Msg: "old", CreatedAt: t0.Add(-time.Hour)}
	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", ptrOf(survivor), survivor, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", ptrOf(survivor), survivor, deletedAt))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	assert.True(t, got.at.Equal(survivor.CreatedAt), "pointer time rewinds to the survivor")
	assert.True(t, got.touchedAt.Equal(deletedAt), "updatedAt source is the delete time, not the survivor's create time")
}

// The rewound buffer entry carries the POINTER (which may be a system notice)
// while the preview carries the survivor — they differ under drift.
func TestCoalescingStore_Rewind_SystemPointerUserSurvivor(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := t0.Add(time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-del", t0, false,
		&model.LastMessagePreview{MessageID: "m-del", Msg: "bye", CreatedAt: t0}))

	pointer := &model.LastMessagePointer{MessageID: "m-sys", CreatedAt: t0.Add(-time.Minute)}
	survivor := &model.LastMessagePreview{MessageID: "m-user", Msg: "older user", CreatedAt: t0.Add(-time.Hour)}
	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", pointer, survivor, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", pointer, survivor, deletedAt))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok)
	assert.Equal(t, "m-sys", got.msgID, "buffered pointer follows the system survivor")
	require.NotNil(t, got.preview)
	assert.Equal(t, "m-user", got.preview.MessageID, "buffered preview follows the user survivor")
}

// A guarded rewind racing an in-flight flush must wait for the BulkWrite, else the
// unguarded $set resurrects the deleted preview. 500ms select probes: pre-fix rewind returns in µs.
func TestCoalescingStore_Rewind_WaitsForInFlightFlush(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var events []string
	record := func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }

	bulk := &blockingBulkWriter{started: started, release: release, onDone: func() { record("bulk-done") }}
	inner := NewMockStore(gomock.NewController(t))
	c := newCoalescingStore(inner, bulk)

	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-del", t0, false,
		&model.LastMessagePreview{MessageID: "m-del", Msg: "bye", CreatedAt: t0}))

	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", nil, nil, gomock.Any()).
		DoAndReturn(func(context.Context, string, string, *model.LastMessagePointer, *model.LastMessagePreview, time.Time) error {
			record("rewind-delegated")
			return nil
		})

	flushDone := make(chan struct{})
	go func() { _ = c.Flush(ctx); close(flushDone) }()
	<-started // the batch with m-del is now in-flight inside BulkWrite

	rewindDone := make(chan struct{})
	go func() {
		_ = c.RewindRoomLastMessage(ctx, "r1", "m-del", nil, nil, t0.Add(time.Second))
		close(rewindDone)
	}()

	select {
	case <-rewindDone:
		t.Fatal("rewind completed while the flush batch was still in flight — unguarded BulkWrite can land after the guarded rewind")
	case <-time.After(500 * time.Millisecond):
	}

	close(release)
	<-flushDone
	<-rewindDone
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"bulk-done", "rewind-delegated"}, events,
		"the guarded rewind must run only after the in-flight flush landed")
}

// Per-room serialization: a rewind for a room NOT in the in-flight batch must proceed
// immediately — a slow flush of room A can't block a delete in room B (the old global lock did).
// 500ms probe: a regressed global lock would keep room B parked until the flush is released.
func TestCoalescingStore_Rewind_UnrelatedRoomDoesNotWaitForFlush(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	bulk := &blockingBulkWriter{started: started, release: release, onDone: func() {}}
	inner := NewMockStore(gomock.NewController(t))
	c := newCoalescingStore(inner, bulk)

	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	// Buffer a create for room A only, then flush — the flush blocks with A in-flight.
	require.NoError(t, c.UpdateRoomLastMessage(ctx, "room-a", "m-a", t0, false,
		&model.LastMessagePreview{MessageID: "m-a", CreatedAt: t0}))

	// Room B's rewind must delegate while A is still in flight (B is not in the batch).
	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "room-b", "m-b", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	flushDone := make(chan struct{})
	go func() { _ = c.Flush(ctx); close(flushDone) }()
	<-started // room A's batch is now in-flight inside BulkWrite

	rewindDone := make(chan struct{})
	go func() {
		_ = c.RewindRoomLastMessage(ctx, "room-b", "m-b", nil,
			&model.LastMessagePreview{MessageID: "m-surv", CreatedAt: t0.Add(-time.Minute)}, t0.Add(time.Second))
		close(rewindDone)
	}()

	select {
	case <-rewindDone: // room B proceeded without waiting for room A's flush
	case <-time.After(500 * time.Millisecond):
		t.Fatal("rewind for room B blocked on an unrelated room A flush — per-room serialization regressed")
	}

	close(release)
	<-flushDone
}

// blockingBulkWriter blocks inside BulkUpdateRoomLastMessage until released, holding a flush batch in flight.
type blockingBulkWriter struct {
	started chan struct{}
	release chan struct{}
	onDone  func()
}

func (b *blockingBulkWriter) BulkUpdateRoomLastMessage(context.Context, map[string]roomLastMsgUpdate) error {
	close(b.started)
	<-b.release
	b.onDone()
	return nil
}

// A rewind with a nil pointer but non-nil survivor must derive the pointer, so buffer
// reconciliation updates the pending entry to the survivor instead of dropping it.
func TestCoalescingStore_Rewind_NilPointerDerivedFromSurvivor(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c, inner := newCoalescerWithStore(t, bulk)
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := t0.Add(time.Second)

	require.NoError(t, c.UpdateRoomLastMessage(ctx, "r1", "m-del", t0, false,
		&model.LastMessagePreview{MessageID: "m-del", Msg: "bye", CreatedAt: t0}))

	survivor := &model.LastMessagePreview{MessageID: "m-old", Msg: "still here", CreatedAt: t0.Add(-time.Minute)}
	// Delegate receives the DERIVED pointer (normalization runs before delegation).
	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", ptrOf(survivor), survivor, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", nil, survivor, deletedAt))
	require.NoError(t, c.Flush(ctx))

	got, ok := bulk.lastCall()["r1"]
	require.True(t, ok, "pending entry must be UPDATED to the survivor, not dropped")
	assert.Equal(t, "m-old", got.msgID)
	require.NotNil(t, got.preview)
	assert.Equal(t, "m-old", got.preview.MessageID)
}
