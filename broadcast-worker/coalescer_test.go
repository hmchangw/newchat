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
	return &coalescingStore{
		Store:   nil, // unused in these unit tests
		bulk:    bulk,
		pending: make(map[string]roomLastMsgUpdate),
	}
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

// newCoalescerWithStore builds a coalescer whose inner Store is a mock, for
// tests that exercise the guarded-write overrides (rewind/edit) which both
// mutate the pending buffer AND delegate to the inner store.
func newCoalescerWithStore(t *testing.T, bulk bulkRoomLastMsgWriter) (*coalescingStore, *MockStore) {
	t.Helper()
	inner := NewMockStore(gomock.NewController(t))
	return &coalescingStore{Store: inner, bulk: bulk, pending: make(map[string]roomLastMsgUpdate)}, inner
}

// A system message advances the buffered pointer but carries no preview; the
// newest USER preview must keep riding the entry so the flush lands the drift
// state {lastMsgId: system, lastMsg: user} instead of losing the preview.
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
	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", survivor, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", survivor, deletedAt))
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

	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", nil, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", nil, deletedAt))
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

	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-del", nil, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-del", nil, deletedAt))
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

	inner.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "m-old-del", nil, deletedAt).Return(nil)
	require.NoError(t, c.RewindRoomLastMessage(ctx, "r1", "m-old-del", nil, deletedAt))
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
