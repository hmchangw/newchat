package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false))
	assert.Equal(t, 0, bulk.callCount(), "buffered updates must not hit Mongo until Flush")
}

func TestCoalescingStore_Flush_WritesPendingBatch(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t0 := time.Unix(1700000000, 0).UTC()
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-a", "msg-a", t0, false))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-b", "msg-b", t0.Add(time.Second), true))

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
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "msg-1", t1, false))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "msg-3", t3, false))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "msg-2", t2, false))

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
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m1", t1, true))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m2", t2, false))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m3", t3, true))

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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m1", t1, true))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-x", "m2", t2, false))

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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false))
	require.NoError(t, c.Flush(context.Background()))
	require.NoError(t, c.Flush(context.Background()))

	assert.Equal(t, 1, bulk.callCount(), "second flush with empty buffer must not call bulk writer")
}

func TestCoalescingStore_Flush_PropagatesBulkError(t *testing.T) {
	wantErr := errors.New("bulk failed")
	bulk := &fakeBulkWriter{err: wantErr}
	c := newCoalescer(bulk)

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false))

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
				_ = c.UpdateRoomLastMessage(context.Background(), "room-shared", "msg", base.Add(time.Duration(g*1000+i)*time.Millisecond), false)
			}
		}(g)
	}
	wg.Wait()

	require.NoError(t, c.Flush(context.Background()))
	require.Equal(t, 1, bulk.callCount())
	got := bulk.lastCall()
	require.Len(t, got, 1, "all writes coalesced into a single room entry")
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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false))

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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "room-1", "msg-1", time.Now(), false))
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	assert.Equal(t, 1, bulk.callCount(), "shutdown must perform a final flush of buffered updates")
}
