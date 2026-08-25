package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/preview"
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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-1", time.Now(), false)))
	assert.Equal(t, 0, bulk.callCount(), "buffered updates must not hit Mongo until Flush")
}

// messages_by_room clusters (created_at DESC, message_id DESC), so created_at alone
// does not order two rows. Coalescing by timestamp only made same-millisecond messages
// resolve by arrival order, which need not be the order the walk reads them back in --
// the room could then store one body under the other's lastMsgId.
func TestCoalescingStore_UpdateRoomLastMessage_BreaksTimestampTiesOnMessageID(t *testing.T) {
	t0 := time.Unix(1700000000, 0).UTC()

	t.Run("a greater id at the same instant wins", func(t *testing.T) {
		bulk := &fakeBulkWriter{}
		c := newCoalescer(bulk)
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-b", t0, false)))
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-a", t0, false)))
		require.NoError(t, c.Flush(context.Background()))
		assert.Equal(t, "msg-b", bulk.lastCall()["room-1"].msgID,
			"the clustering order picks the greater id, whichever arrived first")
	})

	t.Run("arrival order does not decide it", func(t *testing.T) {
		bulk := &fakeBulkWriter{}
		c := newCoalescer(bulk)
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-a", t0, false)))
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-b", t0, false)))
		require.NoError(t, c.Flush(context.Background()))
		assert.Equal(t, "msg-b", bulk.lastCall()["room-1"].msgID,
			"same pair, opposite arrival order, same winner")
	})

	t.Run("a newer timestamp still beats a greater id", func(t *testing.T) {
		bulk := &fakeBulkWriter{}
		c := newCoalescer(bulk)
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-z", t0, false)))
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-a", t0.Add(time.Millisecond), false)))
		require.NoError(t, c.Flush(context.Background()))
		assert.Equal(t, "msg-a", bulk.lastCall()["room-1"].msgID,
			"created_at is still the primary key of the ordering")
	})
}

// The room tuple is small and required; the sealed preview is the large, optional half.
// A stalled flush must shed the second rather than let the buffer grow without limit,
// and must keep buffering the first for every room (#289).
func TestCoalescingStore_ShedsPreviewsPastTheCapButKeepsOrdering(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	t0 := time.Unix(1700000000, 0).UTC()

	const over = maxPendingPreviews + 50
	for i := range over {
		upd := lastMsg("room-"+strconv.Itoa(i), "msg-"+strconv.Itoa(i), t0.Add(time.Duration(i)*time.Millisecond), false)
		upd.Preview = &preview.Sealed{ForMsgID: "msg-" + strconv.Itoa(i)}
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), upd))
	}
	require.NoError(t, c.Flush(context.Background()))

	got := bulk.lastCall()
	require.Len(t, got, over, "every room keeps its ordering fields, capped or not")

	withPreview := 0
	for _, u := range got {
		if u.pvw != nil {
			withPreview++
		}
	}
	assert.Equal(t, maxPendingPreviews, withPreview, "previews are shed at the cap, ordering is not")
	assert.NotEmpty(t, got["room-"+strconv.Itoa(over-1)].msgID,
		"a shed room still carries the last-message tuple it was buffered for")
}

// The cap bounds memory; it must not buy that by reintroducing #224. A shed room whose
// update carries neither a body nor a failure marker would take the flush's key-advance
// branch, stamping the new message's id over the room's PREVIOUS body — a preview
// certified for a message it does not describe.
func TestCoalescingStore_ShedPreviewIsASealFailureNotAKeyAdvance(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	t0 := time.Unix(1700000000, 0).UTC()

	for i := range maxPendingPreviews {
		upd := lastMsg("room-"+strconv.Itoa(i), "m"+strconv.Itoa(i), t0.Add(time.Duration(i)*time.Millisecond), false)
		upd.Preview = &preview.Sealed{ForMsgID: "m" + strconv.Itoa(i)}
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), upd))
	}

	shed := lastMsg("room-shed", "msg-new", t0.Add(time.Hour), false)
	shed.Preview = &preview.Sealed{ForMsgID: "msg-new"}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), shed))
	require.NoError(t, c.Flush(context.Background()))

	got := bulk.lastCall()["room-shed"]
	assert.Nil(t, got.pvw, "the body is what the cap sheds")
	assert.True(t, got.pvwFailed,
		"an eligible message with no composed preview is a seal failure; without the marker "+
			"the flush advances the freshness key over whatever body the room already had")
	assert.Equal(t, "msg-new", got.msgID, "ordering is still recorded")
}

// created_at is a Cassandra timestamp — milliseconds. Two messages differing only below
// that resolution are one clustering position there, so the id tiebreaker must decide
// them; comparing Go's full precision would silently skip it.
func TestCoalescingStore_TiesWithinOneMillisecondUseTheIDTiebreaker(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	t0 := time.Unix(1700000000, 0).UTC()

	// Same millisecond, different nanoseconds — indistinguishable once stored.
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-b", t0.Add(400*time.Microsecond), false)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-a", t0.Add(900*time.Microsecond), false)))
	require.NoError(t, c.Flush(context.Background()))

	assert.Equal(t, "msg-b", bulk.lastCall()["room-1"].msgID,
		"a later nanosecond must not beat a greater id inside one stored millisecond")
}

// A room already holding a preview must keep being updated past the cap: refusing it
// would freeze that room on an older body while newer messages kept arriving.
func TestCoalescingStore_CapDoesNotFreezeARoomAlreadyHoldingAPreview(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	t0 := time.Unix(1700000000, 0).UTC()

	first := lastMsg("room-hot", "msg-1", t0, false)
	first.Preview = &preview.Sealed{ForMsgID: "msg-1"}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), first))

	for i := range maxPendingPreviews + 10 {
		upd := lastMsg("room-"+strconv.Itoa(i), "m"+strconv.Itoa(i), t0.Add(time.Duration(i+1)*time.Millisecond), false)
		upd.Preview = &preview.Sealed{ForMsgID: "m" + strconv.Itoa(i)}
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), upd))
	}

	later := lastMsg("room-hot", "msg-2", t0.Add(time.Hour), false)
	later.Preview = &preview.Sealed{ForMsgID: "msg-2"}
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), later))
	require.NoError(t, c.Flush(context.Background()))

	got := bulk.lastCall()["room-hot"]
	require.NotNil(t, got.pvw)
	assert.Equal(t, "msg-2", got.pvw.ForMsgID, "an already-counted room is not re-capped")
}

func TestCoalescingStore_Flush_WritesPendingBatch(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t0 := time.Unix(1700000000, 0).UTC()
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-a", "msg-a", t0, false)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-b", "msg-b", t0.Add(time.Second), true)))

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

// fakeActivityPublisher records the cross-site activity refreshes a flush emits.
type fakeActivityPublisher struct {
	mu    sync.Mutex
	sent  []roomActivityRefresh
	fails bool
}

func (f *fakeActivityPublisher) publish(_ context.Context, r roomActivityRefresh) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fails {
		return errors.New("publish failed")
	}
	f.sent = append(f.sent, r)
	return nil
}

func (f *fakeActivityPublisher) all() []roomActivityRefresh {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]roomActivityRefresh(nil), f.sent...)
}

func TestCoalescingStore_Flush_PublishesActivityForCrossSiteRoomsOnly(t *testing.T) {
	bulk := &fakeBulkWriter{}
	pub := &fakeActivityPublisher{}
	c := newCoalescer(bulk)
	// r-x is cross-site; r-local is not. Only the former needs a refresh — the
	// destination is the only site without a rooms doc to order by.
	c.crossSite = func(_ context.Context, roomID string) bool { return roomID == "r-x" }
	c.publishActivity = pub.publish

	t0 := time.Unix(1700000000, 0).UTC()
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-x", "m1", t0, false)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-local", "m2", t0, false)))
	require.NoError(t, c.Flush(context.Background()))

	sent := pub.all()
	require.Len(t, sent, 1, "only the cross-site room is refreshed")
	assert.Equal(t, "r-x", sent[0].roomID)
	assert.Equal(t, t0, sent[0].at)
}

func TestCoalescingStore_Flush_CoalescesActivityToOnePublishPerRoom(t *testing.T) {
	bulk := &fakeBulkWriter{}
	pub := &fakeActivityPublisher{}
	c := newCoalescer(bulk)
	c.crossSite = func(_ context.Context, _ string) bool { return true }
	c.publishActivity = pub.publish

	t0 := time.Unix(1700000000, 0).UTC()
	// Many messages in one window collapse to a single refresh carrying the latest
	// position — this is what keeps the cost independent of message rate.
	for i := range 50 {
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-busy", "m", t0.Add(time.Duration(i)*time.Millisecond), false)))
	}
	require.NoError(t, c.Flush(context.Background()))

	sent := pub.all()
	require.Len(t, sent, 1)
	assert.Equal(t, t0.Add(49*time.Millisecond), sent[0].at, "the latest position wins")
}

func TestCoalescingStore_Flush_ThrottlesRefreshPerRoom(t *testing.T) {
	pub := &fakeActivityPublisher{}
	c := newCoalescer(&fakeBulkWriter{})
	c.crossSite = func(_ context.Context, _ string) bool { return true }
	c.publishActivity = pub.publish
	c.refreshInterval = 5 * time.Second
	clock := time.Unix(1700000000, 0).UTC()
	c.now = func() time.Time { return clock }

	msgAt := clock
	send := func() {
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-x", "m", msgAt, false)))
		require.NoError(t, c.Flush(context.Background()))
	}

	send() // first flush publishes
	require.Len(t, pub.all(), 1)

	// Nineteen more flush windows, landing at 4.75s — still inside the interval.
	// The Mongo batch runs every time, but the room must not be re-announced.
	for range 19 {
		clock = clock.Add(250 * time.Millisecond)
		msgAt = clock
		send()
	}
	assert.Len(t, pub.all(), 1, "throttled within the refresh interval")

	clock = clock.Add(5 * time.Second)
	msgAt = clock
	send()
	sent := pub.all()
	require.Len(t, sent, 2, "publishes again once the interval elapses")
	assert.Equal(t, msgAt, sent[1].at, "and carries the latest position, not the one it skipped")
}

func TestCoalescingStore_Flush_ThrottlesRoomsIndependently(t *testing.T) {
	pub := &fakeActivityPublisher{}
	c := newCoalescer(&fakeBulkWriter{})
	c.crossSite = func(_ context.Context, _ string) bool { return true }
	c.publishActivity = pub.publish
	c.refreshInterval = 5 * time.Second
	clock := time.Unix(1700000000, 0).UTC()
	c.now = func() time.Time { return clock }

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-a", "m", clock, false)))
	require.NoError(t, c.Flush(context.Background()))

	// r-b is newly active — its own watermark is unset, so it publishes even
	// though r-a is mid-interval.
	clock = clock.Add(250 * time.Millisecond)
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-a", "m", clock, false)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-b", "m", clock, false)))
	require.NoError(t, c.Flush(context.Background()))

	var rooms []string
	for _, r := range pub.all() {
		rooms = append(rooms, r.roomID)
	}
	assert.Equal(t, []string{"r-a", "r-b"}, rooms)
}

func TestCoalescingStore_Flush_ZeroIntervalPublishesEveryFlush(t *testing.T) {
	pub := &fakeActivityPublisher{}
	c := newCoalescer(&fakeBulkWriter{})
	c.crossSite = func(_ context.Context, _ string) bool { return true }
	c.publishActivity = pub.publish
	c.refreshInterval = 0 // throttling disabled

	for range 3 {
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-x", "m", time.Now(), false)))
		require.NoError(t, c.Flush(context.Background()))
	}
	assert.Len(t, pub.all(), 3)
}

func TestCoalescingStore_Flush_ForgetsWatermarksForQuietRooms(t *testing.T) {
	// The watermark map must stay bounded by rooms active within the interval,
	// not grow with every room ever seen.
	pub := &fakeActivityPublisher{}
	c := newCoalescer(&fakeBulkWriter{})
	c.crossSite = func(_ context.Context, _ string) bool { return true }
	c.publishActivity = pub.publish
	c.refreshInterval = time.Second
	clock := time.Unix(1700000000, 0).UTC()
	c.now = func() time.Time { return clock }

	for i := range 100 {
		require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg(fmt.Sprintf("r-%d", i), "m", clock, false)))
	}
	require.NoError(t, c.Flush(context.Background()))
	assert.Len(t, c.lastRefreshed, 100)

	// Those rooms go quiet; a later flush prunes their watermarks.
	clock = clock.Add(10 * time.Second)
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-live", "m", clock, false)))
	require.NoError(t, c.Flush(context.Background()))
	assert.Len(t, c.lastRefreshed, 1, "only the still-active room retains a watermark")
}

func TestCoalescingStore_Flush_PublishFailureDoesNotFailTheFlush(t *testing.T) {
	// The refresh is decorative and self-healing on the next message; a publish
	// failure must not lose the Mongo batch it rides with.
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)
	c.crossSite = func(_ context.Context, _ string) bool { return true }
	c.publishActivity = (&fakeActivityPublisher{fails: true}).publish

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("r-x", "m1", time.Now(), false)))
	require.NoError(t, c.Flush(context.Background()), "flush must still succeed")
	assert.Equal(t, 1, bulk.callCount(), "and the room batch must still land")
}

func TestCoalescingStore_Update_LatestMessageWinsPerRoom(t *testing.T) {
	bulk := &fakeBulkWriter{}
	c := newCoalescer(bulk)

	t1 := time.Unix(1700000000, 0).UTC()
	t2 := t1.Add(500 * time.Millisecond)
	t3 := t2.Add(500 * time.Millisecond)

	// Send in order: t1, t3, t2. Latest (t3) must win regardless of arrival order.
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-x", "msg-1", t1, false)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-x", "msg-3", t3, false)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-x", "msg-2", t2, false)))

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
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-x", "m1", t1, true)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-x", "m2", t2, false)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-x", "m3", t3, true)))

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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-x", "m1", t1, true)))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-x", "m2", t2, false)))

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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-1", time.Now(), false)))
	require.NoError(t, c.Flush(context.Background()))
	require.NoError(t, c.Flush(context.Background()))

	assert.Equal(t, 1, bulk.callCount(), "second flush with empty buffer must not call bulk writer")
}

func TestCoalescingStore_Flush_PropagatesBulkError(t *testing.T) {
	wantErr := errors.New("bulk failed")
	bulk := &fakeBulkWriter{err: wantErr}
	c := newCoalescer(bulk)

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-1", time.Now(), false)))

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
				_ = c.UpdateRoomLastMessage(context.Background(), lastMsg("room-shared", "msg", base.Add(time.Duration(g*1000+i)*time.Millisecond), false))
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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-1", time.Now(), false)))

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

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), lastMsg("room-1", "msg-1", time.Now(), false)))
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	assert.Equal(t, 1, bulk.callCount(), "shutdown must perform a final flush of buffered updates")
}
