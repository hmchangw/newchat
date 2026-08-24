package readcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
)

type fakeSubSource struct {
	calls       atomic.Int32
	sharedSince *time.Time
	subscribed  bool
	err         error
	block       chan struct{} // when non-nil, blocks until closed
	started     chan struct{} // when non-nil, signals each entry
}

func (f *fakeSubSource) GetHistorySharedSince(ctx context.Context, _, _ string) (*time.Time, bool, error) {
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.block != nil {
		<-f.block
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	f.calls.Add(1)
	return f.sharedSince, f.subscribed, f.err
}

func (f *fakeSubSource) GetSubscription(_ context.Context, _, _ string) (*pkgmodel.Subscription, error) {
	return nil, nil
}

func TestSubscriptionCache_CachesPositive(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{sharedSince: &ts, subscribed: true}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	got1, sub1, err1 := c.GetHistorySharedSince(context.Background(), "alice", "r1")
	require.NoError(t, err1)
	assert.True(t, sub1)
	require.NotNil(t, got1)
	assert.Equal(t, ts, *got1)

	got2, sub2, err2 := c.GetHistorySharedSince(context.Background(), "alice", "r1")
	require.NoError(t, err2)
	assert.True(t, sub2)
	require.NotNil(t, got2)
	assert.Equal(t, ts, *got2)

	assert.Equal(t, int32(1), src.calls.Load(), "positive subscription should be served from cache on the second call")
}

func TestSubscriptionCache_DoesNotCacheNegative(t *testing.T) {
	src := &fakeSubSource{subscribed: false}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		got, sub, err := c.GetHistorySharedSince(context.Background(), "alice", "r1")
		require.NoError(t, err)
		assert.False(t, sub)
		assert.Nil(t, got)
	}
	assert.Equal(t, int32(3), src.calls.Load(), "not-subscribed results must never be cached")
}

func TestSubscriptionCache_DoesNotCacheError(t *testing.T) {
	src := &fakeSubSource{err: errors.New("mongo down")}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	_, _, err1 := c.GetHistorySharedSince(context.Background(), "alice", "r1")
	require.Error(t, err1)
	_, _, err2 := c.GetHistorySharedSince(context.Background(), "alice", "r1")
	require.Error(t, err2)
	assert.Equal(t, int32(2), src.calls.Load(), "errors must never be cached")
}

func TestSubscriptionCache_KeysByAccountAndRoom(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{sharedSince: &ts, subscribed: true}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", "r1")
	_, _, _ = c.GetHistorySharedSince(context.Background(), "bob", "r1")
	_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", "r2")
	assert.Equal(t, int32(3), src.calls.Load(), "distinct (account,room) pairs must be distinct cache keys")
}

func TestSubscriptionCache_Expiry(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{sharedSince: &ts, subscribed: true}
	c, err := NewSubscriptionCache(src, 100, 20*time.Millisecond)
	require.NoError(t, err)

	_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", "r1")
	time.Sleep(40 * time.Millisecond)
	_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", "r1")
	assert.Equal(t, int32(2), src.calls.Load(), "entry should re-load after TTL expiry")
}

func TestSubscriptionCache_SingleflightDedupesConcurrentMisses(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{
		sharedSince: &ts,
		subscribed:  true,
		block:       make(chan struct{}),
		started:     make(chan struct{}, 1),
	}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", "r1")
		}()
	}
	<-src.started    // first loader has entered the source
	close(src.block) // release it
	wg.Wait()

	assert.Equal(t, int32(1), src.calls.Load(), "concurrent misses for the same key should load once")
}

func TestNewSubscriptionCache_InvalidConfig(t *testing.T) {
	src := &fakeSubSource{}
	_, err := NewSubscriptionCache(src, 0, time.Minute)
	assert.Error(t, err)
	_, err = NewSubscriptionCache(src, 100, 0)
	assert.Error(t, err)
}

type fakeRoomSource struct {
	timesCalls      atomic.Int32
	minSeenCalls    atomic.Int32
	lastMsgAt       time.Time
	createdAt       time.Time
	timesErr        error
	minSeen         *time.Time
	minSeenErr      error
	timesByIDsCalls atomic.Int32
	timesByIDsIDs   []string
	timesByIDs      map[string]mongorepo.RoomTimes
	timesByIDsErr   error

	setPreviewCalls   atomic.Int32
	setPreviewArgs    previewWrite
	setPreviewErr     error
	updateBodyCalls   atomic.Int32
	updateBodyArgs    previewWrite
	clearPreviewCalls atomic.Int32
	clearPreviewArgs  previewWrite
	invalidateCalls   atomic.Int32
	invalidateArgs    previewWrite
}

// previewWrite captures the identifying arguments of a preview write so a test can
// assert the cache forwarded them unchanged.
type previewWrite struct {
	roomID   string
	forMsgID string
	asOf     int64
}

func (f *fakeRoomSource) GetRoomTimes(_ context.Context, _ string) (time.Time, time.Time, error) {
	f.timesCalls.Add(1)
	return f.lastMsgAt, f.createdAt, f.timesErr
}

func (f *fakeRoomSource) GetMinUserLastSeenAt(_ context.Context, _ string) (*time.Time, error) {
	f.minSeenCalls.Add(1)
	return f.minSeen, f.minSeenErr
}

func (f *fakeRoomSource) GetRoomUserCount(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (f *fakeRoomSource) GetRoomTimesByIDs(_ context.Context, ids []string) (map[string]mongorepo.RoomTimes, error) {
	f.timesByIDsCalls.Add(1)
	f.timesByIDsIDs = ids
	if f.timesByIDsErr != nil {
		return nil, f.timesByIDsErr
	}
	return f.timesByIDs, nil
}

//nolint:gocritic // hugeParam: the by-value shape is the RoomSource contract under test.
func (f *fakeRoomSource) SetPreviewMessage(_ context.Context, roomID string, _ pkgmodel.PreviewMessage, forMsgID string, asOf int64) error {
	f.setPreviewCalls.Add(1)
	f.setPreviewArgs = previewWrite{roomID: roomID, forMsgID: forMsgID, asOf: asOf}
	return f.setPreviewErr
}

//nolint:gocritic // hugeParam: the by-value shape is the RoomSource contract under test.
func (f *fakeRoomSource) UpdatePreviewBody(_ context.Context, roomID string, _ pkgmodel.PreviewMessage, _ string, asOf int64) (bool, error) {
	f.updateBodyCalls.Add(1)
	f.updateBodyArgs = previewWrite{roomID: roomID, asOf: asOf}
	return true, nil
}

func (f *fakeRoomSource) ClearPreview(_ context.Context, roomID string, asOf int64) (bool, error) {
	f.clearPreviewCalls.Add(1)
	f.clearPreviewArgs = previewWrite{roomID: roomID, asOf: asOf}
	return true, nil
}

func (f *fakeRoomSource) InvalidatePreviewKey(_ context.Context, roomID, msgID string) error {
	f.invalidateCalls.Add(1)
	f.invalidateArgs = previewWrite{roomID: roomID, forMsgID: msgID}
	return nil
}

// The preview writes are pass-throughs, not cached reads: a stale write would be a
// correctness bug, where a stale read is only a stale row.
func TestRoomCache_PreviewWrites_BypassTheCache(t *testing.T) {
	src := &fakeRoomSource{}
	c, err := NewRoomCache(src, 8, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, c.SetPreviewMessage(ctx, "r1", pkgmodel.PreviewMessage{}, "m-9", 100))
	require.NoError(t, c.SetPreviewMessage(ctx, "r1", pkgmodel.PreviewMessage{}, "m-9", 100))
	applied, err := c.UpdatePreviewBody(ctx, "r1", pkgmodel.PreviewMessage{}, "m-observed", 200)
	require.NoError(t, err)
	assert.True(t, applied, "the applied signal must survive the passthrough")
	cleared, err := c.ClearPreview(ctx, "r1", 300)
	require.NoError(t, err)
	assert.True(t, cleared, "the applied signal must survive the passthrough")
	require.NoError(t, c.InvalidatePreviewKey(ctx, "r1", "m-mutated"))

	assert.Equal(t, int32(2), src.setPreviewCalls.Load(), "an identical repeat write must still reach the source")
	assert.Equal(t, previewWrite{roomID: "r1", forMsgID: "m-9", asOf: 100}, src.setPreviewArgs)
	assert.Equal(t, int32(1), src.updateBodyCalls.Load())
	assert.Equal(t, previewWrite{roomID: "r1", asOf: 200}, src.updateBodyArgs)
	assert.Equal(t, int32(1), src.clearPreviewCalls.Load())
	assert.Equal(t, previewWrite{roomID: "r1", asOf: 300}, src.clearPreviewArgs)
	assert.Equal(t, int32(1), src.invalidateCalls.Load())
	assert.Equal(t, previewWrite{roomID: "r1", forMsgID: "m-mutated"}, src.invalidateArgs)
}

func TestRoomCache_SetPreviewMessage_PropagatesError(t *testing.T) {
	src := &fakeRoomSource{setPreviewErr: errors.New("mongo down")}
	c, err := NewRoomCache(src, 8, time.Minute)
	require.NoError(t, err)

	assert.ErrorContains(t, c.SetPreviewMessage(context.Background(), "r1", pkgmodel.PreviewMessage{}, "m-1", 1), "mongo down")
}

func TestRoomCache_CachesRoomTimes(t *testing.T) {
	last := time.Now().UTC()
	created := last.Add(-time.Hour)
	src := &fakeRoomSource{lastMsgAt: last, createdAt: created}
	c, err := NewRoomCache(src, 100, time.Minute)
	require.NoError(t, err)

	l1, cr1, err1 := c.GetRoomTimes(context.Background(), "r1")
	require.NoError(t, err1)
	assert.Equal(t, last, l1)
	assert.Equal(t, created, cr1)

	l2, cr2, err2 := c.GetRoomTimes(context.Background(), "r1")
	require.NoError(t, err2)
	assert.Equal(t, last, l2)
	assert.Equal(t, created, cr2)

	assert.Equal(t, int32(1), src.timesCalls.Load(), "room times should be cached on the second read")
}

func TestRoomCache_DoesNotCacheRoomTimesError(t *testing.T) {
	src := &fakeRoomSource{timesErr: errors.New("no documents")}
	c, err := NewRoomCache(src, 100, time.Minute)
	require.NoError(t, err)

	_, _, err1 := c.GetRoomTimes(context.Background(), "r1")
	require.Error(t, err1)
	_, _, err2 := c.GetRoomTimes(context.Background(), "r1")
	require.Error(t, err2)
	assert.Equal(t, int32(2), src.timesCalls.Load(), "room-times errors must not be cached")
}

func TestRoomCache_CachesMinUserLastSeenAtIncludingNil(t *testing.T) {
	src := &fakeRoomSource{minSeen: nil}
	c, err := NewRoomCache(src, 100, time.Minute)
	require.NoError(t, err)

	got1, err1 := c.GetMinUserLastSeenAt(context.Background(), "r1")
	require.NoError(t, err1)
	assert.Nil(t, got1)

	got2, err2 := c.GetMinUserLastSeenAt(context.Background(), "r1")
	require.NoError(t, err2)
	assert.Nil(t, got2)

	assert.Equal(t, int32(1), src.minSeenCalls.Load(), "a nil min-last-seen is a valid cacheable result")
}

func TestRoomCache_MinUserLastSeenAtValue(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeRoomSource{minSeen: &ts}
	c, err := NewRoomCache(src, 100, time.Minute)
	require.NoError(t, err)

	got, err := c.GetMinUserLastSeenAt(context.Background(), "r1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ts, *got)
}

func TestRoomCache_GetRoomTimesByIDs_BypassesCache(t *testing.T) {
	want := map[string]mongorepo.RoomTimes{
		"r1": {LastMsgAt: time.Now().UTC(), CreatedAt: time.Now().UTC().Add(-time.Hour)},
	}
	src := &fakeRoomSource{timesByIDs: want}
	c, err := NewRoomCache(src, 100, time.Minute)
	require.NoError(t, err)

	got, err := c.GetRoomTimesByIDs(context.Background(), []string{"r1", "r2"})
	require.NoError(t, err)
	assert.Equal(t, want, got)

	_, err = c.GetRoomTimesByIDs(context.Background(), []string{"r1", "r2"})
	require.NoError(t, err)
	assert.Equal(t, int32(2), src.timesByIDsCalls.Load(), "batch read is never cached")
	assert.Equal(t, []string{"r1", "r2"}, src.timesByIDsIDs)
}

func TestRoomCache_GetRoomTimesByIDs_PropagatesError(t *testing.T) {
	wantErr := errors.New("mongo down")
	src := &fakeRoomSource{timesByIDsErr: wantErr}
	c, err := NewRoomCache(src, 100, time.Minute)
	require.NoError(t, err)

	_, err = c.GetRoomTimesByIDs(context.Background(), []string{"r1"})
	require.ErrorIs(t, err, wantErr)
}

func TestSubscriptionCache_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{
		sharedSince: &ts,
		subscribed:  true,
		block:       make(chan struct{}),
		started:     make(chan struct{}),
	}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, _, e := c.GetHistorySharedSince(leaderCtx, "alice", "r1")
		leaderDone <- e
	}()
	<-src.started

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, _, e := c.GetHistorySharedSince(context.Background(), "alice", "r1")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	require.ErrorIs(t, <-leaderDone, context.Canceled)
	close(src.block)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	_, sub, err := c.GetHistorySharedSince(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.True(t, sub)
	assert.Equal(t, int32(1), src.calls.Load(), "shared load should have populated the cache")
}

func TestSubscriptionCache_CallerCancelReturnsCtxErr(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{
		sharedSince: &ts,
		subscribed:  true,
		block:       make(chan struct{}),
		started:     make(chan struct{}),
	}
	defer close(src.block)
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, e := c.GetHistorySharedSince(ctx, "alice", "r1")
		done <- e
	}()
	<-src.started
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}

func TestPreviewCache_Get_CachesPositiveNotNegative(t *testing.T) {
	pc, err := NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	// Positive: loader runs once, second Get is a hit.
	posCalls := 0
	load := func(context.Context) (pkgmodel.PreviewMessage, bool, error) {
		posCalls++
		return pkgmodel.PreviewMessage{MessageID: "m1"}, true, nil
	}
	p, ok, err := pc.Get(ctx, "r1", load)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "m1", p.MessageID)
	_, _, _ = pc.Get(ctx, "r1", load)
	assert.Equal(t, 1, posCalls, "positive result is cached")

	// Negative (found=false): never cached, loader runs every time.
	negCalls := 0
	negLoad := func(context.Context) (pkgmodel.PreviewMessage, bool, error) {
		negCalls++
		return pkgmodel.PreviewMessage{}, false, nil
	}
	_, ok, _ = pc.Get(ctx, "r2", negLoad)
	require.False(t, ok)
	_, _, _ = pc.Get(ctx, "r2", negLoad)
	assert.Equal(t, 2, negCalls, "negative result is not cached")
}

func TestPreviewCache_Get_ErrorNotCachedAndPropagated(t *testing.T) {
	pc, err := NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	wantErr := errors.New("cassandra down")
	calls := 0
	load := func(context.Context) (pkgmodel.PreviewMessage, bool, error) {
		calls++
		return pkgmodel.PreviewMessage{}, false, wantErr
	}
	_, _, err = pc.Get(ctx, "r1", load)
	require.ErrorIs(t, err, wantErr)
	_, _, _ = pc.Get(ctx, "r1", load)
	assert.Equal(t, 2, calls, "errors are not cached")
}

func TestPreviewCache_Get_SingleflightDedupsConcurrentMisses(t *testing.T) {
	pc, err := NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	var calls int32
	started := make(chan struct{}, 1) // the leader loader signals it has entered
	start := make(chan struct{})      // then blocks here until released
	load := func(context.Context) (pkgmodel.PreviewMessage, bool, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			started <- struct{}{}
		}
		<-start // hold the leader inside the loader so followers coalesce onto its flight
		return pkgmodel.PreviewMessage{MessageID: "m1"}, true, nil
	}

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() { defer wg.Done(); _, _, _ = pc.Get(ctx, "r1", load) }()
	}
	<-started    // first loader has entered the flight
	close(start) // release it
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "concurrent misses for the same key should load once")
}
