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

func (f *fakeSubSource) GetHistorySharedSince(_ context.Context, _, _ string) (*time.Time, bool, error) {
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.block != nil {
		<-f.block
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

func TestSubscriptionCache_Stats(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{sharedSince: &ts, subscribed: true}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", "r1") // miss
	_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", "r1") // hit
	_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", "r1") // hit

	s := c.Stats()
	assert.Equal(t, uint64(2), s.Hits)
	assert.Equal(t, uint64(1), s.Misses)
}

func TestNewSubscriptionCache_InvalidConfig(t *testing.T) {
	src := &fakeSubSource{}
	_, err := NewSubscriptionCache(src, 0, time.Minute)
	assert.Error(t, err)
	_, err = NewSubscriptionCache(src, 100, 0)
	assert.Error(t, err)
}

type fakeRoomSource struct {
	timesCalls   atomic.Int32
	minSeenCalls atomic.Int32
	lastMsgAt    time.Time
	createdAt    time.Time
	timesErr     error
	minSeen      *time.Time
	minSeenErr   error
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
