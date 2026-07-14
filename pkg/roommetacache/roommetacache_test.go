package roommetacache_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

// stubProvider is a minimal MetaProvider used by WrapStore tests.
type stubProvider struct {
	calls atomic.Int32
	meta  roommetacache.Meta
}

func (s *stubProvider) GetRoomMeta(_ context.Context, _ string) (roommetacache.Meta, error) {
	s.calls.Add(1)
	return s.meta, nil
}

func makeMeta(id string) roommetacache.Meta {
	return roommetacache.Meta{
		ID:        id,
		Type:      model.RoomTypeChannel,
		Name:      "room " + id,
		SiteID:    "site-a",
		UserCount: 7,
	}
}

func TestCache_GetMissThenHit(t *testing.T) {
	var loaderCalls atomic.Int32
	loader := func(_ context.Context, roomID string) (roommetacache.Meta, error) {
		loaderCalls.Add(1)
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(10, time.Minute, loader)
	require.NoError(t, err)

	// First call: miss, loader runs.
	got, err := c.Get(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, makeMeta("r1"), got)
	assert.Equal(t, int32(1), loaderCalls.Load(), "loader should run on miss")

	// Second call: hit, loader does NOT run again.
	got2, err := c.Get(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, makeMeta("r1"), got2)
	assert.Equal(t, int32(1), loaderCalls.Load(), "loader should not run on hit")
}

func TestCache_LoaderErrorNotCached(t *testing.T) {
	var calls atomic.Int32
	wantErr := errors.New("boom")
	loader := func(_ context.Context, _ string) (roommetacache.Meta, error) {
		calls.Add(1)
		return roommetacache.Meta{}, wantErr
	}
	c, err := roommetacache.New(10, time.Minute, loader)
	require.NoError(t, err)

	_, err = c.Get(context.Background(), "r1")
	assert.ErrorIs(t, err, wantErr)
	_, err = c.Get(context.Background(), "r1")
	assert.ErrorIs(t, err, wantErr)

	assert.Equal(t, int32(2), calls.Load(), "errors should not be cached; loader must run again")
}

func TestCache_TTLExpires(t *testing.T) {
	var calls atomic.Int32
	loader := func(_ context.Context, roomID string) (roommetacache.Meta, error) {
		calls.Add(1)
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(10, 50*time.Millisecond, loader)
	require.NoError(t, err)

	_, err = c.Get(context.Background(), "r1")
	require.NoError(t, err)
	_, err = c.Get(context.Background(), "r1")
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load(), "second call within TTL should be a hit")

	time.Sleep(75 * time.Millisecond)
	_, err = c.Get(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load(), "after TTL expiry, loader runs again")
}

func TestCache_CapacityEviction(t *testing.T) {
	var calls atomic.Int32
	loader := func(_ context.Context, roomID string) (roommetacache.Meta, error) {
		calls.Add(1)
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(2, time.Minute, loader)
	require.NoError(t, err)

	_, err = c.Get(context.Background(), "r1") // miss
	require.NoError(t, err)
	_, err = c.Get(context.Background(), "r2") // miss
	require.NoError(t, err)
	_, err = c.Get(context.Background(), "r3") // miss; evicts r1 (LRU)
	require.NoError(t, err)
	require.Equal(t, int32(3), calls.Load())

	// r1 should be evicted; re-loading produces another miss.
	_, err = c.Get(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, int32(4), calls.Load(), "r1 should have been evicted by capacity")

	// r3 should still be cached.
	_, err = c.Get(context.Background(), "r3")
	require.NoError(t, err)
	assert.Equal(t, int32(4), calls.Load(), "r3 should still be a hit")
}

func TestCache_Invalidate(t *testing.T) {
	var calls atomic.Int32
	loader := func(_ context.Context, roomID string) (roommetacache.Meta, error) {
		calls.Add(1)
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(10, time.Minute, loader)
	require.NoError(t, err)

	_, err = c.Get(context.Background(), "r1")
	require.NoError(t, err)
	_, err = c.Get(context.Background(), "r1")
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load())

	c.Invalidate("r1")

	_, err = c.Get(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load(), "Invalidate should cause next Get to miss")
}

func TestCache_SingleflightDedupsMisses(t *testing.T) {
	var calls atomic.Int32
	gate := make(chan struct{})
	loader := func(_ context.Context, roomID string) (roommetacache.Meta, error) {
		calls.Add(1)
		<-gate // hold so concurrent callers pile up
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(10, time.Minute, loader)
	require.NoError(t, err)

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_, err := c.Get(context.Background(), "r1")
			assert.NoError(t, err)
		}()
	}

	// Give all callers a moment to enter Get and block on singleflight.
	time.Sleep(20 * time.Millisecond)
	close(gate)
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load(),
		"singleflight should collapse N concurrent misses on the same key to 1 loader call")
}

func TestNew_RejectsInvalidArgs(t *testing.T) {
	okLoader := func(_ context.Context, _ string) (roommetacache.Meta, error) { return roommetacache.Meta{}, nil }
	tests := []struct {
		name    string
		size    int
		ttl     time.Duration
		loader  roommetacache.Loader
		wantErr string
	}{
		{"zero size", 0, time.Minute, okLoader, "size must be positive"},
		{"negative size", -1, time.Minute, okLoader, "size must be positive"},
		{"zero ttl", 10, 0, okLoader, "ttl must be positive"},
		{"negative ttl", 10, -1, okLoader, "ttl must be positive"},
		{"nil loader", 10, time.Minute, nil, "loader must not be nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := roommetacache.New(tc.size, tc.ttl, tc.loader)
			assert.Nil(t, c)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestWrapStore_CachesGetRoomMeta(t *testing.T) {
	stub := &stubProvider{meta: makeMeta("r1")}
	w, err := roommetacache.WrapStore(stub, 10, time.Minute)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		got, err := w.GetRoomMeta(context.Background(), "r1")
		require.NoError(t, err)
		assert.Equal(t, makeMeta("r1"), got)
	}
	assert.Equal(t, int32(1), stub.calls.Load(), "wrapper should call inner exactly once across N hits")
}

func TestWrapStore_RejectsInvalidArgs(t *testing.T) {
	stub := &stubProvider{}
	_, err := roommetacache.WrapStore(stub, 0, time.Minute)
	assert.Error(t, err)
	_, err = roommetacache.WrapStore(stub, 10, 0)
	assert.Error(t, err)
}

func TestCache_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	loader := func(ctx context.Context, roomID string) (roommetacache.Meta, error) {
		calls.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-block
		if err := ctx.Err(); err != nil {
			return roommetacache.Meta{}, err
		}
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(10, time.Minute, loader)
	require.NoError(t, err)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, e := c.Get(leaderCtx, "r1")
		leaderDone <- e
	}()
	<-entered

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, e := c.Get(context.Background(), "r1")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	require.ErrorIs(t, <-leaderDone, context.Canceled)
	close(block)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	got, err := c.Get(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, makeMeta("r1"), got)
	assert.Equal(t, int32(1), calls.Load(), "shared load should have populated the cache")
}

func TestCache_CallerCancelReturnsCtxErr(t *testing.T) {
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	defer close(block)
	loader := func(ctx context.Context, roomID string) (roommetacache.Meta, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-block
		if err := ctx.Err(); err != nil {
			return roommetacache.Meta{}, err
		}
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(10, time.Minute, loader)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := c.Get(ctx, "r1")
		done <- e
	}()
	<-entered
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}
