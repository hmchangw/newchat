package roomsubcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type fakeCache struct {
	mu   sync.Mutex
	data map[string][]Member
}

func newFakeCache() *fakeCache { return &fakeCache{data: map[string][]Member{}} }

func (f *fakeCache) Get(_ context.Context, roomID string) ([]Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[roomID]
	if !ok {
		return nil, valkeyutil.ErrCacheMiss
	}
	return v, nil
}
func (f *fakeCache) Set(_ context.Context, roomID string, members []Member, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]Member, len(members))
	copy(cp, members)
	f.data[roomID] = cp
	return nil
}
func (f *fakeCache) Invalidate(_ context.Context, roomID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, roomID)
	return nil
}

type fakeLoader struct {
	calls   atomic.Int32
	out     []Member
	err     error
	delay   time.Duration
	block   chan struct{} // when non-nil, blocks (unconditionally) before returning
	entered chan struct{} // when non-nil (buffered), signals once when entered
}

func (f *fakeLoader) Load(ctx context.Context, _ string) ([]Member, error) {
	f.calls.Add(1)
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.block != nil {
		<-f.block
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return f.out, f.err
}

func TestLookup_HitFromValkey(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []Member{{ID: "u1", Account: "alice"}}}
	require.NoError(t, cache.Set(context.Background(), "r1", loader.out, time.Minute))

	lookup := NewLookup(cache, loader.Load, time.Minute)
	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)
	assert.Equal(t, int32(0), loader.calls.Load())
}

func TestLookup_MissThenPopulate(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []Member{{ID: "u1", Account: "alice"}}}
	lookup := NewLookup(cache, loader.Load, time.Minute)

	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)

	_, err = lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, int32(1), loader.calls.Load())
}

func TestLookup_CacheErrorFallsThrough(t *testing.T) {
	cache := &erroringCache{err: errors.New("valkey down")}
	loader := &fakeLoader{out: []Member{{ID: "u1", Account: "alice"}}}
	lookup := NewLookup(cache, loader.Load, time.Minute)

	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)
	assert.Equal(t, int32(1), loader.calls.Load())
}

func TestLookup_SingleFlightCollapsesMisses(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{
		out:   []Member{{ID: "u1", Account: "alice"}},
		delay: 50 * time.Millisecond,
	}
	lookup := NewLookup(cache, loader.Load, time.Minute)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lookup.GetMembers(context.Background(), "r1")
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), loader.calls.Load(), "single-flight collapses concurrent misses")
}

func TestLookup_InvalidateClearsValkey(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []Member{{ID: "u1", Account: "alice"}}}
	lookup := NewLookup(cache, loader.Load, time.Minute)

	_, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	lookup.Invalidate(context.Background(), "r1")
	loader.out = []Member{{ID: "u2", Account: "bob"}}
	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)

	assert.Equal(t, loader.out, got, "after Invalidate the next read must reload")
}

type erroringCache struct{ err error }

func (e *erroringCache) Get(context.Context, string) ([]Member, error) {
	return nil, e.err
}
func (e *erroringCache) Set(context.Context, string, []Member, time.Duration) error {
	return nil
}
func (e *erroringCache) Invalidate(context.Context, string) error { return nil }

func TestLookup_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{
		out:     []Member{{ID: "u1", Account: "alice"}},
		block:   make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
	lookup := NewLookup(cache, loader.Load, time.Minute)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, e := lookup.GetMembers(leaderCtx, "r1")
		leaderDone <- e
	}()
	<-loader.entered

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, e := lookup.GetMembers(context.Background(), "r1")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	require.ErrorIs(t, <-leaderDone, context.Canceled)
	close(loader.block)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)
	assert.Equal(t, int32(1), loader.calls.Load(), "shared load should have populated the cache")
}

func TestLookup_CallerCancelReturnsCtxErr(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{
		out:     []Member{{ID: "u1", Account: "alice"}},
		block:   make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
	defer close(loader.block)
	lookup := NewLookup(cache, loader.Load, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := lookup.GetMembers(ctx, "r1")
		done <- e
	}()
	<-loader.entered
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}

func TestLookup_NilCacheLoadsDirectly(t *testing.T) {
	loader := &fakeLoader{out: []Member{{ID: "u1", Account: "alice"}}}
	lookup := NewLookup(nil, loader.Load, time.Minute)

	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)

	// No cache to populate, so every read reaches the loader.
	_, err = lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, int32(2), loader.calls.Load())

	// Invalidate must be a safe no-op rather than a nil dereference.
	lookup.Invalidate(context.Background(), "r1")
}

func TestLookup_LoaderErrorIsReturned(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{err: errors.New("mongo down")}
	lookup := NewLookup(cache, loader.Load, time.Minute)

	_, err := lookup.GetMembers(context.Background(), "r1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mongo down")
}
