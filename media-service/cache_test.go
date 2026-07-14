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

// fakeEIDLoader is a hand-rolled eidLoader for cache tests: it counts calls,
// can return not-found for specific accounts or a fixed error, can block inside
// EmployeeID (via block) to drive the singleflight tests, and can signal entry
// (via entered) so a test knows the shared load is in flight.
type fakeEIDLoader struct {
	mu       sync.Mutex
	calls    int
	notFound map[string]bool
	err      error
	block    chan struct{}
	entered  chan struct{} // buffered; receives once when EmployeeID is entered
}

func (f *fakeEIDLoader) EmployeeID(_ context.Context, account string) (string, bool, error) {
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		return "", false, f.err
	}
	if f.notFound[account] {
		return "", false, nil
	}
	return "E:" + account, true, nil
}

func (f *fakeEIDLoader) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestEIDCache_HitServesFromCache(t *testing.T) {
	loader := &fakeEIDLoader{}
	c := newEIDCache(loader, 10, time.Minute)

	eid, found, err := c.get(context.Background(), "alice")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "E:alice", eid)

	eid2, found2, err := c.get(context.Background(), "alice")
	require.NoError(t, err)
	assert.True(t, found2)
	assert.Equal(t, "E:alice", eid2)
	assert.Equal(t, 1, loader.callCount(), "second get must serve from cache")
}

func TestEIDCache_NotFoundNotCached(t *testing.T) {
	loader := &fakeEIDLoader{notFound: map[string]bool{"ghost": true}}
	c := newEIDCache(loader, 10, time.Minute)

	_, found, err := c.get(context.Background(), "ghost")
	require.NoError(t, err)
	assert.False(t, found)

	_, found, err = c.get(context.Background(), "ghost")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 2, loader.callCount(), "not-found must not be cached")
}

func TestEIDCache_StoreErrorPropagates(t *testing.T) {
	loader := &fakeEIDLoader{err: errors.New("mongo down")}
	c := newEIDCache(loader, 10, time.Minute)

	_, found, err := c.get(context.Background(), "alice")
	require.Error(t, err)
	assert.False(t, found)
}

func TestEIDCache_CapacityEvictsLRU(t *testing.T) {
	loader := &fakeEIDLoader{}
	c := newEIDCache(loader, 1, time.Minute) // room for one entry

	// load a, then b (evicts a), then a again (was evicted → reload).
	for _, account := range []string{"a", "b", "a"} {
		_, _, err := c.get(context.Background(), account)
		require.NoError(t, err)
	}

	assert.Equal(t, 3, loader.callCount(), "LRU must evict the oldest entry past capacity")
}

func TestEIDCache_TTLExpiry(t *testing.T) {
	loader := &fakeEIDLoader{}
	c := newEIDCache(loader, 10, 50*time.Millisecond)

	_, _, err := c.get(context.Background(), "alice")
	require.NoError(t, err)
	assert.Equal(t, 1, loader.callCount())

	time.Sleep(60 * time.Millisecond) // let the entry expire
	_, _, err = c.get(context.Background(), "alice")
	require.NoError(t, err)
	assert.Equal(t, 2, loader.callCount(), "expired entry must be reloaded")
}

func TestEIDCache_SingleflightCollapse(t *testing.T) {
	loader := &fakeEIDLoader{block: make(chan struct{})}
	c := newEIDCache(loader, 10, time.Minute)

	const N = 20
	var wg sync.WaitGroup
	ready := make(chan struct{}, N)
	eids := make([]string, N)
	errs := make([]error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ready <- struct{}{}
			eids[idx], _, errs[idx] = c.get(context.Background(), "alice")
		}(i)
	}

	// Each goroutine signals just before its get call. Goroutines that reach
	// sf.Do while the loader is still blocked collapse onto one call; any that
	// arrive after it returns hit the now-populated cache. Either way: 1 call.
	for i := 0; i < N; i++ {
		<-ready
	}
	close(loader.block)
	wg.Wait()

	for i := 0; i < N; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, "E:alice", eids[i])
	}
	assert.Equal(t, 1, loader.callCount(),
		"singleflight must collapse %d concurrent misses into 1 load", N)
}

func TestEIDCache_CallerCancelReturnsCtxErr(t *testing.T) {
	// A caller waiting on the shared load must abandon via its own ctx.Done
	// without blocking on the store.
	loader := &fakeEIDLoader{block: make(chan struct{}), entered: make(chan struct{}, 1)}
	defer close(loader.block)
	c := newEIDCache(loader, 10, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var eid string
	var found bool
	var err error
	go func() {
		eid, found, err = c.get(ctx, "alice")
		close(done)
	}()

	<-loader.entered // shared load is in flight
	cancel()
	<-done

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, eid)
	assert.False(t, found)
}

func TestEIDCache_WinnerCancelDoesNotPoisonWaiters(t *testing.T) {
	// The caller that wins the singleflight race may cancel its ctx mid-load.
	// Because the shared load uses context.WithoutCancel, it must still finish
	// and populate the cache for everyone else.
	loader := &fakeEIDLoader{block: make(chan struct{}), entered: make(chan struct{}, 1)}
	c := newEIDCache(loader, 10, time.Minute)

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	var err1 error
	go func() {
		_, _, err1 = c.get(ctx1, "alice")
		close(done1)
	}()

	<-loader.entered // winner's shared load is in flight

	// A second caller joins the same in-flight load while it is still blocked.
	// It must observe the shared result, not the winner's cancellation.
	type result struct {
		eid   string
		found bool
		err   error
	}
	waiterDone := make(chan result, 1)
	go func() {
		eid, found, err := c.get(context.Background(), "alice")
		waiterDone <- result{eid, found, err}
	}()

	cancel1() // winner abandons via its own ctx
	<-done1
	require.ErrorIs(t, err1, context.Canceled)

	// Release the detached load; it must still complete and feed the waiter.
	close(loader.block)
	waiter := <-waiterDone
	require.NoError(t, waiter.err, "waiter must not be poisoned by the winner's cancel")
	assert.True(t, waiter.found)
	assert.Equal(t, "E:alice", waiter.eid)

	require.Eventually(t, func() bool {
		return c.lru.Contains("alice")
	}, time.Second, 5*time.Millisecond, "shared load should populate cache despite winner cancel")

	// A fresh caller now reads from cache without a second load.
	eid, found, err := c.get(context.Background(), "alice")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "E:alice", eid)
	assert.Equal(t, 1, loader.callCount())
}
