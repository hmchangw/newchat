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

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type fakeCache struct {
	mu     sync.Mutex
	data   map[string]Entry
	slides int
	now    func() time.Time
}

func newFakeCache() *fakeCache {
	return &fakeCache{data: map[string]Entry{}, now: time.Now}
}

func (f *fakeCache) Get(_ context.Context, roomID string) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[roomID]
	if !ok {
		return Entry{}, valkeyutil.ErrCacheMiss
	}
	return v, nil
}
func (f *fakeCache) Set(_ context.Context, roomID string, members []Member, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]Member, len(members))
	copy(cp, members)
	f.data[roomID] = Entry{Members: cp, CachedAt: f.now().UnixMilli()}
	return nil
}
func (f *fakeCache) Slide(_ context.Context, roomID string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slides++
	if _, ok := f.data[roomID]; !ok {
		return nil // EXPIRE no-ops on an absent key
	}
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

func (e *erroringCache) Get(context.Context, string) (Entry, error) {
	return Entry{}, e.err
}
func (e *erroringCache) Set(context.Context, string, []Member, time.Duration) error {
	return nil
}
func (e *erroringCache) Slide(context.Context, string, time.Duration) error { return nil }
func (e *erroringCache) Invalidate(context.Context, string) error           { return nil }

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

func TestGuardLoader_OpenBreakerSkipsTheLoader(t *testing.T) {
	loader := &fakeLoader{err: errors.New("mongo down")}
	b := circuitbreaker.New(2, time.Minute)
	guarded := GuardLoader(loader.Load, b)

	for i := 0; i < 2; i++ {
		_, err := guarded(context.Background(), "r1")
		require.Error(t, err)
	}
	require.Equal(t, int32(2), loader.calls.Load())

	for i := 0; i < 3; i++ {
		_, err := guarded(context.Background(), "r1")
		require.Error(t, err)
	}
	assert.Equal(t, int32(2), loader.calls.Load(), "an open breaker must not reach the loader")
}

func TestGuardLoader_ServesL2WhileBreakerIsOpen(t *testing.T) {
	cache := newFakeCache()
	warm := []Member{{ID: "u1", Account: "alice"}}
	require.NoError(t, cache.Set(context.Background(), "r1", warm, time.Minute))

	loader := &fakeLoader{err: errors.New("mongo down")}
	b := circuitbreaker.New(1, time.Minute)
	lookup := NewLookup(cache, GuardLoader(loader.Load, b), time.Minute)

	// Trip the breaker on a cold room, then confirm the warm room still answers:
	// fencing the loader must never fence the cache in front of it.
	_, err := lookup.GetMembers(context.Background(), "cold")
	require.Error(t, err)

	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, warm, got)
}

func TestGuardLoader_NilBreakerIsPassThrough(t *testing.T) {
	loader := &fakeLoader{out: []Member{{ID: "u1", Account: "alice"}}}
	got, err := GuardLoader(loader.Load, nil)(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)
}

func TestLookup_StaleEntrySurvivesLoaderOutageViaTTLSlide(t *testing.T) {
	cache := newFakeCache()
	warm := []Member{{ID: "u1", Account: "alice"}}
	loader := &fakeLoader{out: warm}
	now := time.Now()
	clock := func() time.Time { return now }
	cache.now = clock
	lookup := newLookupWithClock(cache, loader.Load, time.Hour, clock)
	ctx := context.Background()

	// Populate, then age past the refresh window and break the loader.
	got, err := lookup.GetMembers(ctx, "r1")
	require.NoError(t, err)
	require.Equal(t, warm, got)

	now = now.Add(59 * time.Minute)
	loader.err = errors.New("mongo down")
	loader.out = nil

	// Without the slide the entry would simply expire mid-outage and DM fan-out
	// would start failing — the 5-minute wall this exists to remove.
	got, err = lookup.GetMembers(ctx, "r1")
	require.NoError(t, err, "a warm room must survive the loader being down")
	assert.Equal(t, warm, got)
	assert.Positive(t, cache.slides, "the deadline must be re-armed, not left to expire")
}

func TestLookup_FreshEntryDoesNotReload(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []Member{{ID: "u1", Account: "alice"}}}
	now := time.Now()
	clock := func() time.Time { return now }
	cache.now = clock
	lookup := newLookupWithClock(cache, loader.Load, time.Hour, clock)
	ctx := context.Background()

	_, err := lookup.GetMembers(ctx, "r1")
	require.NoError(t, err)
	now = now.Add(10 * time.Minute) // well inside the refresh window
	_, err = lookup.GetMembers(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, int32(1), loader.calls.Load(), "a fresh entry must not re-validate")
}

func TestLookup_StaleEntryRefreshesWhenLoaderHealthy(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{out: []Member{{ID: "u1", Account: "alice"}}}
	now := time.Now()
	clock := func() time.Time { return now }
	cache.now = clock
	lookup := newLookupWithClock(cache, loader.Load, time.Hour, clock)
	ctx := context.Background()

	_, err := lookup.GetMembers(ctx, "r1")
	require.NoError(t, err)

	// Past the refresh window with a healthy loader: pick up the change a missed
	// invalidation would otherwise hide until the TTL.
	now = now.Add(59 * time.Minute)
	loader.out = []Member{{ID: "u2", Account: "bob"}}
	got, err := lookup.GetMembers(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, []Member{{ID: "u2", Account: "bob"}}, got)
	assert.Equal(t, int32(2), loader.calls.Load())
}

// A stale entry is revalidated once per room, not once per caller. This Lookup
// has no process-local tier in front of it — notification-worker calls it per
// message — so an uncollapsed refresh means every in-flight message for a hot
// room issues its own loader call and its own write the moment the refresh
// window opens.
func TestLookup_SingleFlightCollapsesRefreshes(t *testing.T) {
	cache := newFakeCache()
	warm := []Member{{ID: "u1", Account: "alice"}}
	loader := &fakeLoader{out: warm}
	now := time.Now()
	clock := func() time.Time { return now }
	cache.now = clock
	lookup := newLookupWithClock(cache, loader.Load, time.Hour, clock)
	ctx := context.Background()

	_, err := lookup.GetMembers(ctx, "r1")
	require.NoError(t, err)
	require.Equal(t, int32(1), loader.calls.Load())

	// Age past the refresh window, then hit it concurrently.
	now = now.Add(59 * time.Minute)
	loader.delay = 50 * time.Millisecond

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := lookup.GetMembers(ctx, "r1")
			assert.NoError(t, err)
			assert.Equal(t, warm, got)
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(2), loader.calls.Load(),
		"ten concurrent stale reads must trigger one revalidation, not ten")
}
