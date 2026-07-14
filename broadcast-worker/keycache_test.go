package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// testCacheSize is a capacity comfortably larger than any per-test room
// set, so capacity eviction never interferes with behavior-focused tests.
// Eviction is exercised explicitly in TestCachedKeyProvider_SizeBoundEvictsOldEntries.
const testCacheSize = 1000

// fakeKeyStore is a minimal RoomKeyProvider that records call counts and
// returns preconfigured per-room key pairs (or errors).
type fakeKeyStore struct {
	mu      sync.Mutex
	calls   map[string]int
	byRoom  map[string]*roomkeystore.VersionedKeyPair
	err     error
	block   chan struct{} // when non-nil, Get blocks on it before returning
	entered chan struct{} // signaled (non-blocking) on every Get entry
}

func newFakeKeyStore() *fakeKeyStore {
	return &fakeKeyStore{
		calls:  make(map[string]int),
		byRoom: make(map[string]*roomkeystore.VersionedKeyPair),
	}
}

func (f *fakeKeyStore) set(roomID string, key *roomkeystore.VersionedKeyPair) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byRoom[roomID] = key
}

func (f *fakeKeyStore) callCount(roomID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[roomID]
}

func (f *fakeKeyStore) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, v := range f.calls {
		n += v
	}
	return n
}

func (f *fakeKeyStore) Get(_ context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error) {
	f.mu.Lock()
	f.calls[roomID]++
	block := f.block
	entered := f.entered
	err := f.err
	key := f.byRoom[roomID]
	f.mu.Unlock()

	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if block != nil {
		<-block
	}
	if err != nil {
		return nil, err
	}
	return key, nil
}

// makeKey returns a deterministic VersionedKeyPair for tests.
func makeKey(version int) *roomkeystore.VersionedKeyPair {
	return &roomkeystore.VersionedKeyPair{
		Version: version,
		KeyPair: roomkeystore.RoomKeyPair{
			PrivateKey: []byte("private-" + strconv.Itoa(version)),
		},
	}
}

var _ RoomKeyProvider = (*CachedKeyProvider)(nil)

func TestCachedKeyProvider_MissPopulatesCacheAndReturnsValue(t *testing.T) {
	inner := newFakeKeyStore()
	want := makeKey(1)
	inner.set("room1", want)

	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)

	got, err := c.Get(context.Background(), "room1")
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 1, inner.callCount("room1"))
}

func TestCachedKeyProvider_HitDoesNotCallInner(t *testing.T) {
	inner := newFakeKeyStore()
	inner.set("room1", makeKey(1))

	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)

	// First call populates.
	_, err := c.Get(context.Background(), "room1")
	require.NoError(t, err)

	// Subsequent calls within TTL must not touch inner.
	for i := 0; i < 10; i++ {
		got, err := c.Get(context.Background(), "room1")
		require.NoError(t, err)
		assert.Equal(t, makeKey(1), got)
	}
	assert.Equal(t, 1, inner.callCount("room1"))
}

func TestCachedKeyProvider_ExpiredEntryRefetchesNewVersion(t *testing.T) {
	// Models the key-rotation scenario end to end: within TTL the cached
	// (old) version is served without touching inner; once the entry
	// expires, the next lookup refetches and picks up the new version.
	inner := newFakeKeyStore()
	inner.set("room1", makeKey(1))

	const ttl = 60 * time.Millisecond
	c := NewCachedKeyProvider(inner, testCacheSize, ttl)

	got, err := c.Get(context.Background(), "room1")
	require.NoError(t, err)
	assert.Equal(t, 1, got.Version)
	assert.Equal(t, 1, inner.callCount("room1"))

	// Within TTL: served from cache, inner untouched, old version retained.
	// (This is the documented staleness contract.)
	got, err = c.Get(context.Background(), "room1")
	require.NoError(t, err)
	assert.Equal(t, 1, got.Version)
	assert.Equal(t, 1, inner.callCount("room1"))

	// Rotate: inner now serves a newer version.
	inner.set("room1", makeKey(2))

	// After the entry expires, lookups refetch and pick up v2.
	require.Eventually(t, func() bool {
		got, err := c.Get(context.Background(), "room1")
		return err == nil && got.Version == 2
	}, 2*time.Second, 5*time.Millisecond, "new version must be picked up after TTL expiry")
	assert.GreaterOrEqual(t, inner.callCount("room1"), 2)
}

func TestCachedKeyProvider_ErrorPassesThroughAndIsNotCached(t *testing.T) {
	inner := newFakeKeyStore()
	wantErr := errors.New("valkey down")
	inner.err = wantErr

	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)

	_, err := c.Get(context.Background(), "room1")
	require.ErrorIs(t, err, wantErr)
	// The wrapped error must surface the room ID so logs identify which
	// lookup failed.
	assert.Contains(t, err.Error(), "room1")
	assert.Equal(t, 1, inner.callCount("room1"))

	// Clear the error and ensure the next call retries (i.e. error was not
	// cached). The new value must be returned and inner is called again.
	inner.mu.Lock()
	inner.err = nil
	inner.mu.Unlock()
	inner.set("room1", makeKey(1))

	got, err := c.Get(context.Background(), "room1")
	require.NoError(t, err)
	assert.Equal(t, makeKey(1), got)
	assert.Equal(t, 2, inner.callCount("room1"))
}

func TestCachedKeyProvider_NilResultIsNotCached(t *testing.T) {
	// When the inner store reports no key (nil, nil) for an unprovisioned
	// room, we must NOT cache that absence — otherwise a newly-provisioned
	// key wouldn't be picked up for up to TTL seconds, and broadcasts
	// would keep failing during room setup.
	inner := newFakeKeyStore()
	// Intentionally leave inner.byRoom["room1"] unset → Get returns nil.

	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)

	got, err := c.Get(context.Background(), "room1")
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, 1, inner.callCount("room1"))

	// Provision the key; the next call must reach inner and return it.
	inner.set("room1", makeKey(1))

	got, err = c.Get(context.Background(), "room1")
	require.NoError(t, err)
	assert.Equal(t, makeKey(1), got)
	assert.Equal(t, 2, inner.callCount("room1"))
}

func TestCachedKeyProvider_SingleflightCoalescesConcurrentMissesOnSameRoom(t *testing.T) {
	inner := newFakeKeyStore()
	inner.set("room1", makeKey(1))
	// Block inside inner.Get so many callers pile up at the singleflight
	// gate before any of them returns.
	inner.block = make(chan struct{})
	inner.entered = make(chan struct{}, 1)

	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)

	const N = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]*roomkeystore.VersionedKeyPair, N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = c.Get(context.Background(), "room1")
		}(i)
	}
	close(start)

	// Wait for the first inner call to be in-flight, then release it.
	<-inner.entered
	close(inner.block)
	wg.Wait()

	for i := 0; i < N; i++ {
		require.NoError(t, errs[i], "goroutine %d", i)
		assert.Equal(t, makeKey(1), results[i], "goroutine %d", i)
	}
	// Singleflight + post-flight cache population guarantee exactly one
	// inner call: callers that enter while the flight is active join it;
	// callers that arrive after it finishes hit the populated cache.
	assert.Equal(t, 1, inner.callCount("room1"))
}

func TestCachedKeyProvider_DifferentRoomsAreNotCoalesced(t *testing.T) {
	inner := newFakeKeyStore()
	const N = 20
	for i := 0; i < N; i++ {
		inner.set("room"+strconv.Itoa(i), makeKey(i))
	}

	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := c.Get(context.Background(), "room"+strconv.Itoa(i))
			assert.NoError(t, err)
			assert.Equal(t, makeKey(i), got)
		}(i)
	}
	wg.Wait()

	// Each room should have been fetched exactly once.
	for i := 0; i < N; i++ {
		assert.Equal(t, 1, inner.callCount("room"+strconv.Itoa(i)), "room %d", i)
	}
	assert.Equal(t, N, inner.totalCalls())
}

func TestCachedKeyProvider_SizeBoundEvictsOldEntries(t *testing.T) {
	// The cache must be bounded by its configured capacity so a worker that
	// sees many distinct rooms over its lifetime does not grow without
	// limit. This is the memory-safety guarantee the LRU backing provides
	// over a plain map.
	inner := newFakeKeyStore()
	const size = 50
	const rooms = size * 4
	for i := 0; i < rooms; i++ {
		inner.set("room"+strconv.Itoa(i), makeKey(i))
	}

	c := NewCachedKeyProvider(inner, size, time.Minute)

	for i := 0; i < rooms; i++ {
		_, err := c.Get(context.Background(), "room"+strconv.Itoa(i))
		require.NoError(t, err)
	}

	assert.LessOrEqual(t, c.lru.Len(), size, "cache must not exceed its configured capacity")
}

func TestCachedKeyProvider_HighConcurrencyNoRace(t *testing.T) {
	// Run with -race to catch data races. Many goroutines hammering both
	// the same and different rooms, with a short TTL forcing occasional
	// re-fetches and expirations.
	inner := newFakeKeyStore()
	const Rooms = 8
	for i := 0; i < Rooms; i++ {
		inner.set("room"+strconv.Itoa(i), makeKey(i))
	}

	c := NewCachedKeyProvider(inner, testCacheSize, 5*time.Millisecond)

	const Workers = 32
	const Iter = 200
	var ops atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < Workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < Iter; i++ {
				_, err := c.Get(context.Background(), "room"+strconv.Itoa((w+i)%Rooms))
				if err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}
				ops.Add(1)
			}
		}(w)
	}
	wg.Wait()
	assert.Equal(t, int64(Workers*Iter), ops.Load())
}

func TestCachedKeyProvider_CallerCtxCancelReturnsCtxErr(t *testing.T) {
	// A single caller waiting on the shared fetch must be able to abandon
	// via its own ctx.Done without blocking on the inner store.
	inner := newFakeKeyStore()
	inner.set("room1", makeKey(1))
	inner.block = make(chan struct{})
	inner.entered = make(chan struct{}, 1)
	defer close(inner.block)

	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	var got *roomkeystore.VersionedKeyPair
	var err error
	go func() {
		got, err = c.Get(ctx, "room1")
		close(done)
	}()

	<-inner.entered
	cancel()
	<-done

	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, got)
}

func TestCachedKeyProvider_WinnerCancelDoesNotPoisonOtherCallers(t *testing.T) {
	// The "winner" of the singleflight race may cancel its ctx mid-fetch.
	// Because the shared fetch uses context.WithoutCancel, the inner Get
	// must still complete and populate the cache for subsequent callers.
	inner := newFakeKeyStore()
	inner.set("room1", makeKey(7))
	inner.block = make(chan struct{})
	inner.entered = make(chan struct{}, 1)

	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	var err1 error
	go func() {
		_, err1 = c.Get(ctx1, "room1")
		close(done1)
	}()

	// Wait until the inner fetch is in flight, then cancel the winning
	// caller. The shared fetch must keep running.
	<-inner.entered
	cancel1()
	<-done1
	require.ErrorIs(t, err1, context.Canceled)

	// Release inner; the shared fetch finishes and stores the key.
	close(inner.block)

	// The cache should eventually be populated by the detached fetch.
	require.Eventually(t, func() bool {
		return c.lru.Contains("room1")
	}, time.Second, 5*time.Millisecond, "shared fetch should have populated cache despite winner cancellation")

	// A fresh caller now reads from cache without invoking inner again.
	got, err := c.Get(context.Background(), "room1")
	require.NoError(t, err)
	assert.Equal(t, makeKey(7), got)
	assert.Equal(t, 1, inner.callCount("room1"))
}

func TestCachedKeyProvider_Metrics_HitMissError(t *testing.T) {
	ctx := context.Background()
	inner := newFakeKeyStore()
	inner.set("room1", makeKey(1))
	rec := &fakeRecorder{}
	c := NewCachedKeyProvider(inner, testCacheSize, time.Minute)
	c.metrics = rec

	// Miss: not cached, inner load succeeds.
	_, err := c.Get(ctx, "room1")
	require.NoError(t, err)
	assert.Equal(t, [3]int{0, 1, 0}, [3]int{rec.hits, rec.misses, rec.errs})

	// Hit.
	_, err = c.Get(ctx, "room1")
	require.NoError(t, err)
	assert.Equal(t, [3]int{1, 1, 0}, [3]int{rec.hits, rec.misses, rec.errs})

	// Error: inner load fails.
	inner.err = errors.New("boom")
	_, err = c.Get(ctx, "room2")
	require.Error(t, err)
	assert.Equal(t, [3]int{1, 1, 1}, [3]int{rec.hits, rec.misses, rec.errs})
}

func TestKeyCacheTTLSafe(t *testing.T) {
	tests := []struct {
		name  string
		ttl   time.Duration
		grace time.Duration
		want  bool
	}{
		{"ttl below grace is safe", 10 * time.Minute, 24 * time.Hour, true},
		{"ttl just below grace is safe", time.Hour - time.Second, time.Hour, true},
		{"ttl equal to grace is unsafe", time.Hour, time.Hour, false},
		{"ttl above grace is unsafe", 48 * time.Hour, 24 * time.Hour, false},
		{"zero ttl is unsafe", 0, time.Hour, false},
		{"negative ttl is unsafe", -time.Second, time.Hour, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, keyCacheTTLSafe(tc.ttl, tc.grace))
		})
	}
}
