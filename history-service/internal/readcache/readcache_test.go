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

// TestSubscriptionCache_Evict_ReloadsFreshBoundary is the #414 regression: a
// full-access entry cached before a restricted re-add must be dropped by Evict so
// the next read reflects the new boundary instead of the stale full-access nil.
func TestSubscriptionCache_Evict_ReloadsFreshBoundary(t *testing.T) {
	ctx := context.Background()
	// Full member: subscribed, no lower bound (unbounded history read).
	src := &fakeSubSource{subscribed: true, sharedSince: nil}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	// Seed the cache as a full member, then confirm it is served from cache.
	_, sub, err := c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	require.True(t, sub)
	_, _, _ = c.GetHistorySharedSince(ctx, "alice", "r1")
	require.Equal(t, int32(1), src.calls.Load(), "second read should hit the cache")

	// Membership changes: restricted re-add writes a historySharedSince boundary.
	boundary := time.Now().UTC()
	src.sharedSince = &boundary

	// Without eviction the stale full-access nil persists (the leak this fixes).
	stale, _, err := c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	require.Nil(t, stale, "cache still serves the stale full-access window before eviction")
	require.Equal(t, int32(1), src.calls.Load())

	// Evict → the next read reloads and reflects the fresh restricted boundary.
	c.Evict("alice", "r1")
	got, sub2, err := c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	require.True(t, sub2)
	require.NotNil(t, got)
	assert.Equal(t, boundary, *got)
	assert.Equal(t, int32(2), src.calls.Load(), "post-evict read must consult the source again")
}

// TestSubscriptionCache_Evict_RemovedMemberLosesAccess covers the sibling leak: a
// just-removed member whose subscribed=true entry is cached must be denied on the
// next read once evicted.
func TestSubscriptionCache_Evict_RemovedMemberLosesAccess(t *testing.T) {
	ctx := context.Background()
	src := &fakeSubSource{subscribed: true, sharedSince: nil}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	_, sub, err := c.GetHistorySharedSince(ctx, "bob", "r9")
	require.NoError(t, err)
	require.True(t, sub)

	// Removed: source now reports not-subscribed.
	src.subscribed = false
	c.Evict("bob", "r9")

	_, sub2, err := c.GetHistorySharedSince(ctx, "bob", "r9")
	require.NoError(t, err)
	assert.False(t, sub2, "removed member must not stay subscribed after eviction")
}

// TestSubscriptionCache_Evict_AbsentKey is a no-op that must not panic.
func TestSubscriptionCache_Evict_AbsentKey(t *testing.T) {
	src := &fakeSubSource{}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)
	assert.NotPanics(t, func() { c.Evict("nobody", "nowhere") })
}

// gatedSubSource is a subscription source whose returned access boundary flips
// when revoked is set, and whose first load blocks on release. It models the
// tGD race: the first load reads the OLD (full-access) boundary at entry, pauses,
// and only returns after a membership change has landed. A later re-read (the
// retry loop, or a second caller) reads the NEW (restricted) boundary.
type gatedSubSource struct {
	calls    atomic.Int32
	started  chan struct{}
	release  chan struct{}
	revoked  atomic.Bool
	boundary *time.Time // the restricted boundary served once revoked
	once     sync.Once
}

func (g *gatedSubSource) GetHistorySharedSince(_ context.Context, _, _ string) (*time.Time, bool, error) {
	g.calls.Add(1)
	// Snapshot the access boundary at entry — models "the load reads the
	// subscription" before it pauses.
	revoked := g.revoked.Load()
	first := false
	g.once.Do(func() { first = true })
	if first {
		g.started <- struct{}{}
		<-g.release
	}
	if revoked {
		return g.boundary, true, nil // narrowed access after the membership change
	}
	return nil, true, nil // full access (the stale grant once revoked)
}

func (g *gatedSubSource) GetSubscription(_ context.Context, _, _ string) (*pkgmodel.Subscription, error) {
	return nil, nil
}

// TestSubscriptionCache_Evict_InFlightLoadDoesNotAuthorizeWaiters is the CWE-285
// concurrency regression. An Evict that lands between a load starting and its
// result being returned must leave NO caller of that load holding the
// pre-eviction positive grant — not the in-flight loader, and not a second
// request that arrives after the Evict. Both must observe the fresh, narrowed
// boundary instead. Covers both halves of the fix: sf.Forget in remove (the
// second reader starts a fresh flight) and the generation-stability retry in
// getOrLoad (the in-flight loader re-reads rather than returning its stale read).
func TestSubscriptionCache_Evict_InFlightLoadDoesNotAuthorizeWaiters(t *testing.T) {
	ctx := context.Background()
	boundary := time.Now().UTC()
	src := &gatedSubSource{
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		boundary: &boundary,
	}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	type result struct {
		ss  *time.Time
		sub bool
		err error
	}

	// Reader A: misses, starts the singleflight load, reads full access, pauses.
	aCh := make(chan result, 1)
	go func() {
		ss, sub, err := c.GetHistorySharedSince(ctx, "alice", "r1")
		aCh <- result{ss, sub, err}
	}()
	<-src.started // A's load is in flight, blocked with the old full-access read.

	// Membership is revoked while A is blocked: the source now reports restricted,
	// and the cache is evicted (bumps gen + Forgets A's flight).
	src.revoked.Store(true)
	c.Evict("alice", "r1")

	// Reader B arrives after the Evict. Because remove Forgot A's flight, B starts
	// a fresh load that reads the post-revocation restricted boundary.
	bCh := make(chan result, 1)
	go func() {
		ss, sub, err := c.GetHistorySharedSince(ctx, "alice", "r1")
		bCh <- result{ss, sub, err}
	}()
	b := <-bCh
	require.NoError(t, b.err)
	require.True(t, b.sub)
	require.NotNil(t, b.ss, "second reader must not get the stale full-access nil")
	assert.Equal(t, boundary, *b.ss, "second reader must see the narrowed boundary")

	// Release A. Its load saw gen change, so it must NOT return the stale full
	// access; the retry re-reads and returns the restricted boundary.
	close(src.release)
	a := <-aCh
	require.NoError(t, a.err)
	require.True(t, a.sub)
	require.NotNil(t, a.ss, "in-flight loader must not return the stale full-access nil")
	assert.Equal(t, boundary, *a.ss, "in-flight loader must return the narrowed boundary")
}

// TestSubscriptionCache_Purge_ReloadsAfterReconnect covers the core-NATS
// reconnect gap: an instance that missed evictions while disconnected must not
// keep serving a stale full-access entry. Purge (called from the reconnect
// handler) drops the entry so the next read reloads the current boundary.
func TestSubscriptionCache_Purge_ReloadsAfterReconnect(t *testing.T) {
	ctx := context.Background()
	src := &fakeSubSource{subscribed: true, sharedSince: nil} // full access
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	// Seed the full-access entry.
	_, sub, err := c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	require.True(t, sub)
	require.Equal(t, int32(1), src.calls.Load())

	// While this instance is "disconnected", access is narrowed at the source.
	boundary := time.Now().UTC()
	src.sharedSince = &boundary

	// A second read without Purge would serve the cached full-access nil.
	// Reconnect handler purges → next read must reload the restricted boundary.
	c.Purge()
	got, sub, err := c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	require.True(t, sub)
	require.NotNil(t, got, "post-purge read must reload, not serve the stale full-access nil")
	assert.Equal(t, boundary, *got)
	assert.Equal(t, int32(2), src.calls.Load(), "purge must force a fresh source read")
}

// ctxAwareSubSource blocks in the load until its context is canceled, then
// reports the cancellation. Models a slow Mongo read that a supersede must abort.
type ctxAwareSubSource struct {
	started chan struct{}
}

func (s *ctxAwareSubSource) GetHistorySharedSince(ctx context.Context, _, _ string) (*time.Time, bool, error) {
	s.started <- struct{}{}
	<-ctx.Done()
	return nil, false, ctx.Err()
}

func (s *ctxAwareSubSource) GetSubscription(_ context.Context, _, _ string) (*pkgmodel.Subscription, error) {
	return nil, nil
}

// TestSubscriptionCache_Evict_CancelsSupersededLoad proves fix 3: an Evict that
// lands while a load is blocked in the source cancels that load's context at once,
// so it returns promptly (fail-closed) instead of pinning a goroutine + context
// until fetchTimeout (10s). Without the cancel this read would block ~10s.
func TestSubscriptionCache_Evict_CancelsSupersededLoad(t *testing.T) {
	src := &ctxAwareSubSource{started: make(chan struct{})}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, _, e := c.GetHistorySharedSince(context.Background(), "alice", "r1")
		done <- e
	}()
	<-src.started // the load is now blocked on ctx.Done()

	c.Evict("alice", "r1") // supersede → must cancel the blocked load

	select {
	case e := <-done:
		require.Error(t, e, "a superseded load must fail closed, not return a grant")
	case <-time.After(2 * time.Second):
		t.Fatal("superseded load was not canceled within 2s (it would otherwise block until fetchTimeout)")
	}
}

// capSubSource records the peak concurrent in-flight loads so a test can assert
// the semaphore bounds fan-out. Each call blocks on release.
type capSubSource struct {
	inflight atomic.Int32
	peak     atomic.Int32
	entered  chan struct{}
	release  chan struct{}
}

func (s *capSubSource) GetHistorySharedSince(_ context.Context, _, _ string) (*time.Time, bool, error) {
	n := s.inflight.Add(1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	s.entered <- struct{}{}
	<-s.release
	s.inflight.Add(-1)
	ts := time.Now().UTC()
	return &ts, true, nil
}

func (s *capSubSource) GetSubscription(_ context.Context, _, _ string) (*pkgmodel.Subscription, error) {
	return nil, nil
}

// TestSubscriptionCache_MaxInflight_BoundsConcurrentLoads proves fix 3's cap: with
// maxInflight=2, a burst of 5 distinct-key misses never has more than 2 loads in
// the source at once — the other 3 wait on the semaphore.
func TestSubscriptionCache_MaxInflight_BoundsConcurrentLoads(t *testing.T) {
	src := &capSubSource{entered: make(chan struct{}, 8), release: make(chan struct{})}
	c, err := NewSubscriptionCache(src, 100, time.Minute, WithMaxInflight(2))
	require.NoError(t, err)

	const n = 5
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		key := "r" + string(rune('a'+i)) // distinct keys → no singleflight coalescing
		go func() {
			_, _, _ = c.GetHistorySharedSince(context.Background(), "alice", key)
			done <- struct{}{}
		}()
	}

	// Exactly two loads may enter; a third must be blocked on the semaphore.
	<-src.entered
	<-src.entered
	select {
	case <-src.entered:
		t.Fatal("a third load entered the source: semaphore did not bound concurrency to 2")
	case <-time.After(150 * time.Millisecond):
	}

	close(src.release) // let all loads drain
	for i := 0; i < n; i++ {
		<-done
	}
	assert.LessOrEqual(t, src.peak.Load(), int32(2), "no more than maxInflight loads may run at once")
}

// raceSafeSubSource is a concurrency-safe source whose access boundary can be
// flipped from another goroutine, for the barrier race test.
type raceSafeSubSource struct {
	calls      atomic.Int32
	shared     atomic.Pointer[time.Time]
	subscribed atomic.Bool
}

func (s *raceSafeSubSource) GetHistorySharedSince(_ context.Context, _, _ string) (*time.Time, bool, error) {
	s.calls.Add(1)
	return s.shared.Load(), s.subscribed.Load(), nil
}

func (s *raceSafeSubSource) GetSubscription(_ context.Context, _, _ string) (*pkgmodel.Subscription, error) {
	return nil, nil
}

// TestSubscriptionCache_Barrier_SuspendBypassesStaleGrant proves fix 4
// deterministically: after a disconnect (Suspend) the cache stops serving its
// cached full-access grant and reads fall through to the (now-narrowed) source;
// Resume re-arms it so caching returns.
func TestSubscriptionCache_Barrier_SuspendBypassesStaleGrant(t *testing.T) {
	ctx := context.Background()
	src := &fakeSubSource{subscribed: true, sharedSince: nil} // full access
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	// Seed the full-access grant.
	_, sub, err := c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	require.True(t, sub)
	require.Equal(t, int32(1), src.calls.Load())

	// Missed eviction: source narrows while this instance is "disconnected".
	boundary := time.Now().UTC()
	src.sharedSince = &boundary

	c.Suspend() // disconnect barrier

	// A read during the barrier must reload from the source, not serve the stale nil.
	got, sub, err := c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	require.True(t, sub)
	require.NotNil(t, got, "suspended read must not serve the stale full-access grant")
	assert.Equal(t, boundary, *got)
	assert.Equal(t, int32(2), src.calls.Load())

	// Still suspended: the cache must not populate, so the next read hits the source again.
	_, _, err = c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, int32(3), src.calls.Load(), "suspended cache must not store")

	// Resume re-arms: a miss loads once, the following read is a hit.
	c.Resume()
	_, _, err = c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	_, _, err = c.GetHistorySharedSince(ctx, "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, int32(4), src.calls.Load(), "resumed cache serves the second read from cache")
}

// TestSubscriptionCache_Barrier_RaceReconnectNeverStale proves fix 4 under a race:
// concurrent readers, while Suspend/Resume churns (a flapping connection), never
// observe the stale full-access grant once the source has narrowed.
func TestSubscriptionCache_Barrier_RaceReconnectNeverStale(t *testing.T) {
	ctx := context.Background()
	src := &raceSafeSubSource{}
	src.subscribed.Store(true) // full access (shared=nil)
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	_, _, _ = c.GetHistorySharedSince(ctx, "alice", "r1") // seed the stale full-access grant

	boundary := time.Now().UTC()
	src.shared.Store(&boundary) // narrowed (missed eviction)
	c.Suspend()                 // enter the barrier before readers start

	var staleSeen atomic.Bool
	var readers sync.WaitGroup
	stop := make(chan struct{})

	flapDone := make(chan struct{})
	go func() { // reconnect flap, runs until readers finish
		defer close(flapDone)
		for {
			select {
			case <-stop:
				return
			default:
				c.Resume()
				c.Suspend()
			}
		}
	}()

	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 1000; j++ {
				got, sub, err := c.GetHistorySharedSince(ctx, "alice", "r1")
				if err != nil {
					continue
				}
				if sub && got == nil {
					staleSeen.Store(true) // a full-access nil is the stale grant
				}
			}
		}()
	}
	readers.Wait()
	close(stop)
	<-flapDone

	assert.False(t, staleSeen.Load(), "no read may serve the stale full-access grant after the source narrowed")
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

func (f *fakeRoomSource) InvalidatePreviewKey(_ context.Context, roomID, msgID string, _ int64) error {
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
	require.NoError(t, c.InvalidatePreviewKey(ctx, "r1", "m-mutated", 500))

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
