package atrest

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// fakeL2Valkey is an in-memory valkeyutil.Client for tests. When now is set,
// it honors the per-key TTL the way a real Valkey does, so tests can assert on
// entries expiring (or being kept alive by a slide).
type fakeL2Valkey struct {
	store      map[string]string
	expiry     map[string]time.Time
	now        func() time.Time
	getErr     error
	setErr     error
	delErr     error
	getHits    int
	setHits    int
	delHits    int
	expireHits int
	// afterGet runs after a successful Get returns its value, letting a test
	// interleave a concurrent write (e.g. a rotation's invalidate) between the
	// read and whatever the caller does with the result.
	afterGet func(key string)
}

func newFakeL2Valkey() *fakeL2Valkey {
	return &fakeL2Valkey{store: map[string]string{}, expiry: map[string]time.Time{}}
}

func (f *fakeL2Valkey) Get(_ context.Context, key string) (string, error) {
	f.getHits++
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.store[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	if exp, tracked := f.expiry[key]; f.now != nil && tracked && !f.now().Before(exp) {
		delete(f.store, key)
		delete(f.expiry, key)
		return "", valkeyutil.ErrCacheMiss
	}
	if f.afterGet != nil {
		f.afterGet(key)
	}
	return v, nil
}

func (f *fakeL2Valkey) Set(_ context.Context, key, value string, ttl time.Duration) error {
	f.setHits++
	if f.setErr != nil {
		return f.setErr
	}
	f.store[key] = value
	if f.now != nil && ttl > 0 {
		f.expiry[key] = f.now().Add(ttl)
	}
	return nil
}

func (f *fakeL2Valkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (f *fakeL2Valkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 0, nil }
func (f *fakeL2Valkey) Del(_ context.Context, keys ...string) error {
	f.delHits++
	if f.delErr != nil {
		return f.delErr
	}
	for _, k := range keys {
		delete(f.store, k)
		delete(f.expiry, k)
	}
	return nil
}

// Expire mirrors Valkey: it re-arms an existing key's deadline and reports
// whether the key was there, but never creates one.
func (f *fakeL2Valkey) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	f.expireHits++
	if _, ok := f.store[key]; !ok {
		return false, nil
	}
	if f.now != nil && ttl > 0 {
		f.expiry[key] = f.now().Add(ttl)
	}
	return true, nil
}

func (f *fakeL2Valkey) Close() error { return nil }

// fakeInnerStore is an in-memory DEKStore standing in for Mongo.
type fakeInnerStore struct {
	rows       map[string]RoomDataKey
	getErr     error
	upsertErr  error
	replaceErr error
	getCalls   int
	upsertHit  int
	replHit    int
}

func newFakeInnerStore() *fakeInnerStore {
	return &fakeInnerStore{rows: map[string]RoomDataKey{}}
}

func (s *fakeInnerStore) Get(_ context.Context, roomID string) (*RoomDataKey, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	row, ok := s.rows[roomID]
	if !ok {
		return nil, nil // absent — drives lazy DEK creation; must never be cached
	}
	return &row, nil
}

func (s *fakeInnerStore) Upsert(_ context.Context, key RoomDataKey) error {
	s.upsertHit++
	if s.upsertErr != nil {
		return s.upsertErr
	}
	if _, exists := s.rows[key.ID]; !exists {
		s.rows[key.ID] = key
	}
	return nil
}

func (s *fakeInnerStore) Replace(_ context.Context, key RoomDataKey) error {
	s.replHit++
	if s.replaceErr != nil {
		return s.replaceErr
	}
	s.rows[key.ID] = key
	return nil
}

type spyL2Recorder struct{ hit, miss, err int }

func (s *spyL2Recorder) Hit(context.Context)   { s.hit++ }
func (s *spyL2Recorder) Miss(context.Context)  { s.miss++ }
func (s *spyL2Recorder) Error(context.Context) { s.err++ }

// openBreaker returns a breaker already tripped open (threshold 1, long cooldown).
func openBreaker(t *testing.T) *circuitbreaker.Breaker {
	t.Helper()
	b := circuitbreaker.New(1, time.Hour)
	_ = b.Do(func() error { return errors.New("trip") })
	require.Equal(t, circuitbreaker.StateOpen, b.State())
	return b
}

func healthyBreaker() *circuitbreaker.Breaker { return circuitbreaker.New(5, time.Second) }

// pastRefresh and withinRefresh are stated relative to the store's OWN refresh
// window for a 1h TTL rather than hardcoding a fraction of it, so a change to
// valkeyutil.RefreshAfter moves every refresh test with it instead of silently making
// them assert the wrong side of the boundary.
var (
	pastRefresh   = valkeyutil.RefreshAfter(time.Hour) + time.Minute
	withinRefresh = valkeyutil.RefreshAfter(time.Hour) - time.Minute
)

func seedRow(roomID string) RoomDataKey {
	return RoomDataKey{ID: roomID, WrappedDEK: []byte("wrapped-ciphertext"), CreatedAt: time.Unix(0, 0).UTC()}
}

func TestDEKKey(t *testing.T) {
	assert.Equal(t, "dek:{room1}", DEKKey("room1"))
}

// The refresh window has to outrun the cipher's in-process DEK cache, which
// sits in front of this tier and does not slide. That L1 means an L2 entry is
// read at most once per room per L1 TTL per pod, so a refresh window shorter
// than the L1 TTL makes EVERY L2 hit older than it: the "periodic"
// reconciliation degenerates into a Mongo read plus a full SET on every L1
// miss, which is exactly the load this tier exists to absorb.
func TestRefreshAfter_OutrunsTheInProcessDEKCache(t *testing.T) {
	const (
		l2TTL = 90 * time.Minute // ATREST_DEK_L2_TTL default
		l1TTL = time.Hour        // ATREST_DEK_CACHE_TTL default (Config.DEKCacheTTL)
	)
	assert.Greater(t, valkeyutil.RefreshAfter(l2TTL), l1TTL,
		"refresh window must exceed the L1 DEK cache TTL, or every L1 miss pays a refresh")
	assert.Less(t, valkeyutil.RefreshAfter(l2TTL), l2TTL,
		"an entry must still get a refresh opportunity before its TTL expires")
}

func TestL2DEKStore_Get_MissThenMongoPopulatesL2(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
	assert.Equal(t, 1, inner.getCalls)
	assert.Equal(t, 1, fv.setHits, "a found row must populate L2")
	assert.Equal(t, 1, rec.miss)
}

func TestL2DEKStore_Get_L2HitSkipsMongo(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	_, err := s.Get(context.Background(), "room1") // warm
	require.NoError(t, err)
	before := inner.getCalls

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
	assert.Equal(t, before, inner.getCalls, "an L2 hit must not reach Mongo")
	assert.GreaterOrEqual(t, rec.hit, 1)
}

// The outage case this whole design exists for.
func TestL2DEKStore_Get_ServesFromL2WhileMongoDown(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)
	_, err := s.Get(context.Background(), "room1") // warm L2 while healthy
	require.NoError(t, err)

	inner.getErr = errors.New("mongo unreachable")
	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err, "a warmed room must resolve from L2 during a Mongo outage")
	require.NotNil(t, got)
	assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
}

func TestL2DEKStore_Get_ColdRoomDuringOutageErrors(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.getErr = errors.New("mongo unreachable")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	_, err := s.Get(context.Background(), "coldroom")
	require.Error(t, err, "a cold room has no key and must surface the error")
}

func TestL2DEKStore_Get_AbsentRowNotCached(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	got, err := s.Get(context.Background(), "newroom")
	require.NoError(t, err)
	assert.Nil(t, got, "absent row must surface as (nil, nil) so the cipher lazily creates a DEK")
	assert.Equal(t, 0, fv.setHits, "an absent row must never be cached")
}

func TestL2DEKStore_Get_BreakerOpenFastFailsWithoutMongo(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	s := NewL2DEKStore(inner, fv, time.Hour, openBreaker(t), rec)

	_, err := s.Get(context.Background(), "coldroom")
	require.ErrorIs(t, err, circuitbreaker.ErrOpen)
	assert.Equal(t, 0, inner.getCalls, "an open breaker must not call Mongo")
}

func TestL2DEKStore_Get_ValkeyErrorFailsOpenToMongo(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	fv.getErr = errors.New("valkey unreachable")
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err, "a Valkey error must degrade to Mongo, not fail the call")
	require.NotNil(t, got)
	assert.Equal(t, 1, rec.err)
}

func TestL2DEKStore_Get_NilClientAndNonPositiveTTLBypassL2(t *testing.T) {
	cases := []struct {
		name   string
		client valkeyutil.Client
		ttl    time.Duration
	}{
		{"nil client", nil, time.Hour},
		{"zero ttl", newFakeL2Valkey(), 0},
		{"negative ttl", newFakeL2Valkey(), -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := newFakeInnerStore()
			inner.rows["room1"] = seedRow("room1")
			s := NewL2DEKStore(inner, tc.client, tc.ttl, healthyBreaker(), &spyL2Recorder{})

			got, err := s.Get(context.Background(), "room1")
			require.NoError(t, err)
			require.NotNil(t, got)
			if fv, ok := tc.client.(*fakeL2Valkey); ok {
				assert.Equal(t, 0, fv.getHits, "L2 disabled must not read Valkey")
				assert.Equal(t, 0, fv.setHits, "L2 disabled must not write Valkey")
			}
		})
	}
}

// Rotation correctness: a stale wrapped DEK would fail to unwrap under the new KEK.
func TestL2DEKStore_ReplaceInvalidatesL2(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)
	_, err := s.Get(context.Background(), "room1") // warm
	require.NoError(t, err)
	require.NotEmpty(t, fv.store[DEKKey("room1")])

	rotated := RoomDataKey{ID: "room1", WrappedDEK: []byte("rewrapped"), CreatedAt: time.Unix(1, 0).UTC()}
	require.NoError(t, s.Replace(context.Background(), rotated))
	assert.Empty(t, fv.store[DEKKey("room1")], "rotation must invalidate the stale L2 entry")

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("rewrapped"), got.WrappedDEK, "post-rotation reads must see the new wrapped DEK")
}

func TestL2DEKStore_UpsertInvalidatesL2(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)
	_, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)

	require.NoError(t, s.Upsert(context.Background(), seedRow("room1")))
	assert.Empty(t, fv.store[DEKKey("room1")], "upsert must invalidate the L2 entry")
	assert.Equal(t, 1, inner.upsertHit)
}

// A stale hit while the breaker is already open must not wait on Mongo: the
// breaker answers ErrOpen, and the entry is re-armed so the room stays alive.
func TestL2DEKStore_StaleHitWithOpenBreakerSlidesWithoutMongo(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	b := circuitbreaker.New(1, time.Hour)
	s := newClockedStore(t, inner, fv, time.Hour, b, clock)

	_, err := s.Get(ctx, "room1") // warm while closed
	require.NoError(t, err)

	inner.getErr = errors.New("mongo down")
	_, _ = s.Get(ctx, "coldroom") // one failure trips this breaker open
	require.Equal(t, circuitbreaker.StateOpen, b.State())
	beforeSets, beforeCalls := fv.setHits, inner.getCalls

	clock.Advance(pastRefresh)
	got, err := s.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, beforeCalls, inner.getCalls, "an open breaker must not call Mongo")
	assert.Equal(t, 1, fv.expireHits, "a stale hit during an outage must re-arm the TTL")
	assert.Equal(t, beforeSets, fv.setHits, "re-arming must move the deadline, not rewrite the value")
}

// A slide moves a deadline; it must not write a value. Replace invalidates the
// L2 on KEK rotation precisely so the pre-rotation wrapped DEK stops being
// served — if a slide racing that Del re-Set the entry it had already read, the
// stale ciphertext would come back and fail to unwrap under the new KEK for a
// whole TTL. Re-arming a key that no longer exists must be a no-op.
func TestL2DEKStore_SlideDoesNotResurrectAnInvalidatedEntry(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	b := circuitbreaker.New(1, time.Hour)
	s := newClockedStore(t, inner, fv, time.Hour, b, clock)

	_, err := s.Get(ctx, "room1") // warm while closed
	require.NoError(t, err)

	inner.getErr = errors.New("mongo down")
	_, _ = s.Get(ctx, "coldroom") // trip the breaker
	require.Equal(t, circuitbreaker.StateOpen, b.State())

	// Stand in for a rotation's invalidate landing between the L2 read and the
	// slide: the entry is served, then deleted before the re-arm runs.
	fv.afterGet = func(key string) {
		delete(fv.store, key)
		delete(fv.expiry, key)
	}

	clock.Advance(pastRefresh)
	_, err = s.Get(ctx, "room1")
	require.NoError(t, err)

	_, present := fv.store[DEKKey("room1")]
	assert.False(t, present, "a slide must not recreate an invalidated entry")
}

// The L2 wire form is a cache contract: the envelope's own field and the
// wrapped row's fields are all camelCase, not PascalCase Go field names.
func TestL2DEKStore_L2WireFormIsCamelCaseJSON(t *testing.T) {
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), &spyL2Recorder{})

	_, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(fv.store[DEKKey("room1")]), &raw))
	assert.Contains(t, raw, "cachedAt")
	require.Contains(t, raw, "row")

	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw["row"], &row))
	assert.Contains(t, row, "id")
	assert.Contains(t, row, "wrappedDek")
	assert.Contains(t, row, "createdAt")
}

// A foreign or truncated value under the DEK key decodes cleanly into a
// zero-valued cachedDEK; treating that as a hit would fail Unwrap for the
// entry's whole TTL, so it must fall through to the inner store instead. An
// entry written by an older build (a bare row, no envelope) lands in the same
// bucket, which is what makes the format change safe to roll out.
func TestL2DEKStore_Get_EmptyWrappedDEKTreatedAsMiss(t *testing.T) {
	cases := []struct {
		name   string
		cached string
	}{
		{"json null", "null"},
		{"foreign object", `{"foo":"bar"}`},
		{"pre-envelope entry", `{"id":"room1","wrappedDek":"d3JhcHBlZA=="}`},
		{"explicit empty wrappedDek", `{"row":{"id":"room1","wrappedDek":""},"cachedAt":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
			inner.rows["room1"] = seedRow("room1")
			fv.store[DEKKey("room1")] = tc.cached
			s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

			got, err := s.Get(context.Background(), "room1")
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK,
				"a malformed L2 entry must fall through to the inner store")
			assert.Equal(t, 1, inner.getCalls)
			assert.Equal(t, 0, rec.hit, "a malformed entry is not a hit")
			assert.Equal(t, 1, rec.miss)
		})
	}
}

// fakeClock is a manually advanced time source for the age-driven tests.
type fakeClock struct{ nanos atomic.Int64 }

func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{}
	c.nanos.Store(start.UnixNano())
	return c
}
func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.nanos.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.nanos.Add(int64(d)) }

// Freshness is a property of the entry, not of the process: a pod that starts
// with a warm L2 (a restart, a scale-up) serves a recently written entry with
// no Mongo call and no re-arm, and refreshes one that predates it.
func TestL2DEKStore_EntryAgeSurvivesProcessRestart(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	t.Run("entry written recently by another pod", func(t *testing.T) {
		fv, inner := newFakeL2Valkey(), newFakeInnerStore()
		inner.rows["room1"] = seedRow("room1")
		fv.store[DEKKey("room1")] = mustJSON(t, cachedDEK{
			Row: seedRow("room1"), CachedAt: clock.Now().Add(-time.Minute).UnixMilli(),
		})
		s := newClockedStore(t, inner, fv, time.Hour, healthyBreaker(), clock)

		got, err := s.Get(ctx, "room1")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, 0, inner.getCalls, "a recently confirmed entry needs no Mongo call")
		assert.Equal(t, 0, fv.setHits, "a recently confirmed entry must not be re-armed")
	})

	t.Run("entry older than the refresh interval", func(t *testing.T) {
		fv, inner := newFakeL2Valkey(), newFakeInnerStore()
		inner.rows["room1"] = rotatedRow("room1")
		fv.store[DEKKey("room1")] = mustJSON(t, cachedDEK{
			Row: seedRow("room1"), CachedAt: clock.Now().Add(-pastRefresh).UnixMilli(),
		})
		s := newClockedStore(t, inner, fv, time.Hour, healthyBreaker(), clock)

		got, err := s.Get(ctx, "room1")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, []byte("rewrapped"), got.WrappedDEK)
		assert.Equal(t, 1, inner.getCalls, "an aged entry must be re-resolved")
	})
}

// Mongo has no delete path for DEK rows, so an absent row on refresh is lag or
// an anomaly. Honoring it would send the cipher off to mint a second DEK and
// orphan every message already encrypted under the cached one.
func TestL2DEKStore_RefreshFindingNoRowKeepsCachedKey(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	s := newClockedStore(t, inner, fv, time.Hour, healthyBreaker(), clock)

	_, err := s.Get(ctx, "room1") // warm
	require.NoError(t, err)

	delete(inner.rows, "room1") // the row vanishes from the source of truth
	clock.Advance(pastRefresh)
	got, err := s.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got, "the cached key must not be dropped on an absent row")
	assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)

	// Restamped, so the anomaly is retried once per interval, not once per read.
	clock.Advance(time.Minute)
	_, err = s.Get(ctx, "room1")
	require.NoError(t, err)
	assert.Equal(t, 2, inner.getCalls, "an absent row must not make every read refetch")
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// newClockedStore wires one fake clock into both the store and the Valkey fake,
// so cached-entry age and TTL expiry advance together under the test's control.
func newClockedStore(t *testing.T, inner DEKStore, fv *fakeL2Valkey, ttl time.Duration, b *circuitbreaker.Breaker, clock *fakeClock) *l2DEKStore {
	t.Helper()
	fv.now = clock.Now
	s := NewL2DEKStore(inner, fv, ttl, b, &spyL2Recorder{}).(*l2DEKStore)
	s.now = clock.Now
	return s
}

func rotatedRow(roomID string) RoomDataKey {
	return RoomDataKey{ID: roomID, WrappedDEK: []byte("rewrapped"), CreatedAt: time.Unix(1, 0).UTC()}
}

// The steady-state bug: with the L1 cache in front, a healthy pod can serve L2
// hits for hours without any Get ever falling through to Mongo. Every such hit
// must be a pure read — no re-serialize, no round-trip, no re-arm.
func TestL2DEKStore_HealthyHitsDoNotSlide(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	s := newClockedStore(t, inner, fv, time.Hour, healthyBreaker(), clock)

	_, err := s.Get(ctx, "room1") // warm: one inner fetch, one populate
	require.NoError(t, err)
	require.Equal(t, 1, fv.setHits)

	// Minutes later — long past any breaker cooldown, well inside the refresh
	// interval — the entry is still confirmed-recent.
	for i := 0; i < 5; i++ {
		clock.Advance(time.Minute)
		got, err := s.Get(ctx, "room1")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
	}
	assert.Equal(t, 1, fv.setHits, "a healthy steady-state hit must not re-arm the TTL")
	assert.Equal(t, 1, inner.getCalls, "a confirmed-recent hit must not reach Mongo")
}

// Refresh-on-read: once an entry is older than the refresh interval, the next
// hit re-resolves it from the source of truth — exactly one inner fetch — and
// repopulates with a fresh TTL. This is what keeps the breaker fed with a real
// Mongo health signal in steady state.
func TestL2DEKStore_RefreshesEntryOlderThanRefreshInterval(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	s := newClockedStore(t, inner, fv, time.Hour, healthyBreaker(), clock)

	_, err := s.Get(ctx, "room1") // warm
	require.NoError(t, err)
	require.Equal(t, 1, inner.getCalls)

	inner.rows["room1"] = rotatedRow("room1") // Mongo moved on behind the cache

	clock.Advance(pastRefresh)
	got, err := s.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("rewrapped"), got.WrappedDEK, "a stale entry must re-resolve from the inner store")
	assert.Equal(t, 2, inner.getCalls, "a refresh is exactly one inner fetch")
	assert.Equal(t, 2, fv.setHits, "a refresh repopulates the L2")

	// The refreshed entry is confirmed-now, so the next hit is a pure read again.
	clock.Advance(time.Minute)
	_, err = s.Get(ctx, "room1")
	require.NoError(t, err)
	assert.Equal(t, 2, inner.getCalls, "a just-refreshed entry must not refetch")
	assert.Equal(t, 2, fv.setHits, "a just-refreshed entry must not re-arm")
}

func TestL2DEKStore_DoesNotRefreshFreshEntry(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	s := newClockedStore(t, inner, fv, time.Hour, healthyBreaker(), clock)

	_, err := s.Get(ctx, "room1") // warm
	require.NoError(t, err)

	clock.Advance(withinRefresh)
	got, err := s.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 1, inner.getCalls, "an entry younger than the refresh interval must not refetch")
	assert.Equal(t, 1, fv.setHits, "an entry younger than the refresh interval must not re-arm")
}

// The outage the whole L2 exists for: with Mongo failing and the breaker open,
// an actively read room keeps resolving from the L2 for an unbounded time — the
// slide is what keeps the entry from expiring one TTL after the last populate.
func TestL2DEKStore_OutageKeepsActivelyReadRoomAliveBeyondTTL(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	// Opens after 2 failures and stays open for the whole (wall-clock) test.
	b := circuitbreaker.New(2, time.Hour)
	s := newClockedStore(t, inner, fv, time.Hour, b, clock)

	_, err := s.Get(ctx, "room1") // warm while healthy
	require.NoError(t, err)

	inner.getErr = errors.New("mongo unreachable")
	for i := 0; i < 6; i++ {
		clock.Advance(pastRefresh) // each step ages the entry past the refresh window
		got, err := s.Get(ctx, "room1")
		require.NoError(t, err, "read %d during the outage must resolve from the L2", i)
		require.NotNil(t, got)
		assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
	}
	assert.NotEqual(t, circuitbreaker.StateClosed, b.State(), "repeated refresh failures must open the breaker")
	assert.NotEmpty(t, fv.store[DEKKey("room1")], "an actively read room must outlive its TTL during an outage")
}

// The property the refresh restores: a swallowed invalidation stops being
// permanent. The Del fails, so the L2 keeps serving the pre-rotation wrapped
// DEK — until the entry ages past the refresh interval and self-heals.
func TestL2DEKStore_SwallowedInvalidationSelfHealsWithinRefreshInterval(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	s := newClockedStore(t, inner, fv, time.Hour, healthyBreaker(), clock)

	_, err := s.Get(ctx, "room1") // warm
	require.NoError(t, err)

	fv.delErr = errors.New("valkey unreachable") // the invalidation is swallowed
	require.NoError(t, s.Replace(ctx, rotatedRow("room1")))

	got, err := s.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK, "precondition: the stale entry survived the failed Del")

	clock.Advance(pastRefresh)
	got, err = s.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("rewrapped"), got.WrappedDEK,
		"a missed invalidation must self-heal within one refresh interval")
}

// The mirror of the outage case: nothing keeps an idle entry alive, so a room
// that stops being read expires on its TTL as designed.
func TestL2DEKStore_IdleEntryExpiresOnTTL(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	s := newClockedStore(t, inner, fv, time.Hour, healthyBreaker(), clock)

	_, err := s.Get(ctx, "room1") // warm
	require.NoError(t, err)

	clock.Advance(2 * time.Hour) // no reads in between
	got, err := s.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 2, inner.getCalls, "an expired entry must be re-resolved from the inner store")
}

func TestL2DEKStore_NilRecorderDoesNotPanic(t *testing.T) {
	inner := newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, newFakeL2Valkey(), time.Hour, healthyBreaker(), nil)
	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
}

// Supplementary tests beyond the brief, added to close coverage gaps left by
// the brief's suite (error paths of Upsert/Replace/writeL2/invalidate, and
// DefaultL2Recorder) so pkg/atrest's new L2 code clears the 90% floor.

func TestL2DEKStore_Upsert_InnerErrorPropagatesWithoutInvalidating(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.upsertErr = errors.New("mongo unreachable")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	err := s.Upsert(context.Background(), seedRow("room1"))
	require.Error(t, err)
	assert.Equal(t, 0, fv.delHits, "an inner failure must not attempt an L2 invalidate")
}

func TestL2DEKStore_Replace_InnerErrorPropagatesWithoutInvalidating(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.replaceErr = errors.New("mongo unreachable")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	err := s.Replace(context.Background(), seedRow("room1"))
	require.Error(t, err)
	assert.Equal(t, 0, fv.delHits, "an inner failure must not attempt an L2 invalidate")
}

func TestL2DEKStore_Get_L2WriteErrorIsSwallowed(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	fv.setErr = errors.New("valkey unreachable")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err, "an L2 populate failure must not fail the call")
	require.NotNil(t, got)
	assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
}

func TestL2DEKStore_Invalidate_DelErrorIsSwallowed(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)
	_, err := s.Get(context.Background(), "room1") // warm
	require.NoError(t, err)

	fv.delErr = errors.New("valkey unreachable")
	require.NoError(t, s.Upsert(context.Background(), seedRow("room1")), "a best-effort Del failure must not fail Upsert")
}

func TestL2DEKStore_Invalidate_DisabledL2IsNoOp(t *testing.T) {
	inner, rec := newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, nil, time.Hour, healthyBreaker(), rec)

	require.NoError(t, s.Upsert(context.Background(), seedRow("room1")))
	require.NoError(t, s.Replace(context.Background(), seedRow("room1")))
}

func TestDefaultL2Recorder(t *testing.T) {
	rec := DefaultL2Recorder()
	require.NotNil(t, rec)
	assert.NotPanics(t, func() {
		rec.Hit(context.Background())
		rec.Miss(context.Background())
		rec.Error(context.Background())
	})
}

// TestCipherOverL2_DecryptsAfterL1ExpiryWhileMongoDown is the regression guard
// for the whole feature, exercised end to end through the real cipher:
// dekFor -> L1 expiry -> L2 hit -> Unwrap -> Decrypt. Nothing else in the suite
// covers the user-visible claim, only the decorator in isolation.
func TestCipherOverL2_DecryptsAfterL1ExpiryWhileMongoDown(t *testing.T) {
	ctx := context.Background()
	wrapper := newStaticKeyWrapper(t)

	// Seed the inner (Mongo) store with a genuinely wrapped DEK, so the L2 entry
	// has to survive Unwrap, not just round-trip as opaque bytes.
	dek, wrapped, err := wrapper.GenerateDataKey(ctx)
	require.NoError(t, err)
	require.Len(t, dek, 32)

	inner := newFakeInnerStore()
	inner.rows["room1"] = RoomDataKey{ID: "room1", WrappedDEK: wrapped, CreatedAt: time.Unix(0, 0).UTC()}
	fv := newFakeL2Valkey()
	l2 := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), &spyL2Recorder{})

	clock := newFakeClock(time.Now())
	l1 := newDEKCache(10, time.Minute)
	l1.now = clock.Now
	c := newCipher(wrapper, l2, l1)

	fields := EncryptedFields{Msg: "the quick brown fox"}
	payload, meta, err := c.Encrypt(ctx, "room1", fields)
	require.NoError(t, err)
	require.NotEmpty(t, fv.store[DEKKey("room1")], "the healthy encrypt must populate the L2")
	warmCalls := inner.getCalls

	// Mongo goes away and the L1 entry ages out — the exact window this feature
	// exists to cover.
	inner.getErr = errors.New("mongo unreachable")
	clock.Advance(2 * time.Minute)
	_, cached := l1.get("room1")
	require.False(t, cached, "the L1 entry must be expired for this test to mean anything")

	got, err := c.Decrypt(ctx, "room1", payload, meta)
	require.NoError(t, err, "an L1 expiry during a Mongo outage must be absorbed by the L2")
	assert.Equal(t, fields.Msg, got.Msg, "the payload must decrypt to the original plaintext")
	assert.Equal(t, warmCalls, inner.getCalls, "the DEK must come from the L2, not Mongo")

	// Encrypt must survive the same window: a new write during the outage
	// round-trips through the L2-resolved DEK.
	clock.Advance(2 * time.Minute)
	payload2, meta2, err := c.Encrypt(ctx, "room1", EncryptedFields{Msg: "second"})
	require.NoError(t, err)
	got2, err := c.Decrypt(ctx, "room1", payload2, meta2)
	require.NoError(t, err)
	assert.Equal(t, "second", got2.Msg)
	assert.Equal(t, warmCalls, inner.getCalls, "no Get may reach the downed inner store")
}

// MGet loops the fake's own Get so it cannot drift from single-key behaviour.
func (f *fakeL2Valkey) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		v, err := f.Get(ctx, k)
		if err != nil {
			continue
		}
		out[k] = v
	}
	return out, nil
}

// The DEK tier sits behind a process-local L1 whose TTL (1h by default) is
// LONGER than the L2's refresh window (RefreshAfter(90m) = 67.5m). That ladder
// silently defeats the tier: the L1 miss at 60m finds the L2 entry still fresh,
// serves it and repopulates L1 without touching the L2 deadline — so the L2
// expires at 90m and the next L1 miss at 120m finds nothing, precisely during
// the outage the L2 exists to survive.
//
// Re-arming on every serve closes it and costs nothing else: Fresh() is computed
// from CachedAt, not the Valkey deadline, so the staleness bound is unchanged,
// and SlideTTL uses EXPIRE, which no-ops on an absent key — an entry busted
// meanwhile stays busted.
func TestL2DEKStore_FreshServeReArmsTheDeadline(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	start := time.Now()
	clock := newFakeClock(start)
	const ttl = 90 * time.Minute
	s := newClockedStore(t, inner, fv, ttl, healthyBreaker(), clock)

	_, err := s.Get(ctx, "room1") // populate; deadline is start+90m
	require.NoError(t, err)

	// The L1 in front expires at 60m, which lands INSIDE the fresh window.
	clock.Advance(60 * time.Minute)
	got, err := s.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, 1, inner.getCalls, "a fresh entry must still not refetch")
	assert.Greater(t, fv.expireHits, 0, "a fresh serve must re-arm the deadline")
	assert.True(t, fv.expiry[DEKKey("room1")].After(start.Add(ttl)),
		"the deadline must move past the original expiry, or the next L1 miss finds nothing")
}

// The ladder end-to-end, under the condition that actually matters: with Mongo
// unreachable, reads arriving only once per L1 TTL (1h) must keep resolving.
// Without the slide the L2 entry dies at 90m and the 120m read has nothing to
// fall back to — a healthy inner store would mask this entirely, which is why
// the breaker is opened first.
func TestL2DEKStore_SurvivesAnL1LongerThanTheRefreshWindow(t *testing.T) {
	ctx := context.Background()
	fv, inner := newFakeL2Valkey(), newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	clock := newFakeClock(time.Now())
	b := circuitbreaker.New(2, time.Hour) // opens after 2 failures, stays open
	s := newClockedStore(t, inner, fv, 90*time.Minute, b, clock)

	_, err := s.Get(ctx, "room1") // warm while healthy
	require.NoError(t, err)

	inner.getErr = errors.New("mongo unreachable")

	// Six L1 lifetimes — six hours, past several unslid 90m deadlines.
	for i := range 6 {
		clock.Advance(time.Hour)
		got, err := s.Get(ctx, "room1")
		require.NoError(t, err, "read %d during the outage must resolve from the L2", i)
		require.NotNil(t, got, "read %d during the outage must resolve from the L2", i)
	}
}
