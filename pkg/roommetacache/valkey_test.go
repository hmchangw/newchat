package roommetacache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// withFetcherForTest overrides the read-through's Mongo fetch so this package's
// cache decisions can be exercised without a Mongo. It lives here rather than
// in valkey.go because a test-only hook must not ship in the binary.
func withFetcherForTest(f func(context.Context, *mongo.Collection, string) (Meta, error)) tierOption {
	return func(o *tierOpts) { o.fetch = f }
}

// fakeValkey is an in-memory valkeyutil.Client for unit tests.
type fakeValkey struct {
	mu      sync.Mutex
	data    map[string]string
	dels    []string
	sets    []string // keys passed to Set, regardless of setErr
	expires int
	getErr  error
	setErr  error
	delErr  error
}

func newFakeValkey() *fakeValkey { return &fakeValkey{data: map[string]string{}} }

func (f *fakeValkey) Get(_ context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.data[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}

func (f *fakeValkey) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets = append(f.sets, key)
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	return nil
}

func (f *fakeValkey) Del(_ context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dels = append(f.dels, keys...)
	if f.delErr != nil {
		return f.delErr
	}
	for _, k := range keys {
		delete(f.data, k)
	}
	return nil
}
func (f *fakeValkey) Expire(_ context.Context, key string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expires++
	_, ok := f.data[key]
	return ok, nil
}

func (f *fakeValkey) Close() error { return nil }

// SetNX / IncrEx satisfy valkeyutil.Client but are unused here; panic on any call.
func (f *fakeValkey) SetNX(_ context.Context, _, _ string, _ time.Duration) (bool, error) {
	panic("fakeValkey.SetNX not implemented")
}

func (f *fakeValkey) IncrEx(_ context.Context, _ string, _ time.Duration) (int64, error) {
	panic("fakeValkey.IncrEx not implemented")
}

// TestMetaKey pins both halves of the key contract. The version segment is what
// keeps a rolling deploy safe: the value under this key changed from a bare Meta
// to the valkeyutil.Box[Meta] envelope, and an old binary decodes that envelope into Meta
// with no JSON error and every field zero — an unversioned key would hand old
// broadcast-worker pods an empty room type and old gatekeeper pods UserCount 0.
// The {roomID} hash tag must survive the change so the entry stays in the same
// cluster slot as the room's encryption key (pkg/roomkeystore).
func TestMetaKey(t *testing.T) {
	got := MetaKey("r123")
	assert.Equal(t, "room:{r123}:meta:"+cacheKeySchemaVersion, got)
	assert.Contains(t, got, "{r123}", "hash tag must be preserved for slot colocation")
	assert.NotEqual(t, "room:{r123}:meta", got, "must not reuse the pre-envelope key")
}

func TestReadThrough_L2Hit(t *testing.T) {
	fake := newFakeValkey()
	want := Meta{ID: "r1", Type: model.RoomTypeChannel, Name: "general", SiteID: "site-a", UserCount: 7}
	raw, err := json.Marshal(valkeyutil.Box[Meta]{V: want, CachedAt: time.Now().UnixMilli()})
	require.NoError(t, err)
	fake.data[MetaKey("r1")] = string(raw)

	// nil *mongo.Collection is safe: on an L2 hit, Mongo is never touched.
	got, err := readThrough(context.Background(), fake, nil, "r1", time.Minute, &fakeRecorder{})
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// TestReadThrough_L2Hit_DoesNotPopulate confirms that on a cache hit the
// populate (Set) path is never reached — Mongo is not consulted and no Set
// call is issued.
//
// NOTE: the miss→populate and error-fallthrough paths inside ReadThrough
// require a live *mongo.Collection to avoid a nil-dereference; those paths
// are covered by integration_test.go (Task 2).
func TestReadThrough_L2Hit_DoesNotPopulate(t *testing.T) {
	fake := newFakeValkey()
	want := Meta{ID: "r1", Type: model.RoomTypeChannel, Name: "general", SiteID: "site-a", UserCount: 3}
	raw, err := json.Marshal(valkeyutil.Box[Meta]{V: want, CachedAt: time.Now().UnixMilli()})
	require.NoError(t, err)
	fake.data[MetaKey("r1")] = string(raw)

	// nil *mongo.Collection is safe on a hit — Mongo must never be touched.
	got, err := readThrough(context.Background(), fake, nil, "r1", time.Minute, &fakeRecorder{})
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Empty(t, fake.sets, "Set must not be called on a cache hit")
}

// The guard exists to fence the Mongo fetch, not the cache in front of it. If
// it fenced the whole read-through, a circuit breaker that opened during a
// Mongo outage would also refuse L2 hits — disabling the cache at the exact
// moment it is the only thing that can answer.
func TestReadThrough_FetchGuard_NotAppliedToL2Hit(t *testing.T) {
	fake := newFakeValkey()
	want := Meta{ID: "r1", Type: model.RoomTypeChannel, Name: "general", SiteID: "site-a", UserCount: 7}
	raw, err := json.Marshal(valkeyutil.Box[Meta]{V: want, CachedAt: time.Now().UnixMilli()})
	require.NoError(t, err)
	fake.data[MetaKey("r1")] = string(raw)

	guardCalls := 0
	got, err := readThrough(context.Background(), fake, nil, "r1", time.Minute, &fakeRecorder{},
		withFetchGuard(func(func() error) error {
			guardCalls++
			return errors.New("guard refused")
		}))

	require.NoError(t, err, "an L2 hit must be served even while the guard is refusing")
	assert.Equal(t, want, got)
	assert.Zero(t, guardCalls, "the guard must not wrap the L2 read")
}

// On a miss the fetch runs inside the guard, so a refusing guard short-circuits
// before Mongo is touched (nil collection proves it was never dereferenced).
func TestReadThrough_FetchGuard_WrapsMongoFetch(t *testing.T) {
	fake := newFakeValkey()
	errRefused := errors.New("guard refused")

	guardCalls := 0
	_, err := readThrough(context.Background(), fake, nil, "missing", time.Minute, &fakeRecorder{},
		withFetchGuard(func(func() error) error {
			guardCalls++
			return errRefused
		}))

	require.ErrorIs(t, err, errRefused)
	assert.Equal(t, 1, guardCalls, "the fetch must run inside the guard")
	assert.Empty(t, fake.sets, "nothing may be cached when the fetch never ran")
}

// A decoded-but-zero entry must be treated as a miss, not served. Any
// well-formed JSON that is not a Meta unmarshals to the zero value, and a Meta
// with an empty SiteID/Type feeds routing decisions downstream — so a stray
// "null" under this key would misroute a room's events for a whole TTL.
// FetchFromMongo always populates ID, so ID=="" cannot be a legitimate entry.
func TestReadThrough_ZeroValueL2Entry_TreatedAsMiss(t *testing.T) {
	for _, tt := range []struct{ name, stored string }{
		{"json null", "null"},
		{"empty object", "{}"},
		{"foreign value", `{"unrelated":"field"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeValkey()
			fake.data[MetaKey("r1")] = tt.stored
			rec := &fakeRecorder{}

			guardCalls := 0
			_, err := readThrough(context.Background(), fake, nil, "r1", time.Minute, rec,
				withFetchGuard(func(func() error) error {
					guardCalls++
					return errors.New("fell through to the source of truth")
				}))

			require.Error(t, err, "a zero entry must not be served as a hit")
			assert.Equal(t, 1, guardCalls, "a zero entry must fall through to Mongo")
		})
	}
}

// TestBustMeta_CallsDel pins that a bust clears BOTH key generations. Versioning
// the key stopped old binaries decoding the new envelope, but it also split the
// entry in two: during a rolling deploy an old pod still populates and reads
// legacyMetaKey. A bust that dropped only the current key would leave that pod
// serving metadata for a room that was just renamed or deleted, for a full TTL.
// The legacy key goes away with the compatibility window, not before.
func TestBustMeta_CallsDel(t *testing.T) {
	fake := newFakeValkey()
	fake.data[MetaKey("r1")] = "{}"
	fake.data[legacyMetaKey("r1")] = "{}"

	BustMeta(context.Background(), fake, "r1")

	assert.ElementsMatch(t, []string{MetaKey("r1"), legacyMetaKey("r1")}, fake.dels)
	_, present := fake.data[MetaKey("r1")]
	assert.False(t, present, "current key must be evicted")
	_, legacyPresent := fake.data[legacyMetaKey("r1")]
	assert.False(t, legacyPresent, "pre-v2 key must be evicted during the rolling-deploy window")
}

func TestBustMeta_NilClient_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() { BustMeta(context.Background(), nil, "r1") })
}

func TestBustMeta_FailOpen(t *testing.T) {
	fake := newFakeValkey()
	fake.delErr = errors.New("valkey down")
	// Must not panic and must not propagate — best-effort.
	assert.NotPanics(t, func() { BustMeta(context.Background(), fake, "r1") })
	assert.ElementsMatch(t, []string{MetaKey("r1"), legacyMetaKey("r1")}, fake.dels)
}

// MGet loops the fake's own Get so it cannot drift from single-key behaviour.
func (f *fakeValkey) MGet(ctx context.Context, keys []string) (map[string]string, error) {
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

// withFetchGuard and withFetcherForTest are the raw tier options. Production
// takes a breaker (see NewL2Tier) and never needs either; these exist so a test
// can drive the guard's three outcomes — pass through, fail fast, count calls —
// without standing up a breaker and tripping it, and so cache decisions can be
// exercised without a live Mongo. They live here because nothing but a test
// constructs them.
func withFetchGuard(guard func(fn func() error) error) tierOption {
	return func(o *tierOpts) { o.guard = guard }
}

// readThrough and readThroughAt build a one-shot L2Tier per call. They exist
// only so these tests keep reading as one call per scenario; production holds a
// tier (see L2Tier's doc for why building one per read is not free), which is
// exactly why these shims live in the test file rather than beside it.
func readThrough(ctx context.Context, client valkeyutil.Client, rooms *mongo.Collection, roomID string, ttl time.Duration, rec Recorder, opts ...tierOption) (Meta, error) {
	return newL2TierWithClock(client, rooms, ttl, rec, time.Now, opts...).Get(ctx, roomID)
}

func readThroughAt(ctx context.Context, client valkeyutil.Client, rooms *mongo.Collection, roomID string, ttl time.Duration, rec Recorder, now time.Time, opts ...tierOption) (Meta, error) {
	return newL2TierWithClock(client, rooms, ttl, rec, func() time.Time { return now }, opts...).Get(ctx, roomID)
}

// readL2 and writeL2 are the cache-side halves these tests drive directly, to
// place an entry on a chosen side of the refresh window and to inspect what a
// read-through left behind. They live here rather than in valkey.go because
// production no longer has a caller for either — valkeyutil.Tier owns both — and
// a test helper must not sit in production code.
//
// They deliberately reuse valkeyutil.Box[Meta].Usable and MetaKey, so a change to what
// this package considers a usable entry, or to where it stores one, moves the
// helpers with it instead of letting them drift into testing a private fiction.
func readL2(ctx context.Context, client valkeyutil.Client, roomID string, rec Recorder) (valkeyutil.Box[Meta], bool) {
	return valkeyutil.ReadCachedJSON(ctx, client, MetaKey(roomID), "room meta", rec,
		func(b *valkeyutil.Box[Meta]) bool { return b.CachedAt != 0 && usableMeta(&b.V) }, "room_id", roomID)
}

func writeL2(ctx context.Context, client valkeyutil.Client, roomID string, meta *Meta, ttl time.Duration, now time.Time) {
	entry := valkeyutil.Box[Meta]{V: *meta, CachedAt: now.UnixMilli()}
	if err := valkeyutil.SetJSONWithTTL(ctx, client, MetaKey(roomID), entry, ttl); err != nil {
		panic(err) // a fake client that cannot store makes every test below meaningless
	}
}

// TestReadThroughAt_StaleEntrySurvivesFetchOutageViaTTLSlide is the guarantee
// that keeps channel delivery alive: broadcast-worker treats a room-meta failure
// as fatal, so an entry that merely expires mid-outage stops delivery for that
// room until Mongo returns.
func TestReadThroughAt_StaleEntrySurvivesFetchOutageViaTTLSlide(t *testing.T) {
	client := newFakeValkey()
	ctx := context.Background()
	now := time.Now()
	meta := Meta{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", UserCount: 3}
	writeL2(ctx, client, "r1", &meta, time.Hour, now)

	got, err := readThroughAt(ctx, client, nil, "r1", time.Hour, &fakeRecorder{}, now.Add(59*time.Minute),
		withFetchGuard(func(func() error) error { return errors.New("mongo down") }))
	require.NoError(t, err, "a warm room must survive the fetch being down")
	assert.Equal(t, meta, got)
	assert.Positive(t, client.expires, "the deadline must be re-armed, not left to expire")
}

func TestReadThroughAt_FreshEntryDoesNotRefetch(t *testing.T) {
	client := newFakeValkey()
	ctx := context.Background()
	now := time.Now()
	meta := Meta{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	writeL2(ctx, client, "r1", &meta, time.Hour, now)

	fetched := false
	got, err := readThroughAt(ctx, client, nil, "r1", time.Hour, &fakeRecorder{}, now.Add(time.Minute),
		withFetchGuard(func(fn func() error) error { fetched = true; return fn() }))
	require.NoError(t, err)
	assert.Equal(t, meta, got)
	assert.False(t, fetched, "a fresh entry must not re-validate")
}

func TestReadThroughAt_PreEnvelopeEntryIsTreatedAsMiss(t *testing.T) {
	client := newFakeValkey()
	ctx := context.Background()
	// An entry written before the envelope existed: a bare Meta with no CachedAt.
	raw, err := json.Marshal(Meta{ID: "r1", Type: model.RoomTypeChannel})
	require.NoError(t, err)
	require.NoError(t, client.Set(ctx, MetaKey("r1"), string(raw), time.Hour))

	_, err = readThroughAt(ctx, client, nil, "r1", time.Hour, &fakeRecorder{}, time.Now(),
		withFetchGuard(func(func() error) error { return errors.New("mongo down") }))
	require.Error(t, err, "an un-stamped entry must reload rather than be served forever")
}

func TestReadThroughAt_StaleEntryRefreshesWhenFetchHealthy(t *testing.T) {
	client := newFakeValkey()
	ctx := context.Background()
	now := time.Now()
	old := Meta{ID: "r1", Type: model.RoomTypeChannel, Name: "old name", SiteID: "site-a"}
	writeL2(ctx, client, "r1", &old, time.Hour, now)

	// Past the refresh window with a healthy fetch: pick up the change a missed
	// bust would otherwise hide until the TTL, and re-stamp so the next read is
	// fresh again.
	stale := now.Add(59 * time.Minute)
	fresh := Meta{ID: "r1", Type: model.RoomTypeChannel, Name: "new name", SiteID: "site-a"}
	got, err := readThroughAt(ctx, client, nil, "r1", time.Hour, &fakeRecorder{}, stale,
		withFetcherForTest(func(context.Context, *mongo.Collection, string) (Meta, error) { return fresh, nil }))
	require.NoError(t, err)
	assert.Equal(t, fresh, got)

	// Re-read at the same instant: the fresh stamp must make it a pure read.
	fetched := false
	got, err = readThroughAt(ctx, client, nil, "r1", time.Hour, &fakeRecorder{}, stale,
		withFetchGuard(func(fn func() error) error { fetched = true; return fn() }))
	require.NoError(t, err)
	assert.Equal(t, fresh, got)
	assert.False(t, fetched, "the rewrite must reset the refresh window")
}

// A room Mongo confirms is gone must be evicted, not served from the stale
// entry. The stale branch re-arms the deadline on every failure, so treating a
// confirmed deletion like an outage keeps the deleted room alive for as long as
// anyone keeps reading it — indefinitely, not merely until the TTL. The cold
// path already fails closed on the same error; this makes the warm path agree.
func TestReadThroughAt_ConfirmedDeletionEvictsRatherThanServingStale(t *testing.T) {
	client := newFakeValkey()
	ctx := context.Background()
	now := time.Now()
	meta := Meta{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	writeL2(ctx, client, "r1", &meta, time.Hour, now)

	stale := now.Add(59 * time.Minute)
	_, err := readThroughAt(ctx, client, nil, "r1", time.Hour, &fakeRecorder{}, stale,
		withFetcherForTest(func(context.Context, *mongo.Collection, string) (Meta, error) {
			return Meta{}, fmt.Errorf("fetch room meta r1: %w", mongo.ErrNoDocuments)
		}))
	require.Error(t, err, "a confirmed deletion must not be answered from cache")
	require.ErrorIs(t, err, mongo.ErrNoDocuments, "the sentinel must survive for callers to branch on")

	if _, found := readL2(ctx, client, "r1", &fakeRecorder{}); found {
		t.Fatal("the deleted room's entry must be evicted, or the next read resurrects it")
	}
}

// The counterpart guard: an unreachable Mongo is NOT a confirmed deletion, so it
// must still slide and serve. Without this, the fix above would turn every
// outage into a cache wipe.
func TestReadThroughAt_OutageStillSlidesAndServes(t *testing.T) {
	client := newFakeValkey()
	ctx := context.Background()
	now := time.Now()
	meta := Meta{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	writeL2(ctx, client, "r1", &meta, time.Hour, now)

	got, err := readThroughAt(ctx, client, nil, "r1", time.Hour, &fakeRecorder{}, now.Add(59*time.Minute),
		withFetcherForTest(func(context.Context, *mongo.Collection, string) (Meta, error) {
			return Meta{}, errors.New("server selection error: context deadline exceeded")
		}))
	require.NoError(t, err)
	assert.Equal(t, meta, got)
	if _, found := readL2(ctx, client, "r1", &fakeRecorder{}); !found {
		t.Fatal("an outage must not evict the entry it exists to preserve")
	}
}
