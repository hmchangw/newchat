package subauthcache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// TestFromSubscription covers the pure Mongo-doc-to-SubAuth mapping used by
// FetchFromMongo, without touching a real database.
func TestFromSubscription(t *testing.T) {
	t.Run("nil HistorySharedSince maps to nil", func(t *testing.T) {
		sub := &model.Subscription{
			User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
			Roles:  []model.Role{model.RoleMember},
			RoomID: "room1",
		}
		got := fromSubscription(sub)
		assert.Equal(t, "u1", got.ID)
		assert.Equal(t, "alice", got.Account)
		assert.Equal(t, []model.Role{model.RoleMember}, got.Roles)
		assert.Nil(t, got.HistorySharedSince)
	})

	t.Run("non-nil HistorySharedSince maps to epoch millis", func(t *testing.T) {
		since := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		sub := &model.Subscription{
			User:               model.SubscriptionUser{ID: "u2", Account: "bob"},
			Roles:              []model.Role{model.RoleOwner},
			HistorySharedSince: &since,
		}
		got := fromSubscription(sub)
		require.NotNil(t, got.HistorySharedSince)
		assert.Equal(t, since.UnixMilli(), *got.HistorySharedSince)
	})
}

// fakeValkey is an in-memory valkeyutil.Client for tests.
type fakeValkey struct {
	store      map[string]string
	getErr     error
	setErr     error
	delErr     error
	expireErr  error
	getHits    int
	setHits    int
	delHits    int // count of Del *calls*, not keys — asserts round-trip batching
	expireHits int
	onDel      func(ctx context.Context) // observes the context Del actually runs under
}

func newFakeValkey() *fakeValkey { return &fakeValkey{store: map[string]string{}} }

func (f *fakeValkey) Get(_ context.Context, key string) (string, error) {
	f.getHits++
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.store[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}
func (f *fakeValkey) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.setHits++
	if f.setErr != nil {
		return f.setErr
	}
	f.store[key] = value
	return nil
}
func (f *fakeValkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (f *fakeValkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 0, nil }
func (f *fakeValkey) Del(ctx context.Context, keys ...string) error {
	f.delHits++
	if f.onDel != nil {
		f.onDel(ctx)
	}
	if f.delErr != nil {
		return f.delErr
	}
	for _, k := range keys {
		delete(f.store, k)
	}
	return nil
}

// Expire mirrors Valkey: it re-arms an existing key's deadline and reports
// whether the key was there, but never creates one.
func (f *fakeValkey) Expire(_ context.Context, key string, _ time.Duration) (bool, error) {
	f.expireHits++
	if f.expireErr != nil {
		return false, f.expireErr
	}
	_, ok := f.store[key]
	return ok, nil
}

func (f *fakeValkey) Close() error { return nil }

// spyRecorder counts hit/miss/error.
type spyRecorder struct{ hit, miss, err int }

func (s *spyRecorder) Hit(context.Context)   { s.hit++ }
func (s *spyRecorder) Miss(context.Context)  { s.miss++ }
func (s *spyRecorder) Error(context.Context) { s.err++ }

// fakeClock drives the refresh window deterministically.
type fakeClock struct{ t time.Time }

func newFakeClock(t time.Time) *fakeClock    { return &fakeClock{t: t} }
func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// pastRefresh and withinRefresh are stated relative to the tier's OWN refresh
// window for a 1h TTL rather than hardcoding a fraction, so a change to
// valkeyutil.RefreshAfter moves every refresh test with it.
var (
	pastRefresh   = valkeyutil.RefreshAfter(time.Hour) + time.Minute
	withinRefresh = valkeyutil.RefreshAfter(time.Hour) - time.Minute
)

func newClockedTier(c valkeyutil.Client, ttl time.Duration, clock *fakeClock, loader Loader) *Tier {
	return newTierWithClock(c, ttl, &spyRecorder{}, loader, clock.Now)
}

// newTestTier is the fixed-clock form for tests that do not exercise refresh.
func newTestTier(c valkeyutil.Client, ttl time.Duration, rec Recorder, loader Loader) *Tier {
	frozen := time.Now()
	return newTierWithClock(c, ttl, rec, loader, func() time.Time { return frozen })
}

func TestSubKey(t *testing.T) {
	assert.Equal(t, "sub:{room1}:alice", SubKey("room1", "alice"))
}

func TestReadThrough_L2Hit_SkipsLoader(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	// pre-populate L2
	seed := SubAuth{ID: "u1", Account: "alice", Roles: []model.Role{model.RoleOwner}}
	_, _, err := newTestTier(fv, time.Hour, rec, func(context.Context, string, string) (SubAuth, bool, error) { return seed, true, nil }).Resolve(context.Background(), "room1", "alice")
	require.NoError(t, err)
	// second call: loader must NOT be invoked
	got, subscribed, err := newTestTier(fv, time.Hour, rec, func(context.Context, string, string) (SubAuth, bool, error) {
		t.Fatal("loader must not run on L2 hit")
		return SubAuth{}, false, nil
	}).Resolve(context.Background(), "room1", "alice")
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, []model.Role{model.RoleOwner}, got.Roles)
	assert.GreaterOrEqual(t, rec.hit, 1)
}

// An L2 entry that decodes to a zero SubAuth must never be served: its mere
// presence would read as "subscribed", handing back an authorization decision
// with no identity attached. Any well-formed JSON that isn't a SubAuth ("null",
// a foreign value, an older build's shape) unmarshals to exactly this.
func TestReadThrough_ZeroValueL2Entry_TreatedAsMiss(t *testing.T) {
	tests := []struct {
		name   string
		stored string
	}{
		{"json null", "null"},
		{"empty object", "{}"},
		{"foreign value", `{"unrelated":"field"}`},
		{"account without id", `{"account":"alice"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fv := newFakeValkey()
			fv.store[SubKey("room1", "alice")] = tt.stored
			rec := &spyRecorder{}

			loads := 0
			got, subscribed, err := newTestTier(fv, time.Hour, rec, func(context.Context, string, string) (SubAuth, bool, error) {
				loads++
				return SubAuth{ID: "u1", Account: "alice"}, true, nil
			}).Resolve(context.Background(), "room1", "alice")

			require.NoError(t, err)
			assert.Equal(t, 1, loads, "a zero entry must fall through to the loader")
			assert.True(t, subscribed)
			assert.Equal(t, "u1", got.ID, "the loader's value must win, not the zero entry")
			assert.Equal(t, 0, rec.hit, "a zero entry is not a hit")
		})
	}
}

func TestReadThrough_L2Miss_LoadsAndPopulates(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	loads := 0
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		loads++
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	}
	_, subscribed, err := newTestTier(fv, time.Hour, rec, loader).Resolve(context.Background(), "room1", "alice")
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, 1, loads)
	assert.Equal(t, 1, fv.setHits, "subscribed result must populate L2")
	assert.Equal(t, 1, rec.miss)
}

func TestReadThrough_NotSubscribed_NotCached(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{}, false, nil // confirmed non-subscriber
	}
	_, subscribed, err := newTestTier(fv, time.Hour, rec, loader).Resolve(context.Background(), "room1", "bob")
	require.NoError(t, err)
	assert.False(t, subscribed)
	assert.Equal(t, 0, fv.setHits, "negative result must not be cached")
}

func TestReadThrough_LoaderError_Propagates_NoCache(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	sentinel := errors.New("mongo down / breaker open")
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{}, false, sentinel
	}
	_, _, err := newTestTier(fv, time.Hour, rec, loader).Resolve(context.Background(), "room1", "alice")
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 0, fv.setHits)
}

func TestReadThrough_NilClient_FailsOpenToLoader(t *testing.T) {
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1"}, true, nil
	}
	got, subscribed, err := newTestTier(nil, time.Hour, rec, loader).Resolve(context.Background(), "room1", "alice")
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
}

func TestReadThrough_ValkeySetError_SwallowedReturnsLoaded(t *testing.T) {
	fv := newFakeValkey()
	fv.setErr = errors.New("valkey unreachable")
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	}
	got, subscribed, err := newTestTier(fv, time.Hour, rec, loader).Resolve(context.Background(), "room1", "alice")
	require.NoError(t, err, "a Valkey Set failure must be swallowed, not fail the call")
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 1, fv.setHits, "populate must have been attempted")
}

func TestReadThrough_NonPositiveTTL_BypassesL2(t *testing.T) {
	cases := []struct {
		name string
		ttl  time.Duration
	}{
		{"zero ttl", 0},
		{"negative ttl", -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fv := newFakeValkey()
			rec := &spyRecorder{}
			loader := func(context.Context, string, string) (SubAuth, bool, error) {
				return SubAuth{ID: "u1", Account: "alice"}, true, nil
			}
			got, subscribed, err := newTestTier(fv, tc.ttl, rec, loader).Resolve(context.Background(), "room1", "alice")
			require.NoError(t, err)
			assert.True(t, subscribed)
			assert.Equal(t, "u1", got.ID)
			assert.Equal(t, 0, fv.getHits, "non-positive ttl must skip the L2 read")
			assert.Equal(t, 0, fv.setHits, "non-positive ttl must skip the L2 populate")
		})
	}
}

func TestReadThrough_NilRecorder_DoesNotPanic(t *testing.T) {
	fv := newFakeValkey()
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	}
	// A nil Recorder must fall back to the no-op recorder, not nil-panic.
	got, subscribed, err := newTestTier(fv, time.Hour, nil, loader).Resolve(context.Background(), "room1", "alice")
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
}

// --- BustSub tests ---

func TestBustSub_DeletesTheKey(t *testing.T) {
	fv := newFakeValkey()
	fv.store[SubKey("room1", "alice")] = `{"id":"u1"}`
	BustSub(context.Background(), fv, "room1", "alice")
	_, ok := fv.store[SubKey("room1", "alice")]
	assert.False(t, ok, "BustSub must delete the L2 entry")
}

func TestBustSub_NilClient_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() { BustSub(context.Background(), nil, "room1", "alice") })
}

func TestBustSub_ValkeyErrorSwallowed(t *testing.T) {
	fv := newFakeValkey()
	fv.store[SubKey("room1", "alice")] = `{"id":"u1"}`
	fv.delErr = errors.New("valkey down")
	assert.NotPanics(t, func() { BustSub(context.Background(), fv, "room1", "alice") })
}

func TestReadThrough_ValkeyGetError_FailsOpenToLoader(t *testing.T) {
	fv := newFakeValkey()
	fv.getErr = errors.New("valkey unreachable")
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1"}, true, nil
	}
	got, subscribed, err := newTestTier(fv, time.Hour, rec, loader).Resolve(context.Background(), "room1", "alice")
	require.NoError(t, err, "a Valkey error must degrade to the loader, not fail the call")
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 1, rec.err)
}

// A slide re-arms an expiry; it must never write a value. Between the GET that
// served the hit and the re-arm, a write site may have busted the key — writing
// the value back would resurrect the entry the bust deleted, restoring access
// that was explicitly revoked and keeping it alive for a full TTL. Extending the
// expiry of a key that no longer exists must be a no-op.
func TestTier_SlideDoesNotResurrectABustedEntry(t *testing.T) {
	fv, clock := newFakeValkey(), newFakeClock(time.Now())
	tier := newClockedTier(fv, time.Hour, clock, func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	})
	_, _, err := tier.Resolve(context.Background(), "room1", "alice") // warms
	require.NoError(t, err)

	// Stand in for a bust landing between the L2 read and the re-arm: the refresh
	// fails, and by the time the slide runs the key is gone.
	fv.onDel = nil
	clock.Advance(pastRefresh)
	tier.loader = func(context.Context, string, string) (SubAuth, bool, error) {
		BustSub(context.Background(), fv, "room1", "alice")
		return SubAuth{}, false, errors.New("mongo down")
	}
	_, _, err = tier.Resolve(context.Background(), "room1", "alice")
	require.NoError(t, err)

	_, ok := fv.store[SubKey("room1", "alice")]
	assert.False(t, ok, "the slide must not recreate a key that was busted")
}

// A failed re-arm is best-effort: the value was already served.
func TestTier_SlideRearmFailureStillServes(t *testing.T) {
	fv, clock := newFakeValkey(), newFakeClock(time.Now())
	tier := newClockedTier(fv, time.Hour, clock, func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	})
	_, _, err := tier.Resolve(context.Background(), "room1", "alice") // warms
	require.NoError(t, err)

	fv.expireErr = errors.New("valkey expire failed")
	clock.Advance(pastRefresh)
	tier.loader = func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{}, false, errors.New("mongo down")
	}
	got, subscribed, err := tier.Resolve(context.Background(), "room1", "alice")

	require.NoError(t, err, "a failed TTL re-arm must not fail the read")
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
}

// --- BustSubs (batched) tests ---

func TestBustSubs_SingleDelCallWithAllKeys(t *testing.T) {
	fv := newFakeValkey()
	fv.store[SubKey("room1", "alice")] = `{"id":"u1"}`
	fv.store[SubKey("room1", "bob")] = `{"id":"u2"}`
	fv.store[SubKey("room1", "carol")] = `{"id":"u3"}`

	BustSubs(context.Background(), fv, "room1", []string{"alice", "bob", "carol"})

	assert.Equal(t, 1, fv.delHits, "must be exactly one Del round trip for all accounts")
	_, aliceLeft := fv.store[SubKey("room1", "alice")]
	_, bobLeft := fv.store[SubKey("room1", "bob")]
	_, carolLeft := fv.store[SubKey("room1", "carol")]
	assert.False(t, aliceLeft)
	assert.False(t, bobLeft)
	assert.False(t, carolLeft)
}

func TestBustSubs_EmptySlice_NoOp(t *testing.T) {
	fv := newFakeValkey()
	BustSubs(context.Background(), fv, "room1", nil)
	assert.Equal(t, 0, fv.delHits, "an empty account list must not call Del at all")
}

func TestBustSubs_NilClient_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() { BustSubs(context.Background(), nil, "room1", []string{"alice"}) })
}

// The bust deadline is tier policy applied here, not a per-service choice — a
// hung Valkey must never stall the write path that triggered the invalidation.
// The Del must therefore see a bounded context even when handed an unbounded one.
func TestBustSubs_AppliesTheTierDeadline(t *testing.T) {
	fv := newFakeValkey()
	var gotDeadline bool
	fv.onDel = func(ctx context.Context) {
		_, gotDeadline = ctx.Deadline()
	}

	BustSubs(context.Background(), fv, "room1", []string{"alice"})

	assert.True(t, gotDeadline, "Del must run under the tier's bounded context")
}

func TestBustSubs_ValkeyErrorSwallowed(t *testing.T) {
	fv := newFakeValkey()
	fv.delErr = errors.New("valkey down")
	assert.NotPanics(t, func() {
		BustSubs(context.Background(), fv, "room1", []string{"alice", "bob"})
	})
	assert.Equal(t, 1, fv.delHits, "the Del must have been attempted, not skipped")
}

// --- BustSubWithTimeout / BustSubsWithTimeout (shared timeout-wrapping) tests ---
//
// These exist so every write-site's hand-rolled "bustSub" helper (room-worker,
// room-service, inbox-worker, bot-room-service) can delegate its
// context.WithTimeout + delegate boilerplate here instead of duplicating it.

// --- Tier tests ---

// The tier owns the breaker-guarded loader and the degraded predicate, so a
// service wires it with one call instead of hand-assembling both. A warm entry
// must resolve without touching the loader at all.
func TestTier_Resolve_ServesFromL2WithoutLoading(t *testing.T) {
	fv := newFakeValkey()
	loads := 0
	tier := NewTierWithLoader(fv, time.Hour, &spyRecorder{},
		func(context.Context, string, string) (SubAuth, bool, error) {
			loads++
			return SubAuth{ID: "u1", Account: "alice"}, true, nil
		})

	_, _, err := tier.Resolve(context.Background(), "room1", "alice") // warms
	require.NoError(t, err)
	got, subscribed, err := tier.Resolve(context.Background(), "room1", "alice")

	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 1, loads, "the second resolve must be served from L2")
}

// A loader failure (Mongo down, or the breaker refusing) propagates and is
// never cached.
func TestTier_Resolve_LoaderErrorPropagates(t *testing.T) {
	fv := newFakeValkey()
	boom := errors.New("breaker open")
	tier := NewTierWithLoader(fv, time.Hour, &spyRecorder{},
		func(context.Context, string, string) (SubAuth, bool, error) {
			return SubAuth{}, false, boom
		})

	_, _, err := tier.Resolve(context.Background(), "room1", "alice")

	require.ErrorIs(t, err, boom)
	assert.Equal(t, 0, fv.setHits, "a failed load must not populate L2")
}

// --- refresh-on-read ---

// An entry younger than the refresh window is a pure read: no loader call, no
// write. That is the steady state, since the process-local L1 in front of this
// tier means most L2 reads are already minutes apart.
func TestTier_FreshEntryServedWithoutLoading(t *testing.T) {
	fv, clock := newFakeValkey(), newFakeClock(time.Now())
	loads := 0
	tier := newClockedTier(fv, time.Hour, clock, func(context.Context, string, string) (SubAuth, bool, error) {
		loads++
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	})

	_, _, err := tier.Resolve(context.Background(), "room1", "alice") // warms
	require.NoError(t, err)
	clock.Advance(withinRefresh)
	_, subscribed, err := tier.Resolve(context.Background(), "room1", "alice")

	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, 1, loads, "a fresh entry must not re-resolve")
	assert.Equal(t, 1, fv.setHits, "a fresh entry must not be rewritten")
}

// Past the refresh window the entry is re-resolved, which is the only way a pod
// whose reads are all L2 hits ever observes the source of truth's health.
func TestTier_StaleEntryRefreshes(t *testing.T) {
	fv, clock := newFakeValkey(), newFakeClock(time.Now())
	roles := []model.Role{model.RoleMember}
	tier := newClockedTier(fv, time.Hour, clock, func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1", Account: "alice", Roles: roles}, true, nil
	})

	_, _, err := tier.Resolve(context.Background(), "room1", "alice") // warms
	require.NoError(t, err)
	roles = []model.Role{model.RoleOwner} // role changed in Mongo, bust was missed
	clock.Advance(pastRefresh)
	got, subscribed, err := tier.Resolve(context.Background(), "room1", "alice")

	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, []model.Role{model.RoleOwner}, got.Roles,
		"a stale entry must pick up the change the missed bust left behind")
}

// This is the property the tier exists for: when the refresh fails, the entry's
// deadline is re-armed and the cached decision keeps being served, so an
// actively read room survives an outage of any length rather than one TTL.
func TestTier_RefreshFailureServesCachedAndRearms(t *testing.T) {
	fv, clock := newFakeValkey(), newFakeClock(time.Now())
	var loadErr error
	tier := newClockedTier(fv, time.Hour, clock, func(context.Context, string, string) (SubAuth, bool, error) {
		if loadErr != nil {
			return SubAuth{}, false, loadErr
		}
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	})

	_, _, err := tier.Resolve(context.Background(), "room1", "alice") // warms
	require.NoError(t, err)
	loadErr = errors.New("mongo down")
	clock.Advance(pastRefresh)
	got, subscribed, err := tier.Resolve(context.Background(), "room1", "alice")

	require.NoError(t, err, "a warm entry must survive a failed refresh")
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 1, fv.expireHits, "the deadline must be re-armed")
	assert.Equal(t, 1, fv.setHits, "a failed refresh must not rewrite the value")
}

// A refresh that comes back "not subscribed" means the subscription is gone and
// a bust was missed. The entry must be deleted rather than left to expire, so
// revoked access dies within the refresh window instead of the full TTL.
func TestTier_RefreshFindingUnsubscribedEvictsTheEntry(t *testing.T) {
	fv, clock := newFakeValkey(), newFakeClock(time.Now())
	subscribed := true
	tier := newClockedTier(fv, time.Hour, clock, func(context.Context, string, string) (SubAuth, bool, error) {
		if !subscribed {
			return SubAuth{}, false, nil
		}
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	})

	_, _, err := tier.Resolve(context.Background(), "room1", "alice") // warms
	require.NoError(t, err)
	subscribed = false // removed from the room; the Del was swallowed
	clock.Advance(pastRefresh)
	_, stillSubscribed, err := tier.Resolve(context.Background(), "room1", "alice")

	require.NoError(t, err)
	assert.False(t, stillSubscribed, "a revoked subscription must stop being served")
	_, present := fv.store[SubKey("room1", "alice")]
	assert.False(t, present, "the stale entry must be evicted, not left to expire")
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
