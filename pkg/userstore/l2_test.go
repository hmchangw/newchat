package userstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// fakeValkey is an in-memory stand-in for the Valkey client. Expiries are
// tracked but not enforced by a clock — tests drive staleness through the
// L2Store's injected now().
type fakeValkey struct {
	data     map[string]string
	ttls     map[string]time.Duration
	getErr   error
	setErr   error
	gets     int
	mgets    int
	mgetKeys int
	expires  int
}

func newFakeValkey() *fakeValkey {
	return &fakeValkey{
		data: map[string]string{},
		ttls: map[string]time.Duration{},
	}
}

func (f *fakeValkey) Get(_ context.Context, key string) (string, error) {
	f.gets++
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.data[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}

func (f *fakeValkey) Set(_ context.Context, key, value string, ttl time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	f.ttls[key] = ttl
	return nil
}

func (f *fakeValkey) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(f.data, k)
		delete(f.ttls, k)
	}
	return nil
}

func (f *fakeValkey) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	f.expires++
	if _, ok := f.data[key]; !ok {
		return false, nil
	}
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeValkey) Close() error { return nil }
func (f *fakeValkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (f *fakeValkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 1, nil }

var alice = model.User{ID: "u-alice", Account: "alice", EngName: "Alice", SiteID: "site-a"}

func TestL2Store_MissPopulatesBothKeys(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)

	got, err := s.FindUserByAccount(context.Background(), "alice")
	require.NoError(t, err)
	assert.Equal(t, &alice, got)
	require.Equal(t, 1, inner.calls)

	// Both key spaces must be populated from one fetch, so a later by-ID lookup
	// is a hit rather than another Mongo round-trip.
	assert.Contains(t, vk.data, accountKey("alice"))
	assert.Contains(t, vk.data, idKey("u-alice"))
}

func TestL2Store_HitSkipsTheStore(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)
	ctx := context.Background()

	_, err := s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err)
	require.Equal(t, 1, inner.calls)

	got, err := s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err)
	assert.Equal(t, &alice, got)
	assert.Equal(t, 1, inner.calls, "a warm entry must not reach the store")
}

func TestL2Store_StaleEntrySurvivesStoreOutageViaTTLSlide(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	now := time.Now()
	s := newL2StoreWithClock(inner, vk, time.Minute, func() time.Time { return now })
	ctx := context.Background()

	_, err := s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err)

	// Age the entry past the refresh window, then break the store. The refresh
	// must fail open: keep serving the cached user and re-arm its deadline so it
	// outlives an outage longer than the TTL.
	now = now.Add(59 * time.Second)
	inner.err = errors.New("mongo down")
	inner.user = nil

	got, err := s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err, "a warm entry must survive the source being down")
	assert.Equal(t, &alice, got)
	assert.Positive(t, vk.expires, "the deadline must be re-armed, not left to expire mid-outage")
}

// hookStore fails every call, running onCall first — used to evict the entry
// between the L2 read and the TTL slide, which is exactly the window a real
// bust can land in.
type hookStore struct {
	UserStore
	onCall func()
}

func (h *hookStore) FindUserByID(context.Context, string) (*model.User, error) {
	h.onCall()
	return nil, errors.New("mongo down")
}

func TestL2Store_SlideUsesExpireSoItCannotResurrect(t *testing.T) {
	vk := newFakeValkey()
	now := time.Now()
	ctx := context.Background()

	// Warm the entry through a healthy store first.
	warm := newL2StoreWithClock(&countingStore{user: &alice}, vk, time.Minute, func() time.Time { return now })
	_, err := warm.FindUserByID(ctx, "u-alice")
	require.NoError(t, err)
	require.Contains(t, vk.data, idKey("u-alice"))

	// Now age it past the refresh window and fail the refresh, with the entry
	// evicted mid-refresh. EXPIRE must no-op on the absent key; a SET-based
	// slide would write the stale user back and un-delete a busted record.
	evicting := &hookStore{onCall: func() {
		_ = vk.Del(ctx, idKey("u-alice"), accountKey("alice"))
	}}
	s := newL2StoreWithClock(evicting, vk, time.Minute, func() time.Time { return now })
	now = now.Add(59 * time.Second)

	got, err := s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err, "the caller still gets the record it read")
	assert.Equal(t, &alice, got)
	assert.Positive(t, vk.expires, "the slide must have been attempted")
	assert.NotContains(t, vk.data, idKey("u-alice"), "a slide must never resurrect an evicted entry")
}

func TestL2Store_ColdMissDuringOutageStillFails(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{err: errors.New("mongo down")}
	s := NewL2Store(inner, vk, time.Minute)

	_, err := s.FindUserByID(context.Background(), "nobody")
	require.Error(t, err, "an uncached user cannot be invented")
}

func TestL2Store_NotFoundIsNotCached(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{err: fmt.Errorf("find user u1: %w", ErrUserNotFound)}
	s := NewL2Store(inner, vk, time.Minute)

	_, err := s.FindUserByID(context.Background(), "u1")
	require.ErrorIs(t, err, ErrUserNotFound)
	assert.Empty(t, vk.data, "absence must not be cached: it would outlive the user being created")
}

func TestL2Store_FindUsersByAccounts_ReturnsPartialHitsOnStoreError(t *testing.T) {
	vk := newFakeValkey()
	bob := model.User{ID: "u-bob", Account: "bob"}
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)
	ctx := context.Background()

	// Warm alice only.
	_, err := s.FindUserByAccount(ctx, "alice")
	require.NoError(t, err)

	inner.err = errors.New("mongo down")
	inner.user = &bob

	got, err := s.FindUsersByAccounts(ctx, []string{"alice", "bob"})
	require.Error(t, err, "the caller must learn the lookup degraded")
	require.Len(t, got, 1, "the cached half must still come back")
	assert.Equal(t, "alice", got[0].Account)
}

func TestL2Store_FindUsersByAccounts_OnlyFetchesMisses(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)
	ctx := context.Background()

	_, err := s.FindUserByAccount(ctx, "alice")
	require.NoError(t, err)
	inner.calls = 0
	inner.lastAccounts = nil

	_, err = s.FindUsersByAccounts(ctx, []string{"alice", "bob"})
	require.NoError(t, err)
	assert.Equal(t, []string{"bob"}, inner.lastAccounts, "cached accounts must not be re-fetched")
}

func TestL2Store_ValkeyDownFallsThroughToStore(t *testing.T) {
	vk := newFakeValkey()
	vk.getErr = errors.New("valkey down")
	vk.setErr = errors.New("valkey down")
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)

	got, err := s.FindUserByAccount(context.Background(), "alice")
	require.NoError(t, err, "a broken cache must never fail a request the store can serve")
	assert.Equal(t, &alice, got)
}

func TestL2Store_NilClientIsPassThrough(t *testing.T) {
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, nil, time.Minute)

	got, err := s.FindUserByID(context.Background(), "u-alice")
	require.NoError(t, err)
	assert.Equal(t, &alice, got)
	assert.Equal(t, 1, inner.calls)
}

func TestL2Store_BustRemovesBothKeys(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)
	ctx := context.Background()

	_, err := s.FindUserByAccount(ctx, "alice")
	require.NoError(t, err)
	require.Len(t, vk.data, 2)

	Bust(ctx, vk, "u-alice", "alice")
	assert.Empty(t, vk.data, "both key spaces must be dropped or one serves a stale user")
}

func TestL2Store_StaleEntryEvictedWhenUserGenuinelyGone(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	now := time.Now()
	s := newL2StoreWithClock(inner, vk, time.Minute, func() time.Time { return now })
	ctx := context.Background()

	_, err := s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err)
	require.Len(t, vk.data, 2)

	// ErrUserNotFound is a healthy answer, not an outage: the record is really
	// gone, so it must be evicted now rather than served until the TTL lapses.
	now = now.Add(59 * time.Second)
	inner.err = fmt.Errorf("find user u-alice: %w", ErrUserNotFound)
	inner.user = nil

	_, err = s.FindUserByID(ctx, "u-alice")
	require.ErrorIs(t, err, ErrUserNotFound)
	assert.Empty(t, vk.data, "a deleted user must not linger under either key")
}

func TestL2Store_SuccessfulRefreshRewritesAndResetsTheWindow(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	now := time.Now()
	s := newL2StoreWithClock(inner, vk, time.Minute, func() time.Time { return now })
	ctx := context.Background()

	_, err := s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err)

	// Past the refresh window: re-validate and pick up the change a missed bust
	// would otherwise hide until the TTL.
	now = now.Add(59 * time.Second)
	renamed := model.User{ID: "u-alice", Account: "alice", EngName: "Alice Renamed", SiteID: "site-a"}
	inner.user = &renamed

	got, err := s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err)
	assert.Equal(t, "Alice Renamed", got.EngName)
	require.Equal(t, 2, inner.calls)

	// The rewrite stamps a fresh CachedAt, so the next read is inside the window
	// again and serves without another round-trip.
	got, err = s.FindUserByID(ctx, "u-alice")
	require.NoError(t, err)
	assert.Equal(t, "Alice Renamed", got.EngName)
	assert.Equal(t, 2, inner.calls, "a just-refreshed entry must not re-validate again")
}

// MGet reads the map directly rather than looping Get, so the gets counter
// keeps measuring only genuine single-key reads and a test can tell the two
// paths apart.
func (f *fakeValkey) MGet(_ context.Context, keys []string) (map[string]string, error) {
	f.mgets++
	f.mgetKeys += len(keys)
	if f.getErr != nil {
		return nil, f.getErr
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		if v, ok := f.data[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func TestL2Store_FindUsersByAccounts_UsesOneBatchRead(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)
	ctx := context.Background()

	accounts := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		accounts = append(accounts, fmt.Sprintf("user-%d", i))
	}
	_, err := s.FindUsersByAccounts(ctx, accounts)
	require.NoError(t, err)

	// Mention count is driven by message content, so a per-key read would put a
	// serialized round-trip on the persist path for every account named.
	assert.Equal(t, 1, vk.mgets, "the whole account set must be fetched in one batch")
	assert.Equal(t, 0, vk.gets, "no per-account Get may remain on this path")
	assert.Equal(t, 50, vk.mgetKeys)
}

func TestL2Store_FindUsersByAccounts_DedupesKeysBeforeFetching(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)

	_, err := s.FindUsersByAccounts(context.Background(), []string{"a", "b", "a", "b", "a"})
	require.NoError(t, err)
	assert.Equal(t, 2, vk.mgetKeys, "a repeated mention must not be fetched twice")
}

func TestL2Store_FindUsersByAccounts_BatchReadFailureFallsThroughToStore(t *testing.T) {
	vk := newFakeValkey()
	vk.getErr = errors.New("valkey down")
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)

	got, err := s.FindUsersByAccounts(context.Background(), []string{"alice"})
	require.NoError(t, err, "a broken cache must not fail a lookup the store can serve")
	require.Len(t, got, 1)
	assert.Equal(t, "alice", got[0].Account)
}

func TestL2Store_FindUsersByAccounts_MixedHitsPreserveOrderAndCompleteness(t *testing.T) {
	vk := newFakeValkey()
	inner := &countingStore{user: &alice}
	s := NewL2Store(inner, vk, time.Minute)
	ctx := context.Background()

	_, err := s.FindUserByAccount(ctx, "alice")
	require.NoError(t, err)

	bob := model.User{ID: "u-bob", Account: "bob"}
	inner.user = &bob
	got, err := s.FindUsersByAccounts(ctx, []string{"alice", "bob"})
	require.NoError(t, err)

	accounts := make([]string, 0, len(got))
	for _, u := range got {
		accounts = append(accounts, u.Account)
	}
	assert.ElementsMatch(t, []string{"alice", "bob"}, accounts,
		"a cached account and a fetched one must both come back")
	assert.Equal(t, []string{"bob"}, inner.lastAccounts, "only the miss goes to the store")
}

func TestL2Store_FindUsersByAccounts_RejectsUnusableEntries(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "malformed json", raw: "{not json"},
		{name: "json null", raw: "null"},
		{name: "envelope with no user", raw: `{"cachedAt":123}`},
		{name: "user with empty id", raw: `{"user":{"account":"alice"},"cachedAt":123}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vk := newFakeValkey()
			vk.data[accountKey("alice")] = tc.raw
			inner := &countingStore{user: &alice}
			s := NewL2Store(inner, vk, time.Minute)

			got, err := s.FindUsersByAccounts(context.Background(), []string{"alice"})
			require.NoError(t, err)

			// An unusable entry must degrade to a miss and be re-fetched, never be
			// served as a half-populated user.
			require.Len(t, got, 1)
			assert.Equal(t, "u-alice", got[0].ID)
			assert.Equal(t, []string{"alice"}, inner.lastAccounts, "the bad entry must fall through to the store")
		})
	}
}
