package roomsubcache_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type fakeClient struct {
	store    map[string]string
	ttls     map[string]time.Duration
	setErr   error
	getErr   error
	delErr   error
	delCalls [][]string
	expired  []string
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		store: make(map[string]string),
		ttls:  make(map[string]time.Duration),
	}
}

func (f *fakeClient) Get(_ context.Context, key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.store[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}

func (f *fakeClient) Set(_ context.Context, key, value string, ttl time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	f.store[key] = value
	f.ttls[key] = ttl
	return nil
}

func (f *fakeClient) Del(_ context.Context, keys ...string) error {
	f.delCalls = append(f.delCalls, keys)
	if f.delErr != nil {
		return f.delErr
	}
	for _, k := range keys {
		delete(f.store, k)
		delete(f.ttls, k)
	}
	return nil
}
func (f *fakeClient) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	f.expired = append(f.expired, key)
	if _, ok := f.store[key]; !ok {
		return false, nil // EXPIRE no-ops on an absent key
	}
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeClient) Close() error { return nil }

// SetNX / IncrEx satisfy valkeyutil.Client but are unused here; panic on any call.
func (f *fakeClient) SetNX(_ context.Context, _, _ string, _ time.Duration) (bool, error) {
	panic("roomsubcache.fakeClient.SetNX not implemented")
}

func (f *fakeClient) IncrEx(_ context.Context, _ string, _ time.Duration) (int64, error) {
	panic("roomsubcache.fakeClient.IncrEx not implemented")
}

func TestValkeyCache_SetThenGet_RoundTrip(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	members := []roomsubcache.Member{
		{ID: "u1", Account: "alice"},
		{ID: "u2", Account: "bob"},
	}

	require.NoError(t, cache.Set(ctx, "room123", members, time.Minute))

	got, err := cache.Get(ctx, "room123")
	require.NoError(t, err)
	assert.Equal(t, members, got.Members)
}

func TestValkeyCache_Set_UsesExpectedKey(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "roomABC", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))

	_, ok := client.store["room:v4:roomABC:subs"]
	assert.True(t, ok, "expected cache key room:v4:roomABC:subs to be set; got keys: %v", keysOf(client.store))
}

func TestValkeyCache_Set_PropagatesTTL(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "r1", nil, 90*time.Second))
	assert.Equal(t, 90*time.Second, client.ttls["room:v4:r1:subs"])
}

// A pre-upgrade cache entry (written under the unversioned key by an older
// binary, or under a stale schema version) must be a miss, not a hit that
// silently deserializes into a Member with zero-valued new fields (e.g.
// HomeSiteID == ""), or — worse — with a stale field carrying the wrong
// semantics (v2's siteId was populated from Subscription.SiteID, the ROOM's
// home site, not the member's).
func TestValkeyCache_Get_PreUpgradeKey_IsMiss(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.store["room:roomABC:subs"] = `[{"id":"u1","account":"a"}]` // legacy unversioned key
	// v2 entry: siteId held the room's home site (the bug fixed by the v3 bump) —
	// it must never be served as a member's home site.
	client.store["room:v2:roomABC:subs"] = `[{"id":"u1","account":"a","siteId":"room-site"}]`
	// v3 entry: a bare []Member array, the shape written before the value became
	// the Entry{Members, CachedAt} envelope. Reusing v3 for the envelope would
	// point two incompatible shapes at one key, so a rolling deploy would have
	// old binaries unmarshalling an object into a slice and new binaries
	// unmarshalling an array into a struct.
	client.store["room:v3:roomABC:subs"] = `[{"id":"u1","account":"a","homeSiteId":"site-a"}]`
	cache := roomsubcache.NewValkeyCache(client)

	_, err := cache.Get(ctx, "roomABC")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss, "every pre-envelope key generation must miss, not decode")
}

func TestValkeyCache_Get_Miss_ReturnsErrCacheMiss(t *testing.T) {
	ctx := context.Background()
	cache := roomsubcache.NewValkeyCache(newFakeClient())

	_, err := cache.Get(ctx, "missing")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

func TestValkeyCache_Get_EmptyListIsCacheHit(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	// Empty list is a valid cached value (negative cache for empty/deleted rooms).
	require.NoError(t, cache.Set(ctx, "empty-room", []roomsubcache.Member{}, time.Minute))

	got, err := cache.Get(ctx, "empty-room")
	require.NoError(t, err)
	assert.NotNil(t, got.Members, "empty cache hit must return non-nil slice to distinguish from miss")
	assert.Empty(t, got.Members)
}

func TestValkeyCache_Get_MalformedJSON_IsNotMiss(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.store["room:v4:bad:subs"] = "{not json"
	cache := roomsubcache.NewValkeyCache(client)

	_, err := cache.Get(ctx, "bad")
	require.Error(t, err)
	assert.NotErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

func TestValkeyCache_Get_TransportError_IsWrapped(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.getErr = errors.New("boom")
	cache := roomsubcache.NewValkeyCache(client)

	_, err := cache.Get(ctx, "r")
	require.Error(t, err)
	assert.NotErrorIs(t, err, valkeyutil.ErrCacheMiss)
	assert.Contains(t, err.Error(), "boom")
}

func TestValkeyCache_Set_TransportError_IsWrapped(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.setErr = errors.New("boom")
	cache := roomsubcache.NewValkeyCache(client)

	err := cache.Set(ctx, "r", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestValkeyCache_Invalidate_CallsDelOnExpectedKey(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "r1", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))
	cache.Invalidate(ctx, "r1")

	// Both generations are dropped, but in SEPARATE Del calls. These keys carry
	// no hash tag, so "room:v4:r1:subs" and "room:v3:r1:subs" hash to different
	// cluster slots and a single multi-key DEL is a CROSSSLOT error against a
	// real cluster — which would fail every invalidation, not just the legacy
	// half. The split is now enforced by valkeyutil.BustKeys, which groups the
	// keys it is given by hash tag, rather than by this package hand-obeying it.
	require.Len(t, client.delCalls, 2, "one Del per key — a batched DEL is CROSSSLOT in cluster mode")
	assert.Equal(t, []string{"room:v4:r1:subs"}, client.delCalls[0])
	assert.Equal(t, []string{"room:v3:r1:subs"}, client.delCalls[1])

	_, err := cache.Get(ctx, "r1")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

// Invalidation is best-effort: the authoritative write has already committed
// and the TTL reconciles a missed bust, so a transport failure is swallowed
// rather than surfaced. Both generations are still attempted — one failing
// slot must not skip the other.
func TestValkeyCache_Invalidate_TransportError_IsSwallowed(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.delErr = errors.New("boom")
	cache := roomsubcache.NewValkeyCache(client)

	require.NotPanics(t, func() { cache.Invalidate(ctx, "r1") })
	assert.Len(t, client.delCalls, 2, "a failure on one generation must not skip the other")
}

// The reason this package routes through valkeyutil.BustKeys rather than
// calling Del itself: a bust runs AFTER the authoritative membership write has
// committed, so inheriting the caller's cancellation would let a request that
// finishes at that instant skip the DEL — leaving the room's member list stale
// for the full 90-minute TTL.
func TestValkeyCache_Invalidate_RunsAfterCallerCancellation(t *testing.T) {
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cache.Invalidate(ctx, "r1")

	assert.Len(t, client.delCalls, 2, "a cancelled caller must not skip the invalidation")
}

func TestValkeyCache_EmptyRoomID_ReturnsError(t *testing.T) {
	ctx := context.Background()
	cache := roomsubcache.NewValkeyCache(newFakeClient())

	tests := []struct {
		name string
		call func() error
	}{
		{"Get", func() error { _, err := cache.Get(ctx, ""); return err }},
		{"Set", func() error { return cache.Set(ctx, "", nil, time.Minute) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "empty roomID")
		})
	}
}

func TestValkeyCache_Get_OversizedBlob_ReturnsError(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMaxValueBytes(100))

	// Stash a value larger than the cap directly through the fake — simulates
	// a compromised or misbehaving Valkey writer.
	client.store["room:v4:big:subs"] = strings.Repeat("x", 101)

	_, err := cache.Get(ctx, "big")
	require.Error(t, err)
	assert.NotErrorIs(t, err, valkeyutil.ErrCacheMiss)
	assert.Contains(t, err.Error(), "exceeds")
}

func TestValkeyCache_Get_BlobAtMaxSize_IsAllowed(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	// Use a max large enough to comfortably hold a small valid JSON array.
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMaxValueBytes(1024))

	require.NoError(t, cache.Set(ctx, "ok", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))

	got, err := cache.Get(ctx, "ok")
	require.NoError(t, err)
	assert.Len(t, got.Members, 1)
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestMember_JSONRoundTrip_NewFields(t *testing.T) {
	hss := int64(1700000000000)
	in := roomsubcache.Member{
		ID:                 "u1",
		Account:            "alice",
		RoomType:           model.RoomTypeChannel,
		IsBot:              true,
		Muted:              true,
		HistorySharedSince: &hss,
		HomeSiteID:         "site-a",
	}
	data, err := json.Marshal(in)
	require.NoError(t, err)

	var out roomsubcache.Member
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}

// HomeSiteID feeds notification-worker's per-recipient badge-count grouping
// and must round-trip through the cache codec like any other field. The wire
// tag is homeSiteId — distinct from v2's siteId, which carried the room's home
// site and must never decode into this field.
func TestMember_HomeSiteID_RoundTrip(t *testing.T) {
	in := roomsubcache.Member{ID: "u1", Account: "alice", HomeSiteID: "site-b"}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	require.Contains(t, string(data), `"homeSiteId":"site-b"`)

	var out roomsubcache.Member
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, "site-b", out.HomeSiteID)

	// A v2-shaped blob (room-site siteId) must not populate HomeSiteID.
	var fromV2 roomsubcache.Member
	require.NoError(t, json.Unmarshal([]byte(`{"id":"u1","account":"alice","siteId":"room-site"}`), &fromV2))
	assert.Empty(t, fromV2.HomeSiteID, "legacy siteId must not decode into HomeSiteID")
}

func TestMember_RoomType_RoundTrip(t *testing.T) {
	for _, rt := range []model.RoomType{
		model.RoomTypeChannel,
		model.RoomTypeDM,
		model.RoomTypeBotDM,
		model.RoomTypeDiscussion,
	} {
		m := roomsubcache.Member{ID: "u1", Account: "alice", RoomType: rt}
		data, err := json.Marshal(m)
		require.NoError(t, err)
		var out roomsubcache.Member
		require.NoError(t, json.Unmarshal(data, &out))
		assert.Equal(t, rt, out.RoomType, "RoomType %q should round-trip", rt)
	}
}

func TestMember_OmitemptyOnZeroValues(t *testing.T) {
	in := roomsubcache.Member{ID: "u1", Account: "alice"}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	got := string(data)

	// Only id + account on the wire; no zero-valued booleans / strings / pointers.
	assert.JSONEq(t, `{"id":"u1","account":"alice"}`, got)
}

// MGet loops the fake's own Get so it cannot drift from single-key behaviour.
func (f *fakeClient) MGet(ctx context.Context, keys []string) (map[string]string, error) {
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

func TestValkeyCache_Slide_ReArmsTTLWithoutRewriting(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)
	members := []roomsubcache.Member{{ID: "u1", Account: "alice"}}
	require.NoError(t, cache.Set(ctx, "r1", members, time.Minute))
	before := client.store["room:v4:r1:subs"]

	cache.Slide(ctx, "r1", time.Hour)

	assert.Equal(t, []string{"room:v4:r1:subs"}, client.expired)
	assert.Equal(t, time.Hour, client.ttls["room:v4:r1:subs"], "the deadline must move")
	assert.Equal(t, before, client.store["room:v4:r1:subs"], "the value must not be rewritten")
}

func TestValkeyCache_Slide_DoesNotResurrectAnEvictedEntry(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	// EXPIRE on an absent key must be a no-op, not a create — otherwise a room
	// invalidated mid-outage would come back with its stale member list.
	cache.Slide(ctx, "never-written", time.Hour)
	assert.NotContains(t, client.store, "room:v3:never-written:subs")
}

// An empty roomID is a no-op rather than an error: Slide is best-effort and
// has no caller left to inform.
func TestValkeyCache_Slide_EmptyRoomID_IsANoOp(t *testing.T) {
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)
	cache.Slide(context.Background(), "", time.Hour)
	assert.Empty(t, client.expired, "an empty roomID must not issue an EXPIRE")
}
