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
	"github.com/hmchangw/chat/pkg/valkeyfake"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func TestValkeyCache_SetThenGet_RoundTrip(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
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
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "roomABC", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))

	ok := client.Has("room:v4:roomABC:subs")
	assert.True(t, ok, "expected cache key room:v4:roomABC:subs to be set; got keys: %v", client.Keys())
}

func TestValkeyCache_Set_PropagatesTTL(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "r1", nil, 90*time.Second))
	assert.Equal(t, 90*time.Second, mustTTL(t, client, "room:v4:r1:subs"))
}

// A pre-upgrade cache entry (written under the unversioned key by an older
// binary, or under a stale schema version) must be a miss, not a hit that
// silently deserializes into a Member with zero-valued new fields (e.g.
// HomeSiteID == ""), or — worse — with a stale field carrying the wrong
// semantics (v2's siteId was populated from Subscription.SiteID, the ROOM's
// home site, not the member's).
func TestValkeyCache_Get_PreUpgradeKey_IsMiss(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	client.Seed("room:roomABC:subs", `[{"id":"u1","account":"a"}]`, time.Minute) // legacy unversioned key
	// v2 entry: siteId held the room's home site (the bug fixed by the v3 bump) —
	// it must never be served as a member's home site.
	client.Seed("room:v2:roomABC:subs", `[{"id":"u1","account":"a","siteId":"room-site"}]`, time.Minute)
	// v3 entry: a bare []Member array, the shape written before the value became
	// the Entry{Members, CachedAt} envelope. Reusing v3 for the envelope would
	// point two incompatible shapes at one key, so a rolling deploy would have
	// old binaries unmarshalling an object into a slice and new binaries
	// unmarshalling an array into a struct.
	client.Seed("room:v3:roomABC:subs", `[{"id":"u1","account":"a","homeSiteId":"site-a"}]`, time.Minute)
	cache := roomsubcache.NewValkeyCache(client)

	_, err := cache.Get(ctx, "roomABC")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss, "every pre-envelope key generation must miss, not decode")
}

func TestValkeyCache_Get_Miss_ReturnsErrCacheMiss(t *testing.T) {
	ctx := context.Background()
	cache := roomsubcache.NewValkeyCache(valkeyfake.New())

	_, err := cache.Get(ctx, "missing")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

func TestValkeyCache_Get_EmptyListIsCacheHit(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
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
	client := valkeyfake.New()
	client.Seed("room:v4:bad:subs", "{not json", time.Minute)
	cache := roomsubcache.NewValkeyCache(client)

	_, err := cache.Get(ctx, "bad")
	require.Error(t, err)
	assert.NotErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

func TestValkeyCache_Get_TransportError_IsWrapped(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	client.FailGet(errors.New("boom"))
	cache := roomsubcache.NewValkeyCache(client)

	_, err := cache.Get(ctx, "r")
	require.Error(t, err)
	assert.NotErrorIs(t, err, valkeyutil.ErrCacheMiss)
	assert.Contains(t, err.Error(), "boom")
}

func TestValkeyCache_Set_TransportError_IsWrapped(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	client.FailSet(errors.New("boom"))
	cache := roomsubcache.NewValkeyCache(client)

	err := cache.Set(ctx, "r", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestValkeyCache_Invalidate_CallsDelOnExpectedKey(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "r1", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))
	cache.Invalidate(ctx, "r1")

	// Both generations are dropped in one call. These keys carry no hash tag, so
	// "room:v4:r1:subs" and "room:v3:r1:subs" hash to different cluster slots
	// and a single multi-key DEL would be a CROSSSLOT error against a real
	// cluster — the client pipelines one DEL per key so it never is. That is the
	// client's business now, not this package's.
	require.Len(t, client.DelBatches(), 1)
	assert.Equal(t, []string{"room:v4:r1:subs", "room:v3:r1:subs"}, client.DelBatches()[0])

	_, err := cache.Get(ctx, "r1")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

// Invalidation is best-effort: the authoritative write has already committed
// and the TTL reconciles a missed bust, so a transport failure is swallowed
// rather than surfaced. Both generations are still attempted — one failing
// slot must not skip the other.
func TestValkeyCache_Invalidate_TransportError_IsSwallowed(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	client.FailDel(errors.New("boom"))
	cache := roomsubcache.NewValkeyCache(client)

	require.NotPanics(t, func() { cache.Invalidate(ctx, "r1") })
	require.Len(t, client.DelBatches(), 1)
	assert.Len(t, client.DelBatches()[0], 2, "a failure must not drop either generation from the attempt")
}

// The reason this package routes through valkeyutil.BustKeys rather than
// calling Del itself: a bust runs AFTER the authoritative membership write has
// committed, so inheriting the caller's cancellation would let a request that
// finishes at that instant skip the DEL — leaving the room's member list stale
// for the full 90-minute TTL.
func TestValkeyCache_Invalidate_RunsAfterCallerCancellation(t *testing.T) {
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cache.Invalidate(ctx, "r1")

	require.Len(t, client.DelBatches(), 1, "a cancelled caller must not skip the invalidation")
	assert.Len(t, client.DelBatches()[0], 2, "both generations are still cleared")
}

func TestValkeyCache_EmptyRoomID_ReturnsError(t *testing.T) {
	ctx := context.Background()
	cache := roomsubcache.NewValkeyCache(valkeyfake.New())

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
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMaxValueBytes(100))

	// Stash a value larger than the cap directly through the fake — simulates
	// a compromised or misbehaving Valkey writer.
	client.Seed("room:v4:big:subs", strings.Repeat("x", 101), time.Minute)

	_, err := cache.Get(ctx, "big")
	require.Error(t, err)
	assert.NotErrorIs(t, err, valkeyutil.ErrCacheMiss)
	assert.Contains(t, err.Error(), "exceeds")
}

func TestValkeyCache_Get_BlobAtMaxSize_IsAllowed(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	// Use a max large enough to comfortably hold a small valid JSON array.
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMaxValueBytes(1024))

	require.NoError(t, cache.Set(ctx, "ok", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))

	got, err := cache.Get(ctx, "ok")
	require.NoError(t, err)
	assert.Len(t, got.Members, 1)
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

func TestValkeyCache_Slide_ReArmsTTLWithoutRewriting(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)
	members := []roomsubcache.Member{{ID: "u1", Account: "alice"}}
	require.NoError(t, cache.Set(ctx, "r1", members, time.Minute))
	before := client.Value("room:v4:r1:subs")

	cache.Slide(ctx, "r1", time.Hour)

	assert.Equal(t, []string{"room:v4:r1:subs"}, client.ExpiredKeys())
	assert.Equal(t, time.Hour, mustTTL(t, client, "room:v4:r1:subs"), "the deadline must move")
	after := client.Value("room:v4:r1:subs")
	assert.Equal(t, before, after, "the value must not be rewritten")
}

func TestValkeyCache_Slide_DoesNotResurrectAnEvictedEntry(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)

	// EXPIRE on an absent key must be a no-op, not a create — otherwise a room
	// invalidated mid-outage would come back with its stale member list.
	cache.Slide(ctx, "never-written", time.Hour)
	assert.False(t, client.Has("room:v3:never-written:subs"))
}

// An empty roomID is a no-op rather than an error: Slide is best-effort and
// has no caller left to inform.
func TestValkeyCache_Slide_EmptyRoomID_IsANoOp(t *testing.T) {
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)
	cache.Slide(context.Background(), "", time.Hour)
	assert.Empty(t, client.ExpiredKeys(), "an empty roomID must not issue an EXPIRE")
}

// mustTTL reads a key's TTL, failing the test when the key is absent.
func mustTTL(t *testing.T, c *valkeyfake.Client, key string) time.Duration {
	t.Helper()
	ttl, ok := c.TTL(key)
	require.True(t, ok, "expected %s to be present", key)
	return ttl
}
