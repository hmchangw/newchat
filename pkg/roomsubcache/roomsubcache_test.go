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

	got, ok := cache.Get(ctx, "room123")
	require.True(t, ok)
	assert.Equal(t, members, got.V)
}

func TestValkeyCache_Set_UsesExpectedKey(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "roomABC", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))

	ok := client.Has("room:v5:roomABC:subs")
	assert.True(t, ok, "expected cache key room:v5:roomABC:subs to be set; got keys: %v", client.Keys())
}

func TestValkeyCache_Set_PropagatesTTL(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "r1", nil, 90*time.Second))
	assert.Equal(t, 90*time.Second, mustTTL(t, client, "room:v5:r1:subs"))
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
	// v4 entry: the envelope shape, but written before IsSubscribed existed. It
	// would decode cleanly with IsSubscribed silently false for the full TTL,
	// which is exactly what the v5 bump exists to prevent.
	client.Seed("room:v4:roomABC:subs",
		`{"v":[{"id":"u1","account":"a","homeSiteId":"site-a"}],"cachedAt":1700000000000}`, time.Minute)
	cache := roomsubcache.NewValkeyCache(client)

	_, ok := cache.Get(ctx, "roomABC")
	assert.False(t, ok, "every superseded key generation must miss, not decode")
}

func TestValkeyCache_Get_Miss_IsNotServed(t *testing.T) {
	ctx := context.Background()
	cache := roomsubcache.NewValkeyCache(valkeyfake.New())

	_, ok := cache.Get(ctx, "missing")
	assert.False(t, ok)
}

func TestValkeyCache_Get_EmptyListIsCacheHit(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client)

	// Empty list is a valid cached value (negative cache for empty/deleted rooms).
	require.NoError(t, cache.Set(ctx, "empty-room", []roomsubcache.Member{}, time.Minute))

	got, ok := cache.Get(ctx, "empty-room")
	require.True(t, ok)
	assert.NotNil(t, got.V, "empty cache hit must return non-nil slice to distinguish from miss")
	assert.Empty(t, got.V)
}

// Malformed JSON is not served. That it is recorded as an Error rather than a
// Miss — the distinction Get no longer returns — is asserted in metrics_test.go.
func TestValkeyCache_Get_MalformedJSON_IsNotServed(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	client.Seed("room:v5:bad:subs", "{not json", time.Minute)
	cache := roomsubcache.NewValkeyCache(client)

	_, ok := cache.Get(ctx, "bad")
	assert.False(t, ok)
}

// A transport failure is not served, and is recorded as an Error rather than a
// Miss (see metrics_test.go) so a broken Valkey does not read as a cold cache.
func TestValkeyCache_Get_TransportError_IsNotServed(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	client.FailGet(errors.New("boom"))
	cache := roomsubcache.NewValkeyCache(client)

	_, ok := cache.Get(ctx, "r")
	assert.False(t, ok)
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

	// Every live generation is dropped in one call. v4 is included because a
	// rolling v4->v5 deploy still has v4 binaries serving from their own key: if
	// a bust skipped it, those binaries would keep serving a member list for a
	// room whose membership just changed, for a full TTL. These keys carry no
	// hash tag, so they hash to different cluster slots and a single multi-key
	// DEL would be a CROSSSLOT error against a real cluster — the client
	// pipelines one DEL per key so it never is.
	require.Len(t, client.DelBatches(), 1)
	assert.Equal(t,
		[]string{"room:v5:r1:subs", "room:v4:r1:subs", "room:v3:r1:subs"},
		client.DelBatches()[0])

	_, ok := cache.Get(ctx, "r1")
	assert.False(t, ok)
}

// Invalidation is best-effort: the authoritative write has already committed
// and the TTL reconciles a missed bust, so a transport failure is swallowed
// rather than surfaced. Every generation is still attempted — one failing slot
// must not skip the others.
func TestValkeyCache_Invalidate_TransportError_IsSwallowed(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	client.FailDel(errors.New("boom"))
	cache := roomsubcache.NewValkeyCache(client)

	require.NotPanics(t, func() { cache.Invalidate(ctx, "r1") })
	require.Len(t, client.DelBatches(), 1)
	assert.Len(t, client.DelBatches()[0], 3, "a failure must not drop any generation from the attempt")
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
	assert.Len(t, client.DelBatches()[0], 3, "every generation is still cleared")
}

func TestValkeyCache_EmptyRoomID_ReturnsError(t *testing.T) {
	ctx := context.Background()
	cache := roomsubcache.NewValkeyCache(valkeyfake.New())

	tests := []struct {
		name string
		call func() error
	}{
		{"Set", func() error { return cache.Set(ctx, "", nil, time.Minute) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "empty roomID")
		})
	}

	// Get has no error to return, so an empty roomID is simply not a hit.
	_, ok := cache.Get(ctx, "")
	assert.False(t, ok)
}

func TestValkeyCache_Get_OversizedBlob_IsNotServed(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMaxValueBytes(100))

	// Stash a value larger than the cap directly through the fake — simulates
	// a compromised or misbehaving Valkey writer.
	client.Seed("room:v5:big:subs", strings.Repeat("x", 101), time.Minute)

	_, ok := cache.Get(ctx, "big")
	assert.False(t, ok, "a blob past the cap must never be decoded, let alone served")
}

func TestValkeyCache_Get_BlobAtMaxSize_IsAllowed(t *testing.T) {
	ctx := context.Background()
	client := valkeyfake.New()
	// Use a max large enough to comfortably hold a small valid JSON array.
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMaxValueBytes(1024))

	require.NoError(t, cache.Set(ctx, "ok", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))

	got, ok := cache.Get(ctx, "ok")
	require.True(t, ok)
	assert.Len(t, got.V, 1)
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
		IsSubscribed:       true,
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

// IsSubscribed is the per-user sidebar/subscription flag denormalised onto the
// subscription. It is stable — flipped by an explicit subscribe/unsubscribe,
// never rewritten per message — so it is safe to serve from a TTL'd entry.
func TestMember_IsSubscribed_RoundTrip(t *testing.T) {
	in := roomsubcache.Member{ID: "u1", Account: "alice", IsSubscribed: true}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	require.Contains(t, string(data), `"isSubscribed":true`)

	var out roomsubcache.Member
	require.NoError(t, json.Unmarshal(data, &out))
	assert.True(t, out.IsSubscribed)

	// A v4-shaped blob predates the field; it must decode to the zero value
	// rather than anything invented. The v5 key bump is what stops such a blob
	// reaching this decoder in the first place.
	var fromV4 roomsubcache.Member
	require.NoError(t, json.Unmarshal([]byte(`{"id":"u1","account":"alice"}`), &fromV4))
	assert.False(t, fromV4.IsSubscribed)
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
	before := client.Value("room:v5:r1:subs")

	cache.Slide(ctx, "r1", time.Hour)

	assert.Equal(t, []string{"room:v5:r1:subs"}, client.ExpiredKeys())
	assert.Equal(t, time.Hour, mustTTL(t, client, "room:v5:r1:subs"), "the deadline must move")
	after := client.Value("room:v5:r1:subs")
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
