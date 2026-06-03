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

func (f *fakeClient) Close() error { return nil }

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
	assert.Equal(t, members, got)
}

func TestValkeyCache_Set_UsesExpectedKey(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "roomABC", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))

	_, ok := client.store["room:roomABC:subs"]
	assert.True(t, ok, "expected cache key room:roomABC:subs to be set; got keys: %v", keysOf(client.store))
}

func TestValkeyCache_Set_PropagatesTTL(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	require.NoError(t, cache.Set(ctx, "r1", nil, 90*time.Second))
	assert.Equal(t, 90*time.Second, client.ttls["room:r1:subs"])
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
	assert.NotNil(t, got, "empty cache hit must return non-nil slice to distinguish from miss")
	assert.Empty(t, got)
}

func TestValkeyCache_Get_MalformedJSON_IsNotMiss(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.store["room:bad:subs"] = "{not json"
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
	require.NoError(t, cache.Invalidate(ctx, "r1"))

	require.Len(t, client.delCalls, 1)
	assert.Equal(t, []string{"room:r1:subs"}, client.delCalls[0])

	_, err := cache.Get(ctx, "r1")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

func TestValkeyCache_Invalidate_TransportError_IsWrapped(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.delErr = errors.New("boom")
	cache := roomsubcache.NewValkeyCache(client)

	err := cache.Invalidate(ctx, "r1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
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
		{"Invalidate", func() error { return cache.Invalidate(ctx, "") }},
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
	client.store["room:big:subs"] = strings.Repeat("x", 101)

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
	assert.Len(t, got, 1)
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
	}
	data, err := json.Marshal(in)
	require.NoError(t, err)

	var out roomsubcache.Member
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
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
