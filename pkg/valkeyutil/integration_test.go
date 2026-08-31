//go:build integration

package valkeyutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// setupClusterClient starts a cluster-mode Valkey container via the shared
// testutil helper and returns a Client backed by clusterClient. ConnectCluster
// itself cannot be used here because its auto-discovery follows CLUSTER SLOTS,
// which returns the container-internal 127.0.0.1:6379 — unreachable from the
// host. testutil.StartValkeyCluster applies the ClusterSlots override; we then
// wrap the resulting *redis.ClusterClient directly (same-package access).
// ConnectCluster's error-wrapping path is covered by TestConnectCluster_ErrorPath.
func setupClusterClient(t *testing.T) Client {
	t.Helper()
	t.Cleanup(func() { testutil.FlushValkey(t) })
	return &clusterClient{c: testutil.SharedValkeyCluster(t)}
}

func TestClusterRedisClient_Integration_GetSetDel(t *testing.T) {
	client := setupClusterClient(t)
	ctx := context.Background()

	require.NoError(t, client.Set(ctx, "k1", "hello", time.Hour))

	val, err := client.Get(ctx, "k1")
	require.NoError(t, err)
	assert.Equal(t, "hello", val)

	require.NoError(t, client.Del(ctx, "k1"))

	_, err = client.Get(ctx, "k1")
	assert.ErrorIs(t, err, ErrCacheMiss)
}

func TestClusterRedisClient_Integration_CacheMiss(t *testing.T) {
	client := setupClusterClient(t)
	ctx := context.Background()

	_, err := client.Get(ctx, "no-such-key")
	assert.ErrorIs(t, err, ErrCacheMiss)
}

func TestClusterRedisClient_Integration_DelEmpty(t *testing.T) {
	client := setupClusterClient(t)
	ctx := context.Background()

	require.NoError(t, client.Del(ctx))
}

// TestClusterRedisClient_Integration_SetNX: first caller acquires; second is refused; value preserved.
func TestClusterRedisClient_Integration_SetNX(t *testing.T) {
	client := setupClusterClient(t)
	ctx := context.Background()

	acquired, err := client.SetNX(ctx, "sentinel", "first", time.Hour)
	require.NoError(t, err)
	assert.True(t, acquired, "unset key must be acquired")

	acquired, err = client.SetNX(ctx, "sentinel", "second", time.Hour)
	require.NoError(t, err)
	assert.False(t, acquired, "already-set key must be refused")

	got, err := client.Get(ctx, "sentinel")
	require.NoError(t, err)
	assert.Equal(t, "first", got, "existing value must be preserved on NX refusal")
}

// TestClusterRedisClient_Integration_IncrEx: fixed-window recipe against real Valkey.
func TestClusterRedisClient_Integration_IncrEx(t *testing.T) {
	client := setupClusterClient(t)
	ctx := context.Background()

	n, err := client.IncrEx(ctx, "rl:alice", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	n, err = client.IncrEx(ctx, "rl:alice", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	n, err = client.IncrEx(ctx, "rl:alice", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
}

// TestClusterRedisClient_Integration_MGetCrossSlot is the point of MGet: these
// keys deliberately hash to different slots, which a plain MGET would reject
// with CROSSSLOT. The pipelined implementation must fetch them anyway, in one
// call, and report absent keys by omission rather than by error.
func TestClusterRedisClient_Integration_MGetCrossSlot(t *testing.T) {
	client := setupClusterClient(t)
	ctx := context.Background()

	keys := make([]string, 0, 32)
	want := make(map[string]string, 32)
	for i := 0; i < 32; i++ {
		k := fmt.Sprintf("user:acct:mget-%d", i)
		keys = append(keys, k)
		if i%2 == 0 { // leave the odd keys absent
			v := fmt.Sprintf("value-%d", i)
			require.NoError(t, client.Set(ctx, k, v, time.Hour))
			want[k] = v
		}
	}
	keys = append(keys, "user:acct:never-written")

	got, err := client.MGet(ctx, keys)
	require.NoError(t, err)
	assert.Equal(t, want, got, "present keys come back; absent ones are simply omitted")
}

// The Del counterpart of MGetCrossSlot, and the reason invalidation callers can
// hand over any key set: these keys hash to different slots, which a plain
// multi-key DEL rejects with CROSSSLOT — clearing none of them rather than some.
func TestClusterRedisClient_Integration_DelCrossSlot(t *testing.T) {
	client := setupClusterClient(t)
	ctx := context.Background()

	keys := make([]string, 0, 32)
	for i := 0; i < 32; i++ {
		k := fmt.Sprintf("user:acct:del-%d", i)
		keys = append(keys, k)
		require.NoError(t, client.Set(ctx, k, "v", time.Hour))
	}
	// An absent key among them must not fail the call: DEL counts, it does not error.
	keys = append(keys, "user:acct:del-never-written")

	require.NoError(t, client.Del(ctx, keys...))

	got, err := client.MGet(ctx, keys)
	require.NoError(t, err)
	assert.Empty(t, got, "every key must be gone, not just the first slot's")
}

// The write-side counterpart of MGetCrossSlot: a bulk fill spans slots, and the
// pipeline must store every entry rather than failing on the first foreign one.
func TestClusterRedisClient_Integration_MSetCrossSlot(t *testing.T) {
	client := setupClusterClient(t)
	ctx := context.Background()

	entries := make([]KV, 0, 32)
	keys := make([]string, 0, 32)
	want := make(map[string]string, 32)
	for i := 0; i < 32; i++ {
		k := fmt.Sprintf("user:acct:mset-%d", i)
		v := fmt.Sprintf("value-%d", i)
		entries = append(entries, KV{Key: k, Value: v})
		keys = append(keys, k)
		want[k] = v
	}

	require.NoError(t, client.MSet(ctx, entries, time.Hour))

	got, err := client.MGet(ctx, keys)
	require.NoError(t, err)
	assert.Equal(t, want, got, "every entry must land, not just the first slot's")
}

func TestClusterRedisClient_Integration_MSetEmptyEntries(t *testing.T) {
	require.NoError(t, setupClusterClient(t).MSet(context.Background(), nil, time.Hour))
}

func TestClusterRedisClient_Integration_MGetEmptyKeys(t *testing.T) {
	client := setupClusterClient(t)

	got, err := client.MGet(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got, "an empty key set must not round-trip")
}
