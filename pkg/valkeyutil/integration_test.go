//go:build integration

package valkeyutil

import (
	"context"
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
