package presencestore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Presence is Valkey-only — there is no database behind it — so an unreachable
// cluster at startup must not stop the pod coming up. A fatal probe turns any
// rollout or node drain during a Valkey outage into a CrashLoopBackOff that
// outlives the outage itself; a lazily-connected client self-heals on the next
// call once Valkey returns.
func TestNewValkeyStore_UnreachableClusterStillReturnsStore(t *testing.T) {
	store, err := NewValkeyStore(
		context.Background(),
		ClusterConfig{Addrs: []string{"127.0.0.1:1"}},
		time.Minute, time.Minute,
	)
	require.NoError(t, err, "an unreachable cluster must not fail construction")
	require.NotNil(t, store)
	t.Cleanup(func() { _ = store.Close() })
}

// The store of record must carry StoreProfile's budget, not go-redis defaults.
// Its Lua EVALs have no fallback to absorb a manufactured timeout, so the
// cache-tight CacheProfile would be actively harmful here. Asserted on the
// constructed client rather than on an options helper, so the wiring itself is
// covered rather than a restatement of ClusterOptionsFor's own test.
func TestNewValkeyStore_UsesStoreProfile(t *testing.T) {
	store, err := NewValkeyStore(
		context.Background(),
		ClusterConfig{Addrs: []string{"127.0.0.1:1"}},
		time.Minute, time.Minute,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	opts := store.c.Options()
	assert.Equal(t, valkeyutil.StoreProfile.ReadTimeout, opts.ReadTimeout)
	assert.Equal(t, valkeyutil.StoreProfile.WriteTimeout, opts.WriteTimeout)
	assert.Equal(t, valkeyutil.StoreProfile.PoolTimeout, opts.PoolTimeout)
	assert.Equal(t, valkeyutil.StoreProfile.MaxRetries, opts.MaxRetries)
	assert.True(t, opts.ContextTimeoutEnabled, "caller deadlines must bound socket reads")
}
