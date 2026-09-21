package valkeyutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ContextTimeoutEnabled is the load-bearing field. Without it go-redis ignores
// the caller's context deadline for socket reads and only ReadTimeout applies,
// which makes the rest of the profile decorative.
func TestClusterOptionsFor_AppliesProfile(t *testing.T) {
	opts := ClusterOptionsFor([]string{"h:6379"}, "pw", StoreProfile)

	assert.Equal(t, []string{"h:6379"}, opts.Addrs)
	assert.Equal(t, "pw", opts.Password)
	assert.Equal(t, StoreProfile.DialTimeout, opts.DialTimeout)
	assert.Equal(t, StoreProfile.ReadTimeout, opts.ReadTimeout)
	assert.Equal(t, StoreProfile.WriteTimeout, opts.WriteTimeout)
	assert.Equal(t, StoreProfile.PoolTimeout, opts.PoolTimeout)
	assert.Equal(t, StoreProfile.MaxRetries, opts.MaxRetries)
	assert.True(t, opts.ContextTimeoutEnabled, "caller deadlines must bound socket reads")
}

// The budgets differ because the consumers differ: a cache has a backing store
// to fall through to, the presence store does not.
func TestProfiles_CacheIsTighterThanStore(t *testing.T) {
	require.Less(t, CacheProfile.ReadTimeout, StoreProfile.ReadTimeout,
		"a cache with a fallback should give up sooner than the store of record")
	require.Less(t, CacheProfile.WriteTimeout, StoreProfile.WriteTimeout)

	for _, p := range []struct {
		name string
		p    Profile
	}{{"cache", CacheProfile}, {"store", StoreProfile}} {
		t.Run(p.name, func(t *testing.T) {
			assert.Positive(t, p.p.DialTimeout)
			assert.Positive(t, p.p.ReadTimeout)
			assert.Positive(t, p.p.WriteTimeout)
			assert.Positive(t, p.p.PoolTimeout)
			assert.GreaterOrEqual(t, p.p.MaxRetries, 0)
			// Bounded end to end: a blackholing Valkey must not outlast this.
			worst := time.Duration(p.p.MaxRetries+1) * p.p.ReadTimeout
			assert.Less(t, worst, 2*time.Second,
				"worst-case read budget must stay well under the old 3s default")
		})
	}
}

// Callers that pass no profile must still get a bounded client — an unbounded
// default is the trap this package exists to close.
func TestNewConnectConfig_DefaultsToCacheProfile(t *testing.T) {
	assert.Equal(t, CacheProfile, newConnectConfig().profile)
	assert.Equal(t, StoreProfile, newConnectConfig(WithProfile(StoreProfile)).profile)
}
