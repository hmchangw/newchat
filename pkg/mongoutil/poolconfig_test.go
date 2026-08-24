package mongoutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestWithPool_AppliesToClient(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithPool(PoolConfig{MaxPoolSize: 300, MinPoolSize: 20})).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.MaxPoolSize)
	require.NotNil(t, clientOpts.MinPoolSize)
	assert.Equal(t, uint64(300), *clientOpts.MaxPoolSize)
	assert.Equal(t, uint64(20), *clientOpts.MinPoolSize)
}

// Both limits are authoritative, zero included: Validate() checks the same pair
// that reaches the driver, and MONGO_MIN_POOL_SIZE=0 clears a URI-supplied floor
// rather than letting it survive above the forced maximum.
func TestWithPool_AppliesZeroMinPoolSizeAuthoritatively(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithPool(PoolConfig{MaxPoolSize: 500, MinPoolSize: 0})).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.MaxPoolSize)
	assert.Equal(t, uint64(500), *clientOpts.MaxPoolSize)
	require.NotNil(t, clientOpts.MinPoolSize, "an explicit zero must reach the driver, not be skipped")
	assert.Equal(t, uint64(0), *clientOpts.MinPoolSize)
}

// A URI minPoolSize above the configured maximum must not survive into the
// client: the driver rejects min > max at construction, so the pod would fail to
// start on a rollout that only changed the max.
func TestWithPool_OverridesURIMinPoolSizeAboveMax(t *testing.T) {
	clientOpts := options.Client().SetMinPoolSize(200)
	newConnectConfig(WithPool(PoolConfig{MaxPoolSize: 150, MinPoolSize: 0})).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.MinPoolSize)
	assert.Equal(t, uint64(0), *clientOpts.MinPoolSize, "the configured pair must win over the URI floor")
	assert.Equal(t, uint64(150), *clientOpts.MaxPoolSize)
}

func TestPoolConfig_Validate_AcceptsValid(t *testing.T) {
	require.NoError(t, PoolConfig{MaxPoolSize: 500, MinPoolSize: 0}.Validate())
	require.NoError(t, PoolConfig{MaxPoolSize: 100, MinPoolSize: 100}.Validate())
}

func TestPoolConfig_Validate_RejectsZeroMax(t *testing.T) {
	err := PoolConfig{MaxPoolSize: 0}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_MAX_POOL_SIZE")
}

func TestPoolConfig_Validate_RejectsMinAboveMax(t *testing.T) {
	err := PoolConfig{MaxPoolSize: 100, MinPoolSize: 200}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_MIN_POOL_SIZE")
}
