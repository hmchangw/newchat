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

// MinPoolSize=0 (the default) must NOT emit a min option — otherwise it clobbers
// a minPoolSize supplied via the connection URI, defeating the nil-skip contract
// in tuning.go. Max stays authoritative (always applied).
func TestWithPool_OmitsMinPoolSizeWhenZero(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithPool(PoolConfig{MaxPoolSize: 500, MinPoolSize: 0})).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.MaxPoolSize)
	assert.Equal(t, uint64(500), *clientOpts.MaxPoolSize)
	assert.Nil(t, clientOpts.MinPoolSize, "zero MinPoolSize must leave a URI/driver value intact")
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
