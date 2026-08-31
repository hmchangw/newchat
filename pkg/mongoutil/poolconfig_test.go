package mongoutil

import (
	"testing"
	"time"

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

// The driver's own maxIdleTimeMS default is 0, which means never reap, so a pool
// that grew during a burst holds those sockets for the life of the process. The
// default here is what stops that fleet-wide.
func TestWithPool_AppliesMaxIdleTime(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithPool(PoolConfig{MaxPoolSize: 150, MaxIdleTime: 5 * time.Minute})).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.MaxConnIdleTime)
	assert.Equal(t, 5*time.Minute, *clientOpts.MaxConnIdleTime)
}

// Zero is the documented "never reap" escape hatch and must leave a URI-supplied
// maxIdleTimeMS intact, matching how MinPoolSize=0 behaves.
func TestWithPool_OmitsMaxIdleTimeWhenZero(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithPool(PoolConfig{MaxPoolSize: 150, MaxIdleTime: 0})).applyTuning(clientOpts)

	assert.Nil(t, clientOpts.MaxConnIdleTime, "zero MaxIdleTime must leave a URI/driver value intact")
}

// A negative duration is a typo, not a request to disable reaping.
func TestPoolConfig_Validate_RejectsNegativeMaxIdleTime(t *testing.T) {
	err := PoolConfig{MaxPoolSize: 150, MaxIdleTime: -time.Second}.Validate()

	require.Error(t, err)
	assert.Equal(t, "MONGO_MAX_IDLE_TIME must be >= 0, got -1s", err.Error())
}

// A stopped MongoDB does not error, it goes quiet: the driver's 30s default
// server-selection wait turns an outage into a hang, and every fail-open path
// downstream depends on the read erroring while the request still has budget.
// The bound rides on PoolConfig so a service gets it by adopting one field,
// rather than each of eighteen remembering its own.
func TestWithPool_AppliesServerSelectionTimeout(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithPool(PoolConfig{
		MaxPoolSize: 150, ServerSelectionTimeout: 2 * time.Second,
	})).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.ServerSelectionTimeout)
	assert.Equal(t, 2*time.Second, *clientOpts.ServerSelectionTimeout)
}

// Zero means "unset", and the driver reads a zero server-selection timeout as
// no bound at all — so it must leave the URI/driver value alone rather than
// being applied literally.
func TestWithPool_OmitsServerSelectionTimeoutWhenZero(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithPool(PoolConfig{MaxPoolSize: 150})).applyTuning(clientOpts)

	assert.Nil(t, clientOpts.ServerSelectionTimeout)
}

// Negative is the dangerous typo: the driver reads <= 0 as unbounded, which is
// the exact hang this setting exists to prevent, so it is refused at load.
func TestPoolConfig_Validate_RejectsNegativeServerSelectionTimeout(t *testing.T) {
	err := PoolConfig{MaxPoolSize: 150, ServerSelectionTimeout: -time.Second}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_SERVER_SELECTION_TIMEOUT")
}
