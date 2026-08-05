package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// baseValid returns a Config with all tunable knobs at valid values so each test
// only varies the field under test. Other required fields are zeroed — validate()
// doesn't touch them.
func baseValid() Config {
	return Config{
		SubCacheSize:     100000,
		SubCacheTTL:      2 * time.Minute,
		RoomCacheSize:    50000,
		RoomCacheTTL:     10 * time.Second,
		PreviewCacheSize: 50000,
		PreviewCacheTTL:  10 * time.Second,
		MaxConcurrency:   256,
		RequestTimeout:   10 * time.Second,
		Mongo: MongoConfig{
			MaxPoolSize: 100,
			MinPoolSize: 0,
		},
	}
}

func TestValidate_AcceptsDefaults(t *testing.T) {
	cfg := baseValid()
	require.NoError(t, validate(&cfg))
}

func TestValidate_AcceptsZerosAsDisable(t *testing.T) {
	cfg := baseValid()
	cfg.SubCacheSize = 0
	cfg.SubCacheTTL = 0
	cfg.RoomCacheSize = 0
	cfg.RoomCacheTTL = 0
	cfg.PreviewCacheSize = 0
	cfg.PreviewCacheTTL = 0
	require.NoError(t, validate(&cfg), "zero is the documented disable value")
}

func TestValidate_RejectsNegativeMsgCacheTTL(t *testing.T) {
	cfg := baseValid()
	cfg.MsgCacheTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_MSG_CACHE_TTL")
}

func TestValidate_RejectsNegativeMsgCacheL1Size(t *testing.T) {
	cfg := baseValid()
	cfg.MsgCacheL1Size = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_MSG_CACHE_L1_SIZE")
}

func TestValidate_RejectsNegativeMsgGenTTL(t *testing.T) {
	cfg := baseValid()
	cfg.MsgGenTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_MSG_GEN_TTL")
}

func TestValidate_AcceptsZeroMsgCacheAsDisable(t *testing.T) {
	cfg := baseValid()
	cfg.MsgCacheTTL = 0
	cfg.MsgCacheL1Size = 0
	cfg.MsgGenTTL = 0
	require.NoError(t, validate(&cfg), "zero is the documented disable value")
}

func TestValidate_RejectsNegativeSubCacheSize(t *testing.T) {
	cfg := baseValid()
	cfg.SubCacheSize = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_SUB_CACHE_SIZE")
}

func TestValidate_RejectsNegativeSubCacheTTL(t *testing.T) {
	cfg := baseValid()
	cfg.SubCacheTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_SUB_CACHE_TTL")
}

func TestValidate_RejectsNegativeRoomCacheSize(t *testing.T) {
	cfg := baseValid()
	cfg.RoomCacheSize = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_ROOM_CACHE_SIZE")
}

func TestValidate_RejectsNegativeRoomCacheTTL(t *testing.T) {
	cfg := baseValid()
	cfg.RoomCacheTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_ROOM_CACHE_TTL")
}

// maxPoolSize=0 makes the driver treat the pool as unbounded — the opposite of
// an explicit cap — so it is rejected rather than silently uncapping the pool.
func TestValidate_RejectsZeroMaxPoolSize(t *testing.T) {
	cfg := baseValid()
	cfg.Mongo.MaxPoolSize = 0
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_MAX_POOL_SIZE")
}

func TestValidate_RejectsMinPoolSizeAboveMax(t *testing.T) {
	cfg := baseValid()
	cfg.Mongo.MaxPoolSize = 100
	cfg.Mongo.MinPoolSize = 200
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_MIN_POOL_SIZE")
}

func TestValidate_AcceptsMinPoolSizeEqualToMax(t *testing.T) {
	cfg := baseValid()
	cfg.Mongo.MaxPoolSize = 100
	cfg.Mongo.MinPoolSize = 100
	require.NoError(t, validate(&cfg))
}

// 0 disables the concurrency cap (unbounded spawn); it is the documented
// disable value, so it must validate.
func TestValidate_AcceptsZeroMaxConcurrencyAsDisable(t *testing.T) {
	cfg := baseValid()
	cfg.MaxConcurrency = 0
	require.NoError(t, validate(&cfg))
}

func TestValidate_RejectsNegativeMaxConcurrency(t *testing.T) {
	cfg := baseValid()
	cfg.MaxConcurrency = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAX_CONCURRENCY")
}

// 0 disables the per-request timeout; it is the documented disable value.
func TestValidate_AcceptsZeroRequestTimeoutAsDisable(t *testing.T) {
	cfg := baseValid()
	cfg.RequestTimeout = 0
	require.NoError(t, validate(&cfg))
}

func TestValidate_RejectsNegativeRequestTimeout(t *testing.T) {
	cfg := baseValid()
	cfg.RequestTimeout = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REQUEST_TIMEOUT")
}

func TestValidate_RejectsNegativePreviewCacheSize(t *testing.T) {
	cfg := baseValid()
	cfg.PreviewCacheSize = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_PREVIEW_CACHE_SIZE")
}

func TestValidate_RejectsNegativePreviewCacheTTL(t *testing.T) {
	cfg := baseValid()
	cfg.PreviewCacheTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_PREVIEW_CACHE_TTL")
}
