package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// baseValid returns a Config with all four cache knobs at non-negative values so
// each test only varies the field under test. Other required fields are zeroed —
// validate() doesn't touch them.
func baseValid() Config {
	return Config{
		SubCacheSize:                 100000,
		SubCacheTTL:                  2 * time.Minute,
		RoomCacheSize:                50000,
		RoomCacheTTL:                 10 * time.Second,
		MessageReadBucketConcurrency: 1,
		MessageReadEscalateAfter:     4,
	}
}

func TestValidate_RejectsBucketConcurrencyBelowOne(t *testing.T) {
	cfg := baseValid()
	cfg.MessageReadBucketConcurrency = 0
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MESSAGE_READ_BUCKET_CONCURRENCY")
}

func TestValidate_RejectsEscalateAfterBelowOne(t *testing.T) {
	cfg := baseValid()
	cfg.MessageReadEscalateAfter = 0
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MESSAGE_READ_ESCALATE_AFTER")
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
