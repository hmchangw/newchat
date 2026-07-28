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
		SubCacheSize:         100000,
		SubCacheTTL:          2 * time.Minute,
		RoomCacheSize:        50000,
		RoomCacheTTL:         10 * time.Second,
		SubL2TTL:             90 * time.Minute,
		MongoBreakerFails:    5,
		MongoBreakerCooldown: 10 * time.Second,
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
	cfg.SubL2TTL = 0
	cfg.MongoBreakerFails = 0
	cfg.MongoBreakerCooldown = 0
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

func TestValidate_RejectsNegativeSubL2TTL(t *testing.T) {
	cfg := baseValid()
	cfg.SubL2TTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_SUB_L2_TTL")
}

func TestValidate_RejectsNegativeMongoBreakerFails(t *testing.T) {
	cfg := baseValid()
	cfg.MongoBreakerFails = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_MONGO_BREAKER_FAILS")
}

func TestValidate_RejectsNegativeMongoBreakerCooldown(t *testing.T) {
	cfg := baseValid()
	cfg.MongoBreakerCooldown = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_MONGO_BREAKER_COOLDOWN")
}
