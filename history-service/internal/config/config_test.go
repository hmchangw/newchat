package config

import (
	"os"
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
		SubCacheSize:         100000,
		SubCacheTTL:          2 * time.Minute,
		RoomCacheSize:        50000,
		RoomCacheTTL:         10 * time.Second,
		PreviewCacheSize:     50000,
		PreviewCacheTTL:      10 * time.Second,
		MaxConcurrency:       256,
		RequestTimeout:       10 * time.Second,
		SubL2TTL:             90 * time.Minute,
		MongoBreakerFails:    5,
		MongoBreakerCooldown: 10 * time.Second,
		DEKL2TTL:             90 * time.Minute,
		DEKBreakerFails:      5,
		DEKBreakerCooldown:   10 * time.Second,
		Mongo: MongoConfig{
			MaxPoolSize:            100,
			MinPoolSize:            0,
			ServerSelectionTimeout: 2 * time.Second,
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

func TestValidate_RejectsInvalidReadPreference(t *testing.T) {
	cfg := baseValid()
	cfg.Mongo.ReadPreference = "quorum"
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_READ_PREFERENCE")
}

func TestValidate_AcceptsValidReadPreferences(t *testing.T) {
	for _, rp := range []string{"", "primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"} {
		name := rp
		if name == "" {
			name = "empty defaults to primary"
		}
		t.Run(name, func(t *testing.T) {
			cfg := baseValid()
			cfg.Mongo.ReadPreference = rp
			require.NoError(t, validate(&cfg))
		})
	}
}

func TestLoad_DefaultsReadPreferenceToSecondaryPreferred(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("CASSANDRA_HOSTS", "localhost")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	unsetEnv(t, "MONGO_READ_PREFERENCE") // the default only applies when unset

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "secondaryPreferred", cfg.Mongo.ReadPreference)
}

// unsetEnv removes key for the duration of the test and restores its prior
// presence/value on cleanup, so a default-value test can't be perturbed by an
// externally set variable.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, prev)
		}
	})
}

// Every knob this branch added is rejected when negative. Table-driven: the
// next validated field is one row, not another six-line copy.
func TestValidate_RejectsNegativeValues(t *testing.T) {
	tests := []struct {
		name    string
		set     func(*Config)
		wantEnv string
	}{
		{"sub L2 TTL", func(c *Config) { c.SubL2TTL = -time.Second }, "HISTORY_SUB_L2_TTL"},
		{"mongo breaker fails", func(c *Config) { c.MongoBreakerFails = -1 }, "HISTORY_MONGO_BREAKER_FAILS"},
		{"mongo breaker cooldown", func(c *Config) { c.MongoBreakerCooldown = -time.Second }, "HISTORY_MONGO_BREAKER_COOLDOWN"},
		{"DEK L2 TTL", func(c *Config) { c.DEKL2TTL = -time.Second }, "ATREST_DEK_L2_TTL"},
		{"DEK breaker fails", func(c *Config) { c.DEKBreakerFails = -1 }, "ATREST_DEK_BREAKER_FAILS"},
		{"DEK breaker cooldown", func(c *Config) { c.DEKBreakerCooldown = -time.Second }, "ATREST_DEK_BREAKER_COOLDOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseValid()
			tt.set(&cfg)
			err := validate(&cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantEnv)
		})
	}
}

func TestValidate_RejectsNonPositiveServerSelectionTimeout(t *testing.T) {
	cfg := baseValid()
	cfg.Mongo.ServerSelectionTimeout = 0
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_SERVER_SELECTION_TIMEOUT")
}

// The bound only works if it undercuts the request budget — otherwise the
// handler deadline fires first and the read never returns the error the
// fail-open paths need.
func TestValidate_RejectsServerSelectionTimeoutAtOrAboveRequestTimeout(t *testing.T) {
	cfg := baseValid()
	cfg.RequestTimeout = 5 * time.Second
	cfg.Mongo.ServerSelectionTimeout = 5 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be less than REQUEST_TIMEOUT")
}
