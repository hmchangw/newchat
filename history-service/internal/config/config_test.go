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
		SubCacheSize:    100000,
		SubCacheTTL:     2 * time.Minute,
		RoomCacheSize:   50000,
		RoomCacheTTL:    10 * time.Second,
		PreviewKeyEpoch: 1,
		MaxConcurrency:  256,
		RequestTimeout:  10 * time.Second,
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

// The epoch is part of the preview DEK id, so a non-positive value mints a
// sentinel rotation could never move forward from.
func TestValidate_RejectsNonPositivePreviewKeyEpoch(t *testing.T) {
	for _, epoch := range []int{0, -1} {
		cfg := baseValid()
		cfg.PreviewKeyEpoch = epoch
		err := validate(&cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PREVIEW_KEY_EPOCH")
	}
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

// Trimming is on unless an operator turns it off. Flipping this default would
// silently switch every deployment that does not set the var back to the
// pre-pagefit behaviour of letting the broker refuse the reply.
func TestLoad_PageTrimming(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want bool
	}{
		{name: "defaults to enabled", env: "", want: true},
		{name: "disabled by the operator", env: "false", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setRequiredEnv(t)
			if tc.env == "" {
				unsetEnv(t, "PAGE_TRIMMING_ENABLED")
			} else {
				t.Setenv("PAGE_TRIMMING_ENABLED", tc.env)
			}

			cfg, err := Load()
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.PageTrimming)
		})
	}
}

// setRequiredEnv fills the vars Load rejects when absent, so a test can vary
// the one knob it cares about.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CASSANDRA_HOSTS", "localhost")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
}
