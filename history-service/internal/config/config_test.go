package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
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
		PreviewKeyEpoch:  1,
		PreviewCacheSize: 50000,
		PreviewCacheTTL:  10 * time.Second,
		Pool:             mongoutil.PoolConfig{MaxPoolSize: 500, MinPoolSize: 0},
		Guard:            natsrouter.GuardConfig{MaxConcurrency: 256, RequestTimeout: 10 * time.Second},
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

func TestValidate_RejectsNegativeBucketCacheTTL(t *testing.T) {
	cfg := baseValid()
	cfg.BucketCacheTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_BUCKET_CACHE_TTL")
}

func TestValidate_RejectsNegativeBucketCacheL1MaxBytes(t *testing.T) {
	cfg := baseValid()
	cfg.BucketCacheL1MaxBytes = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_BUCKET_CACHE_L1_MAX_BYTES")
}

func TestValidate_RejectsNegativeBucketCacheMaxRows(t *testing.T) {
	cfg := baseValid()
	cfg.BucketCacheMaxRows = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_BUCKET_CACHE_MAX_ROWS")
}

func TestValidate_AcceptsZeroBucketCacheAsDisable(t *testing.T) {
	cfg := baseValid()
	cfg.BucketCacheTTL = 0
	cfg.BucketCacheL1MaxBytes = 0
	cfg.BucketCacheMaxRows = 0
	require.NoError(t, validate(&cfg), "zero is the documented disable value")
}

func TestConfig_BucketCacheEnabled(t *testing.T) {
	// The cache needs an explicit opt-in as well as usable knobs. Zero is the
	// documented disable value for every knob, so any one of them at zero must
	// keep Valkey unconnected and the cache uninstalled.
	tests := []struct {
		name     string
		mutate   func(*Config)
		expected bool
	}{
		{name: "all set", mutate: func(*Config) {}, expected: true},
		{name: "no valkey addrs", mutate: func(c *Config) { c.ValkeyAddrs = nil }},
		{name: "zero l1 budget", mutate: func(c *Config) { c.BucketCacheL1MaxBytes = 0 }},
		{name: "zero ttl", mutate: func(c *Config) { c.BucketCacheTTL = 0 }},
		{name: "zero max rows", mutate: func(c *Config) { c.BucketCacheMaxRows = 0 }},
		// VALKEY_ADDRS is fleet-wide; without the opt-in, a deployment that sets
		// it for other services must not switch this cache on here.
		{name: "addrs set but not opted in", mutate: func(c *Config) { c.BucketCacheOptIn = false }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseValid()
			cfg.BucketCacheOptIn = true
			cfg.ValkeyAddrs = []string{"valkey:6379"}
			cfg.BucketCacheL1MaxBytes = 1 << 20
			cfg.BucketCacheTTL = 10 * time.Minute
			cfg.BucketCacheMaxRows = 2000
			tt.mutate(&cfg)
			assert.Equal(t, tt.expected, cfg.BucketCacheEnabled())
		})
	}
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

// validate() delegates pool checks to mongoutil.PoolConfig.Validate — the
// exhaustive cases live in that package's tests; this just proves it's wired.
func TestValidate_DelegatesPoolValidation(t *testing.T) {
	cfg := baseValid()
	cfg.Pool.MaxPoolSize = 0
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_MAX_POOL_SIZE")
}

// validate() delegates the concurrency/timeout checks to
// natsrouter.GuardConfig.Validate — again just proving the wiring.
func TestValidate_DelegatesGuardValidation(t *testing.T) {
	cfg := baseValid()
	cfg.Guard.MaxConcurrency = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAX_CONCURRENCY")
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

// The per-bucket cache defaults are a tuning decision, not an accident: MaxRows
// sits just above surroundingPageSize (50) so the cache holds the sparse buckets
// that force a multi-bucket walk and declines the dense ones a single bounded
// query already satisfies. Pinned here so a future edit has to be deliberate.
func TestLoad_BucketCacheDefaults(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("CASSANDRA_HOSTS", "localhost")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	for _, k := range []string{
		"HISTORY_BUCKET_CACHE_MAX_ROWS",
		"HISTORY_BUCKET_CACHE_TTL",
		"HISTORY_BUCKET_CACHE_L1_MAX_BYTES",
		"HISTORY_BUCKET_CACHE_ENABLED",
	} {
		unsetEnv(t, k) // defaults only apply when unset
	}

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 50, cfg.BucketCacheMaxRows)
	assert.Equal(t, 10*time.Minute, cfg.BucketCacheTTL)
	assert.Equal(t, int64(256<<20), cfg.BucketCacheL1MaxBytes)
	assert.False(t, cfg.BucketCacheOptIn, "the cache must be off until explicitly enabled")
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
