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
		Pool:             mongoutil.PoolConfig{MaxPoolSize: 500, MinPoolSize: 0, ServerSelectionTimeout: 2 * time.Second},
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
		{"room times L2 TTL", func(c *Config) { c.RoomTimesL2TTL = -time.Second }, "ROOM_TIMES_L2_TTL"},
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

// The knob only exists to be turned. It now rides on the shared
// mongoutil.PoolConfig, mounted unprefixed, so its tag carries the full
// operator-facing name.
func TestLoad_ServerSelectionTimeoutIsSettable(t *testing.T) {
	t.Setenv("CASSANDRA_HOSTS", "cassandra:9042")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("MONGO_SERVER_SELECTION_TIMEOUT", "7s")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 7*time.Second, cfg.Pool.ServerSelectionTimeout)
}

// The doubled name is what a mis-prefixed tag produced when this field lived
// inside the MONGO_-prefixed block. Nothing may answer to it, or an operator
// setting the documented name stays silently on the default.
func TestLoad_DoublePrefixedServerSelectionTimeoutIsNotHonored(t *testing.T) {
	t.Setenv("CASSANDRA_HOSTS", "cassandra:9042")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("MONGO_MONGO_SERVER_SELECTION_TIMEOUT", "9s")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 2*time.Second, cfg.Pool.ServerSelectionTimeout)
}

func TestValidate_RejectsNonPositiveServerSelectionTimeout(t *testing.T) {
	cfg := baseValid()
	cfg.Pool.ServerSelectionTimeout = 0
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_SERVER_SELECTION_TIMEOUT")
}

// The bound only works if it undercuts the request budget — otherwise the
// handler deadline fires first and the read never returns the error the
// fail-open paths need.
func TestValidate_RejectsServerSelectionTimeoutAtOrAboveRequestTimeout(t *testing.T) {
	cfg := baseValid()
	cfg.Guard.RequestTimeout = 5 * time.Second
	cfg.Pool.ServerSelectionTimeout = 5 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be less than REQUEST_TIMEOUT")
}

// setRequiredEnv fills the vars Load rejects when absent, so a test can vary
// the one knob it cares about.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CASSANDRA_HOSTS", "localhost")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
}

// The bucket walk is contiguous and stops after MessageReadMaxBuckets. Since
// 59cb66f its ceiling is the clock rather than the room's last message, so an
// idle room spends that budget crossing the empty gap before it reaches any
// data. If the budget cannot span the configured history floor, an old room's
// read stops early and returns an EMPTY page — and LoadHistory pages by
// `before` = oldest returned row, so an empty page carries no continuation and
// the client cannot advance. History becomes silently unreachable.
//
// The three knobs are independent env vars, so nothing but this check couples
// them. Refusing to boot is the only way that misconfiguration is visible.
func TestValidate_RejectsAWalkBudgetThatCannotSpanTheHistoryFloor(t *testing.T) {
	cfg := baseValid()
	cfg.MessageBucketHours = 24 // 1-day buckets
	cfg.MessageReadMaxBuckets = 122
	cfg.MessageHistoryFloorDays = 730 // needs ~730 buckets, has 122

	err := validate(&cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "MESSAGE_READ_MAX_BUCKETS")
	assert.Contains(t, err.Error(), "MESSAGE_HISTORY_FLOOR_DAYS")
}

// The shipped defaults must pass, with room to spare: 122 x 360h = 1830 days
// against a 730-day floor.
func TestValidate_AcceptsTheDefaultWalkBudget(t *testing.T) {
	cfg := baseValid()
	cfg.MessageBucketHours = 360
	cfg.MessageReadMaxBuckets = 122
	cfg.MessageHistoryFloorDays = 730

	require.NoError(t, validate(&cfg))
}
