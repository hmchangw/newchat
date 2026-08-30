package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// baseValid returns a Config with all tunable knobs at valid values so each test
// only varies the field under test. Other required fields are zeroed — validate()
// doesn't touch them.
func baseValid() Config {
	return Config{
		SubCacheSize:           100000,
		SubCacheTTL:            2 * time.Minute,
		RoomCacheSize:          50000,
		RoomCacheTTL:           10 * time.Second,
		PreviewKeyEpoch:        1,
		PreviewCacheSize:       50000,
		PreviewCacheTTL:        10 * time.Second,
		PreviewWarmBackWorkers: 8,
		PreviewWarmBackQueue:   1024,
		// ServerSelectionTimeout is this branch's: a stopped Mongo goes quiet
		// rather than erroring, so an unbounded selection outlives the caller.
		Pool:  mongoutil.PoolConfig{MaxPoolSize: 500, MinPoolSize: 0, ServerSelectionTimeout: 2 * time.Second},
		Guard: natsrouter.GuardConfig{MaxConcurrency: 256, RequestTimeout: 10 * time.Second},
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
	cfg.SubL2.TTL = 0
	cfg.Breaker.Fails = 0
	cfg.Breaker.Cooldown = 0
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

func TestValidate_RejectsNegativeWarmBackSizes(t *testing.T) {
	tests := []struct {
		name string
		set  func(*Config)
		want string
	}{
		{name: "workers", set: func(c *Config) { c.PreviewWarmBackWorkers = -1 }, want: "PREVIEW_WARMBACK_WORKERS"},
		{name: "queue", set: func(c *Config) { c.PreviewWarmBackQueue = -1 }, want: "PREVIEW_WARMBACK_QUEUE"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseValid()
			tc.set(&cfg)
			err := validate(&cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// Zero is "take the default", not "disable": warm-back is what stops the lazy walk
// repeating forever, so the wiring never turns it off.
func TestValidate_AcceptsZeroWarmBackSizesAsDefaults(t *testing.T) {
	cfg := baseValid()
	cfg.PreviewWarmBackWorkers = 0
	cfg.PreviewWarmBackQueue = 0
	require.NoError(t, validate(&cfg))
}

func TestLoad_WarmBackDefaults(t *testing.T) {
	setRequiredEnv(t)
	testutil.UnsetEnv(t, "PREVIEW_WARMBACK_WORKERS")
	testutil.UnsetEnv(t, "PREVIEW_WARMBACK_QUEUE")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 8, cfg.PreviewWarmBackWorkers)
	assert.Equal(t, 1024, cfg.PreviewWarmBackQueue)
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
	testutil.UnsetEnv(t, "MONGO_READ_PREFERENCE") // the default only applies when unset

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "secondaryPreferred", cfg.Mongo.ReadPreference)
}

// Every knob this branch added is rejected when negative. Table-driven: the
// next validated field is one row, not another six-line copy.
func TestValidate_RejectsNegativeValues(t *testing.T) {
	tests := []struct {
		name    string
		set     func(*Config)
		wantEnv string
	}{
		{"sub L2 TTL", func(c *Config) { c.SubL2.TTL = -time.Second }, "HISTORY_SUB_L2_TTL"},
		{"mongo breaker fails", func(c *Config) { c.Breaker.Fails = -1 }, "HISTORY_MONGO_BREAKER_FAILS"},
		{"mongo breaker cooldown", func(c *Config) { c.Breaker.Cooldown = -time.Second }, "HISTORY_MONGO_BREAKER_COOLDOWN"},
		{"DEK L2 TTL", func(c *Config) { c.DEKL2.TTL = -time.Second }, "ATREST_DEK_L2_TTL"},
		{"DEK breaker fails", func(c *Config) { c.DEKBreaker.Fails = -1 }, "ATREST_DEK_BREAKER_FAILS"},
		{"DEK breaker cooldown", func(c *Config) { c.DEKBreaker.Cooldown = -time.Second }, "ATREST_DEK_BREAKER_COOLDOWN"},
		{"room times L2 TTL", func(c *Config) { c.RoomTimesL2.TTL = -time.Second }, "ROOM_TIMES_L2_TTL"},
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
				testutil.UnsetEnv(t, "PAGE_TRIMMING_ENABLED")
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

// The bucket budget has to cover the walk the READER actually performs, and that
// walk starts at now+clockSkewTolerance, not at now. Near a bucket boundary the
// extra hour pushes the DESC walk onto one more partition than the span alone
// implies, so a budget sized on the span exhausts before reaching the floor and
// the read returns an empty page over a room that has rows.
//
// 24h buckets, a 2-day floor, 3 buckets: the old span-only check computed
// (3-1)*24 = 48h >= 48h and passed. The real worst-case walk spans
// bucket(now+1h) down to bucket(now-48h) — four partitions.
func TestValidate_RejectsBucketBudgetThatIgnoresTheWalkSkew(t *testing.T) {
	cfg := baseValid()
	cfg.MessageBucketHours = 24
	cfg.MessageHistoryFloorDays = 2
	cfg.MessageReadMaxBuckets = 3

	err := validate(&cfg)
	require.Error(t, err, "a boundary-aligned walk needs 4 buckets, not 3")
	assert.Contains(t, err.Error(), "MESSAGE_READ_MAX_BUCKETS")
}

func TestValidate_AcceptsBucketBudgetThatCoversTheWalkSkew(t *testing.T) {
	cfg := baseValid()
	cfg.MessageBucketHours = 24
	cfg.MessageHistoryFloorDays = 2
	cfg.MessageReadMaxBuckets = 4

	require.NoError(t, validate(&cfg))
}

// The shipped defaults must stay comfortably inside the rule — a validation that
// rejects the values every deployment runs is worse than no validation. Loaded
// rather than hand-built, since the defaults live on the env tags.
func TestLoad_ProductionDefaultsCoverTheWalk(t *testing.T) {
	setRequiredEnv(t)
	for _, k := range []string{"MESSAGE_BUCKET_HOURS", "MESSAGE_READ_MAX_BUCKETS", "MESSAGE_HISTORY_FLOOR_DAYS"} {
		testutil.UnsetEnv(t, k)
	}

	cfg, err := Load()
	require.NoError(t, err, "the shipped defaults must satisfy their own validation")
	assert.Equal(t, 360, cfg.MessageBucketHours)
	assert.Equal(t, 122, cfg.MessageReadMaxBuckets)
	assert.Equal(t, 730, cfg.MessageHistoryFloorDays)
}

// The breaker knobs moved onto the shared mongoutil.BreakerConfig under this
// service's HISTORY_ envPrefix. Pinned because a silent rename would leave a
// tuned deployment running the default.
func TestLoad_BreakerEnvNamesUnchanged(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("HISTORY_MONGO_BREAKER_FAILS", "9")
	t.Setenv("HISTORY_MONGO_BREAKER_COOLDOWN", "45s")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 9, cfg.Breaker.Fails)
	assert.Equal(t, 45*time.Second, cfg.Breaker.Cooldown)
}

// The L2 retentions moved onto each tier package's TTLConfig. The composed env
// names must be byte-identical to what they were before the move.
func TestLoad_L2TTLEnvNamesUnchanged(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("HISTORY_SUB_L2_TTL", "22m")
	t.Setenv("ATREST_DEK_L2_TTL", "55m")
	t.Setenv("ROOM_TIMES_L2_TTL", "66m")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 22*time.Minute, cfg.SubL2.TTL)
	assert.Equal(t, 55*time.Minute, cfg.DEKL2.TTL)
	assert.Equal(t, 66*time.Minute, cfg.RoomTimesL2.TTL)
}

func TestValidate_RejectsNegativeUserCacheSize(t *testing.T) {
	cfg := baseValid()
	cfg.UserCacheSize = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "USER_CACHE_SIZE")
}

func TestValidate_RejectsNegativeUserCacheTTL(t *testing.T) {
	cfg := baseValid()
	cfg.UserCacheTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "USER_CACHE_TTL")
}

// Without this, encrypted history cannot be read at all when there is no
// primary: cassrepo/decrypt.go cannot decrypt without the DEK.
func TestLoad_DefaultsKeyReadPreferenceToPrimaryPreferred(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("CASSANDRA_HOSTS", "localhost")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	testutil.UnsetEnv(t, "MONGO_KEY_READ_PREFERENCE")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "primaryPreferred", cfg.Mongo.KeyReadPreference)
}

func TestValidate_RejectsInvalidKeyReadPreference(t *testing.T) {
	cfg := baseValid()
	cfg.Mongo.KeyReadPreference = "quorum"
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_KEY_READ_PREFERENCE")
}

// history-service's DEK handles must bind the same wire name as the other
// key-touching services; its Mongo block carries an envPrefix, so the tag is
// KEY_READ_PREFERENCE and the wire name is MONGO_KEY_READ_PREFERENCE.
func TestLoad_KeyReadPreferenceWireName(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("CASSANDRA_HOSTS", "localhost")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_KEY_READ_PREFERENCE", "nearest") // a value no default would produce

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "nearest", cfg.Mongo.KeyReadPreference,
		"the field must bind to MONGO_KEY_READ_PREFERENCE via the MONGO_ envPrefix")
}
