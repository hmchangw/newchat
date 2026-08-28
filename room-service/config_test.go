package main

import (
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// main wires cfg.Pool.Validate / cfg.Guard.Validate; the exhaustive cases live
// in those packages' tests — these just prove the fields are on config and are
// validated.
func TestConfig_DelegatesPoolValidation(t *testing.T) {
	cfg := config{Pool: mongoutil.PoolConfig{MaxPoolSize: 0}}
	err := cfg.Pool.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_MAX_POOL_SIZE")
}

func TestConfig_DelegatesGuardValidation(t *testing.T) {
	cfg := config{Guard: natsrouter.GuardConfig{MaxConcurrency: -1}}
	err := cfg.Guard.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAX_CONCURRENCY")
}

func TestLegacyRoomOrigins_UnmarshalText(t *testing.T) {
	var l legacyRoomOrigins
	require.NoError(t, l.UnmarshalText([]byte(
		`[{"siteID":"site-a","origin":"https://legacy.site-a.com"},`+
			`{"siteID":"site-b","origin":"https://legacy.site-b.com"}]`)))
	assert.Equal(t, "https://legacy.site-a.com", l.byID["site-a"])
	assert.Equal(t, "https://legacy.site-b.com", l.byID["site-b"])
	assert.Empty(t, l.byID["site-x"], "unconfigured site reads as empty string")
}

func TestLegacyRoomOrigins_UnmarshalText_Errors(t *testing.T) {
	var l legacyRoomOrigins
	assert.Error(t, l.UnmarshalText([]byte(`not-json`)))
	assert.ErrorContains(t,
		l.UnmarshalText([]byte(`[{"siteID":"s1","origin":"a"},{"siteID":"s1","origin":"b"}]`)),
		"duplicate siteID")
}

// LEGACY_ROOM_ORIGINS is populated from a JSON env string; unset or empty is
// valid and means every ${roomOrigin} substitutes to "".
func TestLegacyRoomOrigins_EnvParse(t *testing.T) {
	type originsConfig struct {
		LegacyRoomOrigins legacyRoomOrigins `env:"LEGACY_ROOM_ORIGINS"`
	}

	t.Setenv("LEGACY_ROOM_ORIGINS", `[{"siteID":"site-a","origin":"https://legacy.site-a.com"}]`)
	cfg, err := env.ParseAs[originsConfig]()
	require.NoError(t, err)
	assert.Equal(t, "https://legacy.site-a.com", cfg.LegacyRoomOrigins.byID["site-a"])

	t.Setenv("LEGACY_ROOM_ORIGINS", "")
	cfg, err = env.ParseAs[originsConfig]()
	require.NoError(t, err)
	assert.Empty(t, cfg.LegacyRoomOrigins.byID)

	t.Setenv("LEGACY_ROOM_ORIGINS", `[{"siteID":"s1"`)
	_, err = env.ParseAs[originsConfig]()
	assert.Error(t, err, "malformed JSON must fail startup, not be silently ignored")
}

func TestConfig_ValkeyDisabledByDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	require.NoError(t, os.Unsetenv("VALKEY_ADDRS"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Empty(t, cfg.ValkeyAddrs, "badge cache must be disabled (no Valkey required) unless VALKEY_ADDRS is set")
}

func TestConfig_ValkeyAddrsParsed(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("VALKEY_ADDRS", "node-1:6379,node-2:6379")
	t.Setenv("VALKEY_PASSWORD", "hunter2")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, []string{"node-1:6379", "node-2:6379"}, cfg.ValkeyAddrs)
	assert.Equal(t, "hunter2", cfg.ValkeyPassword)
}

func TestConfig_BadgeCacheTTL(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	t.Run("default", func(t *testing.T) {
		require.NoError(t, os.Unsetenv("BADGE_CACHE_TTL"))
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 24*time.Hour, cfg.BadgeCacheTTL)
	})

	t.Run("override", func(t *testing.T) {
		t.Setenv("BADGE_CACHE_TTL", "48h")
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 48*time.Hour, cfg.BadgeCacheTTL)
	})
}

func TestConfig_MaxConcurrency(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	t.Run("default", func(t *testing.T) {
		require.NoError(t, os.Unsetenv("MAX_CONCURRENCY"))
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 256, cfg.Guard.MaxConcurrency)
	})

	t.Run("override", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENCY", "64")
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 64, cfg.Guard.MaxConcurrency)
	})

	t.Run("zero_disables", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENCY", "0")
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 0, cfg.Guard.MaxConcurrency)
	})
}

func TestConfig_RoomSubjectMode(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	cases := []struct {
		name    string
		env     string // "" means unset — exercise envDefault
		want    subject.RoomRouteMode
		wantErr bool
	}{
		{name: "default_is_global", env: "", want: subject.RouteGlobal},
		{name: "explicit_global", env: "global", want: subject.RouteGlobal},
		{name: "dual", env: "dual", want: subject.RouteDual},
		{name: "local", env: "local", want: subject.RouteLocal},
		{name: "invalid", env: "bogus", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Seed unconditionally so t.Setenv registers a restore of any
			// inherited value; the default case unsets it below.
			t.Setenv("ROOM_SUBJECT_MODE", "global")
			if tc.env == "" {
				require.NoError(t, os.Unsetenv("ROOM_SUBJECT_MODE"))
			} else {
				t.Setenv("ROOM_SUBJECT_MODE", tc.env)
			}
			cfg, err := env.ParseAs[config]()
			require.NoError(t, err)

			mode, err := subject.ParseRoomRouteMode(cfg.RoomSubjectMode)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, mode)
		})
	}
}

// The plain collection handles are created without collection options, so they
// inherit the CLIENT preference. Only 12 of MongoStore's methods use a
// *Secondary handle; every other read fails during a primary-down incident
// unless the client itself falls back.
func TestConfig_ClientReadPreferenceDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MONGO_CLIENT_READ_PREFERENCE", "") // pin cleanup so the host value is restored
	t.Setenv("MONGO_READ_PREFERENCE", "")
	require.NoError(t, os.Unsetenv("MONGO_CLIENT_READ_PREFERENCE"))
	require.NoError(t, os.Unsetenv("MONGO_READ_PREFERENCE"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	// The per-collection override keeps its vetted staleness-tolerant setting...
	assert.Equal(t, "secondaryPreferred", cfg.MongoReadPreference)
	// ...while every handle without an override now falls back instead of failing.
	assert.Equal(t, "primaryPreferred", cfg.MongoClientReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.MongoClientReadPreference)
	require.NoError(t, err)
	assert.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}

// broadcast-worker encrypts against its own room-key handle while key.get is
// served from here. If the two disagree about falling back, the producer keeps
// delivering messages whose keys the consumer cannot fetch. Both read
// MONGO_KEY_READ_PREFERENCE and must default the same way.
func TestConfig_KeyReadPreferenceDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MONGO_KEY_READ_PREFERENCE", "") // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("MONGO_KEY_READ_PREFERENCE"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, "primaryPreferred", cfg.MongoKeyReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.MongoKeyReadPreference)
	require.NoError(t, err)
	assert.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}
