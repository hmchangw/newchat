package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestConfig_Mode(t *testing.T) {
	cases := []struct {
		mode    string
		want    stream.Pipeline
		wantErr bool
	}{
		{"user", stream.PipelineUser, false},
		{"bot", stream.PipelineBot, false},
		{"admin", "", true},
		{"", "", true}, // required
	}
	for _, tc := range cases {
		name := tc.mode
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("MODE", tc.mode) // pin cleanup so host MODE is restored after the test
			if tc.mode == "" {
				require.NoError(t, os.Unsetenv("MODE")) // caarlos0/env treats "" as defined; unset to test the required check
			}
			cfg, err := env.ParseAs[config]()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Mode)
		})
	}
}

func TestConfig_RoomSubjectMode(t *testing.T) {
	t.Setenv("MODE", "user")

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
			require.Equal(t, tc.want, mode)
		})
	}
}

func TestConfig_ThreadViewSubjectEnabled(t *testing.T) {
	t.Setenv("MODE", "user")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.True(t, cfg.ThreadViewSubjectEnabled, "the view lane ships on; the env var is a kill switch")

	t.Setenv("THREAD_VIEW_SUBJECT_ENABLED", "false")
	cfg, err = env.ParseAs[config]()
	require.NoError(t, err)
	require.False(t, cfg.ThreadViewSubjectEnabled)
}

func TestConfig_PoolValidate(t *testing.T) {
	t.Setenv("MODE", "user")

	t.Setenv("MONGO_MAX_POOL_SIZE", "0")
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Error(t, cfg.Pool.Validate()) // zero max is unbounded to the driver — must be rejected

	require.NoError(t, os.Unsetenv("MONGO_MAX_POOL_SIZE"))
	cfg, err = env.ParseAs[config]()
	require.NoError(t, err)
	require.NoError(t, cfg.Pool.Validate()) // envDefault applies
}

// The primary pin makes encrypted-room delivery fail outright when there is no
// primary: currentRoomKey treats a key miss as a hard error. primaryPreferred is
// safe because a stale DEK read cannot diverge ($setOnInsert plus a re-read
// comparison) and a missing room key is already retryable.
func TestConfig_KeyReadPreferenceDefault(t *testing.T) {
	t.Setenv("MODE", "user")
	t.Setenv("MONGO_KEY_READ_PREFERENCE", "") // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("MONGO_KEY_READ_PREFERENCE"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.MongoKeyReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.MongoKeyReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}

// Retention has to outlast a cache entry plus the client's key.get and retry,
// and a secondary key read widens the window it absorbs. The default was exactly
// at the 2x floor with no slack; 30m against a 10m cache restores margin.
func TestConfig_RetiredTTLKeepsMarginOverCacheTTL(t *testing.T) {
	t.Setenv("MODE", "user")
	require.NoError(t, os.Unsetenv("ROOM_KEY_RETIRED_TTL"))
	require.NoError(t, os.Unsetenv("ROOM_KEY_CACHE_TTL"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.True(t, retiredTTLSafe(cfg.RoomKeyRetiredTTL, cfg.RoomKeyCacheTTL))
	require.Greater(t, cfg.RoomKeyRetiredTTL, 2*cfg.RoomKeyCacheTTL,
		"defaults must leave slack above the 2x floor, not sit exactly on it")
}

// broadcast-worker encrypts against its own key handle while room-service's
// key.get serves from another. Both must bind the same wire name.
func TestConfig_KeyReadPreferenceWireName(t *testing.T) {
	t.Setenv("MODE", "user")
	t.Setenv("MONGO_KEY_READ_PREFERENCE", "nearest") // a value no default would produce

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "nearest", cfg.MongoKeyReadPreference,
		"the field must bind to MONGO_KEY_READ_PREFERENCE, not a prefixed variant")
}
