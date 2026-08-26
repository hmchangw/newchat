package main

import (
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"

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

// The breaker knobs moved onto the shared mongoutil.BreakerConfig, mounted
// under this service's own envPrefix. The operator-facing names must be
// byte-identical to what they were before the move — this pins that, since a
// silent rename would leave a tuned deployment running the default.
func TestConfig_BreakerEnvNamesUnchanged(t *testing.T) {
	t.Setenv("MODE", "user")
	t.Setenv("BROADCAST_MONGO_BREAKER_FAILS", "9")
	t.Setenv("BROADCAST_MONGO_BREAKER_COOLDOWN", "45s")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, 9, cfg.Breaker.Fails)
	require.Equal(t, 45*time.Second, cfg.Breaker.Cooldown)
}

// The L2 retentions moved onto each tier package's TTLConfig. The composed env
// names must be byte-identical to what they were before the move — and the two
// shared with other services (ROOM_META_L2_TTL, USER_L2_TTL, ROOMSUBCACHE_TTL)
// now take their default from the tier, so they cannot drift apart.
func TestConfig_L2TTLEnvNamesUnchanged(t *testing.T) {
	t.Setenv("MODE", "user")
	t.Setenv("ROOM_META_L2_TTL", "11m")
	t.Setenv("USER_L2_TTL", "33m")
	t.Setenv("ROOMSUBCACHE_TTL", "44m")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, 11*time.Minute, cfg.RoomMetaL2.TTL)
	require.Equal(t, 33*time.Minute, cfg.UserL2.TTL)
	require.Equal(t, 44*time.Minute, cfg.RoomSubCache.TTL)
}
