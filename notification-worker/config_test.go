package main

import (
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
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
		{"", "", true},
	}
	for _, tc := range cases {
		name := tc.mode
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("VALKEY_ADDRS", "valkey:6379")
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

func TestConfig_UserSettingsDefaults(t *testing.T) {
	t.Setenv("VALKEY_ADDRS", "valkey:6379")
	t.Setenv("MODE", "user")
	// env.ParseAs reads os.Environ(), so an inherited USER_SETTINGS_ENABLED or
	// PRESENCE_RPC_ENABLED on the host would shadow the envDefault this test
	// exists to pin. t.Setenv first so the original value is restored on cleanup,
	// then unset — caarlos0/env treats a defined-but-empty var as set.
	t.Setenv("USER_SETTINGS_ENABLED", "")
	require.NoError(t, os.Unsetenv("USER_SETTINGS_ENABLED"))
	t.Setenv("PRESENCE_RPC_ENABLED", "")
	require.NoError(t, os.Unsetenv("PRESENCE_RPC_ENABLED"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	// Enforcement is on by default: Mongo is already a hard dependency of this
	// service, and a gate that ships defaulted off is a gate nobody turns on.
	// PRESENCE_RPC_ENABLED defaults the other way because presence-service may not exist yet.
	require.True(t, cfg.UserSettingsEnabled, "USER_SETTINGS_ENABLED must default to true")
	require.False(t, cfg.PresenceEnabled, "PRESENCE_RPC_ENABLED must stay defaulted to false")
	require.Equal(t, 512, cfg.UserSettingsBatchSize)
	require.Equal(t, 2*time.Second, cfg.UserSettingsTimeout)
}

func TestConfig_UserSettingsKillSwitch(t *testing.T) {
	t.Setenv("VALKEY_ADDRS", "valkey:6379")
	t.Setenv("MODE", "user")
	t.Setenv("USER_SETTINGS_ENABLED", "false")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.False(t, cfg.UserSettingsEnabled)
}
