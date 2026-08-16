package main

import (
	"os"
	"strings"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestConfig_RoomSubjectMode(t *testing.T) {
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

func TestConfig_ServiceName(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "teams-room-worker")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "teams-room-worker", cfg.ServiceName)
}

func TestDeploymentServiceNamesAreDistinct(t *testing.T) {
	normal, err := os.ReadFile("deploy/docker-compose.yml")
	require.NoError(t, err)
	teams, err := os.ReadFile("deploy/teams/docker-compose.yml")
	require.NoError(t, err)

	require.True(t, hasComposeEnvironmentEntry(string(normal), "OTEL_SERVICE_NAME=room-worker"))
	require.True(t, hasComposeEnvironmentEntry(string(teams), "OTEL_SERVICE_NAME=teams-room-worker"))
}

func TestHasComposeEnvironmentEntry_RejectsCommentsAndSuffixes(t *testing.T) {
	content := "    # - OTEL_SERVICE_NAME=room-worker\n    - OTEL_SERVICE_NAME=room-worker-canary\n"

	assert.False(t, hasComposeEnvironmentEntry(content, "OTEL_SERVICE_NAME=room-worker"))
}

func hasComposeEnvironmentEntry(content, entry string) bool {
	for line := range strings.SplitSeq(content, "\n") {
		if strings.TrimSpace(line) == "- "+entry {
			return true
		}
	}
	return false
}
