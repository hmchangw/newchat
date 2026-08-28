package main

import (
	"os"
	"strings"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestDeploymentServiceNamesAreDistinct pins the two deploys of this binary to
// separate telemetry identities. They share every stream config knob and differ
// only by MODE, so a shared service_name makes the default and Teams pods
// indistinguishable in any chat.nats.* series.
func TestDeploymentServiceNamesAreDistinct(t *testing.T) {
	normal, err := os.ReadFile("deploy/docker-compose.yml")
	require.NoError(t, err)
	teams, err := os.ReadFile("deploy/teams/docker-compose.yml")
	require.NoError(t, err)

	assert.True(t, hasComposeEnvironmentEntry(string(normal), "OTEL_SERVICE_NAME=room-worker"))
	assert.True(t, hasComposeEnvironmentEntry(string(teams), "OTEL_SERVICE_NAME=teams-room-worker"))
	assert.False(t, hasComposeEnvironmentEntry(string(teams), "OTEL_SERVICE_NAME=room-worker"))
}

func TestHasComposeEnvironmentEntry_RejectsCommentsAndSuffixes(t *testing.T) {
	content := "    # - OTEL_SERVICE_NAME=room-worker\n    - OTEL_SERVICE_NAME=room-worker-canary\n"

	assert.False(t, hasComposeEnvironmentEntry(content, "OTEL_SERVICE_NAME=room-worker"))
}

// hasComposeEnvironmentEntry reports whether content has entry as a whole
// compose environment list item, so a commented-out line or a longer name
// sharing the prefix does not count as a match.
func hasComposeEnvironmentEntry(content, entry string) bool {
	for line := range strings.SplitSeq(content, "\n") {
		if strings.TrimSpace(line) == "- "+entry {
			return true
		}
	}
	return false
}

// All five key-touching services must resolve MONGO_KEY_READ_PREFERENCE to the
// same wire name and default. room-worker's field is deliberately top-level (see
// the comment above it) so the MONGO_ envPrefix cannot double it.
func TestConfig_KeyReadPreferenceWireName(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	t.Setenv("MONGO_KEY_READ_PREFERENCE", "nearest") // a value no default would produce
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "nearest", cfg.MongoKeyReadPreference,
		"the field must bind to MONGO_KEY_READ_PREFERENCE, not a prefixed variant")

	require.NoError(t, os.Unsetenv("MONGO_KEY_READ_PREFERENCE"))
	cfg, err = env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.MongoKeyReadPreference)
}
