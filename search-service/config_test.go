package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredSearchEnv seeds every required env var so env.ParseAs succeeds and
// the test can assert on the optional MAX_CONCURRENCY knob in isolation.
func setRequiredSearchEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("SEARCH_URL", "http://localhost:9200")
	t.Setenv("SEARCH_USER_ROOM_INDEX", "userroom-site-a-v1")
	t.Setenv("SEARCH_SPOTLIGHT_INDEX", "spotlight-site-a-v1")
	t.Setenv("SEARCH_SPOTLIGHT_ORG_INDEX", "spotlightorg-site-a-v1")
	t.Setenv("VALKEY_ADDRS", "localhost:7000")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("USERS_API_URL", "http://localhost:8080")
}

// TestResolveIndexNames covers the dual-name window for the three ES index
// vars. search-sync-worker and es-index-migrator read the unprefixed names;
// search-service historically read SEARCH_-prefixed ones. Two names for one
// value drift silently — a wildcard read pattern plus allow_no_indices turns a
// mismatch into an empty result set rather than an error — so both are accepted
// during the migration, with the unprefixed form canonical.
func TestResolveIndexNames(t *testing.T) {
	tests := []struct {
		name      string
		canonical string
		legacy    string
		want      string
		wantErr   bool
	}{
		{name: "unprefixed only", canonical: "spotlight-site-a-v1", want: "spotlight-site-a-v1"},
		{name: "legacy prefixed only", legacy: "spotlight-site-a-v1", want: "spotlight-site-a-v1"},
		{
			name:      "unprefixed wins when both set",
			canonical: "spotlight-new-v2",
			legacy:    "spotlight-old-v1",
			want:      "spotlight-new-v2",
		},
		{name: "neither set", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveIndexName("SPOTLIGHT_INDEX", tt.canonical, tt.legacy)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "SPOTLIGHT_INDEX")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestConfig_UnprefixedIndexNamesParse pins that the unprefixed vars are read
// at all — without a struct field for them, env.ParseAs silently ignores them
// and the service falls back to the legacy value or fails as "not set".
func TestConfig_UnprefixedIndexNamesParse(t *testing.T) {
	setRequiredSearchEnv(t)
	t.Setenv("USER_ROOM_INDEX", "userroom-canonical")
	t.Setenv("SPOTLIGHT_INDEX", "spotlight-canonical-v1")
	t.Setenv("SPOTLIGHT_ORG_INDEX", "spotlightorg-canonical-v1")

	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "userroom-canonical", cfg.Indexes.UserRoomIndex)
	assert.Equal(t, "spotlight-canonical-v1", cfg.Indexes.SpotlightIndex)
	assert.Equal(t, "spotlightorg-canonical-v1", cfg.Indexes.SpotlightOrgIndex)
}

// TestConfig_IndexNamesNoLongerRequiredAtParse guards the deploy-order property:
// parsing must succeed with only the unprefixed names present, so a chart can
// migrate to them without the binary refusing to start.
func TestConfig_IndexNamesNoLongerRequiredAtParse(t *testing.T) {
	setRequiredSearchEnv(t)
	for _, k := range []string{"SEARCH_USER_ROOM_INDEX", "SEARCH_SPOTLIGHT_INDEX", "SEARCH_SPOTLIGHT_ORG_INDEX"} {
		require.NoError(t, os.Unsetenv(k))
	}
	t.Setenv("USER_ROOM_INDEX", "userroom-canonical")
	t.Setenv("SPOTLIGHT_INDEX", "spotlight-canonical-v1")
	t.Setenv("SPOTLIGHT_ORG_INDEX", "spotlightorg-canonical-v1")

	_, err := env.ParseAs[Config]()
	require.NoError(t, err, "unprefixed names alone must be enough to parse")
}

func TestConfig_MaxConcurrency(t *testing.T) {
	setRequiredSearchEnv(t)

	t.Run("default", func(t *testing.T) {
		require.NoError(t, os.Unsetenv("MAX_CONCURRENCY"))
		cfg, err := env.ParseAs[Config]()
		require.NoError(t, err)
		assert.Equal(t, 256, cfg.MaxConcurrency)
	})

	t.Run("override", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENCY", "64")
		cfg, err := env.ParseAs[Config]()
		require.NoError(t, err)
		assert.Equal(t, 64, cfg.MaxConcurrency)
	})
}
