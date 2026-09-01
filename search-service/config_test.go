package main

import (
	"os"
	"testing"
	"time"

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
	t.Setenv("USER_ROOM_INDEX", "userroom-site-a-v1")
	t.Setenv("SPOTLIGHT_INDEX", "spotlight-site-a-v1")
	t.Setenv("SPOTLIGHT_ORG_INDEX", "spotlightorg-site-a-v1")
	t.Setenv("VALKEY_ADDRS", "localhost:7000")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("USERS_API_URL", "http://localhost:8080")
}

// Pins the three ES index vars to the unprefixed spelling shared with
// search-sync-worker and es-index-migrator. On SearchConfig they would silently
// pick up envPrefix:"SEARCH_" and the reader could drift from the writer.
func TestConfig_IndexNamesAreUnprefixed(t *testing.T) {
	setRequiredSearchEnv(t)
	t.Setenv("USER_ROOM_INDEX", "userroom-site-a")
	t.Setenv("SPOTLIGHT_INDEX", "spotlight-site-a-v1")
	t.Setenv("SPOTLIGHT_ORG_INDEX", "spotlightorg-site-a-v1")
	// A stale SEARCH_-prefixed value must have no effect whatsoever.
	t.Setenv("SEARCH_SPOTLIGHT_INDEX", "spotlight-WRONG-v9")

	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "userroom-site-a", cfg.UserRoomIndex)
	assert.Equal(t, "spotlight-site-a-v1", cfg.SpotlightIndex)
	assert.Equal(t, "spotlightorg-site-a-v1", cfg.SpotlightOrgIndex)
}

// An unset or empty index name has no safe default — an empty USER_ROOM_INDEX
// used to parse cleanly and fail per-request inside the terms-lookup; notEmpty
// moves that failure to startup.
func TestConfig_IndexNamesRequired(t *testing.T) {
	for _, missing := range []string{"USER_ROOM_INDEX", "SPOTLIGHT_INDEX", "SPOTLIGHT_ORG_INDEX"} {
		t.Run("unset "+missing, func(t *testing.T) {
			setRequiredSearchEnv(t)
			require.NoError(t, os.Unsetenv(missing))
			_, err := env.ParseAs[Config]()
			require.Error(t, err)
			assert.Contains(t, err.Error(), missing)
		})
		t.Run("empty "+missing, func(t *testing.T) {
			setRequiredSearchEnv(t)
			t.Setenv(missing, "")
			_, err := env.ParseAs[Config]()
			require.Error(t, err)
			assert.Contains(t, err.Error(), missing)
		})
	}
}

func TestConfig_MaxConcurrency(t *testing.T) {
	setRequiredSearchEnv(t)

	t.Run("default", func(t *testing.T) {
		require.NoError(t, os.Unsetenv("MAX_CONCURRENCY"))
		cfg, err := env.ParseAs[Config]()
		require.NoError(t, err)
		assert.Equal(t, 256, cfg.Guard.MaxConcurrency)
	})

	t.Run("override", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENCY", "64")
		cfg, err := env.ParseAs[Config]()
		require.NoError(t, err)
		assert.Equal(t, 64, cfg.Guard.MaxConcurrency)
	})
}

// The four cache knobs are the operator's only control over enrichment
// staleness and pod memory, so both the name and the default are pinned.
func TestConfig_EnrichmentCacheKnobs(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		setRequiredSearchEnv(t)
		for _, k := range []string{"SEARCH_HR_CACHE_SIZE", "SEARCH_HR_CACHE_TTL", "SEARCH_APP_CACHE_SIZE", "SEARCH_APP_CACHE_TTL"} {
			require.NoError(t, os.Unsetenv(k))
		}

		cfg, err := env.ParseAs[Config]()

		require.NoError(t, err)
		assert.Equal(t, 130000, cfg.Search.HRCacheSize)
		assert.Equal(t, 24*time.Hour, cfg.Search.HRCacheTTL)
		assert.Equal(t, 1000, cfg.Search.AppCacheSize)
		assert.Equal(t, 24*time.Hour, cfg.Search.AppCacheTTL)
	})

	t.Run("overrides", func(t *testing.T) {
		setRequiredSearchEnv(t)
		t.Setenv("SEARCH_HR_CACHE_SIZE", "16")
		t.Setenv("SEARCH_HR_CACHE_TTL", "90s")
		t.Setenv("SEARCH_APP_CACHE_SIZE", "8")
		t.Setenv("SEARCH_APP_CACHE_TTL", "30s")

		cfg, err := env.ParseAs[Config]()

		require.NoError(t, err)
		assert.Equal(t, 16, cfg.Search.HRCacheSize)
		assert.Equal(t, 90*time.Second, cfg.Search.HRCacheTTL)
		assert.Equal(t, 8, cfg.Search.AppCacheSize)
		assert.Equal(t, 30*time.Second, cfg.Search.AppCacheTTL)
	})

	// Zero is the documented disable switch, not a parse error — Task 3's
	// constructor turns it into a pass-through store.
	t.Run("zero disables", func(t *testing.T) {
		setRequiredSearchEnv(t)
		t.Setenv("SEARCH_HR_CACHE_SIZE", "0")
		t.Setenv("SEARCH_APP_CACHE_TTL", "0s")

		cfg, err := env.ParseAs[Config]()

		require.NoError(t, err)
		assert.Equal(t, 0, cfg.Search.HRCacheSize)
		assert.Equal(t, time.Duration(0), cfg.Search.AppCacheTTL)
	})
}
