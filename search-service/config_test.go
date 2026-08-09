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
