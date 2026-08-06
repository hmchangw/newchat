package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredTranslationEnv seeds the required env so env.ParseAs succeeds and
// the test can assert on the optional MAX_CONCURRENCY knob in isolation.
func setRequiredTranslationEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("TRANSLATION_BACKEND", "mock")
	t.Setenv("NATS_URL", "nats://localhost:4222")
}

func TestConfig_MaxConcurrencyDefault(t *testing.T) {
	setRequiredTranslationEnv(t)

	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, 100, cfg.MaxConcurrency, "MAX_CONCURRENCY defaults to 100 (preserving the prior MAX_WORKERS default)")
}

func TestConfig_MaxConcurrencyOverride(t *testing.T) {
	setRequiredTranslationEnv(t)
	t.Setenv("MAX_CONCURRENCY", "42")

	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, 42, cfg.MaxConcurrency)
}
