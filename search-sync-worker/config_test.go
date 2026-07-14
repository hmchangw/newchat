package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredConfigEnv sets every `required` env var so env.ParseAs[config]
// succeeds; individual tests then vary only the field under test.
func setRequiredConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-test")
	t.Setenv("SEARCH_URL", "http://localhost:9200")
	t.Setenv("MSG_INDEX_PREFIX", "messages-site-test-v1")
	t.Setenv("SPOTLIGHT_INDEX", "spotlight-site-test-v1")
	t.Setenv("SPOTLIGHT_ORG_INDEX", "spotlightorg-site-test-v1")
	t.Setenv("HR_CENTRAL_SITE_ID", "site-central")
	t.Setenv("USER_ROOM_INDEX", "user-room-mv-site-test")
}

func TestConfig_HRJetStreamDomain(t *testing.T) {
	t.Run("defaults to empty when unset", func(t *testing.T) {
		setRequiredConfigEnv(t)

		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, "", cfg.HRJetStreamDomain)
	})

	t.Run("reads HR_JETSTREAM_DOMAIN when set", func(t *testing.T) {
		setRequiredConfigEnv(t)
		t.Setenv("HR_JETSTREAM_DOMAIN", "hr-hub")

		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, "hr-hub", cfg.HRJetStreamDomain)
	})
}
