package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/mongoutil"
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
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
}

func TestConfig_PoolValidate_RejectsZeroMaxPoolSize(t *testing.T) {
	setRequiredConfigEnv(t)
	t.Setenv("MONGO_MAX_POOL_SIZE", "0")
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Error(t, cfg.Pool.Validate())
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

// The resolver already treats a missing user as a normal outcome
// (teams_user_store.go:31), so secondary lag widens a race this code already
// accepts rather than opening a new one.
func TestConfig_ReadPreferenceDefault(t *testing.T) {
	setRequiredConfigEnv(t)
	t.Setenv("MONGO_READ_PREFERENCE", "")                    // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("MONGO_READ_PREFERENCE")) // the default only applies when unset

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, "secondaryPreferred", cfg.ReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	assert.Equal(t, readpref.SecondaryPreferredMode, rp.Mode())
}

func TestConfig_PipelineDepth(t *testing.T) {
	t.Run("defaults to 2 so one bulk request overlaps the next batch's build", func(t *testing.T) {
		setRequiredConfigEnv(t)

		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 2, cfg.PipelineDepth)
	})

	t.Run("reads PIPELINE_DEPTH when set", func(t *testing.T) {
		setRequiredConfigEnv(t)
		t.Setenv("PIPELINE_DEPTH", "4")

		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 4, cfg.PipelineDepth)
	})
}
