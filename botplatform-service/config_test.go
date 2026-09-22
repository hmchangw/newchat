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

func TestConfig_ReadPreferenceDefault(t *testing.T) {
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MONGO_READ_PREFERENCE", "")                    // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("MONGO_READ_PREFERENCE")) // the default only applies when unset

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, "primaryPreferred", cfg.ReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	assert.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}

// The two admission caps must read distinct operator-facing names, or one env
// var silently drives both surfaces.
func TestConfig_AdmissionCapsAreSeparateKnobs(t *testing.T) {
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("MAX_CONCURRENCY", "7")
	t.Setenv("BOT_MAX_CONCURRENCY", "99")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, 7, cfg.Login.MaxConcurrency, "MAX_CONCURRENCY drives the login route")
	assert.Equal(t, 99, cfg.BotRoutes.MaxConcurrency, "BOT_MAX_CONCURRENCY drives the bot routes")
}

func TestConfig_AdmissionCapDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	require.NoError(t, os.Unsetenv("MAX_CONCURRENCY"))
	require.NoError(t, os.Unsetenv("BOT_MAX_CONCURRENCY"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, 32, cfg.Login.MaxConcurrency)
	assert.Equal(t, 32, cfg.BotRoutes.MaxConcurrency, "on by default; an unset knob must not leave the cap off")
}

// A negative cap reads as "disabled" inside MaxConcurrency, so it has to be
// refused at load rather than silently turning the control off.
func TestValidateConfig_RejectsNegativeAdmissionCaps(t *testing.T) {
	for _, knob := range []string{"MAX_CONCURRENCY", "BOT_MAX_CONCURRENCY"} {
		t.Run(knob, func(t *testing.T) {
			t.Setenv("SITE_ID", "site-a")
			t.Setenv("MONGO_URI", "mongodb://localhost:27017")
			t.Setenv("NATS_URL", "nats://localhost:4222")
			t.Setenv("BCRYPT_COST", "10")
			t.Setenv(knob, "-1")

			cfg, err := env.ParseAs[config]()
			require.NoError(t, err)
			err = validateConfig(&cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "MAX_CONCURRENCY")
		})
	}
}
