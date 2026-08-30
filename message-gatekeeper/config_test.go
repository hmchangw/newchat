package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

func TestConfig_ReadPreferenceDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MONGO_READ_PREFERENCE", "")                    // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("MONGO_READ_PREFERENCE")) // the default only applies when unset

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.ReadPreference,
		"gatekeeper authorises sends from Mongo; a primary-only read takes messaging down with the primary")

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}

func TestConfig_ReadPreferenceRejectsGarbage(t *testing.T) {
	_, err := mongoutil.ParseReadPreference("quorum")
	require.Error(t, err)
}

func TestConfig_ThreadParentRecheckDelayDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("GATEKEEPER_THREAD_PARENT_RECHECK_DELAY", "")                    // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("GATEKEEPER_THREAD_PARENT_RECHECK_DELAY")) // the default only applies when unset

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, defaultThreadParentRecheckDelay, cfg.ThreadParentRecheckDelay,
		"a reply racing its parent's write must get one re-check before the send is rejected")
}

func TestConfig_ThreadParentRecheckDelayIsTunable(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("GATEKEEPER_THREAD_PARENT_RECHECK_DELAY", "0s")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Zero(t, cfg.ThreadParentRecheckDelay, "operators can switch the re-check off")
}
