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
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MONGO_READ_PREFERENCE", "")                    // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("MONGO_READ_PREFERENCE")) // the default only applies when unset

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, "primaryPreferred", cfg.ReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	assert.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}
