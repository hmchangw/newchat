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

// All five key-touching services must resolve MONGO_KEY_READ_PREFERENCE to the
// same wire name and default. If they disagree, broadcast-worker encrypts against
// one preference while key.get serves from another, and clients get messages
// whose keys they cannot fetch.
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
	assert.Equal(t, "primaryPreferred", cfg.MongoKeyReadPreference)
}
