package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requiredEnv is the minimum env needed for a successful parse; each test
// removes exactly one entry to assert startup refuses to guess it.
func requiredEnv() map[string]string {
	return map[string]string{
		"NATS_URL":        "nats://localhost:4222",
		"SITE_ID":         "site-local",
		"MONGO_URI":       "mongodb://localhost:27017",
		"CASSANDRA_HOSTS": "cassandra:9042",
	}
}

func TestParseConfig_AcceptsRequiredEnv(t *testing.T) {
	for key, value := range requiredEnv() {
		t.Setenv(key, value)
	}

	cfg, err := env.ParseAs[config]()

	require.NoError(t, err)
	assert.Equal(t, "cassandra:9042", cfg.CassandraHosts)
}

// A defaulted connection string turns a missing env var into a connection to
// the wrong cluster (CLAUDE.md: never default connection strings).
func TestParseConfig_RejectsMissingCassandraHosts(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		for key, value := range requiredEnv() {
			if key != "CASSANDRA_HOSTS" {
				t.Setenv(key, value)
			}
		}
		unsetEnv(t, "CASSANDRA_HOSTS")

		_, err := env.ParseAs[config]()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "CASSANDRA_HOSTS")
	})

	t.Run("empty", func(t *testing.T) {
		for key, value := range requiredEnv() {
			t.Setenv(key, value)
		}
		t.Setenv("CASSANDRA_HOSTS", "")

		_, err := env.ParseAs[config]()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "CASSANDRA_HOSTS")
	})
}

// unsetEnv removes key for the duration of the test and restores its prior
// presence/value on cleanup, so an externally set variable can't mask the case
// under test.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, prev)
		}
	})
}
