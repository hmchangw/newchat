package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv sets the vars without envDefault; tests override as needed.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("TEAMS_TENANT_ID", "tenant")
	t.Setenv("TEAMS_CLIENT_ID", "client")
	t.Setenv("TEAMS_CLIENT_SECRET", "secret")
	t.Setenv("MONGO_READ_URI", "mongodb://read:27017")
	t.Setenv("MONGO_WRITE_URI", "mongodb://write:27017")
}

func TestConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, 500, cfg.GraphPageSize)
	assert.Empty(t, cfg.GraphBaseURL)
	assert.Empty(t, cfg.GraphTokenURL)
	assert.Equal(t, "chat", cfg.MongoReadDB)
	assert.Equal(t, "chat", cfg.MongoWriteDB)
	assert.Equal(t, "tenant", cfg.TeamsTenantID)
	assert.Equal(t, "mongodb://read:27017", cfg.MongoReadURI)
	assert.Equal(t, "mongodb://write:27017", cfg.MongoWriteURI)
}

func TestConfig_Overrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GRAPH_PAGE_SIZE", "100")
	t.Setenv("GRAPH_BASE_URL", "http://graph.local")
	t.Setenv("GRAPH_TOKEN_URL", "http://token.local")
	t.Setenv("MONGO_READ_DB", "replica")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, 100, cfg.GraphPageSize)
	assert.Equal(t, "http://graph.local", cfg.GraphBaseURL)
	assert.Equal(t, "http://token.local", cfg.GraphTokenURL)
	assert.Equal(t, "replica", cfg.MongoReadDB)
}

func TestConfig_MissingRequiredFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("TEAMS_CLIENT_SECRET", "") // notEmpty rejects the empty string

	_, err := env.ParseAs[config]()
	require.Error(t, err)
}
