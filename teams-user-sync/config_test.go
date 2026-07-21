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
	assert.True(t, cfg.GraphTLSInsecureSkipVerify, "TLS verification is skipped by default (on-prem behind a TLS-intercepting proxy)")
	assert.Empty(t, cfg.GraphProxyURL, "GRAPH_PROXY_URL defaults to empty (fall back to HTTPS_PROXY/HTTP_PROXY)")
	assert.Equal(t, "tenant", cfg.TeamsTenantID)

	assert.Equal(t, "mongodb://read:27017", cfg.MongoReadURI)
	assert.Equal(t, "chat", cfg.MongoReadDB)
	assert.Empty(t, cfg.MongoReadUsername)
	assert.Empty(t, cfg.MongoReadPassword)
	assert.Equal(t, "mongodb://write:27017", cfg.MongoWriteURI)
	assert.Equal(t, "chat", cfg.MongoWriteDB)
	assert.Empty(t, cfg.MongoWriteUsername)
	assert.Empty(t, cfg.MongoWritePassword)
}

func TestConfig_Overrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GRAPH_PAGE_SIZE", "100")
	t.Setenv("GRAPH_BASE_URL", "http://graph.local")
	t.Setenv("GRAPH_TOKEN_URL", "http://token.local")
	t.Setenv("GRAPH_PROXY_URL", "http://proxy.corp:8080")
	t.Setenv("GRAPH_TLS_INSECURE_SKIP_VERIFY", "false")
	t.Setenv("MONGO_READ_DB", "readdb")
	t.Setenv("MONGO_READ_USERNAME", "reader")
	t.Setenv("MONGO_READ_PASSWORD", "readpw")
	t.Setenv("MONGO_WRITE_DB", "writedb")
	t.Setenv("MONGO_WRITE_USERNAME", "writer")
	t.Setenv("MONGO_WRITE_PASSWORD", "writepw")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, 100, cfg.GraphPageSize)
	assert.Equal(t, "http://graph.local", cfg.GraphBaseURL)
	assert.Equal(t, "http://token.local", cfg.GraphTokenURL)
	assert.Equal(t, "http://proxy.corp:8080", cfg.GraphProxyURL)
	assert.False(t, cfg.GraphTLSInsecureSkipVerify, "GRAPH_TLS_INSECURE_SKIP_VERIFY=false overrides the true default")

	assert.Equal(t, "readdb", cfg.MongoReadDB)
	assert.Equal(t, "reader", cfg.MongoReadUsername)
	assert.Equal(t, "readpw", cfg.MongoReadPassword)
	assert.Equal(t, "writedb", cfg.MongoWriteDB)
	assert.Equal(t, "writer", cfg.MongoWriteUsername)
	assert.Equal(t, "writepw", cfg.MongoWritePassword)
}

func TestConfig_MissingRequiredFails(t *testing.T) {
	tests := []struct {
		name  string
		unset string
	}{
		{"missing client secret", "TEAMS_CLIENT_SECRET"},
		{"missing read uri", "MONGO_READ_URI"},
		{"missing write uri", "MONGO_WRITE_URI"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tt.unset, "") // required,notEmpty rejects the empty string

			_, err := env.ParseAs[config]()
			require.Error(t, err)
		})
	}
}
