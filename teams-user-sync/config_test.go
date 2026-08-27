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
	t.Setenv("GRAPH_TENANT_ID", "tenant")
	t.Setenv("GRAPH_CLIENT_ID", "client")
	t.Setenv("GRAPH_CLIENT_SECRET", "secret")
	t.Setenv("MONGO_READ_URI", "mongodb://read:27017")
	t.Setenv("MONGO_WRITE_URI", "mongodb://write:27017")
	// Cleared so the default-value assertions don't depend on the host env; the
	// proxy tests set them explicitly.
	t.Setenv("GRAPH_PROXY_URL", "")
	t.Setenv("GRAPH_PROXY_USERNAME", "")
	t.Setenv("GRAPH_PROXY_PASSWORD", "")
}

func TestConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, 500, cfg.GraphPageSize)
	assert.True(t, cfg.GraphTLSInsecureSkipVerify, "TLS verification is skipped by default (on-prem behind a TLS-intercepting proxy)")
	assert.Empty(t, cfg.GraphProxyURL, "GRAPH_PROXY_URL defaults to empty (fall back to HTTPS_PROXY/HTTP_PROXY)")
	assert.Equal(t, "tenant", cfg.GraphTenantID)

	assert.Equal(t, mongoConfig{URI: "mongodb://read:27017", DB: "chat"}, cfg.MongoRead)
	assert.Equal(t, mongoConfig{URI: "mongodb://write:27017", DB: "chat"}, cfg.MongoWrite)
}

func TestConfig_Overrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GRAPH_PAGE_SIZE", "100")
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
	assert.Equal(t, "http://proxy.corp:8080", cfg.GraphProxyURL)
	assert.False(t, cfg.GraphTLSInsecureSkipVerify, "GRAPH_TLS_INSECURE_SKIP_VERIFY=false overrides the true default")

	assert.Equal(t, mongoConfig{URI: "mongodb://read:27017", DB: "readdb", Username: "reader", Password: "readpw"}, cfg.MongoRead)
	assert.Equal(t, mongoConfig{URI: "mongodb://write:27017", DB: "writedb", Username: "writer", Password: "writepw"}, cfg.MongoWrite)
}

func TestConfig_ZeroMaxPoolSizeFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MONGO_MAX_POOL_SIZE", "0")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Error(t, cfg.Pool.Validate(), "a zero MONGO_MAX_POOL_SIZE must be rejected")
}

func TestConfig_MissingRequiredFails(t *testing.T) {
	tests := []struct {
		name  string
		unset string
	}{
		{"missing client secret", "GRAPH_CLIENT_SECRET"},
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

// TestConfig_GraphProxyCredentials covers the authenticating-proxy settings.
// They are kept out of GRAPH_PROXY_URL so a password carrying URL
// metacharacters needs no percent-encoding, and so the secret is a field of its
// own rather than half of a connection string.
func TestConfig_GraphProxyCredentials(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GRAPH_PROXY_URL", "http://proxy.corp:8080")
	t.Setenv("GRAPH_PROXY_USERNAME", "proxyuser")
	t.Setenv("GRAPH_PROXY_PASSWORD", "p@ss:w/rd")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, "proxyuser", cfg.GraphProxyUsername)
	assert.Equal(t, "p@ss:w/rd", cfg.GraphProxyPassword)
}

func TestConfig_GraphProxyCredentialsDefaultEmpty(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Empty(t, cfg.GraphProxyUsername, "an unauthenticated proxy stays the default")
	assert.Empty(t, cfg.GraphProxyPassword)
}
