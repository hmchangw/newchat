package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv sets the vars without envDefault; tests override as needed.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("VALKEY_ADDRS", "node-1:6379")
	t.Setenv("GRAPH_TENANT_ID", "tenant")
	t.Setenv("GRAPH_CLIENT_ID", "client")
	t.Setenv("GRAPH_CLIENT_SECRET", "secret")
	t.Setenv("GRAPH_ROPC_USERNAME", "svc@corp.com")
	t.Setenv("GRAPH_ROPC_PASSWORD", "pw")
}

// TestConfig_GraphProxyCredentials covers the authenticating-proxy settings for
// the presence Graph client. They are kept out of GRAPH_PROXY_URL so a password
// carrying URL metacharacters needs no percent-encoding, and so the secret is a
// field of its own rather than half of a connection string.
func TestConfig_GraphProxyCredentials(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GRAPH_PROXY_URL", "http://proxy.corp:8080")
	t.Setenv("GRAPH_PROXY_USERNAME", "proxyuser")
	t.Setenv("GRAPH_PROXY_PASSWORD", "p@ss:w/rd")

	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "http://proxy.corp:8080", cfg.GraphProxyURL)
	assert.Equal(t, "proxyuser", cfg.GraphProxyUsername)
	assert.Equal(t, "p@ss:w/rd", cfg.GraphProxyPassword)
}

func TestConfig_GraphProxyCredentialsDefaultEmpty(t *testing.T) {
	setRequiredEnv(t)
	require.NoError(t, os.Unsetenv("GRAPH_PROXY_URL"))
	require.NoError(t, os.Unsetenv("GRAPH_PROXY_USERNAME"))
	require.NoError(t, os.Unsetenv("GRAPH_PROXY_PASSWORD"))

	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Empty(t, cfg.GraphProxyURL, "GRAPH_PROXY_URL defaults to empty (fall back to HTTPS_PROXY/HTTP_PROXY)")
	assert.Empty(t, cfg.GraphProxyUsername, "an unauthenticated proxy stays the default")
	assert.Empty(t, cfg.GraphProxyPassword)
}
