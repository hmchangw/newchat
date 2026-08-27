package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("GRAPH_TENANT_ID", "tenant")
	t.Setenv("GRAPH_CLIENT_ID", "client")
	t.Setenv("GRAPH_CLIENT_SECRET", "secret")
	// Cleared so the default-value assertions don't depend on the host env; the
	// proxy tests set them explicitly.
	t.Setenv("GRAPH_PROXY_URL", "")
	t.Setenv("GRAPH_PROXY_USERNAME", "")
	t.Setenv("GRAPH_PROXY_PASSWORD", "")
}

func TestConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, 8, cfg.MaxWorkers)
	assert.True(t, cfg.GraphTLSInsecureSkipVerify, "TLS verification is skipped by default (on-prem behind a TLS-intercepting proxy)")
	assert.Empty(t, cfg.GraphProxyURL, "GRAPH_PROXY_URL defaults to empty (fall back to HTTPS_PROXY/HTTP_PROXY)")
}

func TestConfig_GraphProxyAndTLSOverrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GRAPH_PROXY_URL", "http://proxy.corp:8080")
	t.Setenv("GRAPH_TLS_INSECURE_SKIP_VERIFY", "false")
	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "http://proxy.corp:8080", cfg.GraphProxyURL)
	assert.False(t, cfg.GraphTLSInsecureSkipVerify, "GRAPH_TLS_INSECURE_SKIP_VERIFY=false overrides the true default")
}

func TestConfig_MissingRequired(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MONGO_URI", "") // required,notEmpty
	_, err := env.ParseAs[Config]()
	require.Error(t, err)
}

func baseConfig() Config {
	return Config{
		MongoURI: "mongodb://localhost:27017", MongoDB: "chat",
		Pool:          mongoutil.PoolConfig{MaxPoolSize: 500},
		MaxWorkers:    8,
		GraphTenantID: "tenant", GraphClientID: "client", GraphClientSecret: "secret",
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(c *Config) {}, false},
		{"zero max workers", func(c *Config) { c.MaxWorkers = 0 }, true},
		{"zero max pool size", func(c *Config) { c.Pool.MaxPoolSize = 0 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			err := validateConfig(cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
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
	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "proxyuser", cfg.GraphProxyUsername)
	assert.Equal(t, "p@ss:w/rd", cfg.GraphProxyPassword)
}

func TestConfig_GraphProxyCredentialsDefaultEmpty(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Empty(t, cfg.GraphProxyUsername, "an unauthenticated proxy stays the default")
	assert.Empty(t, cfg.GraphProxyPassword)
}
