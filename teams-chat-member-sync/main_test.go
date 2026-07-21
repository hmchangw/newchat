package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("GRAPH_TENANT_ID", "tenant")
	t.Setenv("GRAPH_CLIENT_ID", "client")
	t.Setenv("GRAPH_CLIENT_SECRET", "secret")
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
