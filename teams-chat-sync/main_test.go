package main

import (
	"testing"
	"time"

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
	assert.Equal(t, 30*time.Minute, cfg.RunTimeout)
	assert.Equal(t, "2026-04-01T00:00:00Z", cfg.DefaultFrom)
	assert.False(t, cfg.GraphTLSInsecureSkipVerify)

	from, err := time.Parse(time.RFC3339, cfg.DefaultFrom)
	require.NoError(t, err, "the default watermark must be valid RFC3339")
	assert.True(t, from.Equal(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)))
}

func TestConfig_MissingRequired(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	_, err := env.ParseAs[Config]()
	require.Error(t, err)
}
