package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
)

func TestConfig_ServiceName(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("OTEL_SERVICE_NAME", "message-gatekeeper-canary")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "message-gatekeeper-canary", cfg.ServiceName)
}
