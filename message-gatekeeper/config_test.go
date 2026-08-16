package main

import (
	"os"
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

// An unset OTEL_SERVICE_NAME must fall back to the documented default rather
// than an empty label: service_name is the dimension every NATS metric family
// is joined through, and an empty one silently orphans the series.
func TestConfig_ServiceNameDefaultsWhenUnset(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("OTEL_SERVICE_NAME", "")
	require.NoError(t, os.Unsetenv("OTEL_SERVICE_NAME"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "unknown-service", cfg.ServiceName)
}
