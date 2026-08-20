package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MaxDeliver is matched to the shared pkg/stream default so one service isn't
// quietly stricter than the rest. Note this is an attempt budget, not a time
// window: handler.go NAKs without a delay, so redelivery is immediate.
func TestConfig_Defaults(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MODE", "user")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, 20, cfg.MaxDeliver)
	assert.Equal(t, 100, cfg.MaxWorkers)
	assert.Equal(t, ":8081", cfg.HealthAddr)
}

func TestConfig_MaxDeliverOverride(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MODE", "user")
	t.Setenv("MAX_DELIVER", "7")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, 7, cfg.MaxDeliver)
}
