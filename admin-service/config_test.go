package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_Defaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x:4222")
	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Equal(t, "8082", cfg.Port)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, 10, cfg.BcryptCost)
	assert.Equal(t, 5*time.Second, cfg.RoomRPCTimeout)
	assert.Equal(t, 30*time.Second, cfg.FanoutTimeout)
}

func TestLoadConfig_RequiresSiteAndMongo(t *testing.T) {
	_, err := loadConfig()
	assert.Error(t, err)
}

// NATS_URL is required: the room RPC has no fallback transport, so a missing
// value must fail at startup rather than at the first duty toggle.
func TestLoadConfig_RequiresNatsURL(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://x")
	_, err := loadConfig()
	assert.Error(t, err)
}

func TestLoadConfig_TimeoutOverrides(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x:4222")
	t.Setenv("ROOM_RPC_TIMEOUT", "12s")
	t.Setenv("FANOUT_TIMEOUT", "20s")
	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Equal(t, 12*time.Second, cfg.RoomRPCTimeout)
	assert.Equal(t, 20*time.Second, cfg.FanoutTimeout)
}

// A non-positive timeout yields an already-expired context, so every call would
// fail; a timeout at or above the HTTP write timeout lets net/http close the
// connection before the handler can answer. Both must fail at startup, for every
// per-request budget the handler waits on.
func TestLoadConfig_RejectsUnusableHandlerTimeouts(t *testing.T) {
	values := []struct {
		name  string
		value string
	}{
		{name: "zero", value: "0s"},
		{name: "negative", value: "-1s"},
		{name: "equal to write timeout", value: httpWriteTimeout.String()},
		{name: "above write timeout", value: (httpWriteTimeout + 5*time.Second).String()},
	}

	for _, envName := range []string{"ROOM_RPC_TIMEOUT", "FANOUT_TIMEOUT"} {
		for _, tc := range values {
			t.Run(envName+"/"+tc.name, func(t *testing.T) {
				t.Setenv("SITE_ID", "site-local")
				t.Setenv("MONGO_URI", "mongodb://x")
				t.Setenv("NATS_URL", "nats://x:4222")
				t.Setenv(envName, tc.value)
				_, err := loadConfig()
				require.Error(t, err)
				assert.Contains(t, err.Error(), envName)
			})
		}
	}
}
