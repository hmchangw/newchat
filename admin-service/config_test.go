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

func TestLoadConfig_RoomRPCTimeoutOverride(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x:4222")
	t.Setenv("ROOM_RPC_TIMEOUT", "12s")
	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Equal(t, 12*time.Second, cfg.RoomRPCTimeout)
}

// A non-positive timeout yields an already-expired context, so every toggle
// would fail; a timeout at or above the HTTP write timeout lets net/http close
// the connection before the handler can answer. Both must fail at startup.
func TestLoadConfig_RejectsUnusableRoomRPCTimeout(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "zero", value: "0s"},
		{name: "negative", value: "-1s"},
		{name: "equal to write timeout", value: "30s"},
		{name: "above write timeout", value: "45s"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SITE_ID", "site-local")
			t.Setenv("MONGO_URI", "mongodb://x")
			t.Setenv("NATS_URL", "nats://x:4222")
			t.Setenv("ROOM_RPC_TIMEOUT", tc.value)
			_, err := loadConfig()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ROOM_RPC_TIMEOUT")
		})
	}
}
