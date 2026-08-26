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

func TestLoadConfig_ZeroMaxPoolSizeFails(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x:4222")
	t.Setenv("MONGO_MAX_POOL_SIZE", "0")
	_, err := loadConfig()
	assert.Error(t, err)
}

// TestLoadConfig_ClientUpdateDefaults covers a site that HAS opted in.
func TestLoadConfig_ClientUpdateDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("SVCJWT_PRIVATE_KEY", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=")
	t.Setenv("CLIENT_UPDATE_BASE_URL", "http://client-update-service:8080")
	t.Setenv("CLIENT_UPDATE_SERVICE_ACCOUNT", "svc-updater")

	cfg, err := loadConfig()
	require.NoError(t, err)

	assert.Equal(t, "admin-service", cfg.SvcJWTIssuer)
	assert.Equal(t, "client-update-service", cfg.ClientUpdateAudience)
	assert.Equal(t, 5*time.Minute, cfg.SvcJWTTTL)
	assert.Equal(t, 10*time.Minute, cfg.ClientUpdateUploadTimeout)
}

// TestLoadConfig_ClientUpdateOptional pins that publishing is opt-in per site.
// admin-service runs everywhere; requiring the signing key would put a copy of
// the Ed25519 PRIVATE key on every site merely to boot.
func TestLoadConfig_ClientUpdateOptional(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("NATS_URL", "nats://nats:4222")

	cfg, err := loadConfig()
	require.NoError(t, err, "a site that does not publish client updates must still start")
	assert.Empty(t, cfg.ClientUpdateBaseURL)
	assert.Empty(t, cfg.SvcJWTPrivateKey)
}

// TestLoadConfig_ClientUpdateHalfConfigured proves opt-in is all-or-nothing: a
// site that sets the base URL but omits the key or the account fails at startup
// rather than at the first upload.
func TestLoadConfig_ClientUpdateHalfConfigured(t *testing.T) {
	tests := []struct {
		name, key, account string
	}{
		{"base URL without signing key", "", "svc-updater"},
		{"base URL without service account", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SITE_ID", "site-local")
			t.Setenv("MONGO_URI", "mongodb://mongo:27017")
			t.Setenv("NATS_URL", "nats://nats:4222")
			t.Setenv("CLIENT_UPDATE_BASE_URL", "http://client-update-service:8080")
			t.Setenv("SVCJWT_PRIVATE_KEY", tc.key)
			t.Setenv("CLIENT_UPDATE_SERVICE_ACCOUNT", tc.account)

			_, err := loadConfig()
			assert.Error(t, err)
		})
	}
}

// TestLoadConfig_UploadTimeoutEscapesHandlerBudget documents that the upload
// route deliberately exceeds httpWriteTimeout: it sets its own per-request
// deadlines (see extendDeadlines), so checkHandlerTimeout must NOT be applied
// to it. This guards against someone "fixing" the apparent inconsistency.
func TestLoadConfig_UploadTimeoutEscapesHandlerBudget(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("SVCJWT_PRIVATE_KEY", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=")
	t.Setenv("CLIENT_UPDATE_BASE_URL", "http://client-update-service:8080")
	t.Setenv("CLIENT_UPDATE_SERVICE_ACCOUNT", "svc-updater")
	t.Setenv("CLIENT_UPDATE_UPLOAD_TIMEOUT", "10m")

	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Greater(t, cfg.ClientUpdateUploadTimeout, httpWriteTimeout,
		"the upload route sets its own deadlines and is expected to outlive the server write timeout")
}
