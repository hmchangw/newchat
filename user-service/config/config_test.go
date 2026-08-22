package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unsetEnv removes key for the duration of the test and restores its prior
// presence/value on cleanup, so a default-value test can't be perturbed by an
// externally set variable.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, prev)
		}
	})
}

func TestLoad(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("ALL_SITE_IDS", "site-a,site-b")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "site-a", cfg.SiteID)
	require.Equal(t, []string{"site-a", "site-b"}, cfg.AllSiteIDs)
	require.Equal(t, "chat", cfg.Mongo.DB)
	require.Equal(t, 1000, cfg.MaxSubscriptionLimit)
}

func TestLoad_EmptyAllSiteIDs(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("ALL_SITE_IDS", "")
	cfg, err := Load()
	require.NoError(t, err)
	require.Empty(t, cfg.AllSiteIDs) // empty env ⇒ no stray "" site that would later be published to
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	unsetEnv(t, "MONGO_READ_PREFERENCE") // the default only applies when unset
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 1000, cfg.MaxSubscriptionLimit)
	require.Equal(t, 40, cfg.DefaultSubscriptionLimit)
	require.Equal(t, 100, cfg.MaxAppsLimit)
	require.Equal(t, 20, cfg.DefaultAppsLimit)
	require.Equal(t, 15*time.Second, cfg.HandlerTimeout)
	require.Equal(t, 256, cfg.MaxConcurrency)
	require.Equal(t, "secondaryPreferred", cfg.Mongo.ReadPreference)
}

func TestLoad_MaxConcurrency(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")

	t.Run("override", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENCY", "64")
		cfg, err := Load()
		require.NoError(t, err)
		require.Equal(t, 64, cfg.MaxConcurrency)
	})

	// 0 is the documented disable value (unbounded spawn) and must validate.
	t.Run("zero_disables", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENCY", "0")
		cfg, err := Load()
		require.NoError(t, err)
		require.Equal(t, 0, cfg.MaxConcurrency)
	})

	t.Run("negative_rejected", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENCY", "-1")
		_, err := Load()
		require.Error(t, err)
		require.Contains(t, err.Error(), "MAX_CONCURRENCY")
	})
}

func TestLoad_RejectsInvalidReadPreference(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_READ_PREFERENCE", "quorum")
	_, err := Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "MONGO_READ_PREFERENCE")
}

func TestLoad_AcceptsValidReadPreferences(t *testing.T) {
	for _, rp := range []string{"primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"} {
		t.Run(rp, func(t *testing.T) {
			t.Setenv("MONGO_URI", "mongodb://x")
			t.Setenv("NATS_URL", "nats://x")
			t.Setenv("SITE_ID", "site-a")
			t.Setenv("MONGO_READ_PREFERENCE", rp)
			_, err := Load()
			require.NoError(t, err)
		})
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	// notEmpty fires when a required value is unset OR set-but-empty;
	// each required field is exercised independently.
	cases := []struct {
		name                      string
		mongoURI, natsURL, siteID string
	}{
		{"mongo uri empty", "", "nats://x", "site-a"},
		{"nats url empty", "mongodb://x", "", "site-a"},
		{"site id empty", "mongodb://x", "nats://x", ""},
		{"all empty", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MONGO_URI", tc.mongoURI)
			t.Setenv("NATS_URL", tc.natsURL)
			t.Setenv("SITE_ID", tc.siteID)
			_, err := Load()
			require.Error(t, err)
		})
	}
}

func TestLoad_InvalidMaxSubscriptionLimit(t *testing.T) {
	// A non-positive limit must fail at startup, not produce a $limit:0/negative
	// stage that errors at query time.
	for _, v := range []string{"0", "-1"} {
		t.Run("limit="+v, func(t *testing.T) {
			t.Setenv("MONGO_URI", "mongodb://x")
			t.Setenv("NATS_URL", "nats://x")
			t.Setenv("SITE_ID", "site-a")
			t.Setenv("MAX_SUBSCRIPTION_LIMIT", v)
			_, err := Load()
			require.Error(t, err)
		})
	}
}

func TestLoad_InvalidDefaultSubscriptionLimit(t *testing.T) {
	// A non-positive default limit would produce a $limit:0/negative stage.
	for _, v := range []string{"0", "-1"} {
		t.Run("defaultLimit="+v, func(t *testing.T) {
			t.Setenv("MONGO_URI", "mongodb://x")
			t.Setenv("NATS_URL", "nats://x")
			t.Setenv("SITE_ID", "site-a")
			t.Setenv("SUBSCRIPTION_DEFAULT_LIMIT", v)
			_, err := Load()
			require.Error(t, err)
		})
	}
}

func TestLoad_DefaultExceedsMax(t *testing.T) {
	// A default above the max would hand out first pages larger than the ceiling
	// the operator set — reject it at startup rather than silently capping later.
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MAX_SUBSCRIPTION_LIMIT", "50")
	t.Setenv("SUBSCRIPTION_DEFAULT_LIMIT", "100")
	_, err := Load()
	require.Error(t, err)
}

func TestLoad_InvalidMaxAppsLimit(t *testing.T) {
	for _, v := range []string{"0", "-1"} {
		t.Run("max="+v, func(t *testing.T) {
			t.Setenv("MONGO_URI", "mongodb://x")
			t.Setenv("NATS_URL", "nats://x")
			t.Setenv("SITE_ID", "site-a")
			t.Setenv("APPS_MAX_LIMIT", v)
			_, err := Load()
			require.Error(t, err)
		})
	}
}

func TestLoad_InvalidDefaultAppsLimit(t *testing.T) {
	for _, v := range []string{"0", "-1"} {
		t.Run("default="+v, func(t *testing.T) {
			t.Setenv("MONGO_URI", "mongodb://x")
			t.Setenv("NATS_URL", "nats://x")
			t.Setenv("SITE_ID", "site-a")
			t.Setenv("APPS_DEFAULT_LIMIT", v)
			_, err := Load()
			require.Error(t, err)
		})
	}
}

func TestLoad_AppsDefaultExceedsAppsMax(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("APPS_MAX_LIMIT", "10")
	t.Setenv("APPS_DEFAULT_LIMIT", "50")
	_, err := Load()
	require.Error(t, err)
}

func TestLoad_SSODisabledByDefault(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	cfg, err := Load()
	require.NoError(t, err)
	require.Empty(t, cfg.OIDCIssuerURL)
	require.Equal(t, time.Hour, cfg.SSORefreshWindow)
}

func TestLoad_SSOEnabledRequiresAudiencesAndClientID(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("OIDC_ISSUER_URL", "http://keycloak:8080/realms/chatapp")
	_, err := Load()
	require.ErrorContains(t, err, "OIDC_AUDIENCES")

	t.Setenv("OIDC_AUDIENCES", "nats-chat")
	_, err = Load()
	require.ErrorContains(t, err, "OIDC_CLIENT_ID")

	t.Setenv("OIDC_CLIENT_ID", "nats-chat")
	cfg, err := Load()
	require.NoError(t, err)
	require.NotEmpty(t, cfg.OIDCIssuerURL)
	require.Equal(t, []string{"nats-chat"}, cfg.OIDCAudiences)
}

func TestLoad_ValkeyDisabledByDefault(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	cfg, err := Load()
	require.NoError(t, err)
	require.Empty(t, cfg.ValkeyAddrs, "badge cache must be disabled (no Valkey required) unless VALKEY_ADDRS is set")
}

func TestLoad_ValkeyAddrsParsed(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("VALKEY_ADDRS", "node-1:6379,node-2:6379")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, []string{"node-1:6379", "node-2:6379"}, cfg.ValkeyAddrs)
}

func TestLoad_BadgeDefaults(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	unsetEnv(t, "BADGE_CACHE_TTL")
	unsetEnv(t, "BADGE_COUNT_CAP")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 24*time.Hour, cfg.BadgeCacheTTL)
	require.Equal(t, 10, cfg.BadgeCountCap)
}

func TestLoad_BadgeOverrides(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("BADGE_CACHE_TTL", "48h")
	t.Setenv("BADGE_COUNT_CAP", "25")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 48*time.Hour, cfg.BadgeCacheTTL)
	require.Equal(t, 25, cfg.BadgeCountCap)
}

func TestLoad_BadgeInvalidRejected(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")

	t.Setenv("BADGE_CACHE_TTL", "0s")
	_, err := Load()
	require.ErrorContains(t, err, "BADGE_CACHE_TTL")

	t.Setenv("BADGE_CACHE_TTL", "24h")
	t.Setenv("BADGE_COUNT_CAP", "0")
	_, err = Load()
	require.ErrorContains(t, err, "BADGE_COUNT_CAP")
}

func TestLoad_BadgeCountCacheFirst(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	unsetEnv(t, "BADGE_COUNT_CACHE_FIRST")
	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.BadgeCountCacheFirst, "cache-first count must be opt-in (rollout gate)")

	t.Setenv("BADGE_COUNT_CACHE_FIRST", "true")
	cfg, err = Load()
	require.NoError(t, err)
	require.True(t, cfg.BadgeCountCacheFirst)
}

func TestLoad_SSORefreshWindowMustBePositive(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("OIDC_ISSUER_URL", "http://keycloak:8080/realms/chatapp")
	t.Setenv("OIDC_AUDIENCES", "nats-chat")
	t.Setenv("OIDC_CLIENT_ID", "nats-chat")
	t.Setenv("SSO_REFRESH_WINDOW", "0s")
	_, err := Load()
	require.ErrorContains(t, err, "SSO_REFRESH_WINDOW")
}

func TestLoad_BadgeMarkerTTLDefault(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	// An inherited BADGE_MARKER_TTL would change the asserted default, and an
	// inherited short BADGE_CACHE_TTL would make that default invalid.
	unsetEnv(t, "BADGE_CACHE_TTL")
	unsetEnv(t, "BADGE_MARKER_TTL")
	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, cfg.BadgeMarkerTTL)
}

func TestLoad_BadgeMarkerTTLValidation(t *testing.T) {
	tests := []struct {
		name      string
		cacheTTL  string
		markerTTL string
		wantErr   string
	}{
		{"zero is rejected", "24h", "0s", "BADGE_MARKER_TTL"},
		{"negative is rejected", "24h", "-1m", "BADGE_MARKER_TTL"},
		{"sub-second is rejected", "24h", "500ms", "BADGE_MARKER_TTL"},
		{"exactly one second is accepted", "24h", "1s", ""},
		{"longer than cache ttl is rejected", "10m", "1h", "BADGE_MARKER_TTL"},
		{"equal to cache ttl is accepted", "10m", "10m", ""},
		{"shorter than cache ttl is accepted", "24h", "10m", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("MONGO_URI", "mongodb://x")
			t.Setenv("NATS_URL", "nats://x")
			t.Setenv("SITE_ID", "site-a")
			t.Setenv("BADGE_CACHE_TTL", tt.cacheTTL)
			t.Setenv("BADGE_MARKER_TTL", tt.markerTTL)
			_, err := Load()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// requiredEnv sets the minimum env Load needs, so a case can vary one variable.
func requiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
}

func TestLoad_HTTPDefaults(t *testing.T) {
	requiredEnv(t)
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "8080", cfg.HTTP.Port)
	assert.Equal(t, 256, cfg.HTTP.MaxConcurrency)
	assert.Equal(t, 30*time.Second, cfg.HTTP.HandlerTimeout)
	assert.Equal(t, 35*time.Second, cfg.HTTP.WriteTimeout)
	assert.Equal(t, 1024, cfg.HTTP.GzipMinBytes)
	assert.Equal(t, uint64(128), cfg.HTTP.MongoMaxPoolSize)
	assert.Equal(t, uint64(16), cfg.HTTP.MongoMinPoolSize)
	assert.Equal(t, 40, cfg.HTTP.DefaultLimit, "matches the NATS default so an omitted limit behaves the same")
	assert.Equal(t, 400, cfg.HTTP.MaxLimit)
	assert.Equal(t, uint64(100), cfg.Mongo.MaxPoolSize, "the NATS-path pool is now explicit, not the driver default")
	assert.Equal(t, ":8081", cfg.HealthAddr)
	assert.InDelta(t, 0.8, cfg.GoMemLimitFraction, 1e-9)
	assert.Equal(t, 100, cfg.RoomBatchChunk)
	assert.Empty(t, cfg.BotplatformURL)
}

func TestLoad_RejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name, env, val, wantMsg string
	}{
		{"http default above max", "HTTP_SUBSCRIPTION_DEFAULT_LIMIT", "500", "HTTP_SUBSCRIPTION_DEFAULT_LIMIT"},
		{"http default below one", "HTTP_SUBSCRIPTION_DEFAULT_LIMIT", "0", "HTTP_SUBSCRIPTION_DEFAULT_LIMIT"},
		{"http max below one", "HTTP_SUBSCRIPTION_MAX_LIMIT", "0", "HTTP_SUBSCRIPTION_MAX_LIMIT"},
		{"write timeout equals handler timeout", "HTTP_WRITE_TIMEOUT", "30s", "HTTP_WRITE_TIMEOUT"},
		{"write timeout below handler timeout", "HTTP_WRITE_TIMEOUT", "10s", "HTTP_WRITE_TIMEOUT"},
		{"negative concurrency", "HTTP_MAX_CONCURRENCY", "-1", "HTTP_MAX_CONCURRENCY"},
		{"zero mongo pool", "HTTP_MONGO_MAX_POOL_SIZE", "0", "HTTP_MONGO_MAX_POOL_SIZE"},
		{"min pool above max pool", "HTTP_MONGO_MIN_POOL_SIZE", "999", "HTTP_MONGO_MIN_POOL_SIZE"},
		{"zero nats pool", "MONGO_MAX_POOL_SIZE", "0", "MONGO_MAX_POOL_SIZE"},
		{"fraction above one", "GOMEMLIMIT_FRACTION", "1.5", "GOMEMLIMIT_FRACTION"},
		{"fraction zero", "GOMEMLIMIT_FRACTION", "0", "GOMEMLIMIT_FRACTION"},
		{"chunk above the history-service cap", "ROOM_BATCH_CHUNK", "101", "ROOM_BATCH_CHUNK"},
		{"chunk below one", "ROOM_BATCH_CHUNK", "0", "ROOM_BATCH_CHUNK"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requiredEnv(t)
			t.Setenv(tc.env, tc.val)
			_, err := Load()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

func TestLoad_HTTPOverrides(t *testing.T) {
	requiredEnv(t)
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("HTTP_MAX_CONCURRENCY", "512")
	t.Setenv("HTTP_SUBSCRIPTION_DEFAULT_LIMIT", "200")
	t.Setenv("HTTP_SUBSCRIPTION_MAX_LIMIT", "500")
	t.Setenv("BOTPLATFORM_URL", "http://botplatform:8080")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "9090", cfg.HTTP.Port)
	assert.Equal(t, 512, cfg.HTTP.MaxConcurrency)
	assert.Equal(t, 200, cfg.HTTP.DefaultLimit)
	assert.Equal(t, 500, cfg.HTTP.MaxLimit)
	assert.Equal(t, "http://botplatform:8080", cfg.BotplatformURL)
}

// Zero disables the limiter entirely; it must not be rejected as invalid.
func TestLoad_ZeroConcurrencyIsAllowed(t *testing.T) {
	requiredEnv(t)
	t.Setenv("HTTP_MAX_CONCURRENCY", "0")
	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.HTTP.MaxConcurrency)
}

func TestLoad_HTTPMongoMaxIdleTimeDefault(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	unsetEnv(t, "HTTP_MONGO_MAX_IDLE_TIME")

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, cfg.HTTP.MongoMaxIdleTime)
}

func TestLoad_HTTPMongoMaxIdleTimeOverride(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("HTTP_MONGO_MAX_IDLE_TIME", "90s")

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, 90*time.Second, cfg.HTTP.MongoMaxIdleTime)
}

func TestLoad_RejectsNegativeHTTPMongoMaxIdleTime(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("HTTP_MONGO_MAX_IDLE_TIME", "-1s")

	_, err := Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_MONGO_MAX_IDLE_TIME")
}
