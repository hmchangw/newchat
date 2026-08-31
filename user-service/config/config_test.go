package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

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
	testutil.UnsetEnv(t, "MONGO_READ_PREFERENCE") // the default only applies when unset
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

func TestLoad_SortKeyCacheDefaults(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 100000, cfg.SortKeyCacheSize)
	require.Equal(t, 15*time.Second, cfg.SortKeyCacheTTL)
}

func TestLoad_SortKeyCacheOverrideAndDisable(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("SUBS_SORTKEY_CACHE_SIZE", "500")
	t.Setenv("SUBS_SORTKEY_CACHE_TTL", "30s")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 500, cfg.SortKeyCacheSize)
	require.Equal(t, 30*time.Second, cfg.SortKeyCacheTTL)

	// Zero disables the cache (ops escape hatch) — must load, not error.
	t.Setenv("SUBS_SORTKEY_CACHE_TTL", "0s")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), cfg.SortKeyCacheTTL)
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
	require.Empty(t, cfg.Valkey.Addrs, "badge cache must be disabled (no Valkey required) unless VALKEY_ADDRS is set")
}

func TestLoad_ValkeyAddrsParsed(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("VALKEY_ADDRS", "node-1:6379,node-2:6379")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, []string{"node-1:6379", "node-2:6379"}, cfg.Valkey.Addrs)
}

func TestLoad_BadgeDefaults(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	testutil.UnsetEnv(t, "BADGE_CACHE_TTL")
	testutil.UnsetEnv(t, "BADGE_COUNT_CAP")
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
	testutil.UnsetEnv(t, "BADGE_COUNT_CACHE_FIRST")
	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.BadgeCountCacheFirst, "cache-first count must be opt-in (rollout gate)")

	t.Setenv("BADGE_COUNT_CACHE_FIRST", "true")
	cfg, err = Load()
	require.NoError(t, err)
	require.True(t, cfg.BadgeCountCacheFirst)
}

// Load delegates pool checks to mongoutil.PoolConfig.Validate — exhaustive
// cases live in that package; this just proves the wiring.
func TestLoad_DelegatesPoolValidation(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_MAX_POOL_SIZE", "0")
	_, err := Load()
	require.ErrorContains(t, err, "MONGO_MAX_POOL_SIZE")
}

// Load rejects a negative MAX_CONCURRENCY (user-service's bespoke concurrency knob).
func TestLoad_RejectsNegativeMaxConcurrency(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MAX_CONCURRENCY", "-1")
	_, err := Load()
	require.ErrorContains(t, err, "MAX_CONCURRENCY")
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
	testutil.UnsetEnv(t, "BADGE_CACHE_TTL")
	testutil.UnsetEnv(t, "BADGE_MARKER_TTL")
	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, cfg.BadgeMarkerTTL)
}

// The marker must not outlive its set, or a stale zero reads as a fresh one.
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

// The shipped defaults are the contract operators inherit by doing nothing.
func TestLoad_HTTPDefaults(t *testing.T) {
	requiredEnv(t)
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "8080", cfg.HTTP.Port)
	assert.Equal(t, 512, cfg.HTTP.MaxConcurrency)
	assert.Equal(t, 30*time.Second, cfg.HTTP.HandlerTimeout)
	assert.Equal(t, 35*time.Second, cfg.HTTP.WriteTimeout)
	assert.Equal(t, 1024, cfg.HTTP.GzipMinBytes)
	assert.Equal(t, uint64(128), cfg.HTTP.MongoMaxPoolSize)
	assert.Equal(t, uint64(0), cfg.HTTP.MongoMinPoolSize, "no warm floor: a per-member minimum is a standing cluster cost")
	assert.Equal(t, 40, cfg.HTTP.DefaultLimit, "matches the NATS default so an omitted limit behaves the same")
	assert.Equal(t, 400, cfg.HTTP.MaxLimit)
	assert.Equal(t, uint64(150), cfg.Pool.MaxPoolSize, "the NATS-path pool takes the fleet-wide default")
	assert.Equal(t, uint64(0), cfg.Pool.MinPoolSize, "no warm floor: a per-member minimum is a standing cluster cost")
	assert.Equal(t, 5*time.Minute, cfg.Pool.MaxIdleTime, "the driver's own default of 0 never reaps")
	assert.Equal(t, ":8081", cfg.HealthAddr)
	assert.InDelta(t, 0.8, cfg.GoMemLimitFraction, 1e-9)
	assert.Equal(t, 100, cfg.RoomBatchChunk)
	assert.Empty(t, cfg.BotplatformURL)
}

// Every cross-field rule fails fast at startup rather than at first request.
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

// Each knob is reachable from its documented env var.
func TestLoad_HTTPOverrides(t *testing.T) {
	requiredEnv(t)
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("HTTP_MAX_CONCURRENCY", "1024")
	t.Setenv("HTTP_SUBSCRIPTION_DEFAULT_LIMIT", "200")
	t.Setenv("HTTP_SUBSCRIPTION_MAX_LIMIT", "500")
	t.Setenv("BOTPLATFORM_URL", "http://botplatform:8080")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "9090", cfg.HTTP.Port)
	assert.Equal(t, 1024, cfg.HTTP.MaxConcurrency)
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

// A default is required here: the driver's own default of 0 means never reap.
func TestLoad_HTTPMongoMaxIdleTimeDefault(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	testutil.UnsetEnv(t, "HTTP_MONGO_MAX_IDLE_TIME")

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, cfg.HTTP.MongoMaxIdleTime)
}

// Operators tune reaping without a rebuild.
func TestLoad_HTTPMongoMaxIdleTimeOverride(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("HTTP_MONGO_MAX_IDLE_TIME", "90s")

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, 90*time.Second, cfg.HTTP.MongoMaxIdleTime)
}

// A negative duration is a typo, not a request to disable reaping.
func TestLoad_RejectsNegativeHTTPMongoMaxIdleTime(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("HTTP_MONGO_MAX_IDLE_TIME", "-1s")

	_, err := Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_MONGO_MAX_IDLE_TIME")
}

// ginutil.Timeout disables itself at <= 0, so a zero or negative value would
// silently drop the request budget instead of failing loudly at startup.
func TestLoad_RejectsNonPositiveHandlerTimeout(t *testing.T) {
	for _, v := range []string{"0s", "-1s"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("MONGO_URI", "mongodb://x")
			t.Setenv("NATS_URL", "nats://x")
			t.Setenv("SITE_ID", "site-a")
			t.Setenv("HTTP_HANDLER_TIMEOUT", v)

			_, err := Load()

			require.Error(t, err)
			assert.Contains(t, err.Error(), "HTTP_HANDLER_TIMEOUT")
		})
	}
}

// A sidebar renders one line, so the shipped default is what bounds a page's
// preview memory rather than the gatekeeper's 20 KB body ceiling.
func TestLoad_PreviewContentCharsDefault(t *testing.T) {
	requiredEnv(t)
	testutil.UnsetEnv(t, "PREVIEW_CONTENT_CHARS")

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, 50, cfg.PreviewContentChars)
}

// Both the tuned value and the 0 escape hatch have to survive Load.
func TestLoad_PreviewContentCharsOverride(t *testing.T) {
	tests := []struct {
		name string
		set  string
		want int
	}{
		{name: "tuned longer", set: "120", want: 120},
		{name: "zero disables truncation", set: "0", want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requiredEnv(t)
			t.Setenv("PREVIEW_CONTENT_CHARS", tt.set)

			cfg, err := Load()

			require.NoError(t, err)
			assert.Equal(t, tt.want, cfg.PreviewContentChars)
		})
	}
}

// A negative length is a typo, not a request to disable truncation.
func TestLoad_RejectsNegativePreviewContentChars(t *testing.T) {
	requiredEnv(t)
	t.Setenv("PREVIEW_CONTENT_CHARS", "-1")

	_, err := Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "PREVIEW_CONTENT_CHARS")
}

// The NATS pool gets the same reaping and warm-floor knobs as the HTTP one.
func TestLoad_NATSMongoPoolKnobs(t *testing.T) {
	requiredEnv(t)
	t.Setenv("MONGO_MIN_POOL_SIZE", "10")
	t.Setenv("MONGO_MAX_IDLE_TIME", "90s")

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, uint64(10), cfg.Pool.MinPoolSize)
	assert.Equal(t, 90*time.Second, cfg.Pool.MaxIdleTime)
}

// A floor above the ceiling is rejected rather than silently clamped. Asserted on
// the whole message: the HTTP pool's errors differ only by their prefix, so a
// substring match would not prove which pool was rejected.
func TestLoad_RejectsMongoPoolMisconfiguration(t *testing.T) {
	tests := []struct {
		name, env, value, want string
	}{
		{name: "nats min above max", env: "MONGO_MIN_POOL_SIZE", value: "200",
			want: "MONGO_MIN_POOL_SIZE (200) must be <= MONGO_MAX_POOL_SIZE (150)"},
		{name: "nats negative idle time", env: "MONGO_MAX_IDLE_TIME", value: "-1s",
			want: "MONGO_MAX_IDLE_TIME must be >= 0, got -1s"},
		{name: "http min above max", env: "HTTP_MONGO_MIN_POOL_SIZE", value: "200",
			want: "HTTP_MONGO_MIN_POOL_SIZE (200) must be <= HTTP_MONGO_MAX_POOL_SIZE (128)"},
		{name: "http zero max", env: "HTTP_MONGO_MAX_POOL_SIZE", value: "0",
			want: "HTTP_MONGO_MAX_POOL_SIZE must be >= 1, got 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requiredEnv(t)
			t.Setenv(tt.env, tt.value)

			_, err := Load()

			require.Error(t, err)
			assert.Equal(t, tt.want, err.Error())
		})
	}
}

// Trimming is on unless an operator turns it off. Flipping this default would
// silently switch every deployment that does not set the var back to the
// pre-pagefit behaviour of letting the broker refuse the reply.
func TestLoad_PageTrimming(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want bool
	}{
		{name: "defaults to enabled", env: "", want: true},
		{name: "disabled by the operator", env: "false", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requiredEnv(t)
			if tc.env == "" {
				testutil.UnsetEnv(t, "PAGE_TRIMMING_ENABLED")
			} else {
				t.Setenv("PAGE_TRIMMING_ENABLED", tc.env)
			}

			cfg, err := Load()
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.PageTrimming)
		})
	}
}

// Plain collection handles inherit the client preference; only 16 of the 39
// repo methods use a *Secondary handle, so the client is what decides whether
// the rest survive a primary-down incident.
func TestLoad_DefaultsClientReadPreferenceToPrimaryPreferred(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	testutil.UnsetEnv(t, "MONGO_READ_PREFERENCE")
	testutil.UnsetEnv(t, "MONGO_CLIENT_READ_PREFERENCE")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "secondaryPreferred", cfg.Mongo.ReadPreference)
	assert.Equal(t, "primaryPreferred", cfg.Mongo.ClientReadPreference)
}

func TestValidate_RejectsInvalidClientReadPreference(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_CLIENT_READ_PREFERENCE", "quorum")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_CLIENT_READ_PREFERENCE")
}

func TestLoad_BadgeSeedFanoutDefault(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	unsetEnv(t, "MAX_BADGE_SEED_FANOUT")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 8, cfg.MaxBadgeSeedFanout)
}

func TestLoad_BadgeSeedFanoutOverride(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MAX_BADGE_SEED_FANOUT", "4")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 4, cfg.MaxBadgeSeedFanout)
}

// Zero would make the seed semaphore an unbuffered channel whose send blocks
// forever, so it must be rejected at startup rather than deadlock a handler.
func TestLoad_BadgeSeedFanoutInvalidRejected(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MAX_BADGE_SEED_FANOUT", "0")
	_, err := Load()
	require.ErrorContains(t, err, "MAX_BADGE_SEED_FANOUT")
}
