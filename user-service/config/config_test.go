package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, 1000, cfg.MaxSubscriptionLimit)
	require.Equal(t, 40, cfg.DefaultSubscriptionLimit)
	require.Equal(t, 100, cfg.MaxAppsLimit)
	require.Equal(t, 20, cfg.DefaultAppsLimit)
	require.Equal(t, 15*time.Second, cfg.HandlerTimeout)
}

// AC-4.3: the settings payload limit defaults to the documented 64 KiB cap.
func TestLoad_AC_4_3_DefaultSettingsLimit(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")

	cfg, err := Load()

	require.NoError(t, err)
	require.Equal(t, 64*1024, cfg.MaxSettingsBytes)
}

// AC-4.3: USER_SERVICE_MAX_SETTINGS_BYTES overrides the default used by handlers.
func TestLoad_AC_4_3_CustomSettingsLimit(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("USER_SERVICE_MAX_SETTINGS_BYTES", "32768")

	cfg, err := Load()

	require.NoError(t, err)
	require.Equal(t, 32768, cfg.MaxSettingsBytes)
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
