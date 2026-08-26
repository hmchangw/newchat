package main

import (
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Defaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("SVCJWT_PUBLIC_KEY", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=")
	t.Setenv("ALLOWED_SERVICE_ACCOUNTS", "svc-updater")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, 4, cfg.CacheMaxEntries)
	assert.Equal(t, 24*time.Hour, cfg.CacheTTL)
	assert.Equal(t, int64(536870912), cfg.CacheMaxObjectBytes)
	assert.Equal(t, 5*time.Minute, cfg.MinioDownloadTimeout)
	assert.Equal(t, 10*time.Minute, cfg.HTTPWriteTimeout)
}

func TestConfig_RequiresEachRequiredVar(t *testing.T) {
	// env/v11 treats an empty string as "defined", so a missing-var test must
	// os.Unsetenv the target rather than rely on the host environment being clean.
	required := []string{
		"SITE_ID", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET",
		"SVCJWT_PUBLIC_KEY", "ALLOWED_SERVICE_ACCOUNTS",
	}
	for _, missing := range required {
		t.Run("missing_"+missing, func(t *testing.T) {
			for _, k := range required {
				t.Setenv(k, "seed") // t.Setenv restores the original value on cleanup
			}
			require.NoError(t, os.Unsetenv(missing))
			_, err := env.ParseAs[config]()
			assert.Error(t, err, "parse must fail when %s is unset", missing)
		})
	}
}

func TestConfig_AuthDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "key")
	t.Setenv("MINIO_SECRET_KEY", "secret")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("SVCJWT_PUBLIC_KEY", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=")
	t.Setenv("ALLOWED_SERVICE_ACCOUNTS", "svc-updater, svc-other")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, "admin-service", cfg.SvcJWTIssuer)
	assert.Equal(t, "client-update-service", cfg.SvcJWTAudience)
	assert.Equal(t, []string{"svc-updater", " svc-other"}, cfg.AllowedServiceAccounts,
		"env parsing splits on comma only; the middleware trims the entries")
	assert.Equal(t, 10*time.Minute, cfg.HTTPReadTimeout,
		"the read timeout must cover a full upload body, matching the write timeout default")
}

// NOTE: do NOT test a missing required var with t.Setenv(k, "") — env/v11 treats
// an empty string as "defined", so such a test passes vacuously. The existing
// TestConfig_RequiresEachRequiredVar in this file documents the correct idiom:
// seed every required var, then os.Unsetenv the one under test. That existing
// test is extended below rather than duplicated here.
