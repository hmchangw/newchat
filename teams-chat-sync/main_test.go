package main

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("GRAPH_TENANT_ID", "tenant")
	t.Setenv("GRAPH_CLIENT_ID", "client")
	t.Setenv("GRAPH_CLIENT_SECRET", "secret")
	t.Setenv("SYNC_DEFAULT_SITE_ID", "site-default")
}

func TestConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, 8, cfg.MaxWorkers)
	assert.Equal(t, 240*time.Hour, cfg.RunTimeout, "default RUN_TIMEOUT is 10 days (240h; Go durations can't express 'd')")
	assert.Equal(t, "2026-04-01T00:00:00Z", cfg.DefaultFrom)
	assert.Equal(t, "site-default", cfg.DefaultSiteID, "SYNC_DEFAULT_SITE_ID is required,notEmpty")
	assert.Equal(t, 50, cfg.GraphChatsPageSize, "default $top is Graph's max for list chats")
	assert.False(t, cfg.GraphTLSInsecureSkipVerify)

	from, err := time.Parse(time.RFC3339, cfg.DefaultFrom)
	require.NoError(t, err, "the default watermark must be valid RFC3339")
	assert.True(t, from.Equal(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)))
}

func TestConfig_MissingRequired(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MONGO_URI", "") // required,notEmpty — empty must fail
	_, err := env.ParseAs[Config]()
	require.Error(t, err)
}

func TestConfig_MissingDefaultSiteID(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SYNC_DEFAULT_SITE_ID", "") // required,notEmpty — empty must fail
	_, err := env.ParseAs[Config]()
	require.Error(t, err)
}

func baseConfig() Config {
	return Config{
		MongoURI: "mongodb://localhost:27017", MongoDB: "chat",
		MaxWorkers: 8, RunTimeout: 30 * time.Minute,
		DefaultFrom:        "2026-04-01T00:00:00Z",
		GraphTenantID:      "tenant",
		GraphClientID:      "client",
		GraphClientSecret:  "secret",
		GraphChatsPageSize: 50,
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		want    time.Time
		wantErr bool
	}{
		{
			name:   "valid",
			mutate: func(c *Config) {},
			want:   time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "zero max workers",
			mutate:  func(c *Config) { c.MaxWorkers = 0 },
			wantErr: true,
		},
		{
			name:    "negative max workers",
			mutate:  func(c *Config) { c.MaxWorkers = -1 },
			wantErr: true,
		},
		{
			name:    "zero run timeout",
			mutate:  func(c *Config) { c.RunTimeout = 0 },
			wantErr: true,
		},
		{
			name:    "negative run timeout",
			mutate:  func(c *Config) { c.RunTimeout = -time.Second },
			wantErr: true,
		},
		{
			name:    "zero chats page size",
			mutate:  func(c *Config) { c.GraphChatsPageSize = 0 },
			wantErr: true,
		},
		{
			name:    "negative chats page size",
			mutate:  func(c *Config) { c.GraphChatsPageSize = -1 },
			wantErr: true,
		},
		{
			name:    "invalid default from",
			mutate:  func(c *Config) { c.DefaultFrom = "not-a-date" },
			wantErr: true,
		},
		{
			name:    "empty default from",
			mutate:  func(c *Config) { c.DefaultFrom = "" },
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			got, err := validateConfig(cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got))
		})
	}
}
