package main

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseFrom(t *testing.T, kv map[string]string) (config, error) {
	t.Helper()
	return env.ParseAsWithOptions[config](env.Options{Environment: kv})
}

func fullEnv() map[string]string {
	return map[string]string{
		"SITE_ID": "site1", "NATS_URL": "nats://x:4222",
		"SOURCE_MONGO_URI": "mongodb://s:27017", "TARGET_MONGO_URI": "mongodb://t:27017",
		"MAPPING_FILE": "/tmp/m.json",
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg, err := parseFrom(t, fullEnv())
	require.NoError(t, err)
	assert.Equal(t, 8091, cfg.Port)
	assert.Equal(t, "rocketchat", cfg.SourceDB)
	assert.Equal(t, "chat", cfg.TargetDB)
	assert.Equal(t, 2*time.Second, cfg.VerifyPoll)
	assert.Equal(t, 60*time.Second, cfg.VerifyTimeout)
	assert.Equal(t, 32, cfg.MaxChecks)
	assert.Equal(t, 100, cfg.SamplePercent)
	assert.Equal(t, 200, cfg.RecentCap)
	assert.Equal(t, 1000, cfg.FailedCap)
	assert.Equal(t, 5*time.Second, cfg.StatsInterval)
	assert.Equal(t, 360, cfg.MessageBucketHours)
	assert.Equal(t, "primary", cfg.SourceReadPreference)
	assert.False(t, cfg.Bootstrap.Enabled)
	assert.NoError(t, cfg.validate())
}

func TestConfig_BootstrapEnabled(t *testing.T) {
	kv := fullEnv()
	kv["BOOTSTRAP_STREAMS"] = "true"
	cfg, err := parseFrom(t, kv)
	require.NoError(t, err)
	assert.True(t, cfg.Bootstrap.Enabled)
}

func TestConfig_MissingRequired(t *testing.T) {
	kv := fullEnv()
	delete(kv, "SITE_ID")
	_, err := parseFrom(t, kv)
	assert.Error(t, err)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*config)
		wantErr string
	}{
		{"sample percent over 100", func(c *config) { c.SamplePercent = 101 }, "SAMPLE_PERCENT"},
		{"sample percent negative", func(c *config) { c.SamplePercent = -1 }, "SAMPLE_PERCENT"},
		{"zero poll", func(c *config) { c.VerifyPoll = 0 }, "VERIFY_POLL"},
		{"timeout below poll", func(c *config) { c.VerifyTimeout = time.Second }, "VERIFY_TIMEOUT"},
		{"zero max checks", func(c *config) { c.MaxChecks = 0 }, "MAX_CHECKS"},
		{"negative max checks", func(c *config) { c.MaxChecks = -1 }, "MAX_CHECKS"},
		{"zero recent cap", func(c *config) { c.RecentCap = 0 }, "RECENT_CAP"},
		{"zero failed cap", func(c *config) { c.FailedCap = 0 }, "RECENT_CAP and FAILED_CAP"},
		{"zero stats interval", func(c *config) { c.StatsInterval = 0 }, "STATS_INTERVAL"},
		{"negative stats interval", func(c *config) { c.StatsInterval = -time.Second }, "STATS_INTERVAL"},
		{"zero bucket hours", func(c *config) { c.MessageBucketHours = 0 }, "MESSAGE_BUCKET_HOURS"},
		{"bad start time", func(c *config) { c.StartAtTime = "not-a-time" }, "START_AT_TIME"},
		{"ok start time", func(c *config) { c.StartAtTime = "2026-08-10T00:00:00Z" }, ""},
		{"bad read preference", func(c *config) { c.SourceReadPreference = "quorum" }, "SOURCE_READ_PREFERENCE"},
		{"ok read preference", func(c *config) { c.SourceReadPreference = "secondaryPreferred" }, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseFrom(t, fullEnv())
			require.NoError(t, err)
			tt.mutate(&cfg)
			err = cfg.validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
