package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SITE_ID", "site1")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SOURCE_MONGO_URI", "mongodb://localhost:27017")
}

func TestParseConfig_Defaults(t *testing.T) {
	setEnv(t)
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "site1", cfg.SiteID)
	assert.Equal(t, "rocketchat", cfg.SourceDB)
	assert.Equal(t, "rocketchat_message", cfg.SourceMessageCollection)
	assert.Equal(t, "rm", cfg.SoftDeleteType)
	assert.Equal(t, "primaryPreferred", cfg.SourceReadPreference)
	assert.Equal(t, "oplog-transformer", cfg.ConsumerDurable)
	assert.Equal(t, 1000, cfg.MaxDeliver)
	assert.Equal(t, 60, cfg.DeleteMaxDeliver)
}

func TestParseConfig_SoftDeleteTypeOverride(t *testing.T) {
	setEnv(t)
	t.Setenv("SOFT_DELETE_TYPE", "deleted")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "deleted", cfg.SoftDeleteType)
}

func TestParseConfig_ClampsDeleteMaxDeliver(t *testing.T) {
	setEnv(t)
	t.Setenv("MAX_DELIVER", "100")
	t.Setenv("DELETE_MAX_DELIVER", "500") // exceeds MaxDeliver -> clamped down
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, 100, cfg.MaxDeliver)
	assert.Equal(t, 100, cfg.DeleteMaxDeliver, "DELETE_MAX_DELIVER is clamped to MAX_DELIVER")
}

func TestParseConfig_NoClampWhenMaxDeliverUnlimited(t *testing.T) {
	setEnv(t)
	t.Setenv("MAX_DELIVER", "0") // 0 = unlimited; the delete cap must NOT be clamped to unlimited too
	t.Setenv("DELETE_MAX_DELIVER", "60")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, 60, cfg.DeleteMaxDeliver, "delete cap stays finite even when MAX_DELIVER is unlimited")
}

func TestParseConfig_DeleteMaxDeliverNotClampedWhenBelow(t *testing.T) {
	setEnv(t)
	t.Setenv("MAX_DELIVER", "1000")
	t.Setenv("DELETE_MAX_DELIVER", "60")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, 60, cfg.DeleteMaxDeliver, "a DELETE_MAX_DELIVER below MAX_DELIVER is left untouched")
}

func TestParseConfig_MissingRequired(t *testing.T) {
	t.Setenv("SITE_ID", "site1")
	_, err := parseConfig()
	require.Error(t, err)
}
