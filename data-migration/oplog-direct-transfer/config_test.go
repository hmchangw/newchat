package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Setenv("SITE_ID", "site1")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SOURCE_MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("TARGET_MONGO_URI", "mongodb://localhost:27018")
}

func TestParseConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "site1", cfg.SiteID)
	assert.Equal(t, "rocketchat", cfg.SourceDB)
	assert.Equal(t, "chat", cfg.TargetDB)
	assert.Equal(t, "oplog-direct-transfer", cfg.ConsumerDurable)
	assert.Contains(t, cfg.DirectCollections, "rocketchat_avatar")
	assert.Contains(t, cfg.DirectCollections, "user_devices")
	assert.Len(t, cfg.DirectCollections, 8)
}

func TestParseConfig_TrimsAndValidatesRequired(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SITE_ID", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SITE_ID")
}

func TestParseConfig_RejectsEmptyCollectionEntry(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("DIRECT_COLLECTIONS", "rocketchat_avatar,,user_devices")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DIRECT_COLLECTIONS")
}

func TestParseConfig_RejectsDuplicateCollection(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("DIRECT_COLLECTIONS", "rocketchat_avatar,rocketchat_avatar")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}
