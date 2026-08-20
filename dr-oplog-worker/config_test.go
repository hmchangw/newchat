package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Setenv("SITE_ID", "site1")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("TARGET_MONGO_URI", "mongodb://localhost:27017")
}

func TestParseConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "site1", cfg.SiteID)
	assert.Equal(t, "chat_dr_", cfg.TargetDBPrefix)
	assert.Equal(t, "chat_dr_site1", cfg.TargetDB())
	assert.Equal(t, "dr-oplog-worker", cfg.ConsumerDurable)
	assert.Equal(t, 1000, cfg.MaxDeliver)
	assert.Contains(t, cfg.DRCollections, "rooms")
	assert.Contains(t, cfg.DRCollections, "subscriptions")
	assert.Contains(t, cfg.DRCollections, "users")
	assert.Len(t, cfg.DRCollections, 6)
}

// The DR applier is self-contained (updateLookup on the producer): it MUST NOT require a
// source Mongo connection. Parsing succeeds with no SOURCE_MONGO_URI set.
func TestParseConfig_NoSourceMongoRequired(t *testing.T) {
	setRequiredEnv(t)
	_, err := parseConfig()
	require.NoError(t, err)
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
	t.Setenv("DR_COLLECTIONS", "rooms,,users")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DR_COLLECTIONS")
}

func TestParseConfig_RejectsDuplicateCollection(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("DR_COLLECTIONS", "rooms,rooms")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestParseConfig_TargetDBHonoursPrefix(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("TARGET_DB_PREFIX", "lifeboat_")
	t.Setenv("SITE_ID", "tokyo")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "lifeboat_tokyo", cfg.TargetDB())
}
