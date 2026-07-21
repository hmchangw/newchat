package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SITE_ID", "site1")
	t.Setenv("SOURCE_MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("WATCH_COLLECTIONS", "rocketchat_message,rocketchat_room,users")
}

func TestParseConfig_DefaultsAndSlices(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := parseConfig()
	require.NoError(t, err)

	assert.Equal(t, "site1", cfg.SiteID)
	assert.Equal(t, []string{"rocketchat_message", "rocketchat_room", "users"}, cfg.WatchCollections)
	assert.Equal(t, "rocketchat_message", cfg.MessageCollection)
	assert.Equal(t, "migration", cfg.CheckpointDB)
	assert.Equal(t, "rocketchat", cfg.SourceDB)
	assert.Equal(t, "secondary", cfg.ReadPreference)
	assert.Equal(t, 100, cfg.CheckpointEvery)
	assert.Equal(t, 30, cfg.CheckpointMaxAgeSeconds)
	assert.Equal(t, ":9090", cfg.HealthAddr)
	assert.Equal(t, "now", cfg.StartMode)
	assert.False(t, cfg.Bootstrap.Enabled)
}

func TestParseConfig_RejectsDuplicateCollections(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WATCH_COLLECTIONS", "rocketchat_message,users,rocketchat_message")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestParseConfig_RejectsEmptyCollectionEntry(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WATCH_COLLECTIONS", "rocketchat_message,,users")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty entry")
}

func TestParseConfig_TrimsCollectionWhitespace(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WATCH_COLLECTIONS", " rocketchat_message , users ")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, []string{"rocketchat_message", "users"}, cfg.WatchCollections)
}

func TestParseConfig_MessageCollectionNotWatchedPasses(t *testing.T) {
	setRequiredEnv(t)
	// Collections role: the message collection is tailed by a separate deployment, so its absence is legitimate.
	t.Setenv("WATCH_COLLECTIONS", "rocketchat_room,users")
	t.Setenv("MESSAGE_COLLECTION", "rocketchat_message")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.False(t, cfg.watchesMessages())
}

func TestParseConfig_WatchesMessagesWhenPresent(t *testing.T) {
	setRequiredEnv(t) // WATCH_COLLECTIONS includes rocketchat_message
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.True(t, cfg.watchesMessages())
}

func TestParseConfig_EmptyMessageCollectionFails(t *testing.T) {
	setRequiredEnv(t)
	// An empty/whitespace MESSAGE_COLLECTION would match no watcher → the $match never runs.
	t.Setenv("MESSAGE_COLLECTION", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MESSAGE_COLLECTION")
}

func TestParseConfig_DefaultMessageCollectionWatchedPasses(t *testing.T) {
	setRequiredEnv(t)
	// Default WATCH_COLLECTIONS includes the default MESSAGE_COLLECTION.
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "rocketchat_message", cfg.MessageCollection)
	assert.Contains(t, cfg.WatchCollections, cfg.MessageCollection)
}

func TestParseConfig_MissingRequiredFails(t *testing.T) {
	t.Setenv("SITE_ID", "site1")
	// SOURCE_MONGO_URI / NATS_URL / WATCH_COLLECTIONS missing.
	_, err := parseConfig()
	require.Error(t, err)
}

func TestParseConfig_InvalidStartMode(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("START_MODE", "yesterday")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "START_MODE")
}

func TestParseConfig_RejectsBeginningMode(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("START_MODE", "beginning")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "START_MODE")
}

func TestParseConfig_TimeModeRequiresStartAtTime(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("START_MODE", "time")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "START_AT_TIME")
}

func TestParseConfig_BootstrapEnabled(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("BOOTSTRAP_STREAMS", "true")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.True(t, cfg.Bootstrap.Enabled)
}
