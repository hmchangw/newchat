package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv sets the four required env vars that every test needs to succeed.
func setRequiredEnv(t *testing.T) {
	t.Helper()
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
	assert.Equal(t, "nats://localhost:4222", cfg.NatsURL)
	assert.Equal(t, "", cfg.NatsCredsFile)

	assert.Equal(t, "mongodb://localhost:27017", cfg.SourceMongoURI)
	assert.Equal(t, "", cfg.SourceUsername)
	assert.Equal(t, "", cfg.SourcePassword)
	assert.Equal(t, "rocketchat", cfg.SourceDB)

	assert.Equal(t, "mongodb://localhost:27018", cfg.TargetMongoURI)
	assert.Equal(t, "", cfg.TargetUsername)
	assert.Equal(t, "", cfg.TargetPassword)
	assert.Equal(t, "chat", cfg.TargetDB)

	assert.Equal(t, "rocketchat_rooms", cfg.RoomsCollection)
	assert.Equal(t, "rocketchat_subscriptions", cfg.SubscriptionsCollection)
	assert.Equal(t, "company_thread_subscriptions", cfg.ThreadSubsCollection)
	assert.Equal(t, "users", cfg.UsersCollection)

	assert.Equal(t, "primaryPreferred", cfg.SourceReadPreference)
	assert.Equal(t, "oplog-collections-transformer", cfg.ConsumerDurable)
	assert.Equal(t, 1000, cfg.MaxDeliver)
	assert.Equal(t, 60, cfg.DeleteMaxDeliver)
	assert.Equal(t, false, cfg.Bootstrap.Enabled)
	assert.Equal(t, ":9090", cfg.HealthAddr)
}

func TestParseConfig_MissingRequired(t *testing.T) {
	// Only SITE_ID set — NATS_URL, SOURCE_MONGO_URI, TARGET_MONGO_URI missing.
	t.Setenv("SITE_ID", "site1")
	_, err := parseConfig()
	require.Error(t, err)
}

func TestParseConfig_EmptyCollectionNameFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ROOMS_COLLECTION", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ROOMS_COLLECTION")
}

func TestParseConfig_EmptySubscriptionsCollectionFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SUBSCRIPTIONS_COLLECTION", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SUBSCRIPTIONS_COLLECTION")
}

func TestParseConfig_EmptyThreadSubsCollectionFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("THREAD_SUBS_COLLECTION", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "THREAD_SUBS_COLLECTION")
}

func TestParseConfig_EmptyUsersCollectionFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("USERS_COLLECTION", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "USERS_COLLECTION")
}

func TestParseConfig_ClampsDeleteMaxDeliver(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MAX_DELIVER", "100")
	t.Setenv("DELETE_MAX_DELIVER", "500") // exceeds MaxDeliver → clamped
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, 100, cfg.MaxDeliver)
	assert.Equal(t, 100, cfg.DeleteMaxDeliver, "DELETE_MAX_DELIVER is clamped to MAX_DELIVER")
}

func TestParseConfig_DeleteMaxDeliverStaysWhenMaxDeliverZero(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MAX_DELIVER", "0") // 0 = unlimited; delete cap must NOT be clamped to unlimited
	t.Setenv("DELETE_MAX_DELIVER", "60")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, 60, cfg.DeleteMaxDeliver, "delete cap stays finite when MAX_DELIVER is unlimited")
}

func TestParseConfig_DeleteMaxDeliverNotClampedWhenBelow(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MAX_DELIVER", "1000")
	t.Setenv("DELETE_MAX_DELIVER", "60")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, 60, cfg.DeleteMaxDeliver, "a DELETE_MAX_DELIVER below MAX_DELIVER is left untouched")
}

func TestParseConfig_BootstrapEnabled(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("BOOTSTRAP_STREAMS", "true")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.True(t, cfg.Bootstrap.Enabled)
}

func TestParseConfig_CustomCollectionNames(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ROOMS_COLLECTION", "my_rooms")
	t.Setenv("SUBSCRIPTIONS_COLLECTION", "my_subs")
	t.Setenv("THREAD_SUBS_COLLECTION", "my_thread_subs")
	t.Setenv("USERS_COLLECTION", "my_users")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "my_rooms", cfg.RoomsCollection)
	assert.Equal(t, "my_subs", cfg.SubscriptionsCollection)
	assert.Equal(t, "my_thread_subs", cfg.ThreadSubsCollection)
	assert.Equal(t, "my_users", cfg.UsersCollection)
}

func TestParseConfig_WhitespaceSiteID_Errors(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SITE_ID", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SITE_ID must be non-empty")
}

func TestParseConfig_WhitespaceSourceMongoURI_Errors(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SOURCE_MONGO_URI", "  ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOURCE_MONGO_URI must be non-empty")
}

func TestParseConfig_RoomMembersCollectionDefault(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "company_room_members", cfg.RoomMembersCollection)
}

func TestParseConfig_RoomMembersCollectionBlankFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ROOM_MEMBERS_COLLECTION", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ROOM_MEMBERS_COLLECTION")
}
