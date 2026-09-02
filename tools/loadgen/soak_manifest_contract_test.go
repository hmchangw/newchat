package main

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSoakManifest_BSONContract(t *testing.T) {
	now := time.Date(2026, 8, 29, 1, 2, 3, 0, time.UTC)
	manifest := soakManifest{
		ID: "run-a", State: soakManifestRunning, RunMode: soakRunModeContinuous,
		SiteID: "site-a", MongoDatabase: "chat", CassandraKeyspace: "chat",
		ConfigDigest: "digest", BorrowedUserCount: 2, ActiveUserCount: 3,
		ActiveUserIDs: []string{"user-a", "user-b", "user-c"},
		RoomCount:     4, SubscriptionCount: 5, StartedAt: now, UpdatedAt: now,
		SeededAt: &now, CleanedAt: &now, FirstStartedAt: &now, Deadline: &now,
		CompletedAt: &now, LastStoppedAt: &now, LastHeartbeatAt: &now,
		ConfiguredDuration: 24 * time.Hour, RestartCount: 2,
	}

	encoded, err := bson.Marshal(manifest)
	require.NoError(t, err)
	var document bson.M
	require.NoError(t, bson.Unmarshal(encoded, &document))

	assert.Equal(t, []string{
		"_id", "activeUserCount", "activeUserIds", "borrowedUserCount",
		"cassandraKeyspace", "cleanedAt", "completedAt", "configDigest",
		"configuredDuration", "deadline", "firstStartedAt", "lastHeartbeatAt",
		"lastStoppedAt", "mongoDatabase", "restartCount", "roomCount", "runMode",
		"seededAt", "siteId", "startedAt", "state", "subscriptionCount", "updatedAt",
	}, sortedBSONKeys(document))

	var roundTripped soakManifest
	require.NoError(t, bson.Unmarshal(encoded, &roundTripped))
	assert.Equal(t, manifest, roundTripped)
}

func TestSoakManifest_LegacyBSONRemainsReadable(t *testing.T) {
	now := time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC)
	legacy := bson.D{
		{Key: "_id", Value: "legacy-run"},
		{Key: "state", Value: string(soakManifestSeeded)},
		{Key: "siteId", Value: "site-a"},
		{Key: "mongoDatabase", Value: "chat"},
		{Key: "cassandraKeyspace", Value: "chat"},
		{Key: "configDigest", Value: "digest"},
		{Key: "borrowedUserCount", Value: 1},
		{Key: "activeUserCount", Value: 1},
		{Key: "activeUserIds", Value: bson.A{"user-a"}},
		{Key: "roomCount", Value: 1},
		{Key: "subscriptionCount", Value: 1},
		{Key: "startedAt", Value: now},
		{Key: "updatedAt", Value: now},
	}
	encoded, err := bson.Marshal(legacy)
	require.NoError(t, err)

	var manifest soakManifest
	require.NoError(t, bson.Unmarshal(encoded, &manifest))
	assert.Equal(t, "legacy-run", manifest.ID)
	assert.Equal(t, soakManifestSeeded, manifest.State)
	assert.Empty(t, manifest.RunMode)
	assert.Nil(t, manifest.FirstStartedAt)
	assert.Nil(t, manifest.Deadline)
	assert.Nil(t, manifest.LastHeartbeatAt)
	assert.Zero(t, manifest.ConfiguredDuration)
	assert.Zero(t, manifest.RestartCount)

	reencoded, err := bson.Marshal(manifest)
	require.NoError(t, err)
	var document bson.M
	require.NoError(t, bson.Unmarshal(reencoded, &document))
	for _, key := range []string{
		"firstStartedAt", "deadline", "completedAt", "lastStoppedAt",
		"lastHeartbeatAt", "configuredDuration", "restartCount",
	} {
		assert.NotContains(t, document, key)
	}
}

func sortedBSONKeys(document bson.M) []string {
	keys := make([]string, 0, len(document))
	for key := range document {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
