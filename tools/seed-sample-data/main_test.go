package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfig_Defaults(t *testing.T) {
	cfg, err := parseConfig(map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "mongodb://localhost:27017/?directConnection=true", cfg.MongoURI)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, []string{"localhost:6379"}, cfg.ValkeyAddrs)
	assert.Empty(t, cfg.ValkeyPassword)
}

func TestParseConfig_OverridesFromEnv(t *testing.T) {
	cfg, err := parseConfig(map[string]string{
		"MONGO_URI":       "mongodb://example:27017",
		"MONGO_DB":        "altdb",
		"VALKEY_ADDRS":    "host1:6379,host2:6379",
		"VALKEY_PASSWORD": "s3cret",
	})
	require.NoError(t, err)
	assert.Equal(t, "mongodb://example:27017", cfg.MongoURI)
	assert.Equal(t, "altdb", cfg.MongoDB)
	assert.Equal(t, []string{"host1:6379", "host2:6379"}, cfg.ValkeyAddrs)
	assert.Equal(t, "s3cret", cfg.ValkeyPassword)
}

func TestDryRunSummary_EmptySiteIsUnfilteredSingleSitePlan(t *testing.T) {
	got := dryRunSummary("")
	for _, want := range []string{
		"site all",
		"users 11",
		"hr_employee 10",
		"rooms 6",
		"subscriptions 23",
		"room_members 19",
		"messages 23",
		"thread_rooms 1",
		"thread_subscriptions 2",
		"mongo:roomKeys 6",
		"valkey:restrictedCache 4",
	} {
		assert.Contains(t, got, want, "dry-run summary missing %q", want)
	}
}

func TestDryRunSummary_HasAllRowCounts(t *testing.T) {
	got := dryRunSummary("site-local")
	for _, want := range []string{
		"site site-local",
		"users 11",
		"hr_employee 10",
		"rooms 5",
		"subscriptions 19",
		"room_members 16",
		"messages 20",
		"thread_rooms 1",
		"thread_subscriptions 2",
		"mongo:roomKeys 5",
		"valkey:restrictedCache 3",
	} {
		assert.Contains(t, got, want, "dry-run summary missing %q", want)
	}
}

func TestDryRunSummary_RemoteSiteHasOnlyRemoteRoom(t *testing.T) {
	got := dryRunSummary("site-remote")
	for _, want := range []string{
		"site site-remote",
		"users 11",
		"hr_employee 10",
		"rooms 1",
		"subscriptions 4",
		"room_members 3",
		"messages 3",
		"thread_rooms 0",
		"thread_subscriptions 0",
		"mongo:roomKeys 1",
		"valkey:restrictedCache 1",
	} {
		assert.Contains(t, got, want, "dry-run summary missing %q", want)
	}
}
