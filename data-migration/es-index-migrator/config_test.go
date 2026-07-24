package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	env := map[string]string{
		"SITE_ID":             "site-a",
		"SEARCH_URL":          "http://localhost:9200",
		"MSG_INDEX_PREFIX":    "messages-a-v1",
		"SPOTLIGHT_INDEX":     "spotlight-a-v1",
		"USER_ROOM_INDEX":     "user-room-a",
		"MIGRATION_START_AT":  "2025-07-01T00:00:00Z",
		"MIGRATION_END_AT":    "2026-07-01T00:00:00Z",
		"MESSAGE_BUCKET_HOURS": "72",
		"MONGO_URI":           "mongodb://localhost:27017",
		"CASSANDRA_HOSTS":     "localhost",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

func TestLoadConfig_Valid(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := loadConfig()

	require.NoError(t, err)
	assert.Equal(t, "site-a", cfg.SiteID)
	assert.Equal(t, 500, cfg.BulkBatchSize)
	assert.Equal(t, 4, cfg.WorkerConcurrency)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, "chat", cfg.CassandraKeyspace)
	want, _ := time.Parse(time.RFC3339, "2025-07-01T00:00:00Z")
	assert.True(t, cfg.MigrationStartAt.Equal(want))
}

func TestLoadConfig_MissingRequiredField(t *testing.T) {
	for _, field := range []struct {
		name string
		key  string
	}{
		{"SITE_ID", "SITE_ID"},
		{"MONGO_URI", "MONGO_URI"},
		{"SEARCH_URL", "SEARCH_URL"},
	} {
		t.Run(field.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(field.key, "")

			_, err := loadConfig()

			require.Error(t, err)
		})
	}
}

func TestLoadConfig_EndNotAfterStart(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MIGRATION_START_AT", "2026-01-01T00:00:00Z")
	t.Setenv("MIGRATION_END_AT", "2026-01-01T00:00:00Z")

	_, err := loadConfig()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "MIGRATION_END_AT")
}

func TestLoadConfig_NonPositiveBatchSize(t *testing.T) {
	for _, v := range []string{"0", "-5"} {
		t.Run(v, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("BULK_BATCH_SIZE", v)

			_, err := loadConfig()

			require.Error(t, err)
			assert.Contains(t, err.Error(), "BULK_BATCH_SIZE")
		})
	}
}

func TestLoadConfig_NonPositiveWorkerConcurrency(t *testing.T) {
	for _, v := range []string{"0", "-1"} {
		t.Run(v, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("WORKER_CONCURRENCY", v)

			_, err := loadConfig()

			require.Error(t, err)
		})
	}
}

func TestLoadConfig_NonPositiveMessageBucketHours(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MESSAGE_BUCKET_HOURS", "0")

	_, err := loadConfig()

	require.Error(t, err)
}
