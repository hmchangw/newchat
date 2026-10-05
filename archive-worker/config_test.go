package main

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/stream"
)

func validConfig() config {
	return config{
		SiteID: "site-a", FillInterval: 10 * time.Second, BatchEvents: 2000, BatchBytes: 8 << 20,
		PutTimeout: 10 * time.Second, BulkTimeout: 10 * time.Second, WriteAttempts: 2, FetchBatch: 100,
		Replicas: 1, BlobMaxBytes: 100 << 20, BlobAckWait: 10 * time.Minute, BlobWorkers: 4,
		Consumer:       stream.ConsumerSettings{AckWait: 60 * time.Second, MaxAckPending: 12000},
		Vault:          atrest.VaultConfig{Address: "https://vault.test:8200"},
		IndexRetention: "2555d",
	}
}

func TestConfig_Validate(t *testing.T) {
	t.Run("defaults are valid", func(t *testing.T) {
		require.NoError(t, validConfig().validate())
	})
	cases := []struct {
		name   string
		mutate func(*config)
		want   string
	}{
		{"missing vault address", func(c *config) { c.Vault.Address = "" }, "VAULT_ADDR"},
		{"batch time exceeds ack wait", func(c *config) { c.Consumer.AckWait = 40 * time.Second }, "ACK_WAIT"},
		{"zero batch events", func(c *config) { c.BatchEvents = 0 }, "ARCHIVE_BATCH_EVENTS"},
		{"zero batch bytes", func(c *config) { c.BatchBytes = 0 }, "ARCHIVE_BATCH_BYTES"},
		{"zero attempts", func(c *config) { c.WriteAttempts = 0 }, "ARCHIVE_WRITE_ATTEMPTS"},
		{"zero fetch", func(c *config) { c.FetchBatch = 0 }, "ARCHIVE_FETCH_BATCH"},
		{"fetch larger than batch", func(c *config) { c.FetchBatch = 3000 }, "ARCHIVE_FETCH_BATCH"},
		{"zero blob workers", func(c *config) { c.BlobWorkers = 0 }, "ARCHIVE_BLOB_WORKERS"},
		{"bad retention", func(c *config) { c.IndexRetention = "forever" }, "ARCHIVE_INDEX_RETENTION"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(&c)
			err := c.validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestCheckBatchAckCoupling(t *testing.T) {
	assert.Equal(t, "", checkBatchAckCoupling(2000, 12000, 3))
	w := checkBatchAckCoupling(2000, 1000, 3)
	assert.Contains(t, w, "12000")
	assert.Contains(t, w, "CONSUMER_MAX_ACK_PENDING")
}

// The shared stream.ConsumerSettings defaults (ACK_WAIT 30s, MAX_ACK_PENDING 1000)
// are below what this worker needs, so CONSUMER_ACK_WAIT and CONSUMER_MAX_ACK_PENDING
// must be set explicitly. This pins that requirement instead of hiding it behind validConfig().
func TestConfig_ConsumerSettingsMustBeSet(t *testing.T) {
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("ARCHIVE_SEARCH_URL", "http://es:9200")
	t.Setenv("ARCHIVE_S3_ENDPOINT", "minio:9000")
	t.Setenv("ARCHIVE_S3_ACCESS_KEY", "ak")
	t.Setenv("ARCHIVE_S3_SECRET_KEY", "sk")
	t.Setenv("ARCHIVE_BUCKET", "archive-site-a")
	t.Setenv("VAULT_ADDR", "https://vault.test:8200")

	t.Run("required vars only is rejected naming CONSUMER_ACK_WAIT", func(t *testing.T) {
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		err = cfg.validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CONSUMER_ACK_WAIT")
		assert.NotEmpty(t, checkBatchAckCoupling(cfg.BatchEvents, cfg.Consumer.MaxAckPending, cfg.Replicas),
			"the shared MAX_ACK_PENDING default is below replicas x 2 x ARCHIVE_BATCH_EVENTS")
	})

	t.Run("documented production values are accepted", func(t *testing.T) {
		t.Setenv("CONSUMER_ACK_WAIT", "60s")
		t.Setenv("CONSUMER_MAX_ACK_PENDING", "12000")
		t.Setenv("ARCHIVE_REPLICAS", "3")
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		require.NoError(t, cfg.validate())
		assert.Empty(t, checkBatchAckCoupling(cfg.BatchEvents, cfg.Consumer.MaxAckPending, cfg.Replicas))
	})

	t.Run("local compose values are accepted", func(t *testing.T) {
		t.Setenv("CONSUMER_ACK_WAIT", "60s")
		t.Setenv("CONSUMER_MAX_ACK_PENDING", "4000")
		t.Setenv("ARCHIVE_REPLICAS", "1")
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		require.NoError(t, cfg.validate())
		assert.Empty(t, checkBatchAckCoupling(cfg.BatchEvents, cfg.Consumer.MaxAckPending, cfg.Replicas))
	})
}
