package main

import (
	"fmt"
	"regexp"
	"time"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig groups fields only meaningful when standing up dev/integration
// against a NATS instance whose streams don't exist yet; in production streams
// are pre-provisioned and Enabled must stay false.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the service creates the
	// MESSAGES-CANONICAL stream at startup. Leave false in production.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID,required"`

	SearchURL           string `env:"ARCHIVE_SEARCH_URL,required"`
	SearchBackend       string `env:"ARCHIVE_SEARCH_BACKEND"         envDefault:"elasticsearch"`
	SearchUsername      string `env:"ARCHIVE_SEARCH_USERNAME"        envDefault:""`
	SearchPassword      string `env:"ARCHIVE_SEARCH_PASSWORD"        envDefault:""`
	SearchTLSSkipVerify bool   `env:"ARCHIVE_SEARCH_TLS_SKIP_VERIFY" envDefault:"false"`
	IndexRetention      string `env:"ARCHIVE_INDEX_RETENTION"        envDefault:"2555d"`

	S3Endpoint        string `env:"ARCHIVE_S3_ENDPOINT,required"`
	S3AccessKey       string `env:"ARCHIVE_S3_ACCESS_KEY,required"`
	S3SecretKey       string `env:"ARCHIVE_S3_SECRET_KEY,required"`
	S3UseSSL          bool   `env:"ARCHIVE_S3_USE_SSL"          envDefault:"false"`
	Bucket            string `env:"ARCHIVE_BUCKET,required"`
	RequireObjectLock bool   `env:"ARCHIVE_REQUIRE_OBJECT_LOCK" envDefault:"true"`

	FillInterval  time.Duration `env:"ARCHIVE_FILL_INTERVAL"  envDefault:"10s"`
	BatchEvents   int           `env:"ARCHIVE_BATCH_EVENTS"   envDefault:"2000"`
	BatchBytes    int           `env:"ARCHIVE_BATCH_BYTES"    envDefault:"8388608"`
	PutTimeout    time.Duration `env:"ARCHIVE_PUT_TIMEOUT"    envDefault:"10s"`
	BulkTimeout   time.Duration `env:"ARCHIVE_BULK_TIMEOUT"   envDefault:"10s"`
	WriteAttempts int           `env:"ARCHIVE_WRITE_ATTEMPTS" envDefault:"2"`
	FetchBatch    int           `env:"ARCHIVE_FETCH_BATCH"    envDefault:"100"`
	Replicas      int           `env:"ARCHIVE_REPLICAS"       envDefault:"1"` // for the ack-pending check only

	BlobsEnabled bool          `env:"ARCHIVE_BLOBS_ENABLED"   envDefault:"true"`
	BlobMaxBytes int64         `env:"ARCHIVE_BLOB_MAX_BYTES"  envDefault:"104857600"`
	BlobAckWait  time.Duration `env:"ARCHIVE_BLOB_ACK_WAIT"   envDefault:"10m"`
	BlobWorkers  int           `env:"ARCHIVE_BLOB_WORKERS"    envDefault:"4"`
	Drive        drive.Config  `envPrefix:"DRIVE_"`

	Vault     atrest.VaultConfig
	Consumer  stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap bootstrapConfig         `envPrefix:"BOOTSTRAP_"`

	DevMode      bool   `env:"DEV_MODE"      envDefault:"false"`
	HealthAddr   string `env:"HEALTH_ADDR"   envDefault:":8081"`
	PProfEnabled bool   `env:"PPROF_ENABLED" envDefault:"false"`
}

var retentionRe = regexp.MustCompile(`^[1-9][0-9]*(d|h|ms|s|m)$`)

func (c config) validate() error { //nolint:gocritic // hugeParam: value receiver so validConfig().validate() compiles on a non-addressable result
	if c.BatchEvents <= 0 {
		return fmt.Errorf("ARCHIVE_BATCH_EVENTS must be > 0, got %d", c.BatchEvents)
	}
	if c.BatchBytes <= 0 {
		return fmt.Errorf("ARCHIVE_BATCH_BYTES must be > 0, got %d", c.BatchBytes)
	}
	if c.WriteAttempts <= 0 {
		return fmt.Errorf("ARCHIVE_WRITE_ATTEMPTS must be > 0, got %d", c.WriteAttempts)
	}
	if c.FetchBatch <= 0 || c.FetchBatch > c.BatchEvents {
		return fmt.Errorf("ARCHIVE_FETCH_BATCH must be in 1..ARCHIVE_BATCH_EVENTS, got %d", c.FetchBatch)
	}
	if c.BlobWorkers <= 0 {
		return fmt.Errorf("ARCHIVE_BLOB_WORKERS must be > 0, got %d", c.BlobWorkers)
	}
	if !retentionRe.MatchString(c.IndexRetention) {
		return fmt.Errorf("ARCHIVE_INDEX_RETENTION must be an ES duration such as 2555d, got %q", c.IndexRetention)
	}
	worst := c.FillInterval + time.Duration(c.WriteAttempts)*(c.PutTimeout+c.BulkTimeout)
	if worst >= c.Consumer.AckWait {
		return fmt.Errorf("CONSUMER_ACK_WAIT (%s) must exceed ARCHIVE_FILL_INTERVAL + ARCHIVE_WRITE_ATTEMPTS x (ARCHIVE_PUT_TIMEOUT + ARCHIVE_BULK_TIMEOUT) = %s", c.Consumer.AckWait, worst)
	}
	return nil
}

// checkBatchAckCoupling mirrors search-sync-worker's check: one batch
// uploading plus one filling per pod, across every replica of the durable.
func checkBatchAckCoupling(batchEvents, maxAckPending, replicas int) string {
	needed := replicas * 2 * batchEvents
	if needed > maxAckPending {
		return fmt.Sprintf("ARCHIVE_BATCH_EVENTS (%d) at ARCHIVE_REPLICAS %d needs CONSUMER_MAX_ACK_PENDING >= %d but it is %d: the server will stop delivering before a batch fills, so segments seal on the timer undersized", batchEvents, replicas, needed, maxAckPending)
	}
	return ""
}
