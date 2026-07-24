package main

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type config struct {
	SiteID string `env:"SITE_ID,required,notEmpty"`

	SearchURL           string `env:"SEARCH_URL,required,notEmpty"`
	SearchUsername      string `env:"SEARCH_USERNAME"      envDefault:""`
	SearchPassword      string `env:"SEARCH_PASSWORD"      envDefault:""`
	SearchTLSSkipVerify bool   `env:"SEARCH_TLS_SKIP_VERIFY" envDefault:"false"`

	MsgIndexPrefix string `env:"MSG_INDEX_PREFIX,required,notEmpty"`
	SpotlightIndex string `env:"SPOTLIGHT_INDEX,required,notEmpty"`
	UserRoomIndex  string `env:"USER_ROOM_INDEX,required,notEmpty"`

	// MigrationStartAt/MigrationEndAt bound the messages backfill window
	// ([start, end)). Spotlight and user-room backfill the site's full
	// current subscription set unconditionally — see the plan's Global
	// Constraints for why subscriptions aren't windowed.
	MigrationStartAt time.Time `env:"MIGRATION_START_AT,required"`
	MigrationEndAt   time.Time `env:"MIGRATION_END_AT,required"`

	MessageBucketHours int `env:"MESSAGE_BUCKET_HOURS,required"`

	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	CassandraHosts    string `env:"CASSANDRA_HOSTS,required,notEmpty"`
	CassandraKeyspace string `env:"CASSANDRA_KEYSPACE" envDefault:"chat"`
	CassandraUsername string `env:"CASSANDRA_USERNAME" envDefault:""`
	CassandraPassword string `env:"CASSANDRA_PASSWORD" envDefault:""`
	CassandraNumConns int    `env:"CASSANDRA_NUM_CONNS" envDefault:"8"`

	BulkBatchSize     int `env:"BULK_BATCH_SIZE"     envDefault:"500"`
	WorkerConcurrency int `env:"WORKER_CONCURRENCY"  envDefault:"4"`
}

func loadConfig() (config, error) {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}

	if !cfg.MigrationEndAt.After(cfg.MigrationStartAt) {
		return config{}, fmt.Errorf("MIGRATION_END_AT (%s) must be after MIGRATION_START_AT (%s)",
			cfg.MigrationEndAt, cfg.MigrationStartAt)
	}
	if cfg.MessageBucketHours <= 0 {
		return config{}, fmt.Errorf("MESSAGE_BUCKET_HOURS must be positive, got %d", cfg.MessageBucketHours)
	}
	if cfg.BulkBatchSize <= 0 {
		return config{}, fmt.Errorf("BULK_BATCH_SIZE must be positive, got %d", cfg.BulkBatchSize)
	}
	if cfg.WorkerConcurrency <= 0 {
		return config{}, fmt.Errorf("WORKER_CONCURRENCY must be positive, got %d", cfg.WorkerConcurrency)
	}

	return cfg, nil
}
