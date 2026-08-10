package main

import (
	"fmt"
	"time"
)

type config struct {
	SiteID    string `env:"SITE_ID,required"`
	NATSURL   string `env:"NATS_URL,required"`
	CredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	SourceMongoURI      string `env:"SOURCE_MONGO_URI,required"`
	SourceMongoUsername string `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourceMongoPassword string `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB            string `env:"SOURCE_DB" envDefault:"rocketchat"`

	TargetMongoURI      string `env:"TARGET_MONGO_URI,required"`
	TargetMongoUsername string `env:"TARGET_MONGO_USERNAME" envDefault:""`
	TargetMongoPassword string `env:"TARGET_MONGO_PASSWORD" envDefault:""`
	TargetDB            string `env:"TARGET_DB" envDefault:"chat"`

	// Cassandra is required only when the mapping references a cassandra
	// target — enforced in main after the mapping is loaded, not here.
	CassandraHosts    string `env:"CASSANDRA_HOSTS" envDefault:""`
	CassandraKeyspace string `env:"CASSANDRA_KEYSPACE" envDefault:""`
	CassandraUsername string `env:"CASSANDRA_USERNAME" envDefault:""`
	CassandraPassword string `env:"CASSANDRA_PASSWORD" envDefault:""`

	MappingFile        string        `env:"MAPPING_FILE,required"`
	MessageBucketHours int           `env:"MESSAGE_BUCKET_HOURS" envDefault:"72"`
	TrackConsumers     []string      `env:"TRACK_CONSUMERS" envDefault:""`
	StartAtTime        string        `env:"START_AT_TIME" envDefault:""`
	VerifyPoll         time.Duration `env:"VERIFY_POLL" envDefault:"2s"`
	VerifyTimeout      time.Duration `env:"VERIFY_TIMEOUT" envDefault:"60s"`
	MaxChecks          int           `env:"MAX_CHECKS" envDefault:"32"`
	SamplePercent      int           `env:"SAMPLE_PERCENT" envDefault:"100"`
	RecentCap          int           `env:"RECENT_CAP" envDefault:"200"`
	FailedCap          int           `env:"FAILED_CAP" envDefault:"1000"`
	StatsInterval      time.Duration `env:"STATS_INTERVAL" envDefault:"5s"`
	Port               int           `env:"PORT" envDefault:"8091"`
}

func (c config) validate() error {
	if c.SamplePercent < 0 || c.SamplePercent > 100 {
		return fmt.Errorf("SAMPLE_PERCENT must be 0..100, got %d", c.SamplePercent)
	}
	if c.VerifyPoll <= 0 {
		return fmt.Errorf("VERIFY_POLL must be positive, got %s", c.VerifyPoll)
	}
	if c.VerifyTimeout < c.VerifyPoll {
		return fmt.Errorf("VERIFY_TIMEOUT (%s) must be >= VERIFY_POLL (%s)", c.VerifyTimeout, c.VerifyPoll)
	}
	if c.MaxChecks <= 0 {
		return fmt.Errorf("MAX_CHECKS must be positive, got %d", c.MaxChecks)
	}
	if c.RecentCap <= 0 || c.FailedCap <= 0 {
		return fmt.Errorf("RECENT_CAP and FAILED_CAP must be positive")
	}
	if c.MessageBucketHours <= 0 {
		return fmt.Errorf("MESSAGE_BUCKET_HOURS must be positive, got %d", c.MessageBucketHours)
	}
	if c.StartAtTime != "" {
		if _, err := time.Parse(time.RFC3339, c.StartAtTime); err != nil {
			return fmt.Errorf("START_AT_TIME must be RFC3339: %w", err)
		}
	}
	return nil
}

func main() {}
