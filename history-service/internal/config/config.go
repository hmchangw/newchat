package config

import "github.com/caarlos0/env/v11"

// CassandraConfig holds Cassandra connection settings.
// Env vars: CASSANDRA_HOSTS, CASSANDRA_KEYSPACE, CASSANDRA_USERNAME, CASSANDRA_PASSWORD
type CassandraConfig struct {
	Hosts    string `env:"HOSTS"    required:"true"`
	Keyspace string `env:"KEYSPACE" envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
}

// MongoConfig holds MongoDB connection settings.
// Env vars: MONGO_URI, MONGO_DB, MONGO_USERNAME, MONGO_PASSWORD
type MongoConfig struct {
	URI      string `env:"URI"      required:"true"`
	DB       string `env:"DB"       envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
}

// NATSConfig holds NATS connection settings.
// Env vars: NATS_URL, NATS_CREDS_FILE
type NATSConfig struct {
	URL       string `env:"URL" required:"true"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

// ValkeyConfig holds Valkey (Redis-compatible) connection settings.
// Env vars: VALKEY_ADDR, VALKEY_PASSWORD
// Addr is validated only when encryption is enabled (see main.go).
type ValkeyConfig struct {
	Addr     string `env:"ADDR"`
	Password string `env:"PASSWORD" envDefault:""`
}

// EncryptionConfig gates the room-key (Valkey) connection and the
// encrypted-edit publish path.
// Env vars: ENCRYPTION_ENABLED
type EncryptionConfig struct {
	Enabled bool `env:"ENABLED" envDefault:"false"`
}

// Config is the top-level configuration for history-service.
type Config struct {
	SiteID                  string           `env:"SITE_ID"                    envDefault:"site-local"`
	Cassandra               CassandraConfig  `envPrefix:"CASSANDRA_"`
	Mongo                   MongoConfig      `envPrefix:"MONGO_"`
	NATS                    NATSConfig       `envPrefix:"NATS_"`
	Valkey                  ValkeyConfig     `envPrefix:"VALKEY_"`
	Encryption              EncryptionConfig `envPrefix:"ENCRYPTION_"`
	MessageBucketHours      int              `env:"MESSAGE_BUCKET_HOURS"       envDefault:"24"`
	MessageReadMaxBuckets   int              `env:"MESSAGE_READ_MAX_BUCKETS"   envDefault:"365"`
	MessageHistoryFloorDays int              `env:"MESSAGE_HISTORY_FLOOR_DAYS" envDefault:"730"`
}

// Load parses environment variables into Config. Returns error if required vars are missing.
func Load() (Config, error) {
	return env.ParseAs[Config]()
}
