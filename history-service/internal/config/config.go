package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// CassandraConfig holds Cassandra connection settings (env prefix: CASSANDRA_).
type CassandraConfig struct {
	Hosts    string `env:"HOSTS"    required:"true"`
	Keyspace string `env:"KEYSPACE" envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
	// NumConns sets gocql's per-host connection count; zero lets cassutil apply its own default.
	NumConns int `env:"NUM_CONNS" envDefault:"8"`
}

// MongoConfig holds MongoDB connection settings (env prefix: MONGO_).
type MongoConfig struct {
	URI      string `env:"URI"      required:"true"`
	DB       string `env:"DB"       envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
	// ReadPreference is the client-level read preference; secondaryPreferred offloads
	// history reads. DEK reads pin to primary in code regardless.
	ReadPreference string `env:"READ_PREFERENCE" envDefault:"secondaryPreferred"`
	// MaxPoolSize caps connections per server. This is authoritative — it is
	// always applied to the client, overriding any maxPoolSize in the URI — so
	// the pool ceiling is explicit rather than the driver default of 100.
	// Connections are created lazily and are further gated by MAX_CONCURRENCY
	// (a handler holds at most one at a time), so this is a generous headroom
	// ceiling, not a count of connections that get opened. Operators must keep
	// pods × MaxPoolSize under the cluster's connection limit.
	MaxPoolSize uint64 `env:"MAX_POOL_SIZE" envDefault:"500"`
	// MinPoolSize keeps a warm connection floor; 0 lets the pool drain to empty.
	MinPoolSize uint64 `env:"MIN_POOL_SIZE" envDefault:"0"`
}

// NATSConfig holds NATS connection settings (env prefix: NATS_).
type NATSConfig struct {
	URL       string `env:"URL" required:"true"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

// Config is the top-level configuration for the history-service.
type Config struct {
	ServiceName             string          `env:"OTEL_SERVICE_NAME"          envDefault:"unknown-service"`
	SiteID                  string          `env:"SITE_ID"                    envDefault:"site-local"`
	HealthAddr              string          `env:"HEALTH_ADDR"                envDefault:":8081"`
	PProfEnabled            bool            `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr             string          `env:"METRICS_ADDR"               envDefault:":9090"`
	Cassandra               CassandraConfig `envPrefix:"CASSANDRA_"`
	Mongo                   MongoConfig     `envPrefix:"MONGO_"`
	NATS                    NATSConfig      `envPrefix:"NATS_"`
	MessageBucketHours      int             `env:"MESSAGE_BUCKET_HOURS"        envDefault:"360"`
	MessageReadMaxBuckets   int             `env:"MESSAGE_READ_MAX_BUCKETS"    envDefault:"122"`
	MessageHistoryFloorDays int             `env:"MESSAGE_HISTORY_FLOOR_DAYS"  envDefault:"730"`
	LargeRoomThreshold      int             `env:"LARGE_ROOM_THRESHOLD"        envDefault:"500"`
	MaxPinnedPerRoom        int             `env:"MAX_PINNED_PER_ROOM"         envDefault:"10"`
	PinEnabled              bool            `env:"PIN_ENABLED"                 envDefault:"true"`

	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_admin"`

	// MaxConcurrency caps in-flight request handlers so a burst is shed at the
	// door (ErrUnavailable) instead of piling unbounded work onto MongoDB. 0
	// disables the cap (unbounded spawn — the previous behavior).
	MaxConcurrency int `env:"MAX_CONCURRENCY" envDefault:"256"`
	// RequestTimeout bounds each request handler so a slow MongoDB/Cassandra op
	// is cancelled and its pooled connection returned instead of held. 0
	// disables the per-request deadline.
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"10s"`

	// Subscription access-check cache. Only positive subscriptions are cached,
	// so the TTL bounds how long revoked access can stay readable. Set size or
	// ttl to 0 to disable.
	SubCacheSize int           `env:"HISTORY_SUB_CACHE_SIZE" envDefault:"100000"`
	SubCacheTTL  time.Duration `env:"HISTORY_SUB_CACHE_TTL"  envDefault:"2m"`

	// Room metadata cache (room times + minUserLastSeenAt). lastMsgAt advances
	// on every message, so the TTL is short by default; client room hints cover
	// the freshness-sensitive path. Set size or ttl to 0 to disable.
	RoomCacheSize int           `env:"HISTORY_ROOM_CACHE_SIZE" envDefault:"50000"`
	RoomCacheTTL  time.Duration `env:"HISTORY_ROOM_CACHE_TTL"  envDefault:"10s"`

	// Room-list preview cache (resolved last-eligible message per room).
	// Positives-only; lastMsgAt volatility ⇒ short TTL. Set size or ttl to 0 to disable.
	PreviewCacheSize int           `env:"HISTORY_PREVIEW_CACHE_SIZE" envDefault:"50000"`
	PreviewCacheTTL  time.Duration `env:"HISTORY_PREVIEW_CACHE_TTL"  envDefault:"10s"`

	// Per-bucket read cache (Cassandra sealed-bucket LoadHistory reads). L2 is
	// Valkey; when ValkeyAddrs is empty the whole cache is disabled and reads go
	// direct to Cassandra. Only sealed buckets (strictly older than the current
	// one) are cached; the hot current bucket is always read live.
	ValkeyAddrs    []string `env:"VALKEY_ADDRS"                 envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD"              envDefault:""`
	// BucketCacheL1MaxBytes caps the total encoded bucket data held in the
	// per-replica L1, LRU-evicted to stay under budget. Byte-bounded (not entry-
	// count-bounded) so the memory ceiling holds regardless of bucket size.
	// Default 256 MiB.
	BucketCacheL1MaxBytes int64         `env:"HISTORY_BUCKET_CACHE_L1_MAX_BYTES" envDefault:"268435456"`
	BucketCacheTTL        time.Duration `env:"HISTORY_BUCKET_CACHE_TTL"          envDefault:"10m"`
	// BucketCacheMaxRows caps how many rows a bucket may hold to be cacheable;
	// larger (dense) buckets are read live instead of cached whole. The default
	// sits just above the largest ordinary page (surroundingPageSize 50), which
	// is where caching stops paying: the walker fills a page from a dense start
	// bucket in one query with no speculative reads, so caching such a bucket
	// saves no round trip while making every hit decode the whole partition.
	// Below the cap, a page spans several buckets and the walk is what the cache
	// collapses.
	BucketCacheMaxRows int `env:"HISTORY_BUCKET_CACHE_MAX_ROWS" envDefault:"50"`

	Atrest atrest.Config      // env vars are already prefixed ATREST_*
	Vault  atrest.VaultConfig // env vars are already prefixed (VAULT_*, ATREST_VAULT_*)

	// DebugLog gates the X-Debug ladder rate cap and DEBUG_LOG_PAYLOADS
	// (dev-only full request/reply payload logging). Default: payloads off.
	DebugLog logctx.Config `envPrefix:"DEBUG_LOG_"`
}

// BucketCacheEnabled reports whether the per-bucket sealed-read cache should be
// stood up. Zero is the documented disable value for each knob, so any one of
// them at zero keeps Valkey unconnected and the cache uninstalled — including
// MaxRows, where a zero cap would otherwise install a cache that classifies
// every non-empty bucket as oversized and so can never serve a hit.
func (c *Config) BucketCacheEnabled() bool {
	return len(c.ValkeyAddrs) > 0 &&
		c.BucketCacheL1MaxBytes > 0 &&
		c.BucketCacheTTL > 0 &&
		c.BucketCacheMaxRows > 0
}

// Load parses environment variables into Config; returns an error when required vars are absent.
func Load() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, err
	}
	if err := validate(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate rejects negative cache sizes/TTLs that would silently disable a
// cache (because the main wiring guards on size>0 && ttl>0). Zero is the
// documented disable value and is accepted.
func validate(cfg *Config) error {
	if cfg.SubCacheSize < 0 {
		return fmt.Errorf("HISTORY_SUB_CACHE_SIZE must be >= 0, got %d", cfg.SubCacheSize)
	}
	if cfg.SubCacheTTL < 0 {
		return fmt.Errorf("HISTORY_SUB_CACHE_TTL must be >= 0, got %s", cfg.SubCacheTTL)
	}
	if cfg.RoomCacheSize < 0 {
		return fmt.Errorf("HISTORY_ROOM_CACHE_SIZE must be >= 0, got %d", cfg.RoomCacheSize)
	}
	if cfg.RoomCacheTTL < 0 {
		return fmt.Errorf("HISTORY_ROOM_CACHE_TTL must be >= 0, got %s", cfg.RoomCacheTTL)
	}
	if cfg.PreviewCacheSize < 0 {
		return fmt.Errorf("HISTORY_PREVIEW_CACHE_SIZE must be >= 0, got %d", cfg.PreviewCacheSize)
	}
	if cfg.PreviewCacheTTL < 0 {
		return fmt.Errorf("HISTORY_PREVIEW_CACHE_TTL must be >= 0, got %s", cfg.PreviewCacheTTL)
	}
	if cfg.BucketCacheTTL < 0 {
		return fmt.Errorf("HISTORY_BUCKET_CACHE_TTL must be >= 0, got %s", cfg.BucketCacheTTL)
	}
	if cfg.BucketCacheL1MaxBytes < 0 {
		return fmt.Errorf("HISTORY_BUCKET_CACHE_L1_MAX_BYTES must be >= 0, got %d", cfg.BucketCacheL1MaxBytes)
	}
	if cfg.BucketCacheMaxRows < 0 {
		return fmt.Errorf("HISTORY_BUCKET_CACHE_MAX_ROWS must be >= 0, got %d", cfg.BucketCacheMaxRows)
	}
	if _, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference); err != nil {
		return fmt.Errorf("MONGO_READ_PREFERENCE: %w", err)
	}
	// 0 makes the driver treat the pool as unbounded — reject it so the cap stays explicit.
	if cfg.Mongo.MaxPoolSize < 1 {
		return fmt.Errorf("MONGO_MAX_POOL_SIZE must be >= 1, got %d", cfg.Mongo.MaxPoolSize)
	}
	if cfg.Mongo.MinPoolSize > cfg.Mongo.MaxPoolSize {
		return fmt.Errorf("MONGO_MIN_POOL_SIZE (%d) must be <= MONGO_MAX_POOL_SIZE (%d)", cfg.Mongo.MinPoolSize, cfg.Mongo.MaxPoolSize)
	}
	if cfg.MaxConcurrency < 0 {
		return fmt.Errorf("MAX_CONCURRENCY must be >= 0, got %d", cfg.MaxConcurrency)
	}
	if cfg.RequestTimeout < 0 {
		return fmt.Errorf("REQUEST_TIMEOUT must be >= 0, got %s", cfg.RequestTimeout)
	}
	return nil
}
