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
	// ServerSelectionTimeout bounds how long a read waits for a usable server.
	// Deliberately far below the driver's 30s default and below REQUEST_TIMEOUT,
	// so a stopped MongoDB errors while the request still has budget to serve a
	// cached fallback rather than dying on an expired deadline.
	ServerSelectionTimeout time.Duration `env:"MONGO_SERVER_SELECTION_TIMEOUT" envDefault:"2s"`
	MaxPoolSize            uint64        `env:"MAX_POOL_SIZE" envDefault:"500"`
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
	SiteID                  string          `env:"SITE_ID"                    envDefault:"site-local"`
	HealthAddr              string          `env:"HEALTH_ADDR"                envDefault:":8081"`
	PProfEnabled            bool            `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr             string          `env:"METRICS_ADDR"               envDefault:":9090"`
	Cassandra               CassandraConfig `envPrefix:"CASSANDRA_"`
	Mongo                   MongoConfig     `envPrefix:"MONGO_"`
	NATS                    NATSConfig      `envPrefix:"NATS_"`
	MessageBucketHours      int             `env:"MESSAGE_BUCKET_HOURS"        envDefault:"72"`
	MessageReadMaxBuckets   int             `env:"MESSAGE_READ_MAX_BUCKETS"    envDefault:"122"`
	MessageHistoryFloorDays int             `env:"MESSAGE_HISTORY_FLOOR_DAYS"  envDefault:"365"`
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

	// Valkey cluster fronting the shared subauthcache L2. Empty ValkeyAddrs
	// disables the L2 tier (the L1 subscription cache falls straight through
	// to Mongo, breaker-guarded).
	ValkeyAddrs    []string `env:"VALKEY_ADDRS"    envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD" envDefault:""`

	// SubL2TTL is the shared Valkey L2 retention for subscription authz — the
	// outage buffer. Long by design (default 90m) so an L2 hit carries the
	// access decision through a Mongo outage. 0 disables the L2 tier.
	SubL2TTL time.Duration `env:"HISTORY_SUB_L2_TTL" envDefault:"90m"`

	// MongoBreakerFails/MongoBreakerCooldown configure the circuit breaker
	// guarding the subauthcache Mongo loader: opens after MongoBreakerFails
	// consecutive failures and stays open for MongoBreakerCooldown before a
	// half-open probe.
	MongoBreakerFails    int           `env:"HISTORY_MONGO_BREAKER_FAILS"    envDefault:"5"`
	MongoBreakerCooldown time.Duration `env:"HISTORY_MONGO_BREAKER_COOLDOWN" envDefault:"10s"`

	// DEKL2TTL is the Valkey L2 retention for Vault-wrapped at-rest DEKs — the
	// outage buffer for decrypting history. The in-process DEK cache expires on
	// a fixed TTL stamped at fetch time, so without this L2 an active room loses
	// its key partway through a Mongo outage. 0 disables the DEK L2 tier.
	DEKL2TTL time.Duration `env:"ATREST_DEK_L2_TTL" envDefault:"90m"`

	// DEKBreakerFails/DEKBreakerCooldown configure the circuit breaker guarding
	// the Mongo DEK fetch. Kept separate from the subscription breaker so the
	// two failure signals never reset each other.
	DEKBreakerFails    int           `env:"ATREST_DEK_BREAKER_FAILS"    envDefault:"5"`
	DEKBreakerCooldown time.Duration `env:"ATREST_DEK_BREAKER_COOLDOWN" envDefault:"10s"`

	Atrest atrest.Config      // env vars are already prefixed ATREST_*
	Vault  atrest.VaultConfig // env vars are already prefixed (VAULT_*, ATREST_VAULT_*)

	// DebugLog gates the X-Debug ladder rate cap and DEBUG_LOG_PAYLOADS
	// (dev-only full request/reply payload logging). Default: payloads off.
	DebugLog logctx.Config `envPrefix:"DEBUG_LOG_"`
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
	if cfg.Mongo.ServerSelectionTimeout <= 0 {
		return fmt.Errorf("MONGO_SERVER_SELECTION_TIMEOUT must be > 0, got %s", cfg.Mongo.ServerSelectionTimeout)
	}
	// A server-selection bound at or above the request budget cannot do its job:
	// the handler deadline fires first, so the read never returns an error and
	// the fail-open paths that depend on one never run. RequestTimeout == 0 means
	// unbounded, so there is no budget to undercut.
	if cfg.RequestTimeout > 0 && cfg.Mongo.ServerSelectionTimeout >= cfg.RequestTimeout {
		return fmt.Errorf("MONGO_SERVER_SELECTION_TIMEOUT (%s) must be less than REQUEST_TIMEOUT (%s), "+
			"otherwise a stalled MongoDB consumes the whole request budget instead of failing open",
			cfg.Mongo.ServerSelectionTimeout, cfg.RequestTimeout)
	}
	if cfg.SubL2TTL < 0 {
		return fmt.Errorf("HISTORY_SUB_L2_TTL must be >= 0, got %s", cfg.SubL2TTL)
	}
	if cfg.MongoBreakerFails < 0 {
		return fmt.Errorf("HISTORY_MONGO_BREAKER_FAILS must be >= 0, got %d", cfg.MongoBreakerFails)
	}
	if cfg.MongoBreakerCooldown < 0 {
		return fmt.Errorf("HISTORY_MONGO_BREAKER_COOLDOWN must be >= 0, got %s", cfg.MongoBreakerCooldown)
	}
	if cfg.DEKL2TTL < 0 {
		return fmt.Errorf("ATREST_DEK_L2_TTL must be >= 0, got %s", cfg.DEKL2TTL)
	}
	if cfg.DEKBreakerFails < 0 {
		return fmt.Errorf("ATREST_DEK_BREAKER_FAILS must be >= 0, got %d", cfg.DEKBreakerFails)
	}
	if cfg.DEKBreakerCooldown < 0 {
		return fmt.Errorf("ATREST_DEK_BREAKER_COOLDOWN must be >= 0, got %s", cfg.DEKBreakerCooldown)
	}
	return nil
}
