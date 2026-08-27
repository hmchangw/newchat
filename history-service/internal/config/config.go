package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
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

// MongoConfig holds MongoDB connection settings (env prefix: MONGO_). Pool
// sizing lives in the shared Config.Pool (mongoutil.PoolConfig).
type MongoConfig struct {
	URI      string `env:"URI"      required:"true"`
	DB       string `env:"DB"       envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
	// ReadPreference is the client-level read preference; secondaryPreferred offloads
	// history reads. DEK reads pin to primary in code regardless.
	ReadPreference string `env:"READ_PREFERENCE" envDefault:"secondaryPreferred"`
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
	Pool                    mongoutil.PoolConfig
	NATS                    NATSConfig `envPrefix:"NATS_"`
	MessageBucketHours      int        `env:"MESSAGE_BUCKET_HOURS"        envDefault:"360"`
	MessageReadMaxBuckets   int        `env:"MESSAGE_READ_MAX_BUCKETS"    envDefault:"122"`
	MessageHistoryFloorDays int        `env:"MESSAGE_HISTORY_FLOOR_DAYS"  envDefault:"730"`
	LargeRoomThreshold      int        `env:"LARGE_ROOM_THRESHOLD"        envDefault:"500"`
	MaxPinnedPerRoom        int        `env:"MAX_PINNED_PER_ROOM"         envDefault:"10"`
	PinEnabled              bool       `env:"PIN_ENABLED"                 envDefault:"true"`

	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_admin"`

	// Guard bounds in-flight request handlers (MAX_CONCURRENCY) and per-request
	// duration (REQUEST_TIMEOUT) so a burst or a slow dependency can't saturate
	// the Mongo pool with unbounded, indefinitely-held work.
	Guard natsrouter.GuardConfig
	// MaxResponseBytes caps a paginated reply so it is trimmed to fit rather
	// than refused by the broker. 0 derives the cap from the broker's
	// advertised max_payload.
	MaxResponseBytes int64 `env:"MAX_RESPONSE_BYTES" envDefault:"0"`
	// PageTrimming trims a paginated reply to the budget. Off returns the page
	// whole and lets the broker refuse it, so the client falls back to its
	// response_too_large retry — an escape hatch, not a tuning knob.
	PageTrimming bool `env:"PAGE_TRIMMING_ENABLED" envDefault:"true"`

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

	// PreviewKeyEpoch selects the site preview DEK (preview:{siteID}:{epoch}).
	// Must match broadcast-worker, which writes what this service reads: a reader
	// on another epoch treats the stored preview as absent.
	PreviewKeyEpoch int `env:"PREVIEW_KEY_EPOCH" envDefault:"1"`

	// Room-list preview cache, fronting rooms.get's lazy fallback only — a room
	// served from a stored preview never reaches it. Positives-only; lastMsgAt
	// volatility ⇒ short TTL. Set size or ttl to 0 to disable.
	PreviewCacheSize int           `env:"HISTORY_PREVIEW_CACHE_SIZE" envDefault:"50000"`
	PreviewCacheTTL  time.Duration `env:"HISTORY_PREVIEW_CACHE_TTL"  envDefault:"10s"`

	// Per-bucket read cache (Cassandra sealed-bucket LoadHistory reads), stored
	// in Valkey and shared by every replica. Only sealed buckets (strictly older
	// than the current one) are cached; the hot current bucket is always read
	// live.
	//
	// BucketCacheOptIn gates the whole feature. VALKEY_ADDRS alone is not the
	// gate: it is a fleet-wide variable that several other services already
	// consume, so a deployment that injects it everywhere would switch this
	// cache on for history-service as a side effect of merging rather than as a
	// decision.
	//
	// BEFORE ENABLING THIS, weigh two known gaps. None is a bug in the cache's
	// own logic; each is a case where a sealed bucket can change without the
	// cache learning, so reads serve a stale row until the entry expires:
	//
	//  1. Cache-aside refill race (#250). A reader that missed can Put a
	//     pre-mutation snapshot back after a concurrent Bust found the key
	//     absent.
	//  2. Wall-clock sealing. A bucket is sealed when bucket < sizer.Of(now);
	//     nothing makes the partition immutable in Cassandra, so a create that
	//     lands after the boundary — a federation replay, or a JetStream
	//     redelivery, whose backoff runs to minutes — writes into a bucket a
	//     cached copy already claims to hold completely.
	//
	// Until those are addressed, BucketCacheTTL is the mutation-visibility
	// bound for sealed buckets. Enabling this is an operator's deliberate
	// choice to accept that bound.
	//
	// Two gaps that were on this list are now closed. Cross-replica L1
	// staleness is closed by construction: one shared Valkey tier, no
	// per-replica copy, so a Bust is authoritative for every reader
	// immediately. Thread state written by message-worker / bot-message-worker
	// is closed by those workers busting the parent's bucket on write — which
	// is why HISTORY_BUCKET_CACHE_ENABLED gates them too, and why enabling the
	// cache without also enabling it there would reintroduce stale reply
	// counts with no signal.
	BucketCacheOptIn bool          `env:"HISTORY_BUCKET_CACHE_ENABLED" envDefault:"false"`
	ValkeyAddrs      []string      `env:"VALKEY_ADDRS"                 envSeparator:","`
	ValkeyPassword   string        `env:"VALKEY_PASSWORD"              envDefault:""`
	BucketCacheTTL   time.Duration `env:"HISTORY_BUCKET_CACHE_TTL" envDefault:"10m"`
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
// stood up. It requires an explicit opt-in (see BucketCacheOptIn) on top of
// usable knobs. Zero is the documented disable value for each knob, so any one
// of them at zero keeps Valkey unconnected and the cache uninstalled — including
// MaxRows, where a zero cap would otherwise install a cache that classifies
// every non-empty bucket as oversized and so can never serve a hit.
func (c *Config) BucketCacheEnabled() bool {
	return c.BucketCacheOptIn &&
		len(c.ValkeyAddrs) > 0 &&
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
	// Part of a DEK id, so a non-positive value mints a sentinel rotation could
	// never move forward from.
	if cfg.PreviewKeyEpoch < 1 {
		return fmt.Errorf("PREVIEW_KEY_EPOCH must be >= 1, got %d", cfg.PreviewKeyEpoch)
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
	if cfg.BucketCacheMaxRows < 0 {
		return fmt.Errorf("HISTORY_BUCKET_CACHE_MAX_ROWS must be >= 0, got %d", cfg.BucketCacheMaxRows)
	}
	if _, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference); err != nil {
		return fmt.Errorf("MONGO_READ_PREFERENCE: %w", err)
	}
	if err := cfg.Pool.Validate(); err != nil {
		return err
	}
	if err := cfg.Guard.Validate(); err != nil {
		return err
	}
	return nil
}
