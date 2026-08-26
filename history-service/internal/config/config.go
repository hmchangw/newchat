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

// WalkCeilingSkewHours is the clock-skew pad the reader adds above `now` when it
// starts a Cassandra bucket walk (service.clockSkewTolerance is derived from it,
// and a test there pins the two together). It lives in config rather than beside
// the walk because the bucket-budget check below is the only thing that can catch
// a budget too small to cover it, and a validation that carries its own guess at
// the reader's ceiling is a validation that silently stops matching it.
const WalkCeilingSkewHours = 1

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

	// RoomTimesL2TTL is the Valkey L2 retention for a room's last confirmed
	// createdAt, which floors the Cassandra bucket walk. Unlike the other tiers
	// this one is never read while Mongo is healthy, so its TTL governs only how
	// long a room stays cheap to read *during* an outage.
	// 0 disables it, leaving the walk as wide as the configured history floor.
	RoomTimesL2TTL time.Duration `env:"ROOM_TIMES_L2_TTL" envDefault:"90m"`

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
	if _, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference); err != nil {
		return fmt.Errorf("MONGO_READ_PREFERENCE: %w", err)
	}
	if err := cfg.Pool.Validate(); err != nil {
		return err
	}
	if err := cfg.Guard.Validate(); err != nil {
		return err
	}
	if cfg.Pool.ServerSelectionTimeout <= 0 {
		return fmt.Errorf("MONGO_SERVER_SELECTION_TIMEOUT must be > 0, got %s", cfg.Pool.ServerSelectionTimeout)
	}
	// A server-selection bound at or above the request budget cannot do its job:
	// the handler deadline fires first, so the read never returns an error and
	// the fail-open paths that depend on one never run. RequestTimeout == 0 means
	// unbounded, so there is no budget to undercut.
	if cfg.Guard.RequestTimeout > 0 && cfg.Pool.ServerSelectionTimeout >= cfg.Guard.RequestTimeout {
		return fmt.Errorf("MONGO_SERVER_SELECTION_TIMEOUT (%s) must be less than REQUEST_TIMEOUT (%s), "+
			"otherwise a stalled MongoDB consumes the whole request budget instead of failing open",
			cfg.Pool.ServerSelectionTimeout, cfg.Guard.RequestTimeout)
	}
	// The bucket walk is contiguous and stops after MessageReadMaxBuckets, and
	// since the ceiling became the clock rather than the room's last message, an
	// idle room spends that budget crossing empty buckets before reaching data.
	// A budget too small to span the history floor makes an old room's read stop
	// early and return an EMPTY page — and LoadHistory pages by `before` = oldest
	// returned row, so an empty page carries no continuation and the client
	// cannot advance. That is silent history loss, so refuse to start instead.
	//
	// The budget must cover the walk the READER performs, which is wider than the
	// history floor in two ways. It starts at now+WalkCeilingSkewHours, not now;
	// and buckets are absolute time windows, so where that span falls relative to
	// a boundary decides how many partitions it touches. A span of S hours over a
	// W-hour window touches at most floor(S/W)+2 partitions — +1 for the partial
	// bucket at each end. Sizing on S alone passes configurations whose walk
	// exhausts the budget one partition short of the floor, and only near a
	// boundary, which is the worst way to find out.
	//
	// The positive guards are for callers that build a Config directly; at
	// startup main's checkConfig has already rejected a non-positive value for
	// all three, so this relational check cannot be skipped by zeroing one.
	if cfg.MessageBucketHours > 0 && cfg.MessageReadMaxBuckets > 0 && cfg.MessageHistoryFloorDays > 0 {
		spanHours := WalkCeilingSkewHours + cfg.MessageHistoryFloorDays*24
		if needBuckets := spanHours/cfg.MessageBucketHours + 2; cfg.MessageReadMaxBuckets < needBuckets {
			return fmt.Errorf(
				"MESSAGE_READ_MAX_BUCKETS (%d) is short of the %d buckets a worst-case walk needs: "+
					"MESSAGE_HISTORY_FLOOR_DAYS (%d) plus the %dh walk skew spans %dh, which at "+
					"MESSAGE_BUCKET_HOURS (%d) touches up to %d partitions; a history read would stop "+
					"before the floor and return an empty page the client cannot page past",
				cfg.MessageReadMaxBuckets, needBuckets, cfg.MessageHistoryFloorDays,
				WalkCeilingSkewHours, spanHours, cfg.MessageBucketHours, needBuckets)
		}
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
	if cfg.RoomTimesL2TTL < 0 {
		return fmt.Errorf("ROOM_TIMES_L2_TTL must be >= 0, got %s", cfg.RoomTimesL2TTL)
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
