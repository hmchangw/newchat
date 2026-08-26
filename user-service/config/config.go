package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// MongoConfig holds MongoDB connection settings (env prefix: MONGO_).
type MongoConfig struct {
	URI      string `env:"URI,notEmpty"`
	DB       string `env:"DB"       envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
	// ReadPreference routes staleness-tolerant reads to secondaries per read site;
	// the client stays on primary for dedup/read-after-write.
	ReadPreference string `env:"READ_PREFERENCE" envDefault:"secondaryPreferred"`
}

// HTTPConfig configures the client-facing HTTP listener (env prefix: HTTP_).
type HTTPConfig struct {
	Port string `env:"PORT" envDefault:"8080"`
	// MaxConcurrency caps in-flight HTTP handlers, shedding the overflow with 429.
	// Its own budget, so a burst cannot take handler slots or Mongo connections from
	// the NATS path — but the NATS connection and the room/history services behind
	// it are still shared. The default assumes a 2 GiB pod (~1.2 MB live per
	// in-flight page); halve it on a 1 GiB one. 0 disables.
	MaxConcurrency int `env:"MAX_CONCURRENCY" envDefault:"512"`
	// MaxConns bounds accepted TCP connections; see ginutil.LimitListener for why.
	// Must exceed MaxConcurrency, since keep-alives outnumber in-flight requests.
	// 0 disables.
	MaxConns       int           `env:"MAX_CONNS" envDefault:"2048"`
	HandlerTimeout time.Duration `env:"HANDLER_TIMEOUT" envDefault:"30s"`
	// WriteTimeout must exceed HandlerTimeout: net/http starts its clock when the
	// request headers are read, so an equal value cuts the response mid-write.
	WriteTimeout     time.Duration `env:"WRITE_TIMEOUT"      envDefault:"35s"`
	GzipMinBytes     int           `env:"GZIP_MIN_BYTES"     envDefault:"1024"`
	MongoMaxPoolSize uint64        `env:"MONGO_MAX_POOL_SIZE" envDefault:"128"`
	// 0 by default: a warm floor is per replica-set member, a standing cost the
	// cluster carries idle. The price is that a burst after a lull pays connect and
	// auth, throttled by the driver's maxConnecting of 2.
	MongoMinPoolSize uint64 `env:"MONGO_MIN_POOL_SIZE" envDefault:"0"`
	// Pool sizes are per replica-set member, and the driver never reaps idle
	// connections by default, so a burst's peak would be held for the process life.
	MongoMaxIdleTime time.Duration `env:"MONGO_MAX_IDLE_TIME" envDefault:"5m"`
	// Page bounds. The default matches the NATS one so an omitted limit behaves
	// identically on both transports; the max is far higher because no 128 KB
	// payload ceiling applies here.
	DefaultLimit int `env:"SUBSCRIPTION_DEFAULT_LIMIT" envDefault:"40"`
	MaxLimit     int `env:"SUBSCRIPTION_MAX_LIMIT"     envDefault:"400"`
}

// NATSConfig holds NATS connection settings (env prefix: NATS_).
type NATSConfig struct {
	URL       string `env:"URL,notEmpty"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

// Config is the top-level configuration for user-service.
type Config struct {
	// SiteID is required: baked into subscription subjects and inbox routing; missing it would silently federate under a wrong ID.
	SiteID                   string        `env:"SITE_ID,notEmpty"`
	AllSiteIDs               []string      `env:"ALL_SITE_IDS"           envDefault:"" envSeparator:","`
	MaxSubscriptionLimit     int           `env:"MAX_SUBSCRIPTION_LIMIT" envDefault:"1000"`
	DefaultSubscriptionLimit int           `env:"SUBSCRIPTION_DEFAULT_LIMIT" envDefault:"40"`
	MaxAppsLimit             int           `env:"APPS_MAX_LIMIT" envDefault:"100"`
	DefaultAppsLimit         int           `env:"APPS_DEFAULT_LIMIT" envDefault:"20"`
	MaxAccountNames          int           `env:"MAX_ACCOUNT_NAMES"      envDefault:"100"`
	HandlerTimeout           time.Duration `env:"HANDLER_TIMEOUT"        envDefault:"15s"`
	// RoomBatchChunk caps room ids per enrichment RPC. 100 is history-service's hard
	// batch cap and keeps each reply well under the 128 KB NATS payload.
	RoomBatchChunk int `env:"ROOM_BATCH_CHUNK" envDefault:"100"`
	// PreviewContentChars truncates previewMessage.content at read time; the stored
	// message is untouched. It is what bounds a page's preview memory, since a body
	// may reach the gatekeeper's 20 KB ceiling. 0 disables truncation.
	PreviewContentChars int `env:"PREVIEW_CONTENT_CHARS" envDefault:"50"`
	// MaxSiteFanout bounds concurrent enrichment RPCs per request. Chunking makes a
	// large page issue several per site, so this is the knob that throttles the
	// resulting downstream load without a rebuild.
	MaxSiteFanout int `env:"MAX_SITE_FANOUT" envDefault:"8"`
	// Room sort-key cache for subscription.list (see
	// mongorepo.NewSubscriptionRepo). An entry is at most TTL old and staleness
	// only affects ordering; zero or less for either knob turns the cache off.
	SortKeyCacheSize int           `env:"SUBS_SORTKEY_CACHE_SIZE" envDefault:"100000"`
	SortKeyCacheTTL  time.Duration `env:"SUBS_SORTKEY_CACHE_TTL"  envDefault:"15s"`
	// MaxConcurrency caps in-flight request handlers so a burst is shed at the
	// door (ErrUnavailable) instead of piling unbounded work onto MongoDB. 0
	// disables the cap (unbounded spawn).
	MaxConcurrency int `env:"MAX_CONCURRENCY" envDefault:"256"`
	// MaxResponseBytes caps a paginated reply so it is trimmed to fit rather
	// than refused by the broker. 0 derives the cap from the broker's
	// advertised max_payload.
	MaxResponseBytes int64 `env:"MAX_RESPONSE_BYTES" envDefault:"0"`
	// PageTrimming trims a paginated reply to the budget. Off returns the page
	// whole and lets the broker refuse it, so the client falls back to its
	// response_too_large retry — an escape hatch, not a tuning knob.
	PageTrimming bool `env:"PAGE_TRIMMING_ENABLED" envDefault:"true"`
	// OIDC settings for the SSO token vault — optional as a unit: unset OIDC_ISSUER_URL disables the endpoints; the rest is validated in Load.
	OIDCIssuerURL string `env:"OIDC_ISSUER_URL" envDefault:""`
	// OIDCAudiences must include the access-token `aud` — refresh re-verifies the minted
	// access token against this allow-list, so a missing aud fails every refresh.
	OIDCAudiences    []string      `env:"OIDC_AUDIENCES"     envDefault:"" envSeparator:","`
	TLSSkipVerify    bool          `env:"OIDC_TLS_SKIP_VERIFY" envDefault:"false"`
	OIDCClientID     string        `env:"OIDC_CLIENT_ID"     envDefault:""`
	SSORefreshWindow time.Duration `env:"SSO_REFRESH_WINDOW" envDefault:"1h"`
	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX"      envDefault:"p_admin"`
	// ValkeyAddrs seeds the Valkey cluster backing the thread-unread badge
	// accelerator (pkg/badgecache); empty disables it (Phase A deploys need no
	// Valkey — badge.count.batch falls back to computing counts on the fly).
	Valkey valkeyutil.Config
	// BadgeCacheTTL bounds how long an account's badge unread-room set survives
	// without a BumpBatch/Seed/Reseed refresh. Keep identical across the badge
	// cache's writers (user-service, room-service, inbox-worker). Must be >= 1s
	// — pkg/badgecache truncates to whole seconds for Lua's EXPIRE, and a
	// sub-second value reaches Redis as EX 0, which errors after the script's
	// DEL/SADD have already run, silently disabling the cache.
	BadgeCacheTTL time.Duration `env:"BADGE_CACHE_TTL" envDefault:"24h"`
	// BadgeMarkerTTL bounds how long the badge set may go without verification
	// against Mongo: the freshness marker is stamped only by Seed/Reseed (which
	// compute from Mongo) and is never refreshed by bumps, so its lifetime is
	// the maximum badge staleness. Must be <= BADGE_CACHE_TTL — a marker
	// outliving its set would read as a fresh zero and show an empty badge.
	// Must also be >= 1s for the same EX-0 reason as BadgeCacheTTL.
	BadgeMarkerTTL time.Duration `env:"BADGE_MARKER_TTL" envDefault:"10m"`
	// BadgeCountCap caps every badge unread-room count returned to clients and
	// the push pipeline (the UI renders the cap as "N-1+", e.g. 10 → "9+").
	BadgeCountCap int `env:"BADGE_COUNT_CAP" envDefault:"10"`
	// BadgeCountCacheFirst serves subscription.count (unread=true) from the
	// Valkey badge set on freshness-marker hit. Flip to true only after every
	// badge writer runs the marker-aware pkg/badgecache — an old writer's
	// set-only ClearAll leaves a stale "fresh zero" marker for up to the TTL.
	BadgeCountCacheFirst bool        `env:"BADGE_COUNT_CACHE_FIRST" envDefault:"false"`
	Mongo                MongoConfig `envPrefix:"MONGO_"`
	NATS                 NATSConfig  `envPrefix:"NATS_"`
	// Pool caps the NATS-path Mongo connection pool (MONGO_MAX_POOL_SIZE /
	// MONGO_MIN_POOL_SIZE / MONGO_MAX_IDLE_TIME). The HTTP path has its own,
	// HTTP_MONGO_-prefixed, so a large page cannot starve the RPC handlers.
	Pool mongoutil.PoolConfig
	// ShowTeamsRoom controls whether Teams-migrated rooms (origin "teams")
	// appear in the subscription list/count; false hides them (reversible
	// read-time filter — see pkg/model.OriginTeams).
	ShowTeamsRoom bool `env:"SHOW_TEAMS_ROOM" envDefault:"false"`
	// ShowTeamsAccounts allowlists accounts that see Teams rooms even when
	// ShowTeamsRoom is false — an ops-managed set, comma-separated.
	ShowTeamsAccounts []string `env:"SHOW_TEAMS_ROOM_ACCOUNTS" envSeparator:","`
	// HealthAddr serves /healthz and /readyz on their own listener, so a shed API
	// request can never fail a kubelet probe.
	HealthAddr string `env:"HEALTH_ADDR" envDefault:":8081"`
	// BotplatformURL enables the session-token auth branch; unset leaves SSO only.
	BotplatformURL string `env:"BOTPLATFORM_URL" envDefault:""`
	// GoMemLimitFraction sets GOMEMLIMIT to this share of the cgroup quota.
	GoMemLimitFraction float64    `env:"GOMEMLIMIT_FRACTION" envDefault:"0.8"`
	HTTP               HTTPConfig `envPrefix:"HTTP_"`
}

// validateMongoPool checks the HTTP client's pool trio. The NATS client uses
// mongoutil.PoolConfig.Validate; this exists because PoolConfig's env tags are
// fixed to the MONGO_ prefix and cannot be reused under HTTP_MONGO_.
func validateMongoPool(prefix string, maxPool, minPool uint64, idle time.Duration) error {
	if maxPool < 1 {
		return fmt.Errorf("%sMAX_POOL_SIZE must be >= 1, got %d", prefix, maxPool)
	}
	if minPool > maxPool {
		return fmt.Errorf("%[1]sMIN_POOL_SIZE (%[2]d) must be <= %[1]sMAX_POOL_SIZE (%[3]d)", prefix, minPool, maxPool)
	}
	if idle < 0 {
		return fmt.Errorf("%sMAX_IDLE_TIME must be >= 0, got %s", prefix, idle)
	}
	return nil
}

// Load parses environment variables into Config; rejects MAX_SUBSCRIPTION_LIMIT < 1 because $limit:0 errors at query time.
func Load() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse user-service config: %w", err)
	}
	if cfg.MaxSubscriptionLimit < 1 {
		return Config{}, fmt.Errorf("MAX_SUBSCRIPTION_LIMIT must be >= 1, got %d", cfg.MaxSubscriptionLimit)
	}
	if cfg.DefaultSubscriptionLimit < 1 {
		return Config{}, fmt.Errorf("SUBSCRIPTION_DEFAULT_LIMIT must be >= 1, got %d", cfg.DefaultSubscriptionLimit)
	}
	if cfg.DefaultSubscriptionLimit > cfg.MaxSubscriptionLimit {
		return Config{}, fmt.Errorf("SUBSCRIPTION_DEFAULT_LIMIT (%d) must be <= MAX_SUBSCRIPTION_LIMIT (%d)", cfg.DefaultSubscriptionLimit, cfg.MaxSubscriptionLimit)
	}
	if cfg.MaxAppsLimit < 1 {
		return Config{}, fmt.Errorf("APPS_MAX_LIMIT must be >= 1, got %d", cfg.MaxAppsLimit)
	}
	if cfg.DefaultAppsLimit < 1 {
		return Config{}, fmt.Errorf("APPS_DEFAULT_LIMIT must be >= 1, got %d", cfg.DefaultAppsLimit)
	}
	if cfg.DefaultAppsLimit > cfg.MaxAppsLimit {
		return Config{}, fmt.Errorf("APPS_DEFAULT_LIMIT (%d) must be <= APPS_MAX_LIMIT (%d)", cfg.DefaultAppsLimit, cfg.MaxAppsLimit)
	}
	if cfg.HTTP.MaxLimit < 1 {
		return Config{}, fmt.Errorf("HTTP_SUBSCRIPTION_MAX_LIMIT must be >= 1, got %d", cfg.HTTP.MaxLimit)
	}
	if cfg.HTTP.DefaultLimit < 1 {
		return Config{}, fmt.Errorf("HTTP_SUBSCRIPTION_DEFAULT_LIMIT must be >= 1, got %d", cfg.HTTP.DefaultLimit)
	}
	if cfg.HTTP.DefaultLimit > cfg.HTTP.MaxLimit {
		return Config{}, fmt.Errorf("HTTP_SUBSCRIPTION_DEFAULT_LIMIT (%d) must be <= HTTP_SUBSCRIPTION_MAX_LIMIT (%d)", cfg.HTTP.DefaultLimit, cfg.HTTP.MaxLimit)
	}
	if cfg.HTTP.MaxConcurrency < 0 {
		return Config{}, fmt.Errorf("HTTP_MAX_CONCURRENCY must be >= 0, got %d", cfg.HTTP.MaxConcurrency)
	}
	if cfg.HTTP.MaxConns < 0 {
		return Config{}, fmt.Errorf("HTTP_MAX_CONNS must be >= 0, got %d", cfg.HTTP.MaxConns)
	}
	if cfg.HTTP.MaxConns > 0 && cfg.HTTP.MaxConns <= cfg.HTTP.MaxConcurrency {
		return Config{}, fmt.Errorf("HTTP_MAX_CONNS (%d) must exceed HTTP_MAX_CONCURRENCY (%d): keep-alive connections outnumber in-flight requests", cfg.HTTP.MaxConns, cfg.HTTP.MaxConcurrency)
	}
	// Checked before the comparison below: ginutil.Timeout disables itself at <= 0,
	// so a zero would pass "write exceeds handler" while dropping the budget.
	if cfg.HTTP.HandlerTimeout <= 0 {
		return Config{}, fmt.Errorf("HTTP_HANDLER_TIMEOUT must be > 0, got %s", cfg.HTTP.HandlerTimeout)
	}
	if cfg.HTTP.WriteTimeout <= cfg.HTTP.HandlerTimeout {
		return Config{}, fmt.Errorf("HTTP_WRITE_TIMEOUT (%s) must exceed HTTP_HANDLER_TIMEOUT (%s)", cfg.HTTP.WriteTimeout, cfg.HTTP.HandlerTimeout)
	}
	if err := validateMongoPool("HTTP_MONGO_", cfg.HTTP.MongoMaxPoolSize, cfg.HTTP.MongoMinPoolSize, cfg.HTTP.MongoMaxIdleTime); err != nil {
		return Config{}, err
	}
	if cfg.GoMemLimitFraction <= 0 || cfg.GoMemLimitFraction > 1 {
		return Config{}, fmt.Errorf("GOMEMLIMIT_FRACTION must be in (0,1], got %v", cfg.GoMemLimitFraction)
	}
	if cfg.MaxSiteFanout < 1 {
		return Config{}, fmt.Errorf("MAX_SITE_FANOUT must be >= 1, got %d", cfg.MaxSiteFanout)
	}
	if cfg.RoomBatchChunk < 1 || cfg.RoomBatchChunk > 100 {
		return Config{}, fmt.Errorf("ROOM_BATCH_CHUNK must be in [1,100], got %d", cfg.RoomBatchChunk)
	}
	if cfg.PreviewContentChars < 0 {
		return Config{}, fmt.Errorf("PREVIEW_CONTENT_CHARS must be >= 0, got %d", cfg.PreviewContentChars)
	}
	if cfg.MaxConcurrency < 0 {
		return Config{}, fmt.Errorf("MAX_CONCURRENCY must be >= 0, got %d", cfg.MaxConcurrency)
	}
	if cfg.BadgeCacheTTL < time.Second {
		return Config{}, fmt.Errorf("BADGE_CACHE_TTL must be >= 1s, got %s", cfg.BadgeCacheTTL)
	}
	if cfg.BadgeMarkerTTL < time.Second {
		return Config{}, fmt.Errorf("BADGE_MARKER_TTL must be >= 1s, got %s", cfg.BadgeMarkerTTL)
	}
	if cfg.BadgeMarkerTTL > cfg.BadgeCacheTTL {
		return Config{}, fmt.Errorf("BADGE_MARKER_TTL (%s) must be <= BADGE_CACHE_TTL (%s)", cfg.BadgeMarkerTTL, cfg.BadgeCacheTTL)
	}
	if cfg.BadgeCountCap < 1 {
		return Config{}, fmt.Errorf("BADGE_COUNT_CAP must be >= 1, got %d", cfg.BadgeCountCap)
	}
	if cfg.OIDCIssuerURL != "" {
		if len(cfg.OIDCAudiences) == 0 {
			return Config{}, fmt.Errorf("OIDC_AUDIENCES is required when OIDC_ISSUER_URL is set")
		}
		if cfg.OIDCClientID == "" {
			return Config{}, fmt.Errorf("OIDC_CLIENT_ID is required when OIDC_ISSUER_URL is set")
		}
		if cfg.SSORefreshWindow <= 0 {
			return Config{}, fmt.Errorf("SSO_REFRESH_WINDOW must be > 0, got %s", cfg.SSORefreshWindow)
		}
	}
	if _, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference); err != nil {
		return Config{}, fmt.Errorf("MONGO_READ_PREFERENCE: %w", err)
	}
	if err := cfg.Pool.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
