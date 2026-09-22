package main

import (
	"time"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/sessioncache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	Port string `env:"PORT" envDefault:"8080"`

	// SiteID identifies this service's home site; scopes bot streams. Login is not gated on it.
	SiteID string `env:"SITE_ID,required"`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`
	Breaker       mongoutil.BreakerConfig
	// primaryPreferred, not secondaryPreferred: InsertSession then FindSessionByHash
	// on the next request is a read-after-write; secondary lag breaks auth after login.
	ReadPreference string `env:"MONGO_READ_PREFERENCE" envDefault:"primaryPreferred"`
	// SessionCache keeps already-authenticated bots working while Mongo is
	// unreachable. 0 disables the tier. Matches the other L2 tiers at 90m.
	SessionCache sessioncache.TTLConfig

	Pool mongoutil.PoolConfig
	HTTP ginutil.TimeoutConfig

	// Login caps in-flight requests on the unauthenticated /api/v1/login route.
	Login ginutil.ConcurrencyConfig

	// BotRoutes caps in-flight requests on the authenticated bot routes, applied
	// BEFORE requireBot. It is the only control that can run there: botRateLimit
	// needs the principal requireBot establishes, so an invalid token is never
	// metered — and because sessioncache is positive-only, an invalid token
	// misses cache and reaches MongoDB on every attempt.
	//
	// The 32 default is calibrated against BOT_RATE_LIMIT_GLOBAL_PER_MIN (6000/min
	// = ~100 req/s fleet-wide, enforced post-auth): for this cap to bind on
	// legitimate traffic, a single pod would need per-request latency above
	// ~320ms at that rate. So it is effectively non-binding in normal operation
	// and binds only on a pile-up. Two deployments where it CAN bind and the
	// value should be raised: the global rate limit raised well above 6000 or
	// disabled (0), and a deployment with no Valkey — registerBotRoutes omits
	// rate limiting entirely there, leaving this as the only ceiling.
	BotRoutes ginutil.ConcurrencyConfig `envPrefix:"BOT_"`

	// SessionsMaxPerAccount is the per-user FIFO cap; excess sessions are evicted oldest-first.
	SessionsMaxPerAccount int `env:"SESSIONS_MAX_PER_ACCOUNT" envDefault:"100"`

	// BcryptCost matches the legacy Rocket.Chat cost so existing hashes verify without rehash.
	BcryptCost int `env:"BCRYPT_COST" envDefault:"10"`

	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"`

	// ValkeyAddrs seeds the Valkey cluster backing rate-limit + idempotency; empty disables both.
	Valkey valkeyutil.Config

	// BotRateLimitPerCallerPerMin caps requests per bot per 60s window; 0 disables per-caller.
	BotRateLimitPerCallerPerMin int `env:"BOT_RATE_LIMIT_PER_CALLER_PER_MIN" envDefault:"600"`

	// BotRateLimitGlobalPerMin caps aggregate bot requests per 60s window; 0 disables global.
	BotRateLimitGlobalPerMin int `env:"BOT_RATE_LIMIT_GLOBAL_PER_MIN" envDefault:"6000"`

	// BotIdempotencyMsgTTL exceeds the 3s NATS timeout so retries after timeout see the sentinel.
	BotIdempotencyMsgTTL time.Duration `env:"BOT_IDEMPOTENCY_MSG_TTL" envDefault:"30s"`

	// BotIdempotencyRoomMgmtTTL exceeds the 15s NATS timeout for room management.
	BotIdempotencyRoomMgmtTTL time.Duration `env:"BOT_IDEMPOTENCY_ROOM_MGMT_TTL" envDefault:"60s"`

	DevMode bool `env:"DEV_MODE" envDefault:"false"`
}
