package main

import "time"

type config struct {
	Port string `env:"PORT" envDefault:"8080"`

	// SiteID identifies this service's home site; scopes bot streams. Login is not gated on it.
	SiteID string `env:"SITE_ID,required"`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`

	// SessionsMaxPerAccount is the per-user FIFO cap; excess sessions are evicted oldest-first.
	SessionsMaxPerAccount int `env:"SESSIONS_MAX_PER_ACCOUNT" envDefault:"100"`

	// BcryptCost matches the legacy Rocket.Chat cost so existing hashes verify without rehash.
	BcryptCost int `env:"BCRYPT_COST" envDefault:"10"`

	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"`

	// ValkeyAddrs seeds the Valkey cluster backing rate-limit + idempotency; empty disables both.
	ValkeyAddrs    []string `env:"VALKEY_ADDRS" envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD"`

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
