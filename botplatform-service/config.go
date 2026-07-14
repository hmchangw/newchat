package main

type config struct {
	Port string `env:"PORT" envDefault:"8080"`

	// SiteID identifies this service's home site (used for logging). Login is
	// no longer gated on it — any user may authenticate against any cluster.
	SiteID string `env:"SITE_ID,required"`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`

	// SessionsMaxPerAccount is the per-user FIFO cap on the sessions
	// collection. Sessions exceeding the cap are evicted oldest-first by
	// issuedAt at login time.
	SessionsMaxPerAccount int `env:"SESSIONS_MAX_PER_ACCOUNT" envDefault:"100"`

	// BcryptCost matches the legacy Rocket.Chat cost so existing password
	// hashes verify without rehash.
	BcryptCost int `env:"BCRYPT_COST" envDefault:"10"`

	DevMode bool `env:"DEV_MODE" envDefault:"false"`
}
