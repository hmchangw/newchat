package main

// config is hr-sync-worker's environment configuration. One durable consumer
// per entry in SITE_IDS (each site's HR_{siteID} stream).
type config struct {
	SiteIDs []string `env:"SITE_IDS,required,notEmpty" envSeparator:","`

	NatsURL       string `env:"NATS_URL,required,notEmpty"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	MongoWriteURI      string `env:"MONGO_WRITE_URI,required,notEmpty"`
	MongoWriteUsername string `env:"MONGO_WRITE_USERNAME" envDefault:""`
	MongoWritePassword string `env:"MONGO_WRITE_PASSWORD" envDefault:""`
	MongoWriteDB       string `env:"MONGO_WRITE_DB" envDefault:"chat"`

	Bootstrap  bootstrapConfig `envPrefix:"BOOTSTRAP_"`
	HealthAddr string          `env:"HEALTH_ADDR" envDefault:":8081"`
}
