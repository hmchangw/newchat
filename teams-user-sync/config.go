package main

// config is teams-user-sync's environment configuration. Credentials and
// connection strings are required with no default (fail fast); operational
// knobs default to sane dev values.
type config struct {
	// SyncCron is the 5-field cron expression driving updateUsers runs.
	SyncCron string `env:"SYNC_CRON" envDefault:"0 2 * * *"`
	// RunOnStart additionally fires one sync immediately at startup.
	RunOnStart bool `env:"RUN_ON_START" envDefault:"false"`

	TeamsTenantID     string `env:"TEAMS_TENANT_ID,required,notEmpty"`
	TeamsClientID     string `env:"TEAMS_CLIENT_ID,required,notEmpty"`
	TeamsClientSecret string `env:"TEAMS_CLIENT_SECRET,required,notEmpty"`
	// TeamsEmailDomain filters Graph users: only UPNs under this domain are
	// synced, and the local part is the hr.accountName lookup key.
	TeamsEmailDomain string `env:"TEAMS_EMAIL_DOMAIN,required,notEmpty"`
	// GraphPageSize is Graph's $top per page (max 999).
	GraphPageSize int `env:"GRAPH_PAGE_SIZE" envDefault:"500"`

	MongoReadURI      string `env:"MONGO_READ_URI,required,notEmpty"`
	MongoReadUsername string `env:"MONGO_READ_USERNAME" envDefault:""`
	MongoReadPassword string `env:"MONGO_READ_PASSWORD" envDefault:""`
	MongoReadDB       string `env:"MONGO_READ_DB" envDefault:"chat"`

	MongoWriteURI      string `env:"MONGO_WRITE_URI,required,notEmpty"`
	MongoWriteUsername string `env:"MONGO_WRITE_USERNAME" envDefault:""`
	MongoWritePassword string `env:"MONGO_WRITE_PASSWORD" envDefault:""`
	MongoWriteDB       string `env:"MONGO_WRITE_DB" envDefault:"chat"`

	HealthAddr string `env:"HEALTH_ADDR" envDefault:":8081"`
}
