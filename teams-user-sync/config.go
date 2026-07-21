package main

// config is teams-user-sync's environment configuration. The binary runs
// exactly one sync per invocation — scheduling and overlap prevention are
// owned by the Kubernetes CronJob that triggers it (concurrencyPolicy:
// Forbid). Credentials and connection strings are required with no default
// (fail fast); operational knobs default to sane dev values.
type config struct {
	GraphTenantID     string `env:"GRAPH_TENANT_ID,required,notEmpty"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required,notEmpty"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required,notEmpty"`
	// GraphPageSize is Graph's $top per page (max 999).
	GraphPageSize int `env:"GRAPH_PAGE_SIZE" envDefault:"500"`
	// GraphTLSInsecureSkipVerify disables Graph TLS verification. Defaults to
	// true because this job runs on-prem behind a TLS-intercepting proxy that
	// presents its own certificate; set it to false where Graph presents a
	// verifiable certificate chain.
	GraphTLSInsecureSkipVerify bool `env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"true"`
	// GraphProxyURL, when set, routes the Graph client through this proxy
	// explicitly (overriding HTTPS_PROXY/HTTP_PROXY). Must include a scheme and
	// host, e.g. "http://proxy.corp:8080". Empty falls back to the standard proxy
	// env vars.
	GraphProxyURL string `env:"GRAPH_PROXY_URL" envDefault:""`

	// Two Mongo clients, one per lane: the teams_user diff + hr lookup read
	// through a secondary-preferred read client, the teams_user upserts write
	// through a primary write client. Each lane has its own URI, DB and
	// credential pair so read and write can point at different clusters (they
	// may be identical in dev). Connection strings are required with no
	// default; credentials default to empty.
	MongoReadURI       string `env:"MONGO_READ_URI,required,notEmpty"`
	MongoReadDB        string `env:"MONGO_READ_DB" envDefault:"chat"`
	MongoReadUsername  string `env:"MONGO_READ_USERNAME" envDefault:""`
	MongoReadPassword  string `env:"MONGO_READ_PASSWORD" envDefault:""`
	MongoWriteURI      string `env:"MONGO_WRITE_URI,required,notEmpty"`
	MongoWriteDB       string `env:"MONGO_WRITE_DB" envDefault:"chat"`
	MongoWriteUsername string `env:"MONGO_WRITE_USERNAME" envDefault:""`
	MongoWritePassword string `env:"MONGO_WRITE_PASSWORD" envDefault:""`
}
