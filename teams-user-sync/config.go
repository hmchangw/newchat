package main

// config is teams-user-sync's environment configuration. The binary runs
// exactly one sync per invocation — scheduling and overlap prevention are
// owned by the Kubernetes CronJob that triggers it (concurrencyPolicy:
// Forbid). Credentials and connection strings are required with no default
// (fail fast); operational knobs default to sane dev values.
type config struct {
	TeamsTenantID     string `env:"TEAMS_TENANT_ID,required,notEmpty"`
	TeamsClientID     string `env:"TEAMS_CLIENT_ID,required,notEmpty"`
	TeamsClientSecret string `env:"TEAMS_CLIENT_SECRET,required,notEmpty"`
	// GraphPageSize is Graph's $top per page (max 999).
	GraphPageSize int `env:"GRAPH_PAGE_SIZE" envDefault:"500"`
	// GraphBaseURL / GraphTokenURL override the Graph API and OAuth2 token
	// endpoints (integration tests, on-prem gateways); empty means the public
	// Microsoft Graph.
	GraphBaseURL  string `env:"GRAPH_BASE_URL" envDefault:""`
	GraphTokenURL string `env:"GRAPH_TOKEN_URL" envDefault:""`
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

	// One replica set serves both lanes: the teams_user diff + hr lookup read
	// through a secondary-preferred client and the teams_user upserts write
	// through a primary client, so they share one URI, DB and credential pair —
	// only the read preference differs.
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`
}
