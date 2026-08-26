package main

import "time"

type config struct {
	Port   string `env:"PORT" envDefault:"8080"`
	SiteID string `env:"SITE_ID,required"`

	MinioEndpoint        string        `env:"MINIO_ENDPOINT,required"`
	MinioAccessKey       string        `env:"MINIO_ACCESS_KEY,required"`
	MinioSecretKey       string        `env:"MINIO_SECRET_KEY,required"`
	MinioUseSSL          bool          `env:"MINIO_USE_SSL" envDefault:"false"`
	MinioBucket          string        `env:"MINIO_BUCKET,required"`
	MinioDownloadTimeout time.Duration `env:"MINIO_DOWNLOAD_TIMEOUT" envDefault:"5m"`

	HTTPWriteTimeout time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"10m"`
	// HTTPReadTimeout must cover reading the whole upload body: net/http's
	// ReadTimeout spans the body, so a value below the write timeout silently
	// caps upload size no matter what HTTP_WRITE_TIMEOUT says.
	HTTPReadTimeout time.Duration `env:"HTTP_READ_TIMEOUT" envDefault:"10m"`

	CacheMaxEntries     int           `env:"CACHE_MAX_ENTRIES" envDefault:"4"`
	CacheTTL            time.Duration `env:"CACHE_TTL" envDefault:"24h"`
	CacheMaxObjectBytes int64         `env:"CACHE_MAX_OBJECT_BYTES" envDefault:"536870912"`

	// Service-account auth on the upload route. The public key only: this
	// service verifies tokens and can never mint them.
	SvcJWTPublicKey string `env:"SVCJWT_PUBLIC_KEY,required"`
	SvcJWTIssuer    string `env:"SVCJWT_ISSUER" envDefault:"admin-service"`
	SvcJWTAudience  string `env:"SVCJWT_AUDIENCE" envDefault:"client-update-service"`
	// AllowedServiceAccounts is required, not defaulted: an empty allowlist
	// would refuse every upload, and a permissive default would silently
	// reopen the hole this gate closes.
	AllowedServiceAccounts []string `env:"ALLOWED_SERVICE_ACCOUNTS,required" envSeparator:","`
}
