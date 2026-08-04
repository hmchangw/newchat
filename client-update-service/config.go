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

	CacheMaxEntries     int           `env:"CACHE_MAX_ENTRIES" envDefault:"4"`
	CacheTTL            time.Duration `env:"CACHE_TTL" envDefault:"24h"`
	CacheMaxObjectBytes int64         `env:"CACHE_MAX_OBJECT_BYTES" envDefault:"536870912"`
}
