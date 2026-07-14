package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// clusterDomain maps a site to its media-service base URL (incl. scheme).
type clusterDomain struct {
	SiteID string `json:"siteID"`
	Domain string `json:"domain"`
}

// clusterDomains is parsed from the CLUSTER_DOMAINS env var — a JSON array of
// {siteID, domain} objects, indexed at parse time into a siteID→URL map so
// baseURL is an O(1) lookup. It implements encoding.TextUnmarshaler so
// caarlos0/env populates it directly from the env string (rather than env's
// built-in slice/map splitting).
type clusterDomains struct {
	byID map[string]string
}

func (c *clusterDomains) UnmarshalText(text []byte) error {
	var entries []clusterDomain
	if err := json.Unmarshal(text, &entries); err != nil {
		return fmt.Errorf("parse CLUSTER_DOMAINS json: %w", err)
	}
	c.byID = make(map[string]string, len(entries))
	for _, e := range entries {
		if _, dup := c.byID[e.SiteID]; dup {
			return fmt.Errorf("parse CLUSTER_DOMAINS json: duplicate siteID %q", e.SiteID)
		}
		c.byID[e.SiteID] = e.Domain
	}
	return nil
}

// baseURL returns the configured base URL for a site, or "" if not configured.
func (c clusterDomains) baseURL(siteID string) string {
	return c.byID[siteID]
}

type config struct {
	Port     string `env:"PORT" envDefault:"8080"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
	SiteID   string `env:"SITE_ID,required"`

	// CLUSTER_DOMAINS is a JSON array of {siteID, domain} objects mapping each
	// site to that cluster's media-service base URL (incl. scheme), used
	// verbatim as a cross-cluster redirect target.
	ClusterDomains clusterDomains `env:"CLUSTER_DOMAINS,required"`

	EmployeePhotoBaseURL string `env:"EMPLOYEE_PHOTO_BASE_URL,required"`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`

	MinioEndpoint  string `env:"MINIO_ENDPOINT,required"`
	MinioAccessKey string `env:"MINIO_ACCESS_KEY,required"`
	MinioSecretKey string `env:"MINIO_SECRET_KEY,required"`
	MinioUseSSL    bool   `env:"MINIO_USE_SSL" envDefault:"false"`
	MinioBucket    string `env:"MINIO_BUCKET" envDefault:"avatars"`

	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"`

	MaxUploadBytes     int64 `env:"MAX_UPLOAD_BYTES" envDefault:"1048576"`
	CacheMaxAgeSeconds int   `env:"CACHE_MAX_AGE_SECONDS" envDefault:"21600"`

	// Custom-emoji upload limits. Bytes cap the raw body; dimension caps the
	// decoded width AND height independently.
	EmojiMaxUploadBytes int64 `env:"EMOJI_MAX_UPLOAD_BYTES" envDefault:"262144"`
	EmojiMaxDimension   int   `env:"EMOJI_MAX_DIMENSION" envDefault:"512"`

	// EmojiDeleteEnabled gates the emoji.delete RPC (kill-switch, default off).
	EmojiDeleteEnabled bool `env:"EMOJI_DELETE_ENABLED" envDefault:"false"`

	// account→employeeId in-memory cache. The mapping is near-immutable, so the
	// TTL is long (re-fetch is cheap and self-heals rare changes); capacity is
	// sized to the employee population so the cache does not evict.
	EIDCacheTTL      time.Duration `env:"EID_CACHE_TTL" envDefault:"24h"`
	EIDCacheCapacity int           `env:"EID_CACHE_CAPACITY" envDefault:"120000"`
}

// clusterBaseURL returns the configured base URL for a site, or "" if unknown.
func (c *config) clusterBaseURL(siteID string) string { return c.ClusterDomains.baseURL(siteID) }
