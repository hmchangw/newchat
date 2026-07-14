package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// MongoConfig holds MongoDB connection settings (env prefix: MONGO_).
type MongoConfig struct {
	URI      string `env:"URI,notEmpty"`
	DB       string `env:"DB"       envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
}

// NATSConfig holds NATS connection settings (env prefix: NATS_).
type NATSConfig struct {
	URL       string `env:"URL,notEmpty"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

// Config is the top-level configuration for user-service.
type Config struct {
	// SiteID is required: baked into subscription subjects and inbox routing; missing it would silently federate under a wrong ID.
	SiteID                   string        `env:"SITE_ID,notEmpty"`
	AllSiteIDs               []string      `env:"ALL_SITE_IDS"           envDefault:"" envSeparator:","`
	MaxSubscriptionLimit     int           `env:"MAX_SUBSCRIPTION_LIMIT" envDefault:"1000"`
	DefaultSubscriptionLimit int           `env:"SUBSCRIPTION_DEFAULT_LIMIT" envDefault:"40"`
	MaxAppsLimit             int           `env:"APPS_MAX_LIMIT" envDefault:"100"`
	DefaultAppsLimit         int           `env:"APPS_DEFAULT_LIMIT" envDefault:"20"`
	MaxAccountNames          int           `env:"MAX_ACCOUNT_NAMES"      envDefault:"100"`
	HandlerTimeout           time.Duration `env:"HANDLER_TIMEOUT"        envDefault:"15s"`
	MetricsAddr              string        `env:"METRICS_ADDR"           envDefault:":9090"`
	Mongo                    MongoConfig   `envPrefix:"MONGO_"`
	NATS                     NATSConfig    `envPrefix:"NATS_"`
}

// Load parses environment variables into Config; rejects MAX_SUBSCRIPTION_LIMIT < 1 because $limit:0 errors at query time.
func Load() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse user-service config: %w", err)
	}
	if cfg.MaxSubscriptionLimit < 1 {
		return Config{}, fmt.Errorf("MAX_SUBSCRIPTION_LIMIT must be >= 1, got %d", cfg.MaxSubscriptionLimit)
	}
	if cfg.DefaultSubscriptionLimit < 1 {
		return Config{}, fmt.Errorf("SUBSCRIPTION_DEFAULT_LIMIT must be >= 1, got %d", cfg.DefaultSubscriptionLimit)
	}
	if cfg.DefaultSubscriptionLimit > cfg.MaxSubscriptionLimit {
		return Config{}, fmt.Errorf("SUBSCRIPTION_DEFAULT_LIMIT (%d) must be <= MAX_SUBSCRIPTION_LIMIT (%d)", cfg.DefaultSubscriptionLimit, cfg.MaxSubscriptionLimit)
	}
	if cfg.MaxAppsLimit < 1 {
		return Config{}, fmt.Errorf("APPS_MAX_LIMIT must be >= 1, got %d", cfg.MaxAppsLimit)
	}
	if cfg.DefaultAppsLimit < 1 {
		return Config{}, fmt.Errorf("APPS_DEFAULT_LIMIT must be >= 1, got %d", cfg.DefaultAppsLimit)
	}
	if cfg.DefaultAppsLimit > cfg.MaxAppsLimit {
		return Config{}, fmt.Errorf("APPS_DEFAULT_LIMIT (%d) must be <= APPS_MAX_LIMIT (%d)", cfg.DefaultAppsLimit, cfg.MaxAppsLimit)
	}
	return cfg, nil
}
