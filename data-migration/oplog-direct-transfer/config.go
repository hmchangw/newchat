package main

import (
	"fmt"
	"strings"

	"github.com/caarlos0/env/v11"
)

// config holds every tunable, parsed from the environment via caarlos0/env.
// Required fields have no default and fail-fast at startup when absent.
type config struct {
	SiteID string `env:"SITE_ID,required"`

	// DirectCollections is the set of source collections copied verbatim to the same-named
	// destination collection. Config-driven so adding one is an env + WATCH_COLLECTIONS change.
	DirectCollections []string `env:"DIRECT_COLLECTIONS" envSeparator:"," envDefault:"rocketchat_avatar,company_apps_v,company_bot_cmd_men,company_tsso_tokens,rocketchat_uploads,company_bot_authorization,ufsTokens,user_devices"`

	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	// Source legacy Mongo: re-read the full current doc by _id on update events.
	SourceMongoURI string `env:"SOURCE_MONGO_URI,required"`
	SourceUsername string `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourcePassword string `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB       string `env:"SOURCE_DB" envDefault:"rocketchat"`

	// Target new-stack per-site Mongo: verbatim upsert/delete by _id.
	TargetMongoURI string `env:"TARGET_MONGO_URI,required"`
	TargetUsername string `env:"TARGET_MONGO_USERNAME" envDefault:""`
	TargetPassword string `env:"TARGET_MONGO_PASSWORD" envDefault:""`
	TargetDB       string `env:"TARGET_DB" envDefault:"chat"`

	SourceReadPreference string `env:"SOURCE_READ_PREFERENCE" envDefault:"primaryPreferred"`

	ConsumerDurable string `env:"CONSUMER_DURABLE" envDefault:"oplog-direct-transfer"`
	MaxDeliver      int    `env:"MAX_DELIVER" envDefault:"1000"`

	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`

	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9090"`
	LogLevel    string `env:"LOG_LEVEL" envDefault:"info"`
}

type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// parseConfig parses and validates the environment configuration.
func parseConfig() (config, error) {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}
	// `required` only rejects an unset var, not whitespace. Trim + re-validate required scalars.
	cfg.SiteID = strings.TrimSpace(cfg.SiteID)
	cfg.NatsURL = strings.TrimSpace(cfg.NatsURL)
	cfg.SourceMongoURI = strings.TrimSpace(cfg.SourceMongoURI)
	cfg.TargetMongoURI = strings.TrimSpace(cfg.TargetMongoURI)
	for name, v := range map[string]string{
		"SITE_ID":          cfg.SiteID,
		"NATS_URL":         cfg.NatsURL,
		"SOURCE_MONGO_URI": cfg.SourceMongoURI,
		"TARGET_MONGO_URI": cfg.TargetMongoURI,
	} {
		if v == "" {
			return config{}, fmt.Errorf("%s must be non-empty", name)
		}
	}
	// Trim, non-empty, dedup the collection list — each maps to one consumer subject + one lookup.
	seen := make(map[string]struct{}, len(cfg.DirectCollections))
	trimmed := make([]string, 0, len(cfg.DirectCollections))
	for _, c := range cfg.DirectCollections {
		c = strings.TrimSpace(c)
		if c == "" {
			return config{}, fmt.Errorf("DIRECT_COLLECTIONS has an empty entry (check for stray commas)")
		}
		if _, dup := seen[c]; dup {
			return config{}, fmt.Errorf("DIRECT_COLLECTIONS has duplicate entry %q", c)
		}
		seen[c] = struct{}{}
		trimmed = append(trimmed, c)
	}
	if len(trimmed) == 0 {
		return config{}, fmt.Errorf("DIRECT_COLLECTIONS must list at least one collection")
	}
	cfg.DirectCollections = trimmed
	return cfg, nil
}
