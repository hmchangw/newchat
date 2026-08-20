package main

import (
	"fmt"
	"strings"

	"github.com/caarlos0/env/v11"
)

// config holds every tunable, parsed from the environment via caarlos0/env.
// Required fields have no default and fail-fast at startup when absent.
//
// Unlike oplog-direct-transfer, the DR applier is self-contained: the producer-side
// connector opens its change stream with updateLookup, so every insert/replace/update
// event carries the full post-image inline. The applier therefore needs NO source Mongo
// connection and never reads back across the gateway to the origin site.
type config struct {
	// SiteID is the origin site this applier instance replicates. One instance per origin site.
	SiteID string `env:"SITE_ID,required"`

	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	// Target backup Mongo: verbatim upsert/delete by _id into the per-site namespace.
	TargetMongoURI string `env:"TARGET_MONGO_URI,required"`
	TargetUsername string `env:"TARGET_MONGO_USERNAME" envDefault:""`
	TargetPassword string `env:"TARGET_MONGO_PASSWORD" envDefault:""`

	// TargetDBPrefix namespaces each origin site into its own backup database (prefix + siteID).
	// A whole database per origin site keeps sites isolated and gives failback (SP5) a clean
	// boundary for separating backup-authored writes from origin-replicated ones.
	TargetDBPrefix string `env:"TARGET_DB_PREFIX" envDefault:"chat_dr_"`

	// DRCollections is the lifeboat operational slice replicated verbatim to the same-named
	// destination collection. Config-driven so adding one is an env change.
	DRCollections []string `env:"DR_COLLECTIONS" envSeparator:"," envDefault:"rooms,subscriptions,room_members,thread_rooms,thread_subscriptions,users"`

	ConsumerDurable string `env:"CONSUMER_DURABLE" envDefault:"dr-oplog-worker"`
	MaxDeliver      int    `env:"MAX_DELIVER" envDefault:"1000"`

	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`

	HealthAddr string `env:"HEALTH_ADDR" envDefault:":9090"`
}

type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// TargetDB is the backup database name for this instance's origin site.
func (c *config) TargetDB() string {
	return c.TargetDBPrefix + c.SiteID
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
	cfg.TargetMongoURI = strings.TrimSpace(cfg.TargetMongoURI)
	for name, v := range map[string]string{
		"SITE_ID":          cfg.SiteID,
		"NATS_URL":         cfg.NatsURL,
		"TARGET_MONGO_URI": cfg.TargetMongoURI,
	} {
		if v == "" {
			return config{}, fmt.Errorf("%s must be non-empty", name)
		}
	}
	// Trim, non-empty, dedup the collection list — each maps to one consumer subject.
	seen := make(map[string]struct{}, len(cfg.DRCollections))
	trimmed := make([]string, 0, len(cfg.DRCollections))
	for _, c := range cfg.DRCollections {
		c = strings.TrimSpace(c)
		if c == "" {
			return config{}, fmt.Errorf("DR_COLLECTIONS has an empty entry (check for stray commas)")
		}
		if _, dup := seen[c]; dup {
			return config{}, fmt.Errorf("DR_COLLECTIONS has duplicate entry %q", c)
		}
		seen[c] = struct{}{}
		trimmed = append(trimmed, c)
	}
	if len(trimmed) == 0 {
		return config{}, fmt.Errorf("DR_COLLECTIONS must list at least one collection")
	}
	cfg.DRCollections = trimmed
	return cfg, nil
}
