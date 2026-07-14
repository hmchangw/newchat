package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/caarlos0/env/v11"
)

// config holds every tunable, parsed from the environment via caarlos0/env. Required fields have no default and fail-fast at startup when absent.
type config struct {
	SiteID string `env:"SITE_ID,required"`

	// Source legacy Mongo (replica set): change streams are read here and checkpoints written here.
	SourceMongoURI string `env:"SOURCE_MONGO_URI,required"`
	SourceUsername string `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourcePassword string `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB       string `env:"SOURCE_DB"            envDefault:"rocketchat"`
	CheckpointDB   string `env:"CHECKPOINT_DB"        envDefault:"migration"`

	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	WatchCollections []string `env:"WATCH_COLLECTIONS,required"`

	// MessageCollection scopes the federation-origin $match; collections-role deployments legitimately don't watch it.
	MessageCollection string `env:"MESSAGE_COLLECTION" envDefault:"rocketchat_message"`

	ReadPreference string `env:"READ_PREFERENCE" envDefault:"secondary"`

	// CheckpointEvery: save the resume token once every N acked events (and on shutdown); larger = more replay on crash (deduped).
	CheckpointEvery int `env:"CHECKPOINT_EVERY" envDefault:"100"`

	// CheckpointMaxAgeSeconds bounds replay by wall-clock: flush the latest frontier at least this often even below CheckpointEvery.
	CheckpointMaxAgeSeconds int `env:"CHECKPOINT_MAX_AGE" envDefault:"30"`

	// Start-point resolution (see resolveStartPoint).
	StartMode        string `env:"START_MODE"         envDefault:"now"` // now | time
	StartAtTime      string `env:"START_AT_TIME"      envDefault:""`    // RFC3339 or unix-ms
	StartResumeToken string `env:"START_RESUME_TOKEN" envDefault:""`    // _data hex, one-off seed override

	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`

	// MetricsAddr binds the Prometheus /metrics + /healthz listener (k8s probe target).
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9090"`

	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}

// parseConfig parses and validates the environment configuration.
func parseConfig() (config, error) {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}
	// Reject empty entries (e.g. from a trailing/double comma) rather than silently
	// dropping a watcher, which would miss CDC for a collection. Store trimmed values.
	trimmed := make([]string, 0, len(cfg.WatchCollections))
	for _, raw := range cfg.WatchCollections {
		coll := strings.TrimSpace(raw)
		if coll == "" {
			return config{}, fmt.Errorf("WATCH_COLLECTIONS has an empty entry (check for stray commas)")
		}
		trimmed = append(trimmed, coll)
	}
	cfg.WatchCollections = trimmed
	switch cfg.StartMode {
	case "now", "time":
	default:
		return config{}, fmt.Errorf("invalid START_MODE %q (want now|time)", cfg.StartMode)
	}
	if cfg.StartMode == "time" && cfg.StartAtTime == "" {
		return config{}, fmt.Errorf("START_MODE=time requires START_AT_TIME")
	}
	if cfg.CheckpointEvery < 1 {
		return config{}, fmt.Errorf("CHECKPOINT_EVERY must be >= 1, got %d", cfg.CheckpointEvery)
	}
	if cfg.CheckpointMaxAgeSeconds < 1 {
		return config{}, fmt.Errorf("CHECKPOINT_MAX_AGE must be >= 1, got %d", cfg.CheckpointMaxAgeSeconds)
	}
	if dup := firstDuplicate(cfg.WatchCollections); dup != "" {
		return config{}, fmt.Errorf("WATCH_COLLECTIONS has duplicate entry %q (each collection maps to one watcher and one checkpoint)", dup)
	}
	// MESSAGE_COLLECTION must always be defined but need NOT be watched — "watched ⟹ filtered"
	// holds by construction: the $match applies to whichever watcher's name equals it.
	cfg.MessageCollection = strings.TrimSpace(cfg.MessageCollection)
	if cfg.MessageCollection == "" {
		return config{}, fmt.Errorf("MESSAGE_COLLECTION must be non-empty (the federation-origin $match would never run)")
	}
	return cfg, nil
}

// watchesMessages reports whether this deployment watches the federated message collection (message role).
func (c *config) watchesMessages() bool {
	return slices.Contains(c.WatchCollections, c.MessageCollection)
}

// firstDuplicate returns the first repeated (trimmed) entry, or "" if all unique.
func firstDuplicate(items []string) string {
	seen := make(map[string]bool, len(items))
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			continue
		}
		if seen[it] {
			return it
		}
		seen[it] = true
	}
	return ""
}
