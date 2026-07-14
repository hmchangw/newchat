package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/caarlos0/env/v11"
)

type config struct {
	SiteID                  string `env:"SITE_ID,required"`
	NatsURL                 string `env:"NATS_URL,required"`
	NatsCredsFile           string `env:"NATS_CREDS_FILE" envDefault:""`
	SourceMongoURI          string `env:"SOURCE_MONGO_URI,required"`
	SourceUsername          string `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourcePassword          string `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB                string `env:"SOURCE_DB" envDefault:"rocketchat"`
	SourceMessageCollection string `env:"SOURCE_MESSAGE_COLLECTION" envDefault:"rocketchat_message"`
	// SoftDeleteType is the source system-message type marking a removed message (RocketChat: "rm").
	// Configurable so a differently-configured source can be migrated without a code change.
	SoftDeleteType        string        `env:"SOFT_DELETE_TYPE" envDefault:"rm"`
	SourceReadPreference  string        `env:"SOURCE_READ_PREFERENCE" envDefault:"primaryPreferred"`
	ConsumerDurable       string        `env:"CONSUMER_DURABLE" envDefault:"oplog-transformer"`
	HistoryRequestTimeout time.Duration `env:"HISTORY_REQUEST_TIMEOUT" envDefault:"10s"`
	// MaxDeliver bounds redelivery; large because an edit/delete can arrive before message-worker has
	// persisted the insert (async via canonical) and we Nak until the row lands. Finite so a truly-orphaned op Terms.
	MaxDeliver int `env:"MAX_DELIVER" envDefault:"1000"`
	// DeleteMaxDeliver: shorter cap for hard-delete ops — a foreign-origin one carries no doc, can't be recognised, and would Nak to the global cap.
	// The local delete-before-insert race converges in seconds. Tune up if insert-persist lag can exceed DeleteMaxDeliver×2s.
	DeleteMaxDeliver int    `env:"DELETE_MAX_DELIVER" envDefault:"60"`
	MetricsAddr      string `env:"METRICS_ADDR" envDefault:":9090"`
	LogLevel         string `env:"LOG_LEVEL" envDefault:"info"`
}

func parseConfig() (config, error) {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}
	// DeleteMaxDeliver above MaxDeliver is a no-op footgun: the shorter cap would never trip first.
	// Clamp it down loudly.
	if cfg.MaxDeliver > 0 && cfg.DeleteMaxDeliver > cfg.MaxDeliver {
		slog.Warn("DELETE_MAX_DELIVER exceeds MAX_DELIVER — clamping to MAX_DELIVER",
			"deleteMaxDeliver", cfg.DeleteMaxDeliver, "maxDeliver", cfg.MaxDeliver)
		cfg.DeleteMaxDeliver = cfg.MaxDeliver
	}
	return cfg, nil
}
