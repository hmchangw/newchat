package main

import (
	"fmt"
	"time"
)

// Config is the job's environment configuration. One replica-set serves both
// lanes: the teams_chat scan reads through a secondary-preferred client and the
// needCreateRoom flag update writes through a primary client, so they share one
// URI, DB and credential pair — only the read preference differs.
type Config struct {
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	NatsURL       string `env:"NATS_URL,required,notEmpty"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	// BatchSize is the maximum number of chats packed into one room-canonical
	// event. Each site's flagged chats are chunked into batches of this size.
	BatchSize int `env:"ROOM_CREATE_BATCH_SIZE" envDefault:"100"`
	// MaxWorkers bounds concurrent batch publishes across all site groups.
	MaxWorkers int `env:"MAX_WORKERS" envDefault:"8"`
	// RunTimeout is the whole-run deadline.
	RunTimeout time.Duration `env:"RUN_TIMEOUT" envDefault:"30m"`
}

// validateConfig checks the parsed Config for internal consistency. It isolates
// run()'s pure precondition checks so they are unit testable without wiring any
// real dependency.
//
//nolint:gocritic // hugeParam: cfg is passed by value once at startup; not a hot path
func validateConfig(cfg Config) error {
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("invalid config: ROOM_CREATE_BATCH_SIZE must be positive")
	}
	if cfg.MaxWorkers <= 0 {
		return fmt.Errorf("invalid config: MAX_WORKERS must be positive")
	}
	if cfg.RunTimeout <= 0 {
		return fmt.Errorf("invalid config: RUN_TIMEOUT must be positive")
	}
	return nil
}
