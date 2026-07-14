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

	// AllSiteIDs is every federated site. A user statusText change fans to all of them (incl. ours) —
	// status is global-visibility and chat-originated (no other sync carries it), unlike HR fields.
	AllSiteIDs []string `env:"ALL_SITE_IDS" envDefault:"" envSeparator:","`

	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	// Source legacy Mongo (replica set): the connector tails it; this service re-reads
	// the full current doc by _id on update events (the connector forwards only the delta).
	SourceMongoURI string `env:"SOURCE_MONGO_URI,required"`
	SourceUsername string `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourcePassword string `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB       string `env:"SOURCE_DB" envDefault:"rocketchat"`

	// Target new-stack per-site Mongo: users insert-if-absent + thread_room/user FK resolution.
	TargetMongoURI string `env:"TARGET_MONGO_URI,required"`
	TargetUsername string `env:"TARGET_MONGO_USERNAME" envDefault:""`
	TargetPassword string `env:"TARGET_MONGO_PASSWORD" envDefault:""`
	TargetDB       string `env:"TARGET_DB" envDefault:"chat"`

	// Source collection names (the connector's raw collection names).
	RoomsCollection         string `env:"ROOMS_COLLECTION" envDefault:"rocketchat_rooms"`
	SubscriptionsCollection string `env:"SUBSCRIPTIONS_COLLECTION" envDefault:"rocketchat_subscriptions"`
	ThreadSubsCollection    string `env:"THREAD_SUBS_COLLECTION" envDefault:"company_thread_subscriptions"`
	UsersCollection         string `env:"USERS_COLLECTION" envDefault:"users"`
	RoomMembersCollection   string `env:"ROOM_MEMBERS_COLLECTION" envDefault:"company_room_members"`

	SourceReadPreference string `env:"SOURCE_READ_PREFERENCE" envDefault:"primaryPreferred"`

	ConsumerDurable  string `env:"CONSUMER_DURABLE" envDefault:"oplog-collections-transformer"`
	MaxDeliver       int    `env:"MAX_DELIVER" envDefault:"1000"`
	DeleteMaxDeliver int    `env:"DELETE_MAX_DELIVER" envDefault:"60"`

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
	// caarlos0/env `required` only rejects an unset var, not a whitespace-only one. Trim and
	// re-validate the required scalars too, so a value like "  " fails here rather than breaking
	// subject building / connections later at runtime.
	cfg.SiteID = strings.TrimSpace(cfg.SiteID)
	cfg.NatsURL = strings.TrimSpace(cfg.NatsURL)
	cfg.SourceMongoURI = strings.TrimSpace(cfg.SourceMongoURI)
	cfg.TargetMongoURI = strings.TrimSpace(cfg.TargetMongoURI)
	cfg.RoomsCollection = strings.TrimSpace(cfg.RoomsCollection)
	cfg.SubscriptionsCollection = strings.TrimSpace(cfg.SubscriptionsCollection)
	cfg.ThreadSubsCollection = strings.TrimSpace(cfg.ThreadSubsCollection)
	cfg.UsersCollection = strings.TrimSpace(cfg.UsersCollection)
	cfg.RoomMembersCollection = strings.TrimSpace(cfg.RoomMembersCollection)
	for name, v := range map[string]string{
		"SITE_ID":                  cfg.SiteID,
		"NATS_URL":                 cfg.NatsURL,
		"SOURCE_MONGO_URI":         cfg.SourceMongoURI,
		"TARGET_MONGO_URI":         cfg.TargetMongoURI,
		"ROOMS_COLLECTION":         cfg.RoomsCollection,
		"SUBSCRIPTIONS_COLLECTION": cfg.SubscriptionsCollection,
		"THREAD_SUBS_COLLECTION":   cfg.ThreadSubsCollection,
		"USERS_COLLECTION":         cfg.UsersCollection,
		"ROOM_MEMBERS_COLLECTION":  cfg.RoomMembersCollection,
	} {
		if v == "" {
			return config{}, fmt.Errorf("%s must be non-empty", name)
		}
	}
	// DeleteMaxDeliver above MaxDeliver is a no-op footgun: the shorter cap would never trip first.
	// Clamp it down when MaxDeliver is finite (0 = unlimited).
	if cfg.MaxDeliver > 0 && cfg.DeleteMaxDeliver > cfg.MaxDeliver {
		cfg.DeleteMaxDeliver = cfg.MaxDeliver
	}
	return cfg, nil
}
