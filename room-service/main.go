package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	NatsURL           string          `env:"NATS_URL,required"`
	NatsCredsFile     string          `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID            string          `env:"SITE_ID"                   envDefault:"site-local"`
	MongoURI          string          `env:"MONGO_URI,required"`
	MongoDB           string          `env:"MONGO_DB"                  envDefault:"chat"`
	MongoUsername     string          `env:"MONGO_USERNAME"            envDefault:""`
	MongoPassword     string          `env:"MONGO_PASSWORD"            envDefault:""`
	MaxRoomSize       int             `env:"MAX_ROOM_SIZE"             envDefault:"1000"`
	MaxBatchSize      int             `env:"MAX_BATCH_SIZE"            envDefault:"1000"`
	MemberListTimeout time.Duration   `env:"MEMBER_LIST_TIMEOUT"       envDefault:"5s"`
	ValkeyAddr        string          `env:"VALKEY_ADDR,required"`
	ValkeyPassword    string          `env:"VALKEY_PASSWORD"           envDefault:""`
	ValkeyGracePeriod time.Duration   `env:"VALKEY_KEY_GRACE_PERIOD,required"`
	Bootstrap         bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if cfg.MemberListTimeout <= 0 {
		slog.Error("invalid MEMBER_LIST_TIMEOUT: must be > 0", "value", cfg.MemberListTimeout)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "room-service")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)

	keyStore, err := roomkeystore.NewValkeyStore(roomkeystore.Config{
		Addr:        cfg.ValkeyAddr,
		Password:    cfg.ValkeyPassword,
		GracePeriod: cfg.ValkeyGracePeriod,
	})
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	store := NewMongoStore(db)
	// Bounded timeout so a hung createIndexes surfaces at startup.
	ensureCtx, ensureCancel := context.WithTimeout(ctx, 30*time.Second)
	if err := store.EnsureIndexes(ensureCtx); err != nil {
		ensureCancel()
		slog.Error("ensure store indexes failed", "error", err)
		os.Exit(1)
	}
	ensureCancel()
	memberListClient := NewNATSMemberListClient(nc.NatsConn(), cfg.MemberListTimeout)
	handler := NewHandler(store, keyStore, memberListClient, cfg.SiteID, cfg.MaxRoomSize, cfg.MaxBatchSize, func(ctx context.Context, subj string, data []byte) error {
		if _, err := js.Publish(ctx, subj, data); err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	})

	if err := handler.RegisterCRUD(nc); err != nil {
		slog.Error("register CRUD handlers failed", "error", err)
		os.Exit(1)
	}

	slog.Info("room-service running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error {
			if closer, ok := keyStore.(interface{ Close() error }); ok {
				return closer.Close()
			}
			return nil
		},
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
	)
}
