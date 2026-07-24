package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"`
	SiteID        string `env:"SITE_ID,required"`
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`

	// MaxConcurrency caps in-flight req/reply handlers across all routes.
	MaxConcurrency int `env:"MAX_CONCURRENCY" envDefault:"200"`

	HealthAddr   string          `env:"HEALTH_ADDR"    envDefault:":8081"`
	PProfEnabled bool            `env:"PPROF_ENABLED"  envDefault:"false"`
	Bootstrap    bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("bot-message-handler exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("init jetstream: %w", err)
	}
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		return fmt.Errorf("bootstrap streams: %w", err)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	store := newStoreMongo(mongoClient.Database(cfg.MongoDB))

	pub := JetStreamPublisher{JS: js}
	h := newHandler(store, pub, cfg.SiteID)

	router := natsrouter.New(nc, "bot-message-handler", natsrouter.WithMaxConcurrency(cfg.MaxConcurrency))
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
	h.Register(router)

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}

	slog.Info("bot-message-handler running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
	return nil
}
