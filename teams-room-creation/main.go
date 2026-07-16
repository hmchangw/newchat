// Command teams-room-creation is a run-to-completion job (k8s CronJob) that
// turns Teams chats flagged needCreateRoom=true into room-canonical NATS
// events. It lists every such teams_chat, groups them by siteId, publishes each
// group in batches to chat.room.canonical.{siteId}.teams.create, and clears the
// flag for each batch that JetStream acknowledges. One global instance serves
// the whole federation.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("teams-room-creation failed", "error", err)
		os.Exit(1)
	}
}

// run wires dependencies and performs one pass. It returns an error rather than
// calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := validateConfig(cfg); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}
	defer func() {
		if err := obsShutdown(context.Background()); err != nil {
			slog.Error("observability shutdown", "error", err)
		}
	}()

	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo read connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), readClient)

	writeClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo write connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), writeClient)

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			slog.Error("nats drain", "error", err)
		}
	}()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream init: %w", err)
	}

	store := newMongoStore(readClient.Database(cfg.MongoDB), writeClient.Database(cfg.MongoDB))
	r := newRunner(store, newJetStreamPublisher(js), runConfig{
		BatchSize:  cfg.BatchSize,
		MaxWorkers: cfg.MaxWorkers,
		Now:        time.Now,
	})
	if err := r.run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	slog.Info("teams-room-creation done")
	return nil
}
