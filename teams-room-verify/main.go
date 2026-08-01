// Command teams-room-verify is a run-to-completion job (k8s CronJob) that
// audits Teams room creation. It lists every teams_chat with needVerify=true,
// groups them by siteId, and asks each site's teams-room-inspector how many
// rooms and subscriptions it actually holds for those chats. Chats whose room
// exists with one subscription per member have their flag cleared; everything
// else is reported and stays flagged for the next run. One global instance
// serves the whole federation.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

// inspectorTimeout bounds one inspector call. A batch is two projected Mongo
// queries at the far end, so this is generous; a hung site must not eat the
// CronJob's whole deadline.
const inspectorTimeout = 30 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("teams-room-verify failed", "error", err)
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
	sites, err := parseSiteURLs(cfg.SiteURLs)
	if err != nil {
		return err
	}

	// SIGTERM/SIGINT (pod deletion, Job activeDeadlineSeconds) cancels the run so
	// it aborts between operations instead of being killed mid-batch. The run
	// deadline is owned by the Kubernetes CronJob, not an app-level timeout.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	store := newMongoStore(readClient.Database(cfg.MongoDB), writeClient.Database(cfg.MongoDB))
	r := newRunner(store, newHTTPVerifier(inspectorTimeout), runConfig{
		BatchSize:  cfg.BatchSize,
		MaxWorkers: cfg.MaxWorkers,
		SiteURLs:   sites,
	})
	if err := r.run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	slog.Info("teams-room-verify done")
	return nil
}
