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
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

// run executes exactly one updateUsers pass and exits — this binary is
// triggered by a Kubernetes CronJob, which owns the schedule and the
// skip-if-still-running semantics (concurrencyPolicy: Forbid). A non-nil
// return exits 1 so the Job records the failure and the CronJob retries on
// the next fire (writes are idempotent upserts, so reruns are safe).
func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	// Graph rejects $top outside 1..999; fail fast on a bad knob.
	if cfg.GraphPageSize <= 0 || cfg.GraphPageSize > 999 {
		return fmt.Errorf("GRAPH_PAGE_SIZE must be in 1..999, got %d", cfg.GraphPageSize)
	}

	// SIGTERM/SIGINT (pod deletion, Job activeDeadlineSeconds) cancels the run
	// so it aborts between operations instead of being killed mid-batch.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("connect mongo read client: %w", err)
	}
	defer disconnect(readClient)
	writeClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("connect mongo write client: %w", err)
	}
	defer disconnect(writeClient)

	var opts []msgraph.Option
	if cfg.GraphBaseURL != "" {
		opts = append(opts, msgraph.WithBaseURL(cfg.GraphBaseURL))
	}
	if cfg.GraphTokenURL != "" {
		opts = append(opts, msgraph.WithTokenURL(cfg.GraphTokenURL))
	}
	lister, err := msgraph.NewUserListerClient(msgraph.Config{
		TenantID:              cfg.TeamsTenantID,
		ClientID:              cfg.TeamsClientID,
		ClientSecret:          cfg.TeamsClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
		ProxyURL:              cfg.GraphProxyURL,
	}, opts...)
	if err != nil {
		return fmt.Errorf("build user lister client: %w", err)
	}
	store := newMongoStore(readClient.Database(cfg.MongoDB), writeClient.Database(cfg.MongoDB))
	syncer := NewSyncer(store, lister, cfg.GraphPageSize)

	logger := slog.With("requestId", idgen.GenerateRequestID())
	logger.Info("teams user sync started")
	start := time.Now()
	stats, err := syncer.UpdateUsers(ctx)
	logger.Info("teams user sync finished",
		"pages", stats.Pages,
		"seen", stats.Seen,
		"existing", stats.Existing,
		"invalidUpn", stats.InvalidUPN,
		"hrUnmatched", stats.HRUnmatched,
		"upserted", stats.Upserted,
		"durationMs", time.Since(start).Milliseconds(),
		"succeeded", err == nil,
	)
	if err != nil {
		return fmt.Errorf("update users: %w", err)
	}
	return nil
}

// disconnect closes a client under its own timeout — the run context may
// already be canceled by the time the deferred cleanup executes.
func disconnect(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mongoutil.Disconnect(ctx, client)
}
