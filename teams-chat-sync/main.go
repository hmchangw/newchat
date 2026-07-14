// Command teams-chat-sync is a run-to-completion job (k8s CronJob) that
// mirrors Microsoft Teams chats into the teams_chat collection. One global
// instance serves the whole federation: it reads every teams_user, fetches
// each user's chat window from Graph, resolves each chat's site by
// member-majority vote, and advances per-user watermarks on success.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/otelutil"
)

// Config is the job's environment configuration.
type Config struct {
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	MaxWorkers int           `env:"MAX_WORKERS" envDefault:"8"`
	RunTimeout time.Duration `env:"RUN_TIMEOUT" envDefault:"30m"`
	// DefaultFrom is the RFC3339 UTC watermark used for users that have never
	// synced (teams_user docs without a from field).
	DefaultFrom string `env:"SYNC_DEFAULT_FROM" envDefault:"2026-04-01T00:00:00Z"`

	GraphTenantID     string `env:"GRAPH_TENANT_ID,required"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required"`
	// GraphTLSInsecureSkipVerify disables Graph TLS verification (opt-in,
	// default false) for dev/on-prem environments behind a TLS-intercepting
	// proxy. The proxy itself is taken from HTTPS_PROXY/HTTP_PROXY.
	GraphTLSInsecureSkipVerify bool `env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"false"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("teams-chat-sync failed", "error", err)
		os.Exit(1)
	}
}

// validateConfig checks the parsed Config for internal consistency and
// parses DefaultFrom into a time.Time. It isolates run()'s pure logic so it
// is unit testable without wiring any real dependency.
//
//nolint:gocritic // hugeParam: cfg is passed by value once at startup; not a hot path
func validateConfig(cfg Config) (time.Time, error) {
	if cfg.MaxWorkers <= 0 || cfg.RunTimeout <= 0 {
		return time.Time{}, fmt.Errorf("invalid config: MAX_WORKERS and RUN_TIMEOUT must be positive")
	}
	defaultFrom, err := time.Parse(time.RFC3339, cfg.DefaultFrom)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse SYNC_DEFAULT_FROM: %w", err)
	}
	return defaultFrom, nil
}

// run wires dependencies and performs one sync. It returns an error rather
// than calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	defaultFrom, err := validateConfig(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	tracerShutdown, err := otelutil.InitTracer(ctx, "teams-chat-sync")
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() {
		if err := tracerShutdown(context.Background()); err != nil {
			slog.Warn("tracer shutdown", "error", err)
		}
	}()

	client, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), client)
	store := newMongoStore(client.Database(cfg.MongoDB))

	graph := msgraph.NewChatsClient(msgraph.Config{
		TenantID:              cfg.GraphTenantID,
		ClientID:              cfg.GraphClientID,
		ClientSecret:          cfg.GraphClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
	})

	s := newSyncer(store, store, graph, syncConfig{
		MaxWorkers:  cfg.MaxWorkers,
		DefaultFrom: defaultFrom,
		Now:         time.Now,
	})
	if err := s.run(ctx); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	slog.Info("teams-chat-sync done")
	return nil
}
