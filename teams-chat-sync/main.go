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
)

// Config is the job's environment configuration.
type Config struct {
	// A single primary (master) client serves every collection: the teams_user
	// scan, its watermark update and the teams_chat upserts all target the
	// primary, so these freshly populated collections are never read from a
	// lagging secondary.
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	MaxWorkers int           `env:"MAX_WORKERS" envDefault:"8"`
	RunTimeout time.Duration `env:"RUN_TIMEOUT" envDefault:"30m"`
	// DefaultFrom is the RFC3339 UTC watermark used for users that have never
	// synced (teams_user docs without a from field).
	DefaultFrom string `env:"SYNC_DEFAULT_FROM" envDefault:"2026-04-01T00:00:00Z"`
	// DefaultSiteID is the fallback siteID for a chat whose member-majority vote
	// is empty (no member found in teams_user). Required and non-empty so every
	// synced chat is guaranteed a non-empty siteID.
	DefaultSiteID string `env:"SYNC_DEFAULT_SITE_ID,required,notEmpty"`

	GraphTenantID     string `env:"GRAPH_TENANT_ID,required"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required"`
	// GraphChatsPageSize is the $top page size for Graph list-chats requests.
	// 50 is Graph's documented maximum for that endpoint.
	GraphChatsPageSize int `env:"GRAPH_CHATS_PAGE_SIZE" envDefault:"50"`
	// GraphTLSInsecureSkipVerify disables Graph TLS verification. Defaults to
	// true because this job runs on-prem behind a TLS-intercepting proxy that
	// presents its own certificate; set it to false where Graph presents a
	// verifiable certificate chain.
	GraphTLSInsecureSkipVerify bool `env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"true"`
	// GraphProxyURL, when set, routes this service's Graph client through this
	// proxy explicitly (overriding HTTPS_PROXY/HTTP_PROXY). Must include a scheme
	// and host, e.g. "http://proxy.corp:8080". Empty falls back to the standard
	// proxy env vars.
	GraphProxyURL string `env:"GRAPH_PROXY_URL" envDefault:""`
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
	if cfg.GraphChatsPageSize <= 0 {
		return time.Time{}, fmt.Errorf("invalid config: GRAPH_CHATS_PAGE_SIZE must be positive")
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

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), mongoClient)

	store := newMongoStore(mongoClient.Database(cfg.MongoDB))

	graph, err := msgraph.NewChatsClient(msgraph.Config{
		TenantID:              cfg.GraphTenantID,
		ClientID:              cfg.GraphClientID,
		ClientSecret:          cfg.GraphClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
		ProxyURL:              cfg.GraphProxyURL,
	}, msgraph.WithChatsPageSize(cfg.GraphChatsPageSize))
	if err != nil {
		return fmt.Errorf("build chats client: %w", err)
	}

	s := newSyncer(store, store, graph, syncConfig{
		MaxWorkers:    cfg.MaxWorkers,
		DefaultFrom:   defaultFrom,
		Now:           time.Now,
		DefaultSiteID: cfg.DefaultSiteID,
	})
	if err := s.run(ctx); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	slog.Info("teams-chat-sync done")
	return nil
}
