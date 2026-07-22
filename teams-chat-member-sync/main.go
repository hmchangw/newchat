// Command teams-chat-member-sync is a run-to-completion job (k8s CronJob) that
// resolves the authoritative member list for teams_chat documents flagged
// needMemberSync=true, then hands them to the room-creation stage by setting
// needCreateRoom=true.
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
	"github.com/hmchangw/chat/pkg/msgraph"
)

// Config is the job's environment configuration.
type Config struct {
	// One replica set serves both lanes: reads go through a secondary-preferred
	// client (teams_chat scan + teams_user resolution) and writes through a
	// primary client (teams_chat member updates), so they share one URI, DB and
	// credential pair — only the read preference differs.
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	MaxWorkers int `env:"MAX_WORKERS" envDefault:"8"`

	GraphTenantID     string `env:"GRAPH_TENANT_ID,required"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required"`
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
		slog.Error("teams-chat-member-sync failed", "error", err)
		os.Exit(1)
	}
}

// validateConfig checks the parsed Config for internal consistency. It isolates
// run()'s pure logic so it is unit testable without wiring any dependency.
//
//nolint:gocritic // hugeParam: cfg is passed by value once at startup; not a hot path
func validateConfig(cfg Config) error {
	if cfg.MaxWorkers <= 0 {
		return fmt.Errorf("invalid config: MAX_WORKERS must be positive")
	}
	return nil
}

// run wires dependencies and performs one sync. It returns an error rather than
// calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := validateConfig(cfg); err != nil {
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

	// Each worker issues one sequential Graph request at a time, so keep one warm
	// idle Graph connection per worker.
	graph, err := msgraph.NewChatMembersClient(msgraph.Config{
		TenantID:              cfg.GraphTenantID,
		ClientID:              cfg.GraphClientID,
		ClientSecret:          cfg.GraphClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
		ProxyURL:              cfg.GraphProxyURL,
	}, msgraph.WithMaxIdleConns(cfg.MaxWorkers))
	if err != nil {
		return fmt.Errorf("build chat members client: %w", err)
	}

	s := newSyncer(store, store, graph, syncConfig{
		MaxWorkers: cfg.MaxWorkers,
		Now:        time.Now,
	})
	if err := s.run(ctx); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	slog.Info("teams-chat-member-sync done")
	return nil
}
