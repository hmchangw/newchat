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
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// Config is the job's environment configuration.
type Config struct {
	// Mongo traffic is split into a read client (teams_chat scan + teams_user
	// resolution, secondary-preferred) and a write client (teams_chat member
	// updates), mirroring the sibling teams-chat-sync job.
	MongoReadURI      string `env:"MONGO_READ_URI,required,notEmpty"`
	MongoReadUsername string `env:"MONGO_READ_USERNAME" envDefault:""`
	MongoReadPassword string `env:"MONGO_READ_PASSWORD" envDefault:""`
	MongoReadDB       string `env:"MONGO_READ_DB" envDefault:"chat"`

	MongoWriteURI      string `env:"MONGO_WRITE_URI,required,notEmpty"`
	MongoWriteUsername string `env:"MONGO_WRITE_USERNAME" envDefault:""`
	MongoWritePassword string `env:"MONGO_WRITE_PASSWORD" envDefault:""`
	MongoWriteDB       string `env:"MONGO_WRITE_DB" envDefault:"chat"`

	MaxWorkers int           `env:"MAX_WORKERS" envDefault:"8"`
	RunTimeout time.Duration `env:"RUN_TIMEOUT" envDefault:"30m"`

	GraphTenantID     string `env:"GRAPH_TENANT_ID,required"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required"`
	// GraphMembersPageSize is the $top page size for GET /chats/{id}/members.
	GraphMembersPageSize int `env:"GRAPH_MEMBERS_PAGE_SIZE" envDefault:"50"`
	// GraphTLSInsecureSkipVerify disables Graph TLS verification (opt-in,
	// default false) for dev/on-prem environments behind a TLS-intercepting
	// proxy. The proxy is taken from HTTPS_PROXY/HTTP_PROXY.
	GraphTLSInsecureSkipVerify bool `env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"false"`
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
	if cfg.MaxWorkers <= 0 || cfg.RunTimeout <= 0 {
		return fmt.Errorf("invalid config: MAX_WORKERS and RUN_TIMEOUT must be positive")
	}
	if cfg.GraphMembersPageSize <= 0 {
		return fmt.Errorf("invalid config: GRAPH_MEMBERS_PAGE_SIZE must be positive")
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

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoReadURI, cfg.MongoReadUsername, cfg.MongoReadPassword)
	if err != nil {
		return fmt.Errorf("mongo read connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), readClient)

	writeClient, err := mongoutil.Connect(ctx, cfg.MongoWriteURI, cfg.MongoWriteUsername, cfg.MongoWritePassword)
	if err != nil {
		return fmt.Errorf("mongo write connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), writeClient)

	store := newMongoStore(readClient.Database(cfg.MongoReadDB), writeClient.Database(cfg.MongoWriteDB))

	graph := msgraph.NewChatMembersClient(msgraph.Config{
		TenantID:              cfg.GraphTenantID,
		ClientID:              cfg.GraphClientID,
		ClientSecret:          cfg.GraphClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
	}, msgraph.WithMembersPageSize(cfg.GraphMembersPageSize))

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
