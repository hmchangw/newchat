package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/redis/go-redis/v9"

	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

// Config is the sync's environment configuration.
type Config struct {
	SiteID         string        `env:"SITE_ID,required"`
	ExternalTTL    time.Duration `env:"EXTERNAL_TTL" envDefault:"5m"`
	RunTimeout     time.Duration `env:"RUN_TIMEOUT" envDefault:"5m"`
	StaleThreshold time.Duration `env:"PRESENCE_STALE_THRESHOLD" envDefault:"45s"`
	ConnsTTL       time.Duration `env:"PRESENCE_CONNS_TTL" envDefault:"5m"`

	NATSURL       string `env:"NATS_URL,required"`
	NATSCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	ValkeyAddrs    []string `env:"VALKEY_ADDRS,required" envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD" envDefault:""`

	GraphTenantID     string `env:"GRAPH_TENANT_ID,required"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required"`
	GraphROPCUser     string `env:"GRAPH_ROPC_USERNAME,required"`
	GraphROPCPassword string `env:"GRAPH_ROPC_PASSWORD,required"`

	// GraphTLSInsecureSkipVerify disables Graph TLS verification (opt-in, default
	// false) for dev/on-prem environments behind a TLS-intercepting proxy. The
	// proxy itself is taken from the standard HTTPS_PROXY/HTTP_PROXY env vars
	// (msgraph clones the default transport, which honors ProxyFromEnvironment).
	GraphTLSInsecureSkipVerify bool `env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"false"`

	// GraphProxyURL, when set, routes the presence Graph client through this
	// proxy explicitly (overriding HTTPS_PROXY/HTTP_PROXY). Must include a scheme
	// and host, e.g. "http://proxy.corp:8080". Empty falls back to the standard
	// proxy env vars.
	GraphProxyURL string `env:"GRAPH_PROXY_URL" envDefault:""`

	// GraphUserAgent overrides the User-Agent header on presence requests. Empty
	// falls back to the msgraph package's default desktop-browser string. Set this
	// when a fronting proxy/WAF rejects the default agent.
	GraphUserAgent string `env:"GRAPH_USER_AGENT" envDefault:""`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("user-presence-sync failed", "error", err)
		os.Exit(1)
	}
}

// run wires dependencies and performs one reconcile. It returns an error rather
// than calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if cfg.ExternalTTL <= 0 || cfg.RunTimeout <= 0 ||
		cfg.StaleThreshold <= 0 || cfg.ConnsTTL <= 0 {
		return fmt.Errorf("invalid config: EXTERNAL_TTL, RUN_TIMEOUT, " +
			"PRESENCE_STALE_THRESHOLD and PRESENCE_CONNS_TTL must be positive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	tracerShutdown, err := otelutil.InitTracer(ctx, "user-presence-sync")
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() {
		if err := tracerShutdown(context.Background()); err != nil {
			slog.Warn("tracer shutdown", "error", err)
		}
	}()

	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: cfg.ValkeyAddrs, Password: cfg.ValkeyPassword,
	})
	defer func() {
		if err := clusterClient.Close(); err != nil {
			slog.Warn("valkey close", "error", err)
		}
	}()
	store := presencestore.NewValkeyStoreFromClient(clusterClient, cfg.StaleThreshold, cfg.ConnsTTL)

	nc, err := natsutil.Connect(cfg.NATSURL, cfg.NATSCredsFile)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			slog.Warn("nats drain", "error", err)
		}
	}()
	publish := func(ctx context.Context, subj string, data []byte) error {
		return nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
	}

	graphCfg := msgraph.Config{
		TenantID:              cfg.GraphTenantID,
		ClientID:              cfg.GraphClientID,
		ClientSecret:          cfg.GraphClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
		ProxyURL:              cfg.GraphProxyURL,
		UserAgent:             cfg.GraphUserAgent,
	}
	users := msgraph.NewDirectoryClient(graphCfg)
	pres, err := msgraph.NewPresenceClient(graphCfg, msgraph.ROPCCredentials{Username: cfg.GraphROPCUser, Password: cfg.GraphROPCPassword})
	if err != nil {
		return fmt.Errorf("build presence client: %w", err)
	}

	r := newReconciler(
		store, users, pres, store,
		newValkeyInCallIndex(clusterClient),
		newValkeyIDMap(clusterClient),
		natsPublisher{publish: publish, siteID: cfg.SiteID},
		reconcileConfig{SiteID: cfg.SiteID, ExternalTTL: cfg.ExternalTTL},
	)

	if err := r.run(ctx); err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}
	slog.Info("user-presence-sync done", "site", cfg.SiteID)
	return nil
}
