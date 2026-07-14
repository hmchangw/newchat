package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

type NATSConfig struct {
	URL       string `env:"URL,required"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

type ValkeyConfig struct {
	Addrs    []string `env:"ADDRS,required" envSeparator:","`
	Password string   `env:"PASSWORD"        envDefault:""`
}

type MongoConfig struct {
	URI      string `env:"URI,required"`
	DB       string `env:"DB" envDefault:"chat"`
	Username string `env:"USERNAME"`
	Password string `env:"PASSWORD"`
}

type PresenceConfig struct {
	BatchMax          int           `env:"BATCH_MAX"          envDefault:"100"`
	HeartbeatInterval time.Duration `env:"HEARTBEAT_INTERVAL" envDefault:"30s"`
	StaleThreshold    time.Duration `env:"STALE_THRESHOLD"    envDefault:"45s"`
	SweepInterval     time.Duration `env:"SWEEP_INTERVAL"     envDefault:"5s"`
	ConnsTTL          time.Duration `env:"CONNS_TTL"          envDefault:"5m"`
	PeerTimeout       time.Duration `env:"PEER_TIMEOUT"       envDefault:"3s"`
}

type Config struct {
	SiteID        string         `env:"SITE_ID,required"`
	UserCacheSize int            `env:"USER_CACHE_SIZE" envDefault:"10000"`
	UserCacheTTL  time.Duration  `env:"USER_CACHE_TTL"  envDefault:"5m"`
	NATS          NATSConfig     `envPrefix:"NATS_"`
	Valkey        ValkeyConfig   `envPrefix:"VALKEY_"`
	Mongo         MongoConfig    `envPrefix:"MONGO_"`
	Presence      PresenceConfig `envPrefix:"PRESENCE_"`
}

// Compile-time guarantee that the extracted store satisfies the daemon's
// consumer interface (including SetExternal).
var _ PresenceStore = (*presencestore.Store)(nil)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	// Fail fast on non-positive tunables: a zero/negative SweepInterval panics
	// time.NewTicker, and the others produce silently broken runtime behavior.
	if cfg.Presence.BatchMax <= 0 ||
		cfg.Presence.HeartbeatInterval <= 0 ||
		cfg.Presence.StaleThreshold <= 0 ||
		cfg.Presence.SweepInterval <= 0 ||
		cfg.Presence.ConnsTTL <= 0 ||
		cfg.Presence.PeerTimeout <= 0 {
		slog.Error("invalid presence config: all PRESENCE_* tunables must be positive",
			"batchMax", cfg.Presence.BatchMax,
			"heartbeatInterval", cfg.Presence.HeartbeatInterval,
			"staleThreshold", cfg.Presence.StaleThreshold,
			"sweepInterval", cfg.Presence.SweepInterval,
			"connsTTL", cfg.Presence.ConnsTTL,
			"peerTimeout", cfg.Presence.PeerTimeout,
		)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "user-presence-service")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	store, err := presencestore.NewValkeyStore(
		presencestore.ClusterConfig{Addrs: cfg.Valkey.Addrs, Password: cfg.Valkey.Password},
		cfg.Presence.StaleThreshold, cfg.Presence.ConnsTTL,
	)
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	userDir, err := userstore.NewCache(
		userstore.NewMongoStore(mongoClient.Database(cfg.Mongo.DB).Collection("users")),
		cfg.UserCacheSize, cfg.UserCacheTTL)
	if err != nil {
		slog.Error("init user cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("user-cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL)

	nc, err := natsutil.Connect(cfg.NATS.URL, cfg.NATS.CredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	publish := func(ctx context.Context, subj string, data []byte) error {
		return nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
	}

	peer := NewNATSPeerPresenceClient(nc.NatsConn(), cfg.Presence.PeerTimeout)
	handler := NewHandler(store, userDir, peer, publish, cfg.SiteID, cfg.Presence.BatchMax)

	router := natsrouter.Default(nc, "user-presence-service")
	natsrouter.RegisterVoid(router, subject.PresenceHelloPattern(cfg.SiteID), handler.Hello)
	natsrouter.RegisterVoid(router, subject.PresencePingPattern(cfg.SiteID), handler.Ping)
	natsrouter.RegisterVoid(router, subject.PresenceActivityPattern(cfg.SiteID), handler.Activity)
	natsrouter.RegisterVoid(router, subject.PresenceByePattern(cfg.SiteID), handler.Bye)
	natsrouter.Register(router, subject.PresenceManualSetPattern(cfg.SiteID), handler.SetManual)
	natsrouter.Register(router, subject.PresenceQueryBatch(cfg.SiteID), handler.QueryBatch)
	natsrouter.Register(router, subject.PresenceQueryBatchPeer(cfg.SiteID), handler.QueryBatchPeer)

	sweeper := NewSweeper(store, publish, cfg.SiteID, cfg.Presence.SweepInterval)
	sweepCtx, stopSweep := context.WithCancel(ctx)
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		sweeper.Run(sweepCtx)
	}()

	slog.Info("user-presence-service running", "site", cfg.SiteID, "valkey", cfg.Valkey.Addrs)

	shutdown.Wait(ctx, 25*time.Second,
		// Stop the sweeper and wait for Run to return BEFORE draining NATS or
		// closing the store, so no in-flight Sweep/publish races teardown.
		func(ctx context.Context) error {
			stopSweep()
			select {
			case <-sweepDone:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("sweeper shutdown timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(_ context.Context) error { return store.Close() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
	)
}
