package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

type NATSConfig struct {
	URL       string `env:"URL,required"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

type MongoConfig struct {
	URI      string `env:"URI,required"`
	DB       string `env:"DB" envDefault:"chat"`
	Username string `env:"USERNAME"`
	Password string `env:"PASSWORD"`
	// ReadPreference routes staleness-tolerant reads to secondaries per read site;
	// the client stays on primary for dedup/read-after-write.
	ReadPreference string `env:"READ_PREFERENCE" envDefault:"secondaryPreferred"`
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
	SiteID        string        `env:"SITE_ID,required"`
	UserCacheSize int           `env:"USER_CACHE_SIZE" envDefault:"10000"`
	UserCacheTTL  time.Duration `env:"USER_CACHE_TTL"  envDefault:"5m"`
	NATS          NATSConfig    `envPrefix:"NATS_"`
	Valkey        valkeyutil.Config
	Mongo         MongoConfig    `envPrefix:"MONGO_"`
	Presence      PresenceConfig `envPrefix:"PRESENCE_"`

	// Pool caps the Mongo connection pool. RequestTimeout bounds each handler so
	// a slow op frees its connection. No concurrency cap: the presence routes are
	// fire-and-forget (RegisterVoid), which admission control would silently drop
	// under saturation — so this service takes only the timeout knob.
	Pool           mongoutil.PoolConfig
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"10s"`
}

// Compile-time guarantee that the extracted store satisfies the daemon's
// consumer interface (including SetExternal).
var _ PresenceStore = (*presencestore.Store)(nil)

func main() {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	// Presence state lives entirely in Valkey, so this service cannot start
	// without one.
	if err := cfg.Valkey.Validate(); err != nil {
		slog.Error("invalid valkey config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("invalid pool config", "error", err)
		os.Exit(1)
	}
	if cfg.RequestTimeout < 0 {
		slog.Error("invalid REQUEST_TIMEOUT", "value", cfg.RequestTimeout)
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

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	// Dialed through valkeyutil rather than presencestore's own ClusterConfig so
	// this service's Valkey commands carry the same instrumentation and dial
	// policy as every other service's. Presence IS the datastore here, so an
	// uninstrumented client is the one worth least having.
	valkeyClient, err := valkeyutil.ConnectRaw(ctx, cfg.Valkey, valkeyutil.Instrumented(sdk))
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}
	store := presencestore.NewValkeyStoreFromClient(valkeyClient, cfg.Presence.StaleThreshold, cfg.Presence.ConnsTTL)

	readPref, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.Mongo.ReadPreference, "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
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

	nc, err := natsutil.ConnectWithMetrics(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace, sdk.MeterProvider())
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	publish := func(ctx context.Context, subj string, data []byte) error {
		return nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
	}

	peer := NewNATSPeerPresenceClient(nc.NatsConn(), cfg.Presence.PeerTimeout)
	handler := NewHandler(store, userDir, peer, publish, cfg.SiteID, cfg.Presence.BatchMax)

	// No admission cap here: Hello/Ping/Activity/Bye are fire-and-forget
	// (RegisterVoid), and under a saturated concurrency semaphore those are
	// silently dropped (no reply, no redelivery) — a reconnect storm would lose
	// presence updates and strand users online/offline. Only the per-request
	// timeout is applied.
	publishMetrics := natsmetrics.NewFromProviderIfEnabled(sdk.MeterProvider(), sdk.Toggles.Metrics).Publisher(cfg.SiteID)
	router := natsrouter.Default(nc, "user-presence-service",
		natsrouter.WithSiteID(cfg.SiteID), natsrouter.WithMetrics(publishMetrics))
	if cfg.RequestTimeout > 0 {
		router.Use(natsrouter.HandlerTimeout(cfg.RequestTimeout))
	}
	registerRoutes(router, handler, cfg.SiteID)

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
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(_ context.Context) error { return store.Close() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// registerRoutes wires user-presence-service's routes onto the router. It is a
// function rather than inline in main so the registration table has exactly one
// definition, which routes_test.go runs against a golden file. The four
// RegisterVoid routes are fire-and-forget: they name no rpc.method and record no
// server-side sample, so only the three Register routes reach the golden file.
func registerRoutes(router *natsrouter.Router, handler *Handler, siteID string) {
	natsrouter.RegisterVoid(router, subject.PresenceHelloPattern(siteID), handler.Hello)
	natsrouter.RegisterVoid(router, subject.PresencePingPattern(siteID), handler.Ping)
	natsrouter.RegisterVoid(router, subject.PresenceActivityPattern(siteID), handler.Activity)
	natsrouter.RegisterVoid(router, subject.PresenceByePattern(siteID), handler.Bye)
	natsrouter.Register(router, subject.PresenceManualSetPattern(siteID), natsmetrics.MethodSetManualPresence, handler.SetManual)
	natsrouter.Register(router, subject.PresenceQueryBatch(siteID), natsmetrics.MethodBatchGetPresence, handler.QueryBatch)
	natsrouter.Register(router, subject.PresenceQueryBatchPeer(siteID), natsmetrics.MethodBatchGetPeerPresence, handler.QueryBatchPeer)
}
