package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/failoverlane"
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
	// Buddy is the peer cluster hosting this site's standby lanes; the service
	// also answers displaced clients' RPCs there while its own NATS is down.
	Buddy    natsutil.BuddyConfig `envPrefix:"BUDDY_"`
	Valkey   valkeyutil.Config
	Mongo    MongoConfig    `envPrefix:"MONGO_"`
	Presence PresenceConfig `envPrefix:"PRESENCE_"`

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

	// publishOn binds a presence broadcast to one connection.
	publishOn := func(conn *o11ynats.Conn) func(context.Context, string, []byte) error {
		return func(ctx context.Context, subj string, data []byte) error {
			return conn.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
		}
	}

	// No admission cap here: Hello/Ping/Activity/Bye are fire-and-forget
	// (RegisterVoid), and under a saturated concurrency semaphore those are
	// silently dropped (no reply, no redelivery) — a reconnect storm would lose
	// presence updates and strand users online/offline. Only the per-request
	// timeout is applied.
	publishMetrics := natsmetrics.NewFromProviderIfEnabled(sdk.MeterProvider(), sdk.Toggles.Metrics).Publisher(cfg.SiteID)
	// One handler per lane over the same store and user cache: the presence
	// broadcast and the peer-site query both speak NATS and must go out on the
	// connection the lane's requests arrive on.
	dialer := natsutil.BuddyDialer{
		Config: cfg.Buddy, CredsFile: cfg.NATS.CredsFile,
		TracerProvider: sdk.TracerProvider(), Propagator: sdk.Propagator, TracingEnabled: sdk.Toggles.Trace,
	}
	routers, err := failoverlane.BindRouters(ctx, nc, nil, &dialer,
		func(_ context.Context, conn *o11ynats.Conn, _ o11ynats.JetStream, _ subject.Lane) (*natsrouter.Router, error) {
			peer := NewNATSPeerPresenceClient(conn.NatsConn(), cfg.Presence.PeerTimeout)
			handler := NewHandler(store, userDir, peer, publishOn(conn), cfg.SiteID, cfg.Presence.BatchMax)
			router := natsrouter.Default(conn, "user-presence-service",
				natsrouter.WithSiteID(cfg.SiteID), natsrouter.WithMetrics(publishMetrics))
			if cfg.RequestTimeout > 0 {
				router.Use(natsrouter.HandlerTimeout(cfg.RequestTimeout))
			}
			natsrouter.RegisterVoid(router, subject.PresenceHelloPattern(cfg.SiteID), handler.Hello)
			natsrouter.RegisterVoid(router, subject.PresencePingPattern(cfg.SiteID), handler.Ping)
			natsrouter.RegisterVoid(router, subject.PresenceActivityPattern(cfg.SiteID), handler.Activity)
			natsrouter.RegisterVoid(router, subject.PresenceByePattern(cfg.SiteID), handler.Bye)
			natsrouter.Register(router, subject.PresenceManualSetPattern(cfg.SiteID), handler.SetManual)
			natsrouter.Register(router, subject.PresenceQueryBatch(cfg.SiteID), handler.QueryBatch)
			natsrouter.Register(router, subject.PresenceQueryBatchPeer(cfg.SiteID), handler.QueryBatchPeer)
			return router, nil
		})
	if err != nil {
		slog.Error("bind routers failed", "error", err)
		os.Exit(1)
	}

	// The sweeper is not request-driven, so it has no lane: it expires stale
	// sessions in this site's store and announces them on the home connection.
	sweeper := NewSweeper(store, publishOn(nc), cfg.SiteID, cfg.Presence.SweepInterval)
	sweepCtx, stopSweep := context.WithCancel(ctx)
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		sweeper.Run(sweepCtx)
	}()

	slog.Info("user-presence-service running", "site", cfg.SiteID, "valkey", cfg.Valkey.Addrs)

	hooks := []func(context.Context) error{
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
	}
	hooks = append(hooks, routers.ShutdownHooks()...)
	hooks = append(hooks,
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(_ context.Context) error { return store.Close() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
	shutdown.Wait(ctx, 25*time.Second, hooks...)
}
