package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"`
	SiteID        string `env:"SITE_ID,required"`
	// AllSiteIDs is the comma-separated peer list for per-destination outbox federation.
	AllSiteIDs    string `env:"ALL_SITE_IDS"     envDefault:""`
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"         envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`

	// RoomKeyGracePeriod governs how long a rotated-out room key stays readable (roomkeystore.NewMongoStore); matches room-service/room-worker.
	RoomKeyGracePeriod time.Duration `env:"ROOM_KEY_GRACE_PERIOD" envDefault:"24h"`

	// RoomKeyRetiredTTL: retention for rotated-out keys; see roomkeystore.WithRetiredKeys for the 2x-cache-TTL rule.
	RoomKeyRetiredTTL time.Duration `env:"ROOM_KEY_RETIRED_TTL" envDefault:"20m"`

	// Pool caps the Mongo connection pool. MaxConcurrency bounds in-flight
	// handlers — kept at this service's historical 200 default (below the fleet
	// 256) — and RequestTimeout bounds each handler, so a burst or slow
	// dependency can't saturate the pool with unbounded work.
	Pool           mongoutil.PoolConfig
	MaxConcurrency int           `env:"MAX_CONCURRENCY" envDefault:"200"`
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"10s"`

	HealthAddr   string `env:"HEALTH_ADDR"     envDefault:":8081"`
	PProfEnabled bool   `env:"PPROF_ENABLED"   envDefault:"false"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("bot-room-service exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Pool.Validate(); err != nil {
		return fmt.Errorf("validate pool config: %w", err)
	}
	guard := natsrouter.GuardConfig{MaxConcurrency: cfg.MaxConcurrency, RequestTimeout: cfg.RequestTimeout}
	if err := guard.Validate(); err != nil {
		return fmt.Errorf("validate guard config: %w", err)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	nc, err := natsutil.ConnectWithMetrics(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace, sdk.MeterProvider())
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("init jetstream: %w", err)
	}

	mc, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	store := newStoreMongo(mc.Database(cfg.MongoDB))
	// Bounded timeout so a hung createIndexes surfaces at startup.
	ensureCtx, ensureCancel := context.WithTimeout(ctx, 30*time.Second)
	defer ensureCancel()
	if err := store.EnsureIndexes(ensureCtx); err != nil {
		slog.Warn("ensure store indexes failed; continuing (indexes are best-effort)", "error", err)
	}

	keyStore, err := roomkeystore.OpenMongo(ctx, mc.Database(cfg.MongoDB), cfg.RoomKeyGracePeriod, cfg.RoomKeyRetiredTTL)
	if err != nil {
		return fmt.Errorf("open room key store: %w", err)
	}
	keySender := roomkeysender.NewSender(nc.NatsConn())

	// pkg/outbox.Publish wants a raw NATS publish with msgID as Nats-Msg-Id header.
	pubCallback := func(_ context.Context, subj string, data []byte, msgID string) error {
		msg := &nats.Msg{Subject: subj, Data: data, Header: nats.Header{}}
		msg.Header.Set("Nats-Msg-Id", msgID)
		return nc.NatsConn().PublishMsg(msg)
	}

	peers := parsePeers(cfg.AllSiteIDs, cfg.SiteID)
	h := newHandler(store, cfg.SiteID, peers, pubCallback, keyStore, keySender)
	// LOCAL sysmsg emission on create/add/remove; never federated cross-site.
	h.sysmsgPub = jsPublishAdapter{js: js}

	publishMetrics := natsmetrics.NewFromProviderIfEnabled(sdk.MeterProvider(), sdk.Toggles.Metrics).Publisher(cfg.SiteID)
	router := natsrouter.DefaultGuarded(nc, "bot-room-service", guard,
		natsrouter.WithSiteID(cfg.SiteID), natsrouter.WithMetrics(publishMetrics))
	h.Register(router)

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}

	slog.Info("bot-room-service running", "site", cfg.SiteID, "peers", peers)
	shutdown.Wait(ctx, 25*time.Second,
		func(dctx context.Context) error { return router.Shutdown(dctx) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(dctx context.Context) error { mongoutil.Disconnect(dctx, mc); return nil },
		func(dctx context.Context) error { return healthStop(dctx) },
		func(dctx context.Context) error { return obsShutdown(dctx) },
	)
	return nil
}

// parsePeers splits ALL_SITE_IDS into a slice excluding the current site.
func parsePeers(raw, self string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" && p != self {
			out = append(out, p)
		}
	}
	return out
}
