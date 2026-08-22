package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	o11y "github.com/flywindy/o11y"
	o11ygin "github.com/flywindy/o11y/gin"
	o11yredis "github.com/flywindy/o11y/redis"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/badgecache"
	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/memlimit"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/restyutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/historyclient"
	"github.com/hmchangw/chat/user-service/mongorepo"
	"github.com/hmchangw/chat/user-service/presenceclient"
	"github.com/hmchangw/chat/user-service/publisher"
	"github.com/hmchangw/chat/user-service/roomclient"
	"github.com/hmchangw/chat/user-service/service"
)

// Compile-time interface assertions — fail the build if implementations drift.
var (
	_ service.SubscriptionRepository       = (*mongorepo.SubscriptionRepo)(nil)
	_ service.UserRepository               = (*mongorepo.UserRepo)(nil)
	_ service.AppRepository                = (*mongorepo.AppRepo)(nil)
	_ service.ThreadSubscriptionRepository = (*mongorepo.ThreadSubscriptionRepo)(nil)
	_ service.RoomClient                   = (*roomclient.Client)(nil)
	_ service.HistoryClient                = (*historyclient.Client)(nil)
	_ service.PresenceClient               = (*presenceclient.Client)(nil)
	_ service.EventPublisher               = (*publisher.Publisher)(nil)
	_ service.EventPublisher               = (*publisher.CorePublisher)(nil)
	_ service.SSOTokenRepository           = (*mongorepo.SSOTokenRepo)(nil)
	_ service.TokenValidator               = (*pkgoidc.Validator)(nil)
	_ service.TokenRefresher               = (*pkgoidc.Validator)(nil)
)

// badgeCache mirrors service.badgeCache (unexported in that package, so it
// can't be named here) — Go interfaces are satisfied structurally, so this
// local copy lets main hold either a *badgecache.Cache or a noopBadgeCache
// before passing it into service.New.
type badgeCache interface {
	BumpBatch(ctx context.Context, accounts []string, roomID string) map[string]int
	Seed(ctx context.Context, account string, roomIDs []string, triggerRoomID string) (int, bool)
	Reseed(ctx context.Context, account string, roomIDs []string)
	Count(ctx context.Context, account string) (int, bool)
}

// noopBadgeCache is the badge cache used when VALKEY_ADDRS is empty:
// BumpBatch/Seed always miss, Reseed is a no-op. BadgeCountBatch's cappedUnion
// fallback still returns a correct (just uncached) count, and CountSubscriptions'
// Reseed call becomes a harmless no-op — so Phase A deploys need no Valkey.
type noopBadgeCache struct{}

func (noopBadgeCache) BumpBatch(context.Context, []string, string) map[string]int { return nil }
func (noopBadgeCache) Seed(context.Context, string, []string, string) (int, bool) {
	return 0, false
}
func (noopBadgeCache) Reseed(context.Context, string, []string)  {}
func (noopBadgeCache) Count(context.Context, string) (int, bool) { return 0, false }

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	// Before anything allocates: the concurrency cap is sized from an estimate of
	// per-request memory, and a soft limit turns a wrong estimate into GC pressure
	// rather than an OOMKill that would cascade across the fleet.
	if limit, err := memlimit.SetFromCgroup(cfg.GoMemLimitFraction); err != nil {
		slog.Warn("could not derive GOMEMLIMIT from the cgroup; using the runtime default", "error", err)
	} else if limit > 0 {
		slog.Info("GOMEMLIMIT derived from cgroup", "bytes", limit, "fraction", cfg.GoMemLimitFraction)
	}

	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		slog.Error("invalid ADMIN_ACCT_PREFIX", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
		mongoutil.WithObservability(sdk),
		mongoutil.WithMaxPoolSize(cfg.Mongo.MaxPoolSize),
	)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}

	// A pool of its own for HTTP, so a large page cannot exhaust the connections
	// the NATS handlers share. ConnectRead bakes in secondaryPreferred, which is
	// what this read-only path wants.
	httpMongoClient, err := mongoutil.ConnectRead(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
		mongoutil.WithObservability(sdk),
		mongoutil.WithMaxPoolSize(cfg.HTTP.MongoMaxPoolSize),
		mongoutil.WithMinPoolSize(cfg.HTTP.MongoMinPoolSize),
	)
	if err != nil {
		slog.Error("http mongo connect failed", "error", err)
		os.Exit(1)
	}

	// Client stays on primary; each repo opts into secondary reads via
	// WithReadPreference (already validated in config).
	readPref, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.Mongo.ReadPreference, "error", err)
		os.Exit(1)
	}
	slog.Info("mongo secondary-read preference configured", "readPreference", readPref.Mode().String())
	readFromSecondary := mongorepo.WithReadPreference(readPref)

	db := mongoClient.Database(cfg.Mongo.DB)
	subRepo := mongorepo.NewSubscriptionRepo(db, cfg.SiteID, readFromSecondary, mongorepo.WithShowTeamsRoom(cfg.ShowTeamsRoom), mongorepo.WithShowTeamsAccounts(cfg.ShowTeamsAccounts))
	userRepo := mongorepo.NewUserRepo(db, readFromSecondary)
	appRepo := mongorepo.NewAppRepo(db, readFromSecondary)
	threadSubRepo := mongorepo.NewThreadSubscriptionRepo(db)
	ssoTokenRepo := mongorepo.NewSSOTokenRepo(db)
	if err := subRepo.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure subscription indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	if err := userRepo.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure user indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	if err := appRepo.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure app indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	if err := threadSubRepo.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure thread subscription indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	if err := ssoTokenRepo.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure sso token indexes failed; continuing (indexes are best-effort)", "error", err)
	}

	tokenValidator, tokenRefresher, err := oidcValidator(ctx, &cfg)
	if err != nil {
		slog.Error("oidc validator init failed", "error", err)
		os.Exit(1)
	}

	// Empty VALKEY_ADDRS disables the badge cache: badge.count.batch and
	// subscription.count still work, just uncached (dev-safe Phase A default).
	var badge badgeCache = noopBadgeCache{}
	var valkeyClient *redis.ClusterClient
	if len(cfg.ValkeyAddrs) > 0 {
		valkeyClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.ValkeyAddrs,
			Password: cfg.ValkeyPassword,
		})
		// o11yredis.Wrap mutates valkeyClient in place to add tracing+metrics —
		// mirrors pkg/valkeyutil's instrumentCluster so the badge cache's Valkey
		// calls are observable like every other instrumented client in the repo.
		if _, err := o11yredis.Wrap(valkeyClient, sdk.TracerProvider(), sdk.MeterProvider()); err != nil {
			slog.Error("instrument valkey client failed", "error", err)
			os.Exit(1)
		}
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := valkeyClient.Ping(pingCtx).Err()
		cancel()
		if err != nil {
			slog.Error("valkey connect failed", "error", err)
			os.Exit(1)
		}
		badge = badgecache.New(valkeyClient, cfg.BadgeCacheTTL, cfg.BadgeCountCap, badgecache.WithMarkerTTL(cfg.BadgeMarkerTTL))
		slog.Info("badge cache enabled", "ttl", cfg.BadgeCacheTTL, "marker_ttl", cfg.BadgeMarkerTTL, "count_cap", cfg.BadgeCountCap)
	} else {
		slog.Warn("badge cache DISABLED — VALKEY_ADDRS is empty (dev only)")
	}

	svc := service.New(subRepo, userRepo, appRepo, threadSubRepo, roomclient.New(nc, cfg.SiteID), historyclient.New(nc), presenceclient.New(nc), publisher.New(js), publisher.NewCore(nc), badge, ssoTokenRepo, tokenValidator, tokenRefresher, &cfg)

	// A second service instance over the HTTP-only Mongo pool. Everything else --
	// the NATS clients, publishers, badge cache -- is shared and stateless. It is
	// handed to startHTTPServer as a subscriptionLister, so the read-only pool
	// cannot be reached by a write path.
	httpDB := httpMongoClient.Database(cfg.Mongo.DB)
	httpSvc := service.New(
		mongorepo.NewSubscriptionRepo(httpDB, cfg.SiteID,
			mongorepo.WithShowTeamsRoom(cfg.ShowTeamsRoom), mongorepo.WithShowTeamsAccounts(cfg.ShowTeamsAccounts)),
		mongorepo.NewUserRepo(httpDB), mongorepo.NewAppRepo(httpDB), threadSubRepo,
		roomclient.New(nc, cfg.SiteID), historyclient.New(nc), presenceclient.New(nc),
		publisher.New(js), publisher.NewCore(nc), badge, ssoTokenRepo, tokenValidator, tokenRefresher, &cfg)

	// Bound in-flight handlers so a burst is shed at the door (ErrUnavailable)
	// instead of piling unbounded work onto MongoDB. MAX_CONCURRENCY=0 disables.
	routerOpts := []natsrouter.Option{natsrouter.WithSiteID(cfg.SiteID)}
	if cfg.MaxConcurrency > 0 {
		routerOpts = append(routerOpts, natsrouter.WithMaxConcurrency(cfg.MaxConcurrency))
	}
	router := natsrouter.New(nc, "user-service", routerOpts...)
	router.Use(natsrouter.Recovery())
	// RequestID must precede any handler that reads request_id from ctx —
	// otherwise Classify's log line records an empty value.
	router.Use(natsrouter.RequestID())
	router.Use(natsrouter.Logging())
	// After Logging so the timeout wraps the handler chain; bounds the Mongo
	// aggregations from hanging past the configured deadline.
	router.Use(natsrouter.HandlerTimeout(cfg.HandlerTimeout))

	svc.RegisterHandlers(router)

	httpSrv, err := startHTTPServer(&cfg, httpSvc, sdk, tokenValidator)
	if err != nil {
		slog.Error("http server failed to start", "error", err)
		os.Exit(1)
	}

	healthStop, err := health.Serve(cfg.HealthAddr, 5*time.Second, natsutil.HealthCheck(nc))
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("user-service running", "site", cfg.SiteID, "http_port", cfg.HTTP.Port, "health_addr", cfg.HealthAddr)

	// HTTP drains first so in-flight requests can still reach NATS and Mongo.
	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return httpSrv.Shutdown(ctx) },
		healthStop,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, httpMongoClient); return nil },
		func(ctx context.Context) error {
			if valkeyClient == nil {
				return nil
			}
			return valkeyClient.Close()
		},
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// startHTTPServer builds the Gin engine and serves it in the background. It
// returns once the listener is bound, so a port clash fails startup rather than
// surfacing later as a silently missing API.
func startHTTPServer(cfg *config.Config, svc subscriptionLister, sdk *o11y.SDK, sso ssoValidator) (*http.Server, error) {
	shedCounter, err := sdk.MeterProvider().Meter("user-service").Int64Counter(
		"http_requests_shed_total",
		metric.WithDescription("HTTP requests rejected with 429 because the in-flight cap was reached."))
	if err != nil {
		return nil, fmt.Errorf("create http_requests_shed_total counter: %w", err)
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// CORS handles preflight before tracing so OPTIONS noise stays out of Tempo.
	r.Use(ginutil.CORS())
	r.Use(o11ygin.Middleware("user-service", sdk.TracerProvider(), sdk.MeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	// RequestID precedes AccessLog and the handlers, which both read it.
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())

	auth := authDeps{sso: sso}
	if cfg.BotplatformURL != "" {
		auth.bot = botauth.NewValidator(restyutil.New("", restyutil.WithTimeout(5*time.Second)), cfg.BotplatformURL)
		slog.Info("session-token auth enabled", "botplatform_url", cfg.BotplatformURL)
	} else {
		slog.Warn("BOTPLATFORM_URL unset — HTTP session-token auth disabled, ssoToken only")
	}

	registerRoutes(r, newHandler(svc, cfg.HTTP.DefaultLimit, cfg.HTTP.MaxLimit), auth, httpDeps{
		maxConcurrency: cfg.HTTP.MaxConcurrency,
		gzipMinBytes:   cfg.HTTP.GzipMinBytes,
		handlerTimeout: cfg.HTTP.HandlerTimeout,
		onShed:         func() { shedCounter.Add(context.Background(), 1) },
	})

	srv := newHTTPServer(net.JoinHostPort("", cfg.HTTP.Port), r, &cfg.HTTP)
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, fmt.Errorf("http listen on %q: %w", srv.Addr, err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server stopped", "error", err)
		}
	}()
	return srv, nil
}
