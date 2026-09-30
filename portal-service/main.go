package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/restyutil"
	"github.com/hmchangw/chat/pkg/shutdown"
)

// cacheRetryInterval paces reload attempts after a failed directory load, so
// a Mongo blip at startup does not leave the portal unready for a full
// refresh interval.
const cacheRetryInterval = 30 * time.Second

type config struct {
	Port               string `env:"PORT"                         envDefault:"8085"`
	DevMode            bool   `env:"DEV_MODE"                     envDefault:"false"`
	DevFallbackSiteID  string `env:"PORTAL_DEV_FALLBACK_SITE_ID"  envDefault:"site-local"`
	DevFallbackNatsURL string `env:"PORTAL_DEV_FALLBACK_NATS_URL" envDefault:"ws://localhost:9222"`

	// SiteURLs is the per-site URL registry: a JSON object mapping siteId to
	// {baseUrl, natsUrl}. baseUrl is the unified backend origin the client hits
	// for every /api/v1/* RPC (auth, media, etc.); natsUrl is the site's NATS
	// endpoint. A single template can't express sites on different domains, so
	// each site is listed explicitly.
	SiteURLs string `env:"PORTAL_SITE_URLS,required"`

	// BotplatformURL is the cluster-internal botplatform endpoint portal
	// forwards password login to — a single Kubernetes DNS name (not per site).
	BotplatformURL string `env:"BOTPLATFORM_URL" envDefault:"http://botplatform-service:8080"`

	// APIVersion and OTELBaseURL are served to the frontend via GET /api/settings.
	// Critical config — no envDefault, a deployment that forgets them fails fast.
	APIVersion  string `env:"PORTAL_API_VERSION,notEmpty"`
	OTELBaseURL string `env:"PORTAL_OTEL_BASE_URL,notEmpty"`

	// CacheRefreshInterval drives how often the directory is reloaded (users
	// left-joined with hr_employee via $lookup). Shorter than the daily HR
	// cron so a newly provisioned user appears within a couple of hours.
	CacheRefreshInterval time.Duration `env:"PORTAL_CACHE_REFRESH_INTERVAL" envDefault:"2h"`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`
	// MongoReadPreference: read-only service (directory lookups); secondaryPreferred
	// offloads the primary.
	MongoReadPreference string `env:"MONGO_READ_PREFERENCE" envDefault:"secondaryPreferred"`

	// FailoverOpsToken gates the operator failover control surface. Empty
	// disables the internal control server entirely (no control surface).
	FailoverOpsToken string `env:"FAILOVER_OPS_TOKEN" envDefault:""`
	// FailoverInternalAddr is the listen address for the internal-only control
	// surface — kept off the public browser-facing server.
	FailoverInternalAddr string `env:"FAILOVER_INTERNAL_ADDR" envDefault:":8090"`
	// FailoverStateTTL bounds how long portal caches a site's serving target
	// before re-reading it (routing freshness vs. Mongo load).
	FailoverStateTTL time.Duration `env:"FAILOVER_STATE_TTL" envDefault:"5s"`
	// BackupSiteID is the reserved PORTAL_SITE_URLS id served for a failed-over
	// site. Empty in single-site/dev deployments (no failover occurs there).
	BackupSiteID string `env:"PORTAL_BACKUP_SITE_ID" envDefault:""`

	Pool mongoutil.PoolConfig
	HTTP ginutil.TimeoutConfig

	// BotLoginEnabled gates portal's bot-role password login. Flip to false
	// once the dedicated bot-devs client (which talks to botplatform directly)
	// ships — then bot accounts can no longer log in via chat-frontend.
	BotLoginEnabled bool `env:"BOT_LOGIN_ENABLED" envDefault:"true"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Pool.Validate(); err != nil {
		return fmt.Errorf("validate pool config: %w", err)
	}
	if err := cfg.HTTP.Validate(); err != nil {
		return fmt.Errorf("validate http timeout: %w", err)
	}
	// A non-positive TTL expires every cache entry immediately, turning each
	// login into a Mongo read. Reject it at startup rather than degrade quietly.
	if cfg.FailoverStateTTL <= 0 {
		return fmt.Errorf("FAILOVER_STATE_TTL must be positive, got %s", cfg.FailoverStateTTL)
	}

	sites, err := parseSiteURLs(cfg.SiteURLs)
	if err != nil {
		return fmt.Errorf("parse site URL registry: %w", err)
	}

	if err := validateFailoverConfig(cfg.FailoverOpsToken, cfg.BackupSiteID, sites); err != nil {
		return fmt.Errorf("validate failover config: %w", err)
	}

	otelBaseURL, err := parseOTELBaseURL(cfg.OTELBaseURL)
	if err != nil {
		return fmt.Errorf("parse OTEL base URL: %w", err)
	}
	settings := settingsResponse{APIVersion: cfg.APIVersion, OTELBaseURL: otelBaseURL, BotLoginEnabled: cfg.BotLoginEnabled}
	slog.Info("settings config", "apiVersion", settings.APIVersion, "otelBaseUrl", settings.OTELBaseURL, "botLoginEnabled", settings.BotLoginEnabled)

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	readPref, err := mongoutil.ParseReadPreference(cfg.MongoReadPreference)
	if err != nil {
		return fmt.Errorf("parse mongo read preference %q: %w", cfg.MongoReadPreference, err)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())

	store := newMongoDirectoryStore(mongoClient.Database(cfg.MongoDB))
	if err := store.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure directory indexes failed; continuing (indexes are best-effort)", "error", err)
	}

	// Populate the directory cache in the background; /readyz stays
	// unavailable until the first successful load.
	cache := newDirectoryCache()
	refreshCtx, refreshCancel := context.WithCancel(ctx)
	defer refreshCancel()
	var refreshWG sync.WaitGroup
	refreshWG.Go(func() {
		cache.RefreshLoop(refreshCtx, store, cfg.CacheRefreshInterval, cacheRetryInterval)
	})

	slog.Info("directory config", "sites", len(sites), "refreshInterval", cfg.CacheRefreshInterval.String())

	// The failover store backs both the always-on routing reader (SP3) and the
	// optional operator control surface (SP4), so it is built unconditionally.
	failoverStore := newMongoFailoverStore(mongoClient.Database(cfg.MongoDB))
	failoverReader := newFailoverReader(failoverStore, cfg.FailoverStateTTL)

	// controlSurfaceUp is flipped by the internal listener's goroutine when it
	// stops unexpectedly, so /readyz can take this pod out of rotation.
	var controlSurfaceUp atomic.Bool
	controlSurfaceUp.Store(true)

	rc := restyutil.New(cfg.BotplatformURL, restyutil.WithTimeout(5*time.Second))
	opts := []PortalHandlerOption{
		WithRestyClient(rc), WithDirectoryStore(store),
		WithFailoverReader(failoverReader), WithBackupSiteID(cfg.BackupSiteID),
	}
	if cfg.FailoverOpsToken != "" {
		opts = append(opts, WithControlSurfaceHealth(controlSurfaceUp.Load))
	}
	handler := NewPortalHandler(cache, cfg.DevMode,
		cfg.DevFallbackSiteID, cfg.DevFallbackNatsURL, sites, settings, opts...)
	if cfg.DevMode {
		slog.Info("dev mode enabled — unknown accounts fall back to the dev site")
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// CORS handles preflight before tracing so OPTIONS noise does not pollute Tempo.
	r.Use(ginutil.CORS())
	// o11y server-span middleware wraps real requests so downstream slog/handlers
	// are trace-correlated.
	r.Use(o11ygin.Middleware("portal-service", sdk.TracerProvider(), sdk.MeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
	r.Use(cfg.HTTP.Middleware())
	registerRoutes(r, handler)

	// Optional internal-only failover control surface. Bound on a separate
	// listener so no privileged write shares the public discovery server.
	var internalSrv *http.Server
	if cfg.FailoverOpsToken != "" {
		failoverHandler := NewFailoverHandler(failoverStore, sites, cfg.BackupSiteID)

		ir := gin.New()
		ir.Use(gin.Recovery())
		ir.Use(ginutil.RequestID())
		ir.Use(ginutil.AccessLog())
		registerFailoverRoutes(ir, failoverHandler, cfg.FailoverOpsToken)

		// net.Listen up front so a bad bind fails startup, not silently later.
		ln, lerr := net.Listen("tcp", cfg.FailoverInternalAddr)
		if lerr != nil {
			return fmt.Errorf("bind failover control surface %q: %w", cfg.FailoverInternalAddr, lerr)
		}
		internalSrv = &http.Server{
			Handler:      ir,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		go func() {
			slog.Info("failover control surface starting", "addr", cfg.FailoverInternalAddr)
			if serveErr := internalSrv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
				// Not fatal to the public API, but operators have lost the
				// break-glass surface — fail readiness so this pod is replaced.
				controlSurfaceUp.Store(false)
				slog.Error("failover control surface stopped", "error", serveErr)
			}
		}()
	} else {
		slog.Info("failover control surface disabled (FAILOVER_OPS_TOKEN unset)")
	}

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("portal service starting", "addr", addr)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error {
				slog.Info("shutting down portal service")
				if internalSrv != nil {
					if err := internalSrv.Shutdown(ctx); err != nil {
						slog.Error("shutdown failover control surface", "error", err)
					}
				}
				err := srv.Shutdown(ctx)
				refreshCancel()
				refreshWG.Wait()
				mongoutil.Disconnect(ctx, mongoClient)
				return err
			},
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	err = <-srvErr
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen portal server: %w", err)
	}
	<-shutdownDone

	return nil
}
