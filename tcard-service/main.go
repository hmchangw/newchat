package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
)

// cacheRetryInterval paces reload attempts after a failed load so a startup
// Mongo blip does not leave the service unready until the next daily slot.
const cacheRetryInterval = 30 * time.Second

type config struct {
	Port string `env:"PORT" envDefault:"8087"`

	// CacheRefreshAt is the daily wall-clock re-sync time (layout 15:04Z07:00),
	// besides the startup load and the on-demand POST /api/v1/cards/refresh.
	CacheRefreshAt string `env:"TCARD_CACHE_REFRESH_AT" envDefault:"08:00+08:00"`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`
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

	refreshAt, err := parseRefreshAt(cfg.CacheRefreshAt)
	if err != nil {
		return fmt.Errorf("parse TCARD_CACHE_REFRESH_AT: %w", err)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}

	store := newMongoCardStore(mongoClient.Database(cfg.MongoDB))
	if err := store.EnsureIndexes(ctx); err != nil {
		return fmt.Errorf("ensure cards indexes: %w", err)
	}

	// Populate the card cache in the background; /readyz stays unavailable
	// until the first successful load.
	cache := newCardCache()
	refreshCtx, refreshCancel := context.WithCancel(ctx)
	defer refreshCancel()
	var refreshWG sync.WaitGroup
	refreshWG.Go(func() {
		cache.RefreshLoop(refreshCtx, store, refreshAt, cacheRetryInterval)
	})

	slog.Info("card cache config", "refreshAt", cfg.CacheRefreshAt)

	handler := NewCardHandler(cache, store)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// CORS handles preflight before tracing so OPTIONS noise does not pollute Tempo.
	r.Use(ginutil.CORS())
	// o11y server-span middleware wraps real requests so slog/handlers are trace-correlated.
	r.Use(o11ygin.Middleware("tcard-service", sdk.TracerProvider(), sdk.MeterProvider(), sdk.Propagator, o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
	registerRoutes(r, handler)

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("tcard service starting", "addr", addr)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error {
				slog.Info("shutting down tcard service")
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
		return fmt.Errorf("listen tcard server: %w", err)
	}
	<-shutdownDone

	return nil
}
