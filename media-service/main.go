package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/minioutil"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/restyutil"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("media-service exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		return fmt.Errorf("set platform-admin account prefix: %w", err)
	}
	if cfg.EIDCacheCapacity <= 0 {
		return fmt.Errorf("EID_CACHE_CAPACITY must be positive, got %d", cfg.EIDCacheCapacity)
	}
	if cfg.EIDCacheTTL <= 0 {
		return fmt.Errorf("EID_CACHE_TTL must be positive, got %s", cfg.EIDCacheTTL)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithObservability(sdk), mongoutil.WithLazyConnect())
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	store := newMongoStore(mongoClient.Database(cfg.MongoDB))
	defer mongoutil.Disconnect(ctx, mongoClient)

	if err := store.EnsureEmojiIndexes(ctx); err != nil {
		return fmt.Errorf("ensure emoji indexes: %w", err)
	}

	minioClient, err := minioutil.Connect(ctx, cfg.MinioEndpoint, cfg.MinioUseSSL, cfg.MinioAccessKey, cfg.MinioSecretKey, minioutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect minio: %w", err)
	}
	blobs := newMinioBlobStore(minioClient, cfg.MinioBucket)

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}

	h := newHandler(store, store, blobs, &cfg)

	// Bound in-flight handlers so a burst is shed at the door (ErrUnavailable)
	// instead of piling unbounded work onto MongoDB/MinIO. MAX_CONCURRENCY=0 disables.
	routerOpts := []natsrouter.Option{natsrouter.WithSiteID(cfg.SiteID)}
	if cfg.MaxConcurrency > 0 {
		routerOpts = append(routerOpts, natsrouter.WithMaxConcurrency(cfg.MaxConcurrency))
	}
	router := natsrouter.New(nc, "media-service", routerOpts...)
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
	registerEmojiNATS(router, h, cfg.SiteID)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(corsMiddleware())
	r.Use(o11ygin.Middleware("media-service", sdk.TracerProvider(), sdk.MeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	sessions := botauth.NewValidator(
		restyutil.New("", restyutil.WithTimeout(5*time.Second), restyutil.WithMaxIdleConns(32)), cfg.BotplatformURL)
	registerRoutes(r, h, sessions, cfg.SiteID)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("media-service listening", "port", cfg.Port, "site", cfg.SiteID)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error { return router.Shutdown(ctx) },
			func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
			func(ctx context.Context) error {
				slog.Info("shutting down media-service")
				return srv.Shutdown(ctx)
			},
			// obsShutdown LAST so all prior teardown telemetry is exported.
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	if err := <-srvErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}
	<-shutdownDone
	return nil
}
