package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/minioutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if err := validateUploadTokens(cfg.UploadTokens); err != nil {
		return fmt.Errorf("validate upload tokens: %w", err)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	// Logged after obs.Init so it lands in the JSON handler. An operator who sees
	// uploads 401 needs to know the table is empty rather than their token wrong.
	if len(cfg.UploadTokens) == 0 {
		slog.Warn("UPLOAD_TOKENS is empty — POST /api/v1/version will reject every upload; downloads are unaffected",
			"site", cfg.SiteID)
	}

	minioClient, err := minioutil.Connect(ctx, cfg.MinioEndpoint, cfg.MinioUseSSL, cfg.MinioAccessKey, cfg.MinioSecretKey, minioutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("minio connect: %w", err)
	}
	bc, ok := minioClient.(bucketClient)
	if !ok {
		return fmt.Errorf("minio client %T does not support bucket creation", minioClient)
	}
	if err := ensureBucket(ctx, bc, cfg.MinioBucket); err != nil {
		return fmt.Errorf("ensure bucket: %w", err)
	}

	store := newMinioVersionStore(minioClient, cfg.MinioBucket, cfg.MinioDownloadTimeout)
	cache := newBlobCache(cfg.CacheMaxEntries, cfg.CacheTTL, cfg.CacheMaxObjectBytes)
	handler := NewHandler(store, cache)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(o11ygin.Middleware("client-update-service", sdk.TracerProvider(), sdk.MeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	registerRoutes(r, handler, cfg.UploadTokens)

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: cfg.HTTPWriteTimeout,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("client-update-service starting", "addr", addr, "site", cfg.SiteID)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error { return srv.Shutdown(ctx) },
			// obsShutdown LAST so all prior teardown telemetry is exported.
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	err = <-srvErr
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen client-update server: %w", err)
	}
	<-shutdownDone
	return nil
}
