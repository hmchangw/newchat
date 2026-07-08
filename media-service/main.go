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

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/minioutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("media-service exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.Background()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if cfg.EIDCacheCapacity <= 0 {
		return fmt.Errorf("EID_CACHE_CAPACITY must be positive, got %d", cfg.EIDCacheCapacity)
	}
	if cfg.EIDCacheTTL <= 0 {
		return fmt.Errorf("EID_CACHE_TTL must be positive, got %s", cfg.EIDCacheTTL)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	store := newMongoStore(mongoClient.Database(cfg.MongoDB))
	defer mongoutil.Disconnect(ctx, mongoClient)

	if err := store.EnsureEmojiIndexes(ctx); err != nil {
		return fmt.Errorf("ensure emoji indexes: %w", err)
	}

	minioClient, err := minioutil.Connect(ctx, cfg.MinioEndpoint, cfg.MinioUseSSL, cfg.MinioAccessKey, cfg.MinioSecretKey)
	if err != nil {
		return fmt.Errorf("connect minio: %w", err)
	}
	blobs := newMinioBlobStore(minioClient, cfg.MinioBucket)

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}

	h := newHandler(store, store, blobs, &cfg)

	router := natsrouter.New(nc, "media-service")
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
	registerEmojiNATS(router, h, cfg.SiteID)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestIDMiddleware())
	r.Use(ginutil.Metrics("media-service"))
	r.Use(accessLogMiddleware())
	r.Use(corsMiddleware())
	registerRoutes(r, h)

	// /metrics on a separate port so scrapes don't hit the public API listener.
	stopMetrics, err := otelutil.ServeMetrics(cfg.MetricsAddr)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			slog.Info("shutting down media-service")
			return srv.Shutdown(ctx)
		},
		func(ctx context.Context) error { return stopMetrics(ctx) },
	)

	slog.Info("media-service listening", "port", cfg.Port, "site", cfg.SiteID)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}
	return nil
}
