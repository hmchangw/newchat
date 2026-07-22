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

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("botplatform-service exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if cfg.SessionsMaxPerAccount <= 0 {
		return fmt.Errorf("SESSIONS_MAX_PER_ACCOUNT must be positive, got %d", cfg.SessionsMaxPerAccount)
	}
	if cfg.BcryptCost < 4 || cfg.BcryptCost > 31 {
		return fmt.Errorf("BCRYPT_COST must be in [4, 31], got %d", cfg.BcryptCost)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}

	db := mongoClient.Database(cfg.MongoDB)
	if err := session.NewMongoStore(db).EnsureIndexes(ctx); err != nil {
		return fmt.Errorf("ensure session indexes: %w", err)
	}
	st := newStoreMongo(db)
	h := newHandler(st, &cfg)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(ginutil.CORS())
	r.Use(o11ygin.Middleware("botplatform-service", sdk.TracerProvider(), sdk.MeterProvider(), sdk.Propagator, o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(accessLogMiddleware())
	registerRoutes(r, h)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("botplatform-service listening", "port", cfg.Port, "site", cfg.SiteID)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error {
				slog.Info("shutting down botplatform-service")
				err := srv.Shutdown(ctx)
				mongoutil.Disconnect(ctx, mongoClient)
				return err
			},
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	if err := <-srvErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}
	<-shutdownDone
	return nil
}
