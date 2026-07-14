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
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("botplatform-service exited", "error", err)
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
	if cfg.SessionsMaxPerAccount <= 0 {
		return fmt.Errorf("SESSIONS_MAX_PER_ACCOUNT must be positive, got %d", cfg.SessionsMaxPerAccount)
	}
	if cfg.BcryptCost < 4 || cfg.BcryptCost > 31 {
		return fmt.Errorf("BCRYPT_COST must be in [4, 31], got %d", cfg.BcryptCost)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer mongoutil.Disconnect(ctx, mongoClient)

	st, err := newMongoStore(ctx, mongoClient.Database(cfg.MongoDB))
	if err != nil {
		return fmt.Errorf("init mongo store: %w", err)
	}
	h := newHandler(st, &cfg)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(accessLogMiddleware())
	r.Use(ginutil.CORS())
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
		shutdown.Wait(ctx, 25*time.Second, func(ctx context.Context) error {
			slog.Info("shutting down botplatform-service")
			return srv.Shutdown(ctx)
		})
	}()

	if err := <-srvErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}
	<-shutdownDone
	return nil
}
