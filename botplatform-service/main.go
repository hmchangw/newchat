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
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/valkeyutil"
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
	sessionStore := session.NewMongoStore(db)
	if err := sessionStore.EnsureIndexes(ctx); err != nil {
		return fmt.Errorf("ensure session indexes: %w", err)
	}
	st := newStoreMongo(db)
	subStore := newMongoSubscriptionStore(db)
	h := newHandler(st, &cfg)
	h.subs = subStore

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}

	// 3s msg-flow timeout; bot-message-handler receives req/reply on the shared NATS conn.
	h.forwarder = newBotForwarder(nc.NatsConn(), 3*time.Second)
	// 15s DM-ensure timeout (room-mgmt budget) — first-DM creates room + federates member_added.
	h.dmEnsurer = newNATSDMEnsurer(nc.NatsConn(), cfg.SiteID, 15*time.Second)

	// Empty VALKEY_ADDRS silently disables rate-limit + idempotency (dev only; prod must supply).
	var valkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		valkey, err = valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
			valkeyutil.WithObservability(sdk),
			valkeyutil.WithRequireParentSpan(true),
		)
		if err != nil {
			return fmt.Errorf("connect valkey: %w", err)
		}
		slog.Info("bot rate-limit + idempotency enabled",
			"per_caller_per_min", cfg.BotRateLimitPerCallerPerMin,
			"global_per_min", cfg.BotRateLimitGlobalPerMin,
			"msg_ttl", cfg.BotIdempotencyMsgTTL,
			"room_mgmt_ttl", cfg.BotIdempotencyRoomMgmtTTL,
		)
	} else {
		slog.Warn("bot rate-limit + idempotency DISABLED — VALKEY_ADDRS is empty (dev only)")
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(ginutil.CORS())
	r.Use(o11ygin.Middleware("botplatform-service", sdk.TracerProvider(), sdk.MeterProvider(), sdk.Propagator, o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(accessLogMiddleware())
	registerRoutes(r, h)
	registerBotRoutes(r, sessionStore, valkey, &cfg, h)

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
				if drainErr := nc.Drain(); drainErr != nil {
					slog.Warn("nats drain failed", "error", drainErr)
				}
				valkeyutil.Disconnect(valkey)
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
