package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("admin-service exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	// Logged after obs.Init so it lands in the JSON handler like every other line.
	if len(remoteSites(cfg.AllSiteIDs, cfg.SiteID)) == 0 {
		slog.Warn("no remote peers in ALL_SITE_IDS — cross-site permission fanout is disabled; permission changes stay local to this site",
			"site", cfg.SiteID, "all_site_ids", cfg.AllSiteIDs)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}

	db := mongoClient.Database(cfg.MongoDB)
	st := newStoreMongo(db)
	if err := st.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	sessStore := session.NewMongoStore(db)
	if err := sessStore.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure session indexes failed; continuing (indexes are best-effort)", "error", err)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	// PublishMsg (not Publish) so X-Request-ID from ctx rides onto the outgoing
	// message — same shape as user-service/publisher.
	publish := func(ctx context.Context, subj string, data []byte, encoding string) error {
		if _, err := js.PublishMsg(ctx, natsutil.NewMsgEncoded(ctx, subj, data, encoding)); err != nil {
			return fmt.Errorf("publish inbox event: %w", err)
		}
		return nil
	}
	h := newHandler(st, sessStore, cfg, nc, publish)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(ginutil.CORS())
	r.Use(o11ygin.Middleware("admin-service", sdk.TracerProvider(), sdk.MeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
	registerRoutes(r, h, sessStore, cfg.SiteID)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: httpWriteTimeout,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("admin-service listening", "port", cfg.Port, "site", cfg.SiteID)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error {
				slog.Info("shutting down admin-service")
				err := srv.Shutdown(ctx)
				// srv.Shutdown has already waited out any in-flight toggle, so the
				// drain only has the idle connection left to flush.
				if drainErr := natsutil.Drain(ctx, nc); drainErr != nil {
					slog.Warn("drain nats", "error", drainErr)
				}
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
