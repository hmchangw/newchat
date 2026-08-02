// Command teams-room-inspector is a per-site read-only HTTP service that
// reports what this site's Mongo holds for a batch of Teams chat ids: whether
// each chat's room exists and how many subscriptions point at it. It is called
// by the global teams-room-verify CronJob, which compares the counts against
// the chat's member list. One deployment per site.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	o11ygin "github.com/flywindy/o11y/gin"
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
)

// Config is the service's environment configuration. Mongo is this site's own
// operational database — the inspector never reaches across sites.
type Config struct {
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	// SiteID is echoed in every response so a misrouted call is obvious in the
	// caller's logs rather than silently counted against the wrong site.
	SiteID string `env:"SITE_ID,required,notEmpty"`
	Port   string `env:"PORT" envDefault:"8080"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("teams-room-inspector failed", "error", err)
		os.Exit(1)
	}
}

// run wires dependencies and serves until shutdown. It returns an error rather
// than calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	// Reads only, so the scan rides a secondary-preferred client and keeps the
	// primary free. Replication lag can make a just-created room look missing;
	// that chat simply keeps its needVerify flag and is re-checked on the next
	// run, so a lagged read costs a repeat rather than a wrong write.
	mongoClient, err := mongoutil.ConnectRead(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}

	handler := NewHandler(newMongoStore(mongoClient.Database(cfg.MongoDB)), cfg.SiteID)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(o11ygin.Middleware("teams-room-inspector", sdk.TracerProvider(), sdk.MeterProvider(), sdk.Propagator, o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
	registerRoutes(r, handler)

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("teams-room-inspector starting", "addr", addr, "site_id", cfg.SiteID)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error {
				slog.Info("shutting down teams-room-inspector")
				err := srv.Shutdown(ctx)
				mongoutil.Disconnect(ctx, mongoClient)
				return err
			},
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	if err := <-srvErr; err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen teams-room-inspector: %w", err)
	}
	<-shutdownDone
	return nil
}
