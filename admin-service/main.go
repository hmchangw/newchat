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
	"github.com/hmchangw/chat/pkg/svcjwt"
)

func main() {
	if err := run(); err != nil {
		slog.Error("admin-service exited", "error", err)
		os.Exit(1)
	}
}

// applyBaseMiddleware installs admin-service's cross-cutting HTTP middleware.
// obsMW is the observability chain (empty in tests).
//
// It deliberately installs NO blanket per-request timeout. admin-service manages
// its own per-request deadline: the permission handlers pin requestBudget via
// withRequestBudget (just under httpWriteTimeout), and the cross-site permission
// fanout self-limits to min(FanoutTimeout, request deadline). A router timeout
// shorter than FanoutTimeout — e.g. the fleet's shared 10s REQUEST_TIMEOUT —
// would silently cap the fanout and abort multi-site permission changes early
// (see permissions.go:publishPermissionFanout). Keep this timeout-free.
func applyBaseMiddleware(r *gin.Engine, obsMW []gin.HandlerFunc) {
	r.Use(ginutil.CORS())
	r.Use(obsMW...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
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

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk))
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
	publishInbox := func(ctx context.Context, subj string, data []byte) error {
		if _, err := js.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data)); err != nil {
			return fmt.Errorf("publish inbox event: %w", err)
		}
		return nil
	}
	// Client-update publishing is opt-in per site: an empty base URL leaves the
	// forwarder nil and the upload route answers 503. loadConfig has already
	// rejected a half-configured opt-in, so reaching here with a base URL means
	// the key and account are present too.
	var handlerOpts []handlerOption
	if cfg.ClientUpdateBaseURL == "" {
		slog.Warn("client update publishing is disabled: CLIENT_UPDATE_BASE_URL is unset",
			"site", cfg.SiteID)
	} else {
		signer, err := svcjwt.NewSigner(cfg.SvcJWTPrivateKey, cfg.SvcJWTIssuer)
		if err != nil {
			return fmt.Errorf("build service-token signer: %w", err)
		}
		// Minted per forward and never returned to a caller, so no bearer
		// credential for client-update-service ever leaves this process.
		mintClientUpdateToken := func() (string, error) {
			token, _, err := signer.Sign(cfg.ClientUpdateServiceAccount, cfg.ClientUpdateAudience, cfg.SvcJWTTTL)
			if err != nil {
				return "", fmt.Errorf("sign client-update token: %w", err)
			}
			return token, nil
		}
		handlerOpts = append(handlerOpts,
			withClientUpdate(newClientUpdateForwarder(cfg.ClientUpdateBaseURL, cfg.ClientUpdateUploadTimeout, mintClientUpdateToken)))
	}

	h := newHandler(st, sessStore, cfg, nc, publishInbox, handlerOpts...)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	obsMW := o11ygin.Middleware("admin-service", sdk.TracerProvider(), sdk.MeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())
	applyBaseMiddleware(r, obsMW)
	registerRoutes(r, h, sessStore, cfg.SiteID, cfg.ClientUpdateUploadTimeout)

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
