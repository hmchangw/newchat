package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nkeys"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/obs"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	Port                 string        `env:"PORT"                     envDefault:"8080"`
	DevMode              bool          `env:"DEV_MODE"                 envDefault:"false"`
	AuthScopedSigningKey string        `env:"AUTH_SCOPED_SIGNING_KEY,required"`
	AuthAccountPubKey    string        `env:"AUTH_ACCOUNT_PUB_KEY,required"`
	NATSJWTExpiry        time.Duration `env:"NATS_JWT_EXPIRY"           envDefault:"2h"`
	NATSJWTExpiryJitter  float64       `env:"NATS_JWT_EXPIRY_JITTER"    envDefault:"0.1"`

	// OIDC settings — required when DEV_MODE is false.
	OIDCIssuerURL string   `env:"OIDC_ISSUER_URL"`
	OIDCAudiences []string `env:"OIDC_AUDIENCES" envSeparator:","`
	TLSSkipVerify bool     `env:"TLS_SKIP_VERIFY"           envDefault:"false"`

	// Mongo backs the session-token branch of POST /auth: a client supplying
	// authToken (instead of ssoToken) has its session read from the shared
	// sessions collection botplatform-service issues into, and a role-scoped
	// NATS JWT minted. Required, so no deployment silently degrades to 503.
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`
	// Pool bounds the Mongo client's connection pool and server-selection wait
	// (MONGO_MAX_POOL_SIZE / MONGO_MIN_POOL_SIZE / MONGO_SERVER_SELECTION_TIMEOUT).
	Pool mongoutil.PoolConfig
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
	if err := cfg.Pool.Validate(); err != nil {
		return fmt.Errorf("validate pool config: %w", err)
	}

	signingKP, err := nkeys.FromSeed([]byte(cfg.AuthScopedSigningKey))
	if err != nil {
		return fmt.Errorf("parse signing key: %w", err)
	}
	if skPub, err := signingKP.PublicKey(); err != nil || !nkeys.IsValidPublicAccountKey(skPub) {
		return fmt.Errorf("AUTH_SCOPED_SIGNING_KEY is not an account-type signing key")
	}
	if !nkeys.IsValidPublicAccountKey(cfg.AuthAccountPubKey) {
		return fmt.Errorf("AUTH_ACCOUNT_PUB_KEY is not a valid account public key")
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	sessions := session.NewMongoStore(mongoClient.Database(cfg.MongoDB))

	opts := []Option{
		WithJitter(cfg.NATSJWTExpiryJitter),
		WithBotplatformValidator(botauth.NewValidator(sessions)),
	}

	var handler *AuthHandler

	if cfg.DevMode {
		slog.Warn("dev mode enabled — OIDC validation disabled")
		handler = NewAuthHandler(nil, signingKP, cfg.AuthAccountPubKey, cfg.NATSJWTExpiry, true, opts...)
	} else {
		if cfg.OIDCIssuerURL == "" || len(cfg.OIDCAudiences) == 0 {
			return fmt.Errorf("OIDC_ISSUER_URL and OIDC_AUDIENCES are required when DEV_MODE is false")
		}

		// Initialize OIDC validator — connects to issuer and fetches JWKS keys.
		oidcValidator, err := pkgoidc.NewValidator(ctx, pkgoidc.Config{
			IssuerURL:     cfg.OIDCIssuerURL,
			Audiences:     cfg.OIDCAudiences,
			TLSSkipVerify: cfg.TLSSkipVerify,
		})
		if err != nil {
			return fmt.Errorf("create oidc validator: %w", err)
		}
		slog.Info("oidc validator initialized", "issuer", cfg.OIDCIssuerURL)
		handler = NewAuthHandler(oidcValidator, signingKP, cfg.AuthAccountPubKey, cfg.NATSJWTExpiry, false, opts...)
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// CORS handles preflight before tracing so OPTIONS noise does not pollute Tempo.
	r.Use(ginutil.CORS())
	// o11y server-span middleware wraps real requests so downstream slog/handlers
	// are trace-correlated.
	r.Use(o11ygin.Middleware("auth-service", sdk.TracerProvider(), sdk.MeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())...)
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
		slog.Info("auth service starting", "addr", addr)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error {
				slog.Info("shutting down auth service")
				return srv.Shutdown(ctx)
			},
			func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	err = <-srvErr
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen auth server: %w", err)
	}
	<-shutdownDone

	return nil
}
