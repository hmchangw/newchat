package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nkeys"

	"github.com/hmchangw/chat/pkg/ginutil"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	Port                 string        `env:"PORT"                     envDefault:"8080"`
	DevMode              bool          `env:"DEV_MODE"                 envDefault:"false"`
	AuthScopedSigningKey string        `env:"AUTH_SCOPED_SIGNING_KEY,required"`
	AuthAccountPubKey    string        `env:"AUTH_ACCOUNT_PUB_KEY,required"`
	NATSJWTExpiry        time.Duration `env:"NATS_JWT_EXPIRY"           envDefault:"2h"`
	NATSJWTExpiryJitter  float64       `env:"NATS_JWT_EXPIRY_JITTER"    envDefault:"0.1"`
	MetricsAddr          string        `env:"METRICS_ADDR"              envDefault:":9090"`

	// OIDC settings — required when DEV_MODE is false.
	OIDCIssuerURL string   `env:"OIDC_ISSUER_URL"`
	OIDCAudiences []string `env:"OIDC_AUDIENCES" envSeparator:","`
	TLSSkipVerify bool     `env:"TLS_SKIP_VERIFY"           envDefault:"false"`

	// BotplatformURL is the LOCAL site's botplatform-service URL. When set,
	// auth-service exposes the session-token branch of POST /auth: a client
	// supplying authToken (instead of ssoToken) gets its session validated
	// via botplatform's /api/v1/auth/validate and a role-scoped NATS JWT minted.
	// Unset = session-token requests fail with 503 upstream_unavailable.
	BotplatformURL string `env:"BOTPLATFORM_URL"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

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

	opts := []Option{WithJitter(cfg.NATSJWTExpiryJitter)}
	if cfg.BotplatformURL != "" {
		rc := restyutil.New("", restyutil.WithTimeout(5*time.Second))
		opts = append(opts, WithBotplatformValidator(
			newHTTPBotplatformValidator(rc, cfg.BotplatformURL)))
		slog.Info("session-token branch enabled", "botplatform_url", cfg.BotplatformURL)
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
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.Metrics("auth-service"))
	r.Use(ginutil.AccessLog())
	r.Use(ginutil.CORS())
	registerRoutes(r, handler)

	// /metrics on a separate port so scrapes don't hit the public API listener.
	metricsServer := otelutil.MetricsServer()
	metricsLn, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		slog.Error("metrics listen failed", "addr", cfg.MetricsAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("metrics server listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.Serve(metricsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()

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
			func(ctx context.Context) error { return metricsServer.Shutdown(ctx) },
		)
	}()

	err = <-srvErr
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen auth server: %w", err)
	}
	<-shutdownDone

	return nil
}
