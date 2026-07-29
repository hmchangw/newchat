package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/subject"
)

type NATSConfig struct {
	URL       string `env:"URL,required"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

type Config struct {
	SiteID string `env:"SITE_ID,required"`
	// Required (no default): an omitted TRANSLATION_BACKEND fails fast at startup
	// instead of silently serving mock translations in a deployed environment.
	Backend        string        `env:"TRANSLATION_BACKEND,required"`
	Endpoint       string        `env:"TRANSLATION_ENDPOINT"         envDefault:""`
	AccessTokenURL string        `env:"TRANSLATION_ACCESS_TOKEN_URL" envDefault:""`
	J1Token        string        `env:"TRANSLATION_J1_TOKEN"         envDefault:""`
	HTTPTimeout    time.Duration `env:"TRANSLATION_HTTP_TIMEOUT"     envDefault:"30s"`
	TokenSkew      time.Duration `env:"TRANSLATION_TOKEN_SKEW"       envDefault:"60s"`
	MaxWorkers     int           `env:"MAX_WORKERS"                  envDefault:"100"`
	NATS           NATSConfig    `envPrefix:"NATS_"`
}

// newTranslator selects the backend. The stream backend fails fast when its
// endpoint / accessToken URL / J1 token are missing, so a misconfigured
// production deploy dies at startup, not per-request.
func newTranslator(cfg *Config) (Translator, error) {
	switch cfg.Backend {
	case "mock":
		return mockTranslator{}, nil
	case "stream":
		if cfg.Endpoint == "" {
			return nil, fmt.Errorf("TRANSLATION_ENDPOINT is required when TRANSLATION_BACKEND=stream")
		}
		if cfg.AccessTokenURL == "" {
			return nil, fmt.Errorf("TRANSLATION_ACCESS_TOKEN_URL is required when TRANSLATION_BACKEND=stream")
		}
		if cfg.J1Token == "" {
			return nil, fmt.Errorf("TRANSLATION_J1_TOKEN is required when TRANSLATION_BACKEND=stream")
		}
		return newStreamTranslator(cfg.Endpoint, cfg.AccessTokenURL, cfg.J1Token, cfg.HTTPTimeout, cfg.TokenSkew), nil
	default:
		return nil, fmt.Errorf("unknown TRANSLATION_BACKEND %q (want mock|stream)", cfg.Backend)
	}
}

func main() {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	translator, err := newTranslator(&cfg)
	if err != nil {
		slog.Error("init translator failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	publish := func(ctx context.Context, subj string, data []byte) error {
		return nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
	}

	handler := NewHandler(translator, publish)
	// Cap in-flight handlers so a slow translate backend can't accumulate goroutines
	// and outbound connections without ceiling; saturation drops the fire-and-forget
	// request (logged) rather than piling up.
	router := natsrouter.Default(nc, "translation-service", natsrouter.WithMaxConcurrency(cfg.MaxWorkers))
	natsrouter.RegisterVoid(router, subject.TranslateRequestPattern(cfg.SiteID), handler.Translate)

	slog.Info("translation-service running", "site", cfg.SiteID, "backend", cfg.Backend)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}
