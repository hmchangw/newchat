package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	Port int `env:"PORT" envDefault:"8090"`
	// BindAddr is the interface to listen on. Defaults to loopback: the UI is
	// unauthenticated and its session cookie rides plain HTTP, so it must not
	// be reachable off-host by default. The container image sets 0.0.0.0,
	// where the port mapping is the exposure boundary.
	BindAddr string `env:"BIND_ADDR" envDefault:"127.0.0.1"`
	// CredsFile is an optional NATS user credentials file (JWT + NKey). When
	// set, it authenticates every NATS connection the tool opens. Empty means
	// connect without credentials.
	CredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	// IdleTimeout is how long a browser session may go without activity before
	// its hub (and NATS connections) are torn down.
	IdleTimeout time.Duration `env:"SESSION_IDLE_TIMEOUT" envDefault:"30m"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	if cfg.CredsFile != "" {
		if _, err := os.Stat(cfg.CredsFile); err != nil {
			slog.Error("nats creds file not accessible", "path", cfg.CredsFile, "error", err)
			os.Exit(1)
		}
	}

	sessions := newSessionManager(func() Hub { return newNATSHub(cfg.CredsFile) }, cfg.IdleTimeout)
	sessions.start()
	h := newHandler(sessions)

	mux := http.NewServeMux()
	h.registerRoutes(mux)

	srv := &http.Server{
		Addr:        fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.Port),
		Handler:     mux,
		ReadTimeout: 30 * time.Second,
		// WriteTimeout deliberately omitted — SSE connections are long-lived.
		IdleTimeout: 60 * time.Second,
	}

	slog.Info("nats-debug starting", "bind_addr", cfg.BindAddr, "port", cfg.Port)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	shutdown.Wait(context.Background(), 10*time.Second,
		func(ctx context.Context) error { return srv.Shutdown(ctx) },
		func(_ context.Context) error { sessions.shutdown(); return nil },
	)
}
