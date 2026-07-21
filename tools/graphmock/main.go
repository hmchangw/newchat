// graphmock is a fixture-driven mock of the Microsoft Graph surface the HR
// sync uses (token grant, group profile, paged group members). Point
// GRAPH_BASE_URL at http://host:port/v1.0 and GRAPH_TOKEN_URL at
// http://host:port/{tenant}/oauth2/v2.0/token. Dev/e2e only — no auth.
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/caarlos0/env/v11"
)

type config struct {
	Port string `env:"PORT" envDefault:"8080"`
	// FixturesPath optionally seeds the dataset at startup; PUT /__fixtures
	// replaces it at runtime.
	FixturesPath string `env:"FIXTURES_PATH" envDefault:""`
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
	s := &server{}
	if cfg.FixturesPath != "" {
		raw, err := os.ReadFile(cfg.FixturesPath)
		if err != nil {
			return fmt.Errorf("read fixtures: %w", err)
		}
		if err := json.Unmarshal(raw, &s.data); err != nil {
			return fmt.Errorf("decode fixtures: %w", err)
		}
	}
	slog.Info("graphmock listening", "port", cfg.Port, "groups", len(s.data.Groups))
	return newRouter(s).Run(":" + cfg.Port)
}
