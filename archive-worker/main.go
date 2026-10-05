package main

import (
	"log/slog"
	"os"

	"github.com/caarlos0/env/v11"
)

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if err := cfg.validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
}
