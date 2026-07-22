package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig gates stream creation to dev/integration; leave Enabled false in production.
type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamManager is the narrow JetStream surface bootstrapStreams uses, injected by tests.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams creates each site's HR stream when enabled (dev/integration).
// When disabled it verifies they exist so a misconfigured deploy fails at startup.
func bootstrapStreams(ctx context.Context, js streamManager, siteIDs []string, enabled bool) error {
	for _, siteID := range siteIDs {
		cfg := stream.OrgSyncStream(siteID)
		if enabled {
			if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name:     cfg.Name,
				Subjects: cfg.Subjects,
			}); err != nil {
				return fmt.Errorf("create %s stream: %w", cfg.Name, err)
			}
			continue
		}
		if _, err := js.Stream(ctx, cfg.Name); err != nil {
			return fmt.Errorf("verify %s stream: %w", cfg.Name, err)
		}
	}
	return nil
}
