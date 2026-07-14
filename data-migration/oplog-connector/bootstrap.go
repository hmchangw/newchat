package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig gates dev/integration stream creation; in production Enabled stays false (ops/IaC owns the stream).
type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamManager is the minimal JetStream surface bootstrapStreams needs, service-local so tests can fake it without mockgen.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (oteljetstream.Stream, error)
	Stream(ctx context.Context, name string) (oteljetstream.Stream, error)
}

// bootstrapStreams owns the MIGRATION_OPLOG_{siteID} stream — enabled it creates from schema (Name+Subjects), disabled it verifies existence and fails fast. Federation config stays ops/IaC-owned.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	cfg := stream.MigrationOplog(siteID)
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     cfg.Name,
			Subjects: cfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create MIGRATION_OPLOG stream: %w", err)
		}
		return nil
	}
	if _, err := js.Stream(ctx, cfg.Name); err != nil {
		return fmt.Errorf("verify MIGRATION_OPLOG stream: %w", err)
	}
	return nil
}
