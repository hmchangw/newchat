package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// streamManager is the minimal JetStream surface bootstrapStreams needs, service-local so tests can fake it without mockgen.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (oteljetstream.Stream, error)
}

// bootstrapStreams is a no-op in production (this service owns no streams). When Enabled
// (dev/integration) it creates only the MIGRATION_OPLOG_{siteID} schema; inbox-worker owns INBOX.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	if !enabled {
		return nil
	}
	cfg := stream.MigrationOplog(siteID)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	}); err != nil {
		return fmt.Errorf("create MIGRATION_OPLOG stream: %w", err)
	}
	return nil
}
