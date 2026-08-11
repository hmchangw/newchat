package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/stream"
)

// streamManager is the minimal JetStream surface bootstrapStreams needs, service-local so tests
// can fake it without mockgen.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
}

// bootstrapStreams is a no-op in production (streams are ops/IaC-owned). When Enabled
// (dev/integration) it creates only the DR-OPLOG-{siteID} schema (Name + Subjects); the
// cross-gateway routing that lands a remote origin site's publish here is an ops/IaC concern.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	if !enabled {
		return nil
	}
	cfg := stream.DROplog(siteID)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	}); err != nil {
		return fmt.Errorf("create DR-OPLOG stream: %w", err)
	}
	return nil
}
