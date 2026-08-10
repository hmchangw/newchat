package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig gates dev/local stream creation; in production Enabled
// stays false — MIGRATION-OPLOG-{siteID} is owned by oplog-connector/ops
// (CLAUDE.md "Stream bootstrap is opt-in").
type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// bootstrapStreams creates MIGRATION-OPLOG-{siteID} from schema
// (Name+Subjects only) when enabled, for local/dev stacks that have no
// oplog-connector standing the stream up already. A no-op when disabled —
// the tool never creates or mutates the stream in production; a missing
// stream then surfaces as the ordinary js.Stream fail-fast main() already
// performs right after this call.
func bootstrapStreams(ctx context.Context, js jetstream.JetStream, siteID string, enabled bool) error {
	if !enabled {
		return nil
	}
	cfg := stream.MigrationOplog(siteID)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	}); err != nil {
		return fmt.Errorf("create MIGRATION-OPLOG stream: %w", err)
	}
	return nil
}
