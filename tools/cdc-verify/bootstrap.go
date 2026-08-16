package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig gates dev/local stream creation; production keeps Enabled
// false — the stream is owned by oplog-connector/ops ("Stream bootstrap is opt-in").
type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// bootstrapStreams creates MIGRATION-OPLOG-{siteID} from schema (Name+Subjects) for
// dev stacks without oplog-connector; disabled it no-ops and js.Stream fails fast.
// Create-only: an existing stream is owned by the real pipeline and never updated —
// a bare Name+Subjects update would strip its retention/replica config.
func bootstrapStreams(ctx context.Context, js jetstream.JetStream, siteID string, enabled bool) error {
	if !enabled {
		return nil
	}
	cfg := stream.MigrationOplog(siteID)
	_, err := js.Stream(ctx, cfg.Name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("check MIGRATION-OPLOG stream: %w", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	}); err != nil {
		return fmt.Errorf("create MIGRATION-OPLOG stream: %w", err)
	}
	return nil
}
