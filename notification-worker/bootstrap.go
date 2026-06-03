package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig gates stream creation to dev/integration; leave Enabled false in production.
type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamManager is the narrow JetStream surface bootstrapStreams uses, injected by tests.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (oteljetstream.Stream, error)
	Stream(ctx context.Context, name string) (oteljetstream.Stream, error)
}

// bootstrapStreams creates MESSAGES_CANONICAL + PUSH_NOTIFICATIONS when enabled (dev/integration).
// When disabled it verifies MESSAGES_CANONICAL exists so a misconfigured deploy fails at startup.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	canonicalCfg := stream.MessagesCanonical(siteID)
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     canonicalCfg.Name,
			Subjects: canonicalCfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create MESSAGES_CANONICAL stream: %w", err)
		}
		pushCfg := stream.PushNotifications(siteID)
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     pushCfg.Name,
			Subjects: pushCfg.Subjects,
			// S2 storage compression — transparent to publisher/consumer; ~2× ratio on JSON
			// at near-zero CPU. Belt-and-braces alongside the publisher's gzip: gzip shrinks
			// inter-replica wire bytes, S2 shrinks on-disk bytes after gzip overhead.
			Compression: jetstream.S2Compression,
		}); err != nil {
			return fmt.Errorf("create PUSH_NOTIFICATIONS stream: %w", err)
		}
		return nil
	}
	// PUSH_NOTIFICATIONS absence is non-fatal: async publish surfaces errors per-publish.
	if _, err := js.Stream(ctx, canonicalCfg.Name); err != nil {
		return fmt.Errorf("verify MESSAGES_CANONICAL stream: %w", err)
	}
	return nil
}
