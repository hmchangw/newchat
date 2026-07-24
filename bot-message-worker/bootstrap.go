package main

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams verifies BOT_MESSAGES_CANONICAL_{siteID} exists. This worker is a consumer; bot-message-handler owns the stream. Enabled=true is a dev convenience.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	cfg := stream.BotMessagesCanonical(siteID)
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     cfg.Name,
			Subjects: cfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create BOT_MESSAGES_CANONICAL stream: %w", err)
		}
		return nil
	}
	if _, err := js.Stream(ctx, cfg.Name); err != nil {
		return fmt.Errorf("verify BOT_MESSAGES_CANONICAL stream: %w", err)
	}
	return nil
}
