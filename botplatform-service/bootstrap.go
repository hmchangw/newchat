package main

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig gates dev-only CreateOrUpdateStream calls; ops/IaC owns provisioning in prod.
type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams creates/verifies the bot streams. Sets only Name + Subjects; retention/replicas/storage stay with ops/IaC.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	streams := []stream.Config{
		stream.BotMessagesCanonical(siteID),
		stream.BotPushNotif(siteID),
	}
	for _, cfg := range streams {
		if enabled {
			if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name:     cfg.Name,
				Subjects: cfg.Subjects,
			}); err != nil {
				return fmt.Errorf("create %s stream: %w", trimSiteSuffix(cfg.Name, siteID), err)
			}
			continue
		}
		if _, err := js.Stream(ctx, cfg.Name); err != nil {
			return fmt.Errorf("verify %s stream: %w", trimSiteSuffix(cfg.Name, siteID), err)
		}
	}
	return nil
}

// trimSiteSuffix drops the trailing _{siteID} so error messages carry the schema name.
func trimSiteSuffix(name, siteID string) string {
	suffix := "_" + siteID
	if len(name) > len(suffix) && name[len(name)-len(suffix):] == suffix {
		return name[:len(name)-len(suffix)]
	}
	return name
}
