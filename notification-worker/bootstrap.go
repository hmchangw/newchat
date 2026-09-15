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

// bootstrapStreams creates the input+output streams when enabled (dev/integration),
// otherwise verifies both exist so a misconfigured deploy fails at startup.
//
// Both streams are passed as stream.Config, never as loose name/subject strings:
// CreateOrUpdateStream NARROWS an existing stream, so handing it a consumer's
// filter subject (this worker filters on the .created leaf) would rewrite
// MESSAGES-CANONICAL to that leaf and strip .edited/.deleted/.reacted/.pinned
// from every other publisher — last service to boot wins. Taking the Config
// makes that mistake unrepresentable.
//
// Ownership rule (CLAUDE.md): this helper sets ONLY the stream schema, Name +
// Subjects from pkg/stream. Retention, storage, compression and federation are
// ops/IaC concerns layered on in production; app code never sets them.
func bootstrapStreams(ctx context.Context, js streamManager, input, output stream.Config, enabled bool) error {
	if enabled {
		for _, cfg := range []stream.Config{input, output} {
			if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name:     cfg.Name,
				Subjects: cfg.Subjects,
			}); err != nil {
				return fmt.Errorf("create stream %s: %w", cfg.Name, err)
			}
		}
		return nil
	}
	// Both streams are verified. Publishing is synchronous (jsPublisher.PublishMsg
	// returns the PublishMsg error), so a missing output stream is not absorbed
	// per-publish: it naks every notification until MaxDeliver drops it — silent
	// total delivery loss. Fail fast here instead, as message-gatekeeper does.
	for _, cfg := range []stream.Config{input, output} {
		if _, err := js.Stream(ctx, cfg.Name); err != nil {
			return fmt.Errorf("verify stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}
