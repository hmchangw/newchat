package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig groups every field that is ONLY meaningful when the
// service is being stood up in dev or integration tests against a NATS
// instance where the streams it publishes to and consumes from do not yet
// exist. In production streams are pre-provisioned by ops/IaC and
// Bootstrap.Enabled must remain false; the service only creates its own
// durable consumer.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the service calls
	// CreateOrUpdateStream at startup for the streams it publishes to and
	// consumes from. Leave false in production.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamManager is the minimal JetStream surface bootstrapStreams depends on.
// Kept service-local so we don't pollute pkg/ with a multi-method type and so
// tests can inject a fake without mockgen.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (oteljetstream.Stream, error)
	Stream(ctx context.Context, name string) (oteljetstream.Stream, error)
}

// bootstrapStreams handles the JetStream MESSAGES + MESSAGES_CANONICAL streams
// this service uses. When enabled (dev/integration), it creates the streams via
// CreateOrUpdateStream. When disabled (production), it verifies they exist via
// Stream() and returns an error if they don't — fail-fast so a misprovisioned
// deploy surfaces at startup rather than at first publish.
//
// Ownership rule: this helper sets only the stream schema (Name + Subjects)
// from pkg/stream.Messages and pkg/stream.MessagesCanonical. Federation config
// belongs to ops/IaC and is layered on in production. App code never sets it.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	messagesCfg := stream.Messages(siteID)
	canonicalCfg := stream.MessagesCanonical(siteID)
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     messagesCfg.Name,
			Subjects: messagesCfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create MESSAGES stream: %w", err)
		}
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     canonicalCfg.Name,
			Subjects: canonicalCfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create MESSAGES_CANONICAL stream: %w", err)
		}
		return nil
	}
	// Production path: verify each stream exists. Fail fast if any is missing —
	// ops/IaC owns provisioning, and a missing stream means the deploy is
	// broken before the first publish or consume.
	if _, err := js.Stream(ctx, messagesCfg.Name); err != nil {
		return fmt.Errorf("verify MESSAGES stream: %w", err)
	}
	if _, err := js.Stream(ctx, canonicalCfg.Name); err != nil {
		return fmt.Errorf("verify MESSAGES_CANONICAL stream: %w", err)
	}
	return nil
}
