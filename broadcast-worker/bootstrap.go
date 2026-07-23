package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"
)

// bootstrapConfig groups every field that is ONLY meaningful when the
// service is being stood up in dev or integration tests against a NATS
// instance where the streams it consumes do not yet exist. In production
// streams are pre-provisioned by ops/IaC and Bootstrap.Enabled must remain
// false; the service only creates its own durable consumer.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the service calls
	// CreateOrUpdateStream at startup for the streams it consumes.
	// Leave false in production.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamManager is the minimal JetStream surface bootstrapStreams depends on.
// Kept service-local so we don't pollute pkg/ with a multi-method type and so
// tests can inject a fake without mockgen.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams handles the JetStream input stream this service consumes.
// The stream identity is env-driven so both user and bot deployments verify
// their own stream. When enabled (dev/integration), creates the stream via
// CreateOrUpdateStream over (streamName, [subjectFilter]). When disabled
// (production), verifies the stream exists — fail-fast so a misprovisioned
// deploy surfaces at startup rather than at first consume.
//
// Federation config is not set here — that belongs to ops/IaC.
func bootstrapStreams(ctx context.Context, js streamManager, streamName, subjectFilter string, enabled bool) error {
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{subjectFilter},
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", streamName, err)
		}
		return nil
	}
	if _, err := js.Stream(ctx, streamName); err != nil {
		return fmt.Errorf("verify stream %s: %w", streamName, err)
	}
	return nil
}
