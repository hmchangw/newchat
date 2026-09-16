package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"
)

// bootstrapConfig groups fields only meaningful when standing up dev/integration against a NATS
// instance whose streams don't exist yet; in production streams are pre-provisioned and Enabled must stay false.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the service calls CreateOrUpdateStream at startup for the streams it consumes. Leave false in production.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamManager is the minimal JetStream surface bootstrapStreams depends on, kept service-local
// so it doesn't pollute pkg/ and tests can inject a fake without mockgen.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams creates (dev/integration) or verifies (production, fail-fast) the JetStream
// input stream; identity is env-driven so user/bot deployments target their own stream. Federation config belongs to ops/IaC.
//
// The retry stream (RETRY-{siteID}) is dev-only like the input stream — in
// production it is ops/IaC-owned, and the retry consumer binds to it
// regardless of RETRY_LANE_ENABLED (see main.go), so a missing stream there
// surfaces at that bind instead.
func bootstrapStreams(ctx context.Context, js streamManager, streamName, subjectFilter, retryStream, retrySubject string, enabled bool) error {
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{subjectFilter},
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", streamName, err)
		}
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     retryStream,
			Subjects: []string{retrySubject},
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", retryStream, err)
		}
		return nil
	}
	// Retry stream absence is non-fatal here: the retry consumer bind
	// surfaces a genuinely missing RETRY stream at startup.
	if _, err := js.Stream(ctx, streamName); err != nil {
		return fmt.Errorf("verify stream %s: %w", streamName, err)
	}
	return nil
}
