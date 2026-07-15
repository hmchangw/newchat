package main

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig groups every field that is ONLY meaningful when the service is
// being stood up in dev or integration tests against a NATS instance where the
// OUTBOX stream it owns does not yet exist. In production the stream is
// pre-provisioned by ops/IaC and Bootstrap.Enabled must remain false.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the service calls
	// CreateOrUpdateStream at startup for the OUTBOX stream it owns.
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

// bootstrapStreams handles the OUTBOX stream this service owns. When enabled
// (dev/integration) it creates the stream via CreateOrUpdateStream. When
// disabled (production) it verifies the stream exists via Stream() and returns
// an error if it doesn't — fail-fast so a misprovisioned deploy surfaces at
// startup rather than at first consume.
//
// Ownership rule: this helper sets only the stream schema (Name + Subjects) from
// pkg/stream.Outbox. Cross-site routing belongs to ops/IaC; app code never sets it.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	outboxCfg := stream.Outbox(siteID)
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     outboxCfg.Name,
			Subjects: outboxCfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create OUTBOX stream: %w", err)
		}
		return nil
	}
	if _, err := js.Stream(ctx, outboxCfg.Name); err != nil {
		return fmt.Errorf("verify OUTBOX stream: %w", err)
	}
	return nil
}
