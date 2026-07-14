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
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (oteljetstream.Stream, error)
	Stream(ctx context.Context, name string) (oteljetstream.Stream, error)
}

// bootstrapStreams handles the JetStream INBOX stream this service uses. When
// enabled (dev/integration), it creates the stream via CreateOrUpdateStream.
// When disabled (production), it verifies the stream exists via Stream() and
// returns an error if it doesn't — fail-fast so a misprovisioned deploy
// surfaces at startup rather than at first publish.
//
// Ownership rule: this helper sets only the stream schema (Name + Subjects)
// from pkg/stream.Inbox. Cross-site delivery is direct-publish: remote sites
// JetStream-publish onto this site's external.> lane, routed by the NATS
// supercluster/gateway topology, which belongs to ops/IaC. App code never sets
// any sourcing/transform config.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	inboxCfg := stream.Inbox(siteID)
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     inboxCfg.Name,
			Subjects: inboxCfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create INBOX stream: %w", err)
		}
		return nil
	}
	// Production path: verify the stream exists. Fail fast if it doesn't —
	// ops/IaC owns provisioning, and a missing stream means the deploy is
	// broken before the first publish or consume.
	if _, err := js.Stream(ctx, inboxCfg.Name); err != nil {
		return fmt.Errorf("verify INBOX stream: %w", err)
	}
	return nil
}
