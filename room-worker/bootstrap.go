package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

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
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams handles the JetStream streams this service consumes: ROOMS
// (member/create/rename) and ROOMS-TEAMS (the Teams-migration room-create batch).
// When enabled (dev/integration) it creates them via CreateOrUpdateStream; when
// disabled (production) it verifies each exists via Stream() — fail-fast so a
// misprovisioned deploy surfaces at startup, not at first publish.
//
// Ownership rule: sets only the stream schema (Name + Subjects) from pkg/stream.
// Federation config belongs to ops/IaC and is layered on in production.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	for _, c := range []stream.Config{stream.Rooms(siteID), stream.RoomsTeams(siteID)} {
		if enabled {
			if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name:     c.Name,
				Subjects: c.Subjects,
			}); err != nil {
				return fmt.Errorf("create %s stream: %w", c.Name, err)
			}
			continue
		}
		if _, err := js.Stream(ctx, c.Name); err != nil {
			return fmt.Errorf("verify %s stream: %w", c.Name, err)
		}
	}
	return nil
}
