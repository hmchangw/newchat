// Package publisher adapts an *otelnats.Conn to the service.EventPublisher
// interface. Splitting it out of cmd/main.go keeps the wiring code thin and
// matches the per-collaborator structure used elsewhere (cassrepo,
// mongorepo).
package publisher

import (
	"context"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
)

// Publisher publishes byte payloads to NATS subjects via the otelnats wrapper.
type Publisher struct {
	nc *otelnats.Conn
}

// New constructs a Publisher backed by the given otelnats connection.
func New(nc *otelnats.Conn) *Publisher {
	return &Publisher{nc: nc}
}

// Publish sends data to subj. Trace context is propagated automatically by
// the underlying otelnats.Conn.
func (p *Publisher) Publish(ctx context.Context, subj string, data []byte) error {
	return p.nc.Publish(ctx, subj, data)
}
