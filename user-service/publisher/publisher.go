// Package publisher publishes cross-site federation events directly into the
// destination site's INBOX stream via JetStream.
package publisher

import (
	"context"
	"fmt"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// Publisher implements service.EventPublisher using a JetStream publish so the
// event lands in the destination site's INBOX stream across the supercluster.
// Status events are last-write-wins and idempotent, so no Nats-Msg-Id dedup is
// needed — a redelivered status overwrite converges to the same value.
type Publisher struct{ js oteljetstream.JetStream }

// New returns a Publisher backed by the given JetStream context.
func New(js oteljetstream.JetStream) *Publisher { return &Publisher{js: js} }

// Publish sends data to subject via JetStream PublishMsg (blocking on PubAck) so
// X-Request-ID from ctx propagates onto the outgoing message and the event is
// stored in the destination INBOX stream.
func (p *Publisher) Publish(ctx context.Context, subject string, data []byte) error {
	if _, err := p.js.PublishMsg(ctx, natsutil.NewMsg(ctx, subject, data)); err != nil {
		return fmt.Errorf("publish inbox event: %w", err)
	}
	return nil
}
