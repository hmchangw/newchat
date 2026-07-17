package publisher

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// CorePublisher implements service.EventPublisher over core NATS for
// ephemeral client-facing fanout events (e.g. settings.update) — best-effort
// delivery to currently connected clients, no stream persistence, so no
// stream needs to own the subject (same delivery pattern as room-worker's
// subscription.update).
type CorePublisher struct{ nc *o11ynats.Conn }

// NewCore returns a CorePublisher backed by the given connection.
func NewCore(nc *o11ynats.Conn) *CorePublisher { return &CorePublisher{nc: nc} }

// Publish sends data to subject via a core-NATS PublishMsg so X-Request-ID
// from ctx propagates onto the outgoing message.
func (p *CorePublisher) Publish(ctx context.Context, subject string, data []byte) error {
	if err := p.nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subject, data)); err != nil {
		return fmt.Errorf("publish client event: %w", err)
	}
	return nil
}
