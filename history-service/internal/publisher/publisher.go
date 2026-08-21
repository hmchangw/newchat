//go:generate mockgen -source=publisher.go -destination=mock_publisher_test.go -package=publisher

// Package publisher adapts NATS connections to the service.EventPublisher interface.
package publisher

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
)

type failureRecorder interface {
	Failure(context.Context, natsmetrics.DestinationKind, natsmetrics.Operation, error)
}

// Publisher publishes byte payloads to NATS JetStream with dedup support.
type Publisher struct {
	js      o11ynats.JetStream
	metrics failureRecorder
}

type Option func(*Publisher)

// WithMetrics enables bounded publish-failure metrics without changing
// JetStream acknowledgement or deduplication behavior. Successful publishes are
// not recorded; see natsmetrics.Publisher.Failure.
func WithMetrics(metrics natsmetrics.Publisher) Option {
	return withFailureRecorder(metrics)
}

func withFailureRecorder(metrics failureRecorder) Option {
	return func(p *Publisher) { p.metrics = metrics }
}

func New(js o11ynats.JetStream, opts ...Option) *Publisher {
	p := &Publisher{js: js}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish sends data to subj via JetStream with Nats-Msg-Id = msgID.
func (p *Publisher) Publish(ctx context.Context, subj string, data []byte, msgID string) error {
	msg := natsutil.NewMsg(ctx, subj, data)
	_, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
	p.recordFailure(ctx, subj, err)
	if err != nil {
		return fmt.Errorf("publishing to %q: %w", subj, err)
	}
	return nil
}

// PublishMigration behaves like Publish but stamps the X-Migration: live header so
// live-delivery consumers skip re-delivery while persistence/index consumers ingest it.
func (p *Publisher) PublishMigration(ctx context.Context, subj string, data []byte, msgID string) error {
	msg := natsutil.NewMsg(ctx, subj, data)
	natsutil.SetMigrationLive(msg)
	_, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
	p.recordFailure(ctx, subj, err)
	if err != nil {
		return fmt.Errorf("publishing migration to %q: %w", subj, err)
	}
	return nil
}

func (p *Publisher) recordFailure(ctx context.Context, subj string, err error) {
	// A successful JetStream publish is already counted by the broker as
	// jetstream_stream_total_messages, so classify only when there is a
	// failure to attribute — PublishLabelsFromSubject allocates.
	if err == nil || p.metrics == nil {
		return
	}
	destination, operation := natsmetrics.PublishLabelsFromSubject(subj)
	p.metrics.Failure(ctx, destination, operation, err)
}
