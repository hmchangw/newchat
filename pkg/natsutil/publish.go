package natsutil

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// JetStreamMsgPublisher is the one JetStream method the publish helpers need.
// o11ynats.JetStream satisfies it; a test stub needs a single method.
type JetStreamMsgPublisher interface {
	PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// CoreMsgPublisher is the one core-NATS method the publish helpers need.
// *o11ynats.Conn satisfies it.
type CoreMsgPublisher interface {
	PublishMsg(ctx context.Context, msg *nats.Msg) error
}

// PublishOption tunes how a publish helper reports failures.
type PublishOption func(*publishConfig)

type publishConfig struct {
	labels func(subj string) (natsmetrics.DestinationKind, natsmetrics.Operation)
}

// WithPublishLabels pins the failure metric's destination and operation instead
// of deriving them from the subject. Use it on paths whose subject the
// classifier does not know (the failover roots), or where every publish is one
// known kind and the per-failure classification is pure overhead.
func WithPublishLabels(destination natsmetrics.DestinationKind, operation natsmetrics.Operation) PublishOption {
	return func(c *publishConfig) {
		c.labels = func(string) (natsmetrics.DestinationKind, natsmetrics.Operation) {
			return destination, operation
		}
	}
}

func newPublishConfig(opts []PublishOption) publishConfig {
	// Classify only on failure: the classifier allocates, and this runs on every
	// publish. Successes are not counted anywhere — see natsmetrics.Publisher.Failure.
	c := publishConfig{labels: natsmetrics.PublishLabelsFromSubject}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// JetStreamPublishFunc returns the publish closure every JetStream producer in
// the fleet used to hand-write: NewMsg stamps the request-id and debug headers
// from ctx, msgID rides as the Nats-Msg-Id the server dedups on, a failure is
// counted against metrics and wrapped with the subject for triage.
//
// The returned func is assignable to every service's PublishFunc, so a service
// binds one per NATS connection and passes it wherever the old closure went. A
// zero metrics value is the documented "not instrumented" form.
func JetStreamPublishFunc(js JetStreamMsgPublisher, metrics natsmetrics.Publisher, opts ...PublishOption) func(ctx context.Context, subj string, data []byte, msgID string) error {
	c := newPublishConfig(opts)
	return func(ctx context.Context, subj string, data []byte, msgID string) error {
		// WithMsgID only sets the header for a non-empty id, so an unconditional
		// option is exactly the old "if msgID != """ branch.
		_, err := js.PublishMsg(ctx, NewMsg(ctx, subj, data), jetstream.WithMsgID(msgID))
		if err != nil {
			destination, operation := c.labels(subj)
			metrics.Failure(ctx, destination, operation, err)
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	}
}

// CorePublishFunc is JetStreamPublishFunc's core-NATS twin, for fire-and-forget
// client delivery that is never persisted and carries no dedup id.
func CorePublishFunc(nc CoreMsgPublisher, metrics natsmetrics.Publisher, opts ...PublishOption) func(ctx context.Context, subj string, data []byte) error {
	c := newPublishConfig(opts)
	return func(ctx context.Context, subj string, data []byte) error {
		if err := nc.PublishMsg(ctx, NewMsg(ctx, subj, data)); err != nil {
			destination, operation := c.labels(subj)
			metrics.Failure(ctx, destination, operation, err)
			return fmt.Errorf("publish core to %q: %w", subj, err)
		}
		return nil
	}
}
