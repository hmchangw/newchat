package jsiter

import (
	"context"
	"fmt"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerResolver returns the consumer to (re)attach to. It runs on every
// rebuild, so it must re-resolve rather than hand back a cached handle — a
// stopped iterator usually means the durable itself is gone.
type ConsumerResolver func(context.Context) (o11ynats.Consumer, error)

// JetStream is the consumer-creating slice of o11ynats.JetStream that the
// source constructors need.
type JetStream interface {
	CreateOrUpdateConsumer(context.Context, string, jetstream.ConsumerConfig) (o11ynats.Consumer, error)
}

// Resolve builds a ConsumerResolver that recreates the durable each time.
//
//nolint:gocritic // hugeParam: cfg is copied once at wiring time, not per message.
func Resolve(js JetStream, stream string, cfg jetstream.ConsumerConfig) ConsumerResolver {
	return func(ctx context.Context) (o11ynats.Consumer, error) {
		cons, err := js.CreateOrUpdateConsumer(ctx, stream, cfg)
		if err != nil {
			return nil, fmt.Errorf("create %s consumer %s: %w", stream, cfg.Durable, err)
		}
		return cons, nil
	}
}

// PullFrom builds the OpenIterator for NewPump: it re-resolves the consumer and
// opens a fresh pull iterator on it.
func PullFrom(resolve ConsumerResolver, opts ...jetstream.PullMessagesOpt) OpenIterator {
	return func(ctx context.Context) (Iterator, error) {
		cons, err := resolve(ctx)
		if err != nil {
			return nil, err
		}
		iter, err := cons.Messages(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("open message iterator: %w", err)
		}
		return iter, nil
	}
}

// ConsumeFrom builds the OpenConsume for NewSupervisor: it re-resolves the consumer,
// starts Consume, and wires the supervisor's callback into ConsumeErrHandler —
// without which a Consume the server stops reports nothing at all.
func ConsumeFrom(resolve ConsumerResolver, handler o11ynats.JetStreamMsgHandler, opts ...jetstream.PullConsumeOpt) OpenConsume {
	return func(ctx context.Context, onError func(error)) (ConsumeContext, error) {
		cons, err := resolve(ctx)
		if err != nil {
			return nil, err
		}
		// Fresh slice per call: appending to the captured one would stack another
		// handler on every rebuild.
		withHandler := make([]jetstream.PullConsumeOpt, 0, len(opts)+1)
		withHandler = append(withHandler, opts...)
		withHandler = append(withHandler, jetstream.ConsumeErrHandler(
			func(_ jetstream.ConsumeContext, consumeErr error) { onError(consumeErr) },
		))
		cc, err := cons.Consume(ctx, handler, withHandler...)
		if err != nil {
			return nil, fmt.Errorf("start consume: %w", err)
		}
		return cc, nil
	}
}
