package natsutil

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

type redeliveryCtxKey struct{}

// metadataMsg is the slice of jetstream.Msg StampRedelivery needs, so consume
// loops can pass their message and tests can pass a stub.
type metadataMsg interface {
	Metadata() (*jetstream.MsgMetadata, error)
}

// StampRedelivery marks ctx when msg is a JetStream redelivery (NumDelivered
// > 1), so handlers deep in the stack can tell a retry from a first delivery
// without re-deriving it or paying a database probe.
//
// An unreadable delivery count is treated as a redelivery. Consumers use the
// flag to suppress non-idempotent work, where wrongly assuming "first
// delivery" double-applies while wrongly assuming "retry" only costs a
// reconcile.
func StampRedelivery(ctx context.Context, msg metadataMsg) context.Context {
	meta, err := msg.Metadata()
	if err == nil && meta != nil && meta.NumDelivered <= 1 {
		return ctx
	}
	return WithRedelivery(ctx)
}

// WithRedelivery marks ctx as a redelivery directly, for callers that already
// know (and for tests exercising retry behaviour without a live consumer).
func WithRedelivery(ctx context.Context) context.Context {
	return context.WithValue(ctx, redeliveryCtxKey{}, true)
}

// IsRedelivery reports whether ctx was stamped by StampRedelivery. An
// unstamped context reports false.
func IsRedelivery(ctx context.Context) bool {
	v, ok := ctx.Value(redeliveryCtxKey{}).(bool)
	return ok && v
}
